#!/usr/bin/env python3
"""
summarize.py

Fetches article content from resolved URLs in trafik_kazalari.csv,
sends content to local LLM, and outputs structured JSON summaries.

Pipeline:
  1. Load trafik_kazalari.csv
  2. Deduplicate by title similarity (group same accident from different sources)
  3. For each unique accident: fetch article HTML, extract text
  4. Send to LLM → structured JSON
  5. Write results to kazalar_ozet_0001.json, _0002.json, … (1000 items each)
  6. Failed fetches (429, timeout, etc.) logged to kazalar_hata.json for retry

Usage:
  python summarize.py                  # normal run / resume
  python summarize.py --reset          # delete output and start fresh
  python summarize.py --retry-failed   # retry URLs in kazalar_hata.json
  python summarize.py --range 1 74     # only process input files 1-74
"""

import csv
import glob
import json
import os
import re
import ssl
import sys
import time
import unicodedata
import urllib.request
import urllib.error
from html.parser import HTMLParser

# macOS ships without root certificates for Python — disable SSL verification
# for article fetching (we're only reading public news pages, not sending secrets)
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

from tqdm import tqdm

INPUT_PREFIX   = 'trafik_kazalari_resolved'
OUTPUT_DIR     = 'kazalar_ozet'
OUTPUT_PREFIX  = os.path.join(OUTPUT_DIR, 'kazalar_ozet')
ERROR_FILE     = 'kazalar_hata.json'
LM_STUDIO      = 'http://127.0.0.1:1234/v1/chat/completions'
MODEL          = 'qwen/qwen3-4b-2507'

MAX_TOKENS      = 1200
FETCH_TIMEOUT   = 15    # seconds for HTTP article fetch
REQUEST_DELAY   = 0.0   # delay between LLM requests
MAX_CONTENT_LEN = 4000  # max chars of article text sent to LLM

# Titles are considered duplicates if normalized bigram similarity > this
DEDUP_THRESHOLD = 0.95


# ── Output file helpers ───────────────────────────────────────────────────────

def output_filename(n: int) -> str:
    return f'{OUTPUT_PREFIX}_{n:04d}.json'


def all_output_files() -> list:
    return sorted(glob.glob(f'{OUTPUT_PREFIX}_*.json'))



# ── HTML text extractor ───────────────────────────────────────────────────────

class TextExtractor(HTMLParser):
    SKIP_TAGS = {'script', 'style', 'noscript', 'head', 'nav', 'footer',
                 'header', 'aside', 'form', 'button', 'svg', 'img'}

    def __init__(self):
        super().__init__()
        self._skip_depth = 0
        self._chunks = []

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in self.SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._skip_depth == 0:
            text = data.strip()
            if text:
                self._chunks.append(text)

    def get_text(self):
        return ' '.join(self._chunks)


def fetch_article_text(url: str) -> tuple[str, str | None, str]:
    """
    Fetch URL and return (text, error_reason, resolved_url).
    text is empty string on failure.
    error_reason is None on success, or a string like '429' / 'timeout' / 'other'.
    resolved_url is the final URL after redirects.
    """
    if not url:
        return '', 'empty_url', url
    try:
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': (
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/125.0.0.0 Safari/537.36'
                ),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
            },
        )
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT, context=_SSL_CTX) as resp:
            resolved_url = resp.url  # final URL after redirects
            raw = resp.read()
            charset = resp.headers.get_content_charset() or 'utf-8'
            html = raw.decode(charset, errors='replace')
    except urllib.error.HTTPError as e:
        return '', str(e.code), url
    except urllib.error.URLError as e:
        reason = 'timeout' if 'timed out' in str(e).lower() else 'url_error'
        return '', reason, url
    except Exception:
        return '', 'other', url

    # Extract publish date from meta tags / time elements before stripping HTML
    pub_date = None
    date_patterns = [
        r'<meta[^>]+(?:published_time|datePublished|pubdate)[^>]*content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*(?:published_time|datePublished|pubdate)',
        r'<time[^>]+datetime=["\']([^"\']+)["\']',
        r'"datePublished"\s*:\s*"([^"]+)"',
    ]
    for pat in date_patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            pub_date = m.group(1).strip()
            break

    parser = TextExtractor()
    try:
        parser.feed(html)
    except Exception:
        pass
    text = parser.get_text()
    text = re.sub(r'\s+', ' ', text).strip()

    # Prepend the publish date so LLM can use it for tarih_saat
    if pub_date:
        text = f'[Yayın tarihi: {pub_date}] ' + text

    return text[:MAX_CONTENT_LEN], None, resolved_url


# ── Deduplication ─────────────────────────────────────────────────────────────

def normalize_title(title: str) -> str:
    title = unicodedata.normalize('NFKC', title).lower()
    title = re.sub(r'[^\w\s]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    return title


def similarity(a: str, b: str) -> float:
    def bigrams(s):
        return set(s[i:i+2] for i in range(len(s) - 1))
    ba, bb = bigrams(a), bigrams(b)
    if not ba and not bb:
        return 1.0
    if not ba or not bb:
        return 0.0
    return len(ba & bb) / len(ba | bb)


def deduplicate(rows: list) -> list:
    """
    Group rows with very similar titles. Return one representative row per group
    (preferring non-Google URLs).

    Strategy: bucket rows by their first 4 words (prefix key). Only compare
    rows within the same bucket using bigram similarity. This is O(n) for
    well-distributed titles instead of O(n²).
    """
    from collections import defaultdict

    def prefix_key(norm: str) -> str:
        return ' '.join(norm.split()[:4])

    norm_titles = [normalize_title(r['baslik']) for r in rows]

    # Build buckets: prefix → list of (original_index, norm_title)
    buckets = defaultdict(list)
    for i, norm in enumerate(norm_titles):
        buckets[prefix_key(norm)].append(i)

    assigned = [False] * len(rows)
    # Store (original_index, best_row) to restore order after dedup
    keepers = []

    with tqdm(total=len(rows), unit='row', desc='Deduplicating', smoothing=0.05) as pbar:
        for bucket_indices in buckets.values():
            for pos, i in enumerate(bucket_indices):
                if assigned[i]:
                    continue
                group = [i]
                for j in bucket_indices[pos + 1:]:
                    if not assigned[j]:
                        if similarity(norm_titles[i], norm_titles[j]) >= DEDUP_THRESHOLD:
                            group.append(j)
                            assigned[j] = True
                assigned[i] = True

                group_rows = [rows[k] for k in group]
                best = next(
                    (r for r in group_rows if 'news.google.com' not in r['link']),
                    group_rows[0]
                )
                # Use the minimum index in the group to preserve original ordering
                keepers.append((min(group), best))
                pbar.update(len(group))
                pbar.set_postfix(unique=len(keepers), refresh=False)

    # Sort by original position to restore date/city order from input CSVs
    keepers.sort(key=lambda x: x[0])
    return [row for _, row in keepers]


# ── Error log helpers ─────────────────────────────────────────────────────────

def load_error_log() -> dict:
    if not os.path.exists(ERROR_FILE):
        return {}
    with open(ERROR_FILE, encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}


def save_error_log(errors: dict):
    with open(ERROR_FILE, 'w', encoding='utf-8') as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)


# ── LLM call ──────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Sen bir trafik kazası analiz asistanısın.
Verilen haber metninden trafik kazası bilgilerini çıkar ve SADECE JSON formatında döndür.
Başka hiçbir şey yazma, sadece JSON.
Eğer tarih saat ile açıkça verilmiş bir bilgi yoksa bunu haber metninin içinde arayabilirsin."""

USER_PROMPT_TEMPLATE = """Aşağıdaki haber metninden trafik kazası bilgilerini çıkar.
SADECE aşağıdaki JSON formatında yanıt ver, başka hiçbir şey yazma:

{{
  "url": "{url}",
  "ozet": "Kazayı geçmiş zamanda, nerede/ne zaman/nasıl/kaç kişi etkilendi bilgilerini içerecek şekilde 1-2 cümleyle özetle (Türkçe)",
  "tarih_saat": "GG.AA.YYYY SS:DD formatında veya 'Belirtilmemiş'",
  "lokasyon": "İl, ilçe ve yol/mevki bilgisi",
  "kaza_turu": ["Çarpışma/Devrilme/Takla/Şarampole Yuvarlanma/vb"],
  "olu_sayisi": 0,
  "yarali_sayisi": 0,
  "arac_turu": ["Otomobil/TIR/Kamyon/Minibüs/Otobüs/Motosiklet/vb"],
  "kaza_sebebi": ["Hız/Makas/Sis/Buzlanma/Alkol/Uyku/vb veya 'Belirtilmemiş'"],
  "hava_durumu": ["Açık/Yağmurlu/Karlı/Sisli/Buzlu/vb veya 'Belirtilmemiş'"]
}}

Eğer metin bir trafik kazası haberi değilse, sadece şunu döndür:
{{"url": "{url}", "kaza_degil": true}}

Haber metni:
{content}"""


def summarize_article(url: str, content: str) -> dict | None:
    """Send article content to LLM. Returns parsed dict or None on failure."""
    if not content:
        return None

    user_content = USER_PROMPT_TEMPLATE.format(url=url, content=content)
    payload = json.dumps({
        'model': MODEL,
        'messages': [
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user',   'content': user_content},
        ],
        'temperature': 0,
        'max_tokens': MAX_TOKENS,
        'stream': False,
    }).encode()

    req = urllib.request.Request(
        LM_STUDIO,
        data=payload,
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
        text = data['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f'\n  [LLM error] {e}')
        return None

    json_match = re.search(r'\{[\s\S]*\}', text)
    if not json_match:
        return None
    try:
        return json.loads(json_match.group())
    except json.JSONDecodeError:
        return None


# ── Per-file JSON writer ───────────────────────────────────────────────────────

class FileJsonWriter:
    """Writes results to a single JSON output file, appending to existing items."""

    def __init__(self, file_num: int):
        self.path = output_filename(file_num)
        if os.path.exists(self.path):
            with open(self.path, encoding='utf-8') as f:
                try:
                    self._items = json.load(f)
                except json.JSONDecodeError:
                    self._items = []
        else:
            self._items = []

    @property
    def done_urls(self) -> set:
        return {item['url'] for item in self._items}

    def write(self, item: dict):
        self._items.append(item)
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(self._items, f, ensure_ascii=False, indent=2)


# ── Helpers ───────────────────────────────────────────────────────────────────

def file_num_from_path(path: str) -> int:
    """Extract the 4-digit number from trafik_kazalari_resolved_0001.csv → 1"""
    basename = os.path.basename(path)
    num_str = os.path.splitext(basename)[0].split('_')[-1]
    try:
        return int(num_str)
    except ValueError:
        return 0


def parse_range_args() -> tuple[int, int] | None:
    """Parse --range START END from argv. Returns (start, end) inclusive 1-based, or None."""
    if '--range' in sys.argv:
        idx = sys.argv.index('--range')
        try:
            start = int(sys.argv[idx + 1])
            end   = int(sys.argv[idx + 2])
            return start, end
        except (IndexError, ValueError):
            print('Usage: --range START END  (e.g. --range 1 74)')
            sys.exit(1)
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def run_normal(input_files):
    errors = load_error_log()
    permanent_failures = {u for u, r in errors.items() if r not in ('429', 'timeout', '403')}

    total_accepted = 0
    total_not_accident = 0
    total_retryable = 0
    total_fetch_failed = 0

    for path in input_files:
        file_num = file_num_from_path(path)

        with open(path, encoding='utf-8-sig', newline='') as f:
            rows = list(csv.DictReader(f, delimiter=';'))

        writer = FileJsonWriter(file_num)
        done_urls = writer.done_urls
        skip_urls = done_urls | permanent_failures

        # Deduplicate within this file only
        unique_rows = deduplicate(rows)
        pending = [r for r in unique_rows if r['link'] not in skip_urls]

        if not pending:
            print(f'File {file_num:04d}: already done ({len(unique_rows):,} unique rows), skipping.')
            continue

        print(f'File {file_num:04d}: {len(rows):,} rows → {len(unique_rows):,} unique → {len(pending):,} to process')

        fetch_failed = 0
        retryable = 0
        accepted = 0
        not_accident = 0

        with tqdm(total=len(pending), unit='article', smoothing=0.05) as pbar:
            for row in pending:
                url = row['link']

                content, error, resolved_url = fetch_article_text(url)
                if error:
                    errors[url] = error
                    if error in ('429', 'timeout', '403'):
                        retryable += 1
                    else:
                        fetch_failed += 1
                    save_error_log(errors)
                    pbar.update(1)
                    pbar.set_postfix(ok=accepted, retry=retryable, fail=fetch_failed, refresh=False)
                    continue

                result = summarize_article(resolved_url, content)

                if result and not result.get('kaza_degil'):
                    result['id'] = row['Id']
                    writer.write(result)
                    accepted += 1
                else:
                    not_accident += 1

                pbar.update(1)
                pbar.set_postfix(ok=accepted, retry=retryable, fail=fetch_failed, refresh=False)
                time.sleep(REQUEST_DELAY)

        print(f'  → {accepted:,} accepted, {not_accident:,} not accident, '
              f'{retryable:,} retryable, {fetch_failed:,} failed')

        total_accepted += accepted
        total_not_accident += not_accident
        total_retryable += retryable
        total_fetch_failed += fetch_failed

    print(f'\nAll done. {total_accepted:,} summarized, {total_not_accident:,} not accident, '
          f'{total_retryable:,} retryable, {total_fetch_failed:,} permanent failures.')
    if total_retryable:
        print(f'Run with --retry-failed to retry {total_retryable:,} URLs.')


def run_retry_failed(input_files):
    errors = load_error_log()
    retryable_urls = {u for u, r in errors.items() if r in ('429', 'timeout', '403')}
    if not retryable_urls:
        print('No retryable URLs in kazalar_hata.json.')
        return

    print(f'  {len(retryable_urls):,} retryable URLs.')

    # Build url → (row, file_num) map
    url_to_info = {}
    for path in input_files:
        file_num = file_num_from_path(path)
        with open(path, encoding='utf-8-sig', newline='') as f:
            for row in csv.DictReader(f, delimiter=';'):
                url_to_info[row['link']] = (row, file_num)

    # Load already-done URLs across all output files
    done_urls = set()
    for path in all_output_files():
        with open(path, encoding='utf-8') as f:
            try:
                for item in json.load(f):
                    done_urls.add(item['url'])
            except json.JSONDecodeError:
                pass

    pending_urls = [u for u in retryable_urls if u not in done_urls and u in url_to_info]
    print(f'  {len(pending_urls):,} to retry.')
    if not pending_urls:
        print('Nothing to retry.')
        return

    # Cache open writers per file_num
    writers = {}
    resolved = 0
    still_failed = 0

    with tqdm(total=len(pending_urls), unit='article', smoothing=0.05) as pbar:
        for url in pending_urls:
            row, file_num = url_to_info[url]

            content, error, resolved_url = fetch_article_text(url)
            if error:
                errors[url] = error
                still_failed += 1
                save_error_log(errors)
                pbar.update(1)
                pbar.set_postfix(resolved=resolved, failed=still_failed, refresh=False)
                continue

            result = summarize_article(resolved_url, content)
            errors.pop(url, None)
            save_error_log(errors)

            if result and not result.get('kaza_degil'):
                if file_num not in writers:
                    writers[file_num] = FileJsonWriter(file_num)
                result['id'] = row['Id']
                writers[file_num].write(result)
                resolved += 1

            pbar.update(1)
            pbar.set_postfix(resolved=resolved, failed=still_failed, refresh=False)
            time.sleep(REQUEST_DELAY)

    print(f'\nDone. {resolved:,} newly summarized, {still_failed:,} still failing.')


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if '--reset' in sys.argv:
        for path in all_output_files():
            os.remove(path)
            print(f'Deleted {path}.')
        if os.path.exists(ERROR_FILE):
            os.remove(ERROR_FILE)
            print(f'Deleted {ERROR_FILE}.')

    input_files = sorted(glob.glob(os.path.join(INPUT_PREFIX, '*.csv')))
    if not input_files:
        print(f'No input files found in {INPUT_PREFIX}/')
        print('Run resolve_links.py first to resolve URLs.')
        sys.exit(1)

    file_range = parse_range_args()
    if file_range:
        start, end = file_range
        input_files = [f for f in input_files if start <= file_num_from_path(f) <= end]
        print(f'Processing files {start}–{end} ({len(input_files)} files).')
    else:
        print(f'Found {len(input_files)} resolved input files.')

    if '--retry-failed' in sys.argv:
        run_retry_failed(input_files)
    else:
        run_normal(input_files)


if __name__ == '__main__':
    main()
