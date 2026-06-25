#!/usr/bin/env python3
"""
llm_filter.py

Uses local Qwen3 (via LM Studio) to classify Turkish news titles that the
regex in filter_accidents.py did not catch.

Pipeline:
  1. Load tum_haberler.csv
  2. Load trafik_kazalari.csv (already accepted by regex)
  3. Exclude obvious non-accident titles (deprem, siyaset, etc.)
  4. Send remaining titles in batches of BATCH_SIZE to Qwen3
  5. Append newly accepted rows to trafik_kazalari.csv

Usage:
  python llm_filter.py            # normal run / resume
  python llm_filter.py --reset    # clear LLM-added rows, keep regex rows
"""

import csv
import json
import os
import re
import sys
import time
import urllib.request

from tqdm import tqdm

INPUT_FILE  = 'main_files/tum_haberler.csv'
OUTPUT_FILE = 'main_files/trafik_kazalari.csv'
LM_STUDIO   = 'http://127.0.0.1:1234/v1/chat/completions'
MODEL       = 'qwen/qwen3-coder-next'

BATCH_SIZE    = 50    # titles per LLM request
REQUEST_DELAY = 0.0   # no delay needed — model is the bottleneck
MAX_TOKENS    = 500   # no reasoning tokens — output only (~300 for 50 titles)

# ── Same filters as filter_accidents.py ──────────────────────────────────────

TITLE_FILTER = re.compile(
    r'trafik kaza|trafik kazas|kaza(da|ya|yı|nda|sında|dan)|'
    r'çarpışma|çarpıştı|takla att|araç devrildi|devrilme|zincirleme kaza|'
    r'otomobil.{0,20}(çarptı|devrildi|takla|çarpıştı|düştü)|'
    r'(minibüs|otobüs|tır|kamyon|motosiklet|skuter|bisiklet).{0,30}(kaza|çarptı|çarpıştı|devrildi|takla)|'
    r'(kaza|çarpış).{0,30}(otomobil|araç|tır|kamyon|minibüs|otobüs)',
    re.IGNORECASE
)

EXCLUDE_FILTER = re.compile(
    r'deprem|enkaz|fay hat|fay zon|tsunami|sel felaketi|'
    r'hasarlı bina|hasar(lı)? (bina|yapı|konut)|konteyner (kent|ev)|'
    r'yıkım (ekip|çalış)|hapis(e)? (çarptı|mahkum)|'
    r'(milletvekili|belediye başkan|aday|seçim|meclis)',
    re.IGNORECASE
)

USER_PROMPT_TEMPLATE = """Aşağıdaki haber başlıklarını sınıflandır.
Her başlık için SADECE numara:E (trafik kazası) veya numara:H (trafik kazası değil) yaz.
Başka hiçbir şey yazma.

Trafik kazası sayılır: karayolunda/şehir içinde araç kazası, çarpışma, motosiklet/bisiklet kazası, yayaya çarpma, araç devrilmesi/takla atması.
Trafik kazası sayılmaz: deprem, iş kazası, uçak/gemi/tren kazası, suç/cinayet, siyaset, spor, ekonomi, sağlık.

{titles}"""


# ── LLM call ─────────────────────────────────────────────────────────────────

def classify_batch(titles: list[str]) -> list[bool]:
    """
    Send a batch of titles to Qwen3. Returns a list of booleans (True=accident).
    Falls back to False for any title that can't be parsed.
    """
    numbered = '\n'.join(f'{i+1}:{t}' for i, t in enumerate(titles))
    user_content = USER_PROMPT_TEMPLATE.format(titles=numbered)
    payload = json.dumps({
        'model': MODEL,
        'messages': [
            {'role': 'user', 'content': user_content},
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
        return [False] * len(titles)

    # Parse "N:E" / "N:H" lines
    results = [False] * len(titles)
    for line in text.splitlines():
        line = line.strip()
        m = re.match(r'^(\d+)\s*[:\.]\s*([EH])', line, re.IGNORECASE)
        if m:
            idx = int(m.group(1)) - 1
            if 0 <= idx < len(titles):
                results[idx] = m.group(2).upper() == 'E'
    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    # Load input
    print(f'Reading {INPUT_FILE} ...')
    with open(INPUT_FILE, encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')
        fieldnames = reader.fieldnames
        all_rows = list(reader)
    print(f'  {len(all_rows):,} rows loaded.')

    # Load already-accepted IDs (regex + any previous LLM run)
    already_done = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, encoding='utf-8-sig', newline='') as f:
            for row in csv.DictReader(f, delimiter=';'):
                already_done.add(row['Id'])
        print(f'  {len(already_done):,} rows already in output, skipping.')

    # Build candidate list: not already done
    candidates = [
        r for r in all_rows
        if r['Id'] not in already_done
    ]
    print(f'  {len(candidates):,} candidates for LLM classification.')
    if not candidates:
        print('Nothing to do.')
        return

    accepted = 0
    processed = 0

    write_header = not os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, 'a', encoding='utf-8', newline='') as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames, delimiter=';')
        if write_header:
            writer.writeheader()

        with tqdm(total=len(candidates), unit='row', smoothing=0.05) as pbar:
            for i in range(0, len(candidates), BATCH_SIZE):
                batch = candidates[i:i + BATCH_SIZE]
                titles = [r['baslik'] for r in batch]

                decisions = classify_batch(titles)

                for row, is_accident in zip(batch, decisions):
                    if is_accident:
                        writer.writerow(row)
                        accepted += 1

                processed += len(batch)
                out_f.flush()
                pbar.update(len(batch))
                pbar.set_postfix(
                    accepted=accepted,
                    rate=f'{accepted/processed*100:.1f}%',
                    refresh=False,
                )
                time.sleep(REQUEST_DELAY)

    print(f'\nDone. {accepted:,} additional rows saved to {OUTPUT_FILE}.')
    print(f'Total in output: {len(already_done) + accepted:,}')


if __name__ == '__main__':
    main()
