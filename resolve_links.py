#!/usr/bin/env python3
"""
resolve_links.py

Resolves Google News RSS URLs in trafik_kazalari_csv/ to real article URLs
using a headless Chromium browser (Playwright). No rate limiting issues.

Usage:
  python resolve_links.py                        # normal run / resume
  python resolve_links.py --reset                # delete output and start fresh
  python resolve_links.py --retry-failed         # retry unresolved Google URLs
  python resolve_links.py --range 1 50           # only process input files 1-50 (for distributing across machines)

Requirements:
  pip install playwright tqdm
  playwright install chromium
"""

import asyncio
import csv
import glob
import logging
import os
import sys

from tqdm import tqdm
from playwright.async_api import async_playwright

INPUT_DIR    = 'trafik_kazalari_csv'
OUTPUT_DIR   = 'trafik_kazalari_resolved'
LOG_FILE     = 'resolve_links.log'

ROWS_PER_FILE     = 1000
MAX_CONCURRENT    = 5     # parallel browser pages
PAGE_TIMEOUT      = 15000 # ms per page
BROWSER_RESTART_EVERY = 5   # restart browser every N input files to avoid memory leak

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.WARNING,
    format='%(asctime)s %(levelname)s %(message)s',
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def output_filename(n: int) -> str:
    return os.path.join(OUTPUT_DIR, f'trafik_kazalari_resolved_{n:04d}.csv')


def all_output_files() -> list:
    return sorted(glob.glob(os.path.join(OUTPUT_DIR, 'trafik_kazalari_resolved_*.csv')))


def is_google_news_url(url: str) -> bool:
    return 'news.google.com' in url


def load_already_resolved() -> tuple:
    """
    Scan all existing output files.
    Returns (already_done_ids, total_count).
    """
    resolved = set()
    for path in all_output_files():
        with open(path, encoding='utf-8-sig', newline='') as f:
            for row in csv.DictReader(f, delimiter=';'):
                resolved.add(row['Id'])
    return resolved, len(resolved)


def parse_range_args() -> tuple[int, int] | None:
    """
    Parse --range START END from argv.
    Returns (start_file, end_file) both inclusive (1-based), or None if not specified.
    Example: --range 1 50 processes trafik_kazalari_0001.csv to _0050.csv
    """
    if '--range' in sys.argv:
        idx = sys.argv.index('--range')
        try:
            start = int(sys.argv[idx + 1])
            end = int(sys.argv[idx + 2])
            return start, end
        except (IndexError, ValueError):
            print('Usage: --range START END  (e.g. --range 1 50)')
            sys.exit(1)
    return None


# ── Per-file writer ───────────────────────────────────────────────────────────

class FileWriter:
    """Writes rows to a single output file, appending if it already has rows."""
    def __init__(self, file_num: int, fieldnames, append: bool):
        self.file_num = file_num
        path = output_filename(file_num)
        self._f = open(path, 'a' if append else 'w', encoding='utf-8', newline='')
        self._writer = csv.DictWriter(self._f, fieldnames=fieldnames, delimiter=';')
        if not append:
            self._writer.writeheader()

    def write_row(self, row: dict):
        self._writer.writerow(row)
        self._f.flush()

    def close(self):
        if self._f:
            self._f.close()
            self._f = None


# ── Playwright resolver ───────────────────────────────────────────────────────

async def resolve_one(context, semaphore, row: dict) -> dict:
    """Resolve a single row's Google News URL. Returns the row with updated link."""
    url = row['link']
    if not is_google_news_url(url):
        return row  # already resolved

    async with semaphore:
        try:
            page = await context.new_page()
        except Exception as e:
            logging.warning('Failed to open page id=%s: %s', row['Id'], e)
            return row
        try:
            # Navigate and wait for load, ignoring errors (some pages block bots)
            try:
                await page.goto(url, wait_until='load', timeout=PAGE_TIMEOUT)
            except Exception:
                pass  # page may error but redirect already happened

            # Wait for JS redirect to fire — URL changes away from Google
            try:
                await page.wait_for_url(
                    lambda u: 'news.google.com' not in u,
                    timeout=PAGE_TIMEOUT,
                )
            except Exception:
                pass  # timeout = no redirect happened

            resolved = page.url
            if not is_google_news_url(resolved):
                row = dict(row)
                row['link'] = resolved
            else:
                logging.warning('Still Google URL after nav: id=%s', row['Id'])
        except Exception as e:
            logging.warning('Failed id=%s url=%s: %s', row['Id'], url, e)
        finally:
            await page.close()

    return row


async def resolve_all_for_file(file_num: int, rows: list, fieldnames, already_done: set,
                               context, semaphore, pbar) -> tuple[int, int]:
    """Resolve all rows for one input file, writing to the matching output file."""
    pending = [r for r in rows if r['Id'] not in already_done]
    if not pending:
        return 0, 0

    out_path = output_filename(file_num)
    append = os.path.exists(out_path)
    writer = FileWriter(file_num, fieldnames, append=append)

    resolved_count = 0
    failed_count = 0

    tasks = [resolve_one(context, semaphore, row) for row in pending]
    for coro in asyncio.as_completed(tasks):
        row = await coro
        writer.write_row(row)
        if is_google_news_url(row['link']):
            failed_count += 1
        else:
            resolved_count += 1
        pbar.update(1)
        pbar.set_postfix(resolved=resolved_count, failed=failed_count,
                         file=file_num, refresh=False)

    writer.close()
    return resolved_count, failed_count


# ── Modes ─────────────────────────────────────────────────────────────────────

def load_input_files(file_range=None) -> list[tuple[int, str]]:
    """Returns list of (file_num, path) pairs, sorted, filtered by range."""
    all_input_files = sorted(glob.glob(os.path.join(INPUT_DIR, '*.csv')))
    if not all_input_files:
        print(f'No input files found in {INPUT_DIR}/')
        sys.exit(1)

    if file_range:
        start, end = file_range
        all_input_files = all_input_files[start - 1:end]
        print(f'Processing input files {start}–{end} ({len(all_input_files)} files) ...')
    else:
        print(f'Found {len(all_input_files)} files in {INPUT_DIR}/ ...')

    # Extract file number from filename (e.g. trafik_kazalari_0006.csv → 6)
    result = []
    for path in all_input_files:
        basename = os.path.basename(path)
        num_str = os.path.splitext(basename)[0].split('_')[-1]
        try:
            file_num = int(num_str)
        except ValueError:
            file_num = len(result) + 1
        result.append((file_num, path))
    return result


async def run_normal(file_range=None):
    input_files = load_input_files(file_range)

    already_done, already_count = load_already_resolved()
    if already_done:
        print(f'  Resuming — {already_count:,} rows already resolved, skipping.')

    # Count total pending rows across all files
    all_pending = []
    file_rows = []
    for file_num, path in input_files:
        with open(path, encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f, delimiter=';')
            fieldnames = reader.fieldnames
            rows = list(reader)
        pending = [r for r in rows if r['Id'] not in already_done]
        file_rows.append((file_num, rows, fieldnames))
        all_pending.extend(pending)

    print(f'  {len(all_pending):,} rows to resolve.')
    if not all_pending:
        print('Nothing to do.')
        return

    resolved_total = 0
    failed_total = 0

    async def make_context(p):
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox'],
        )
        context = await browser.new_context(
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/125.0.0.0 Safari/537.36'
            ),
            viewport={'width': 1280, 'height': 800},
            locale='tr-TR',
        )
        return browser, context

    async with async_playwright() as p:
        browser, context = await make_context(p)
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        with tqdm(total=len(all_pending), unit='row', smoothing=0.1) as pbar:
            for i, (file_num, rows, fieldnames) in enumerate(file_rows):
                # Restart browser periodically to free memory
                if i > 0 and i % BROWSER_RESTART_EVERY == 0:
                    await browser.close()
                    browser, context = await make_context(p)
                    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

                resolved, failed = await resolve_all_for_file(
                    file_num, rows, fieldnames, already_done, context, semaphore, pbar
                )
                resolved_total += resolved
                failed_total += failed
                # Mark this file's IDs as done so resume works within a run
                for r in rows:
                    already_done.add(r['Id'])

        await browser.close()

    print(f'\nDone. {resolved_total:,} resolved, {failed_total:,} still unresolved (Google URL).')
    if failed_total:
        print('Run --retry-failed to retry unresolved rows.')


async def run_retry_failed():
    files = all_output_files()
    if not files:
        print('No output files found. Run without --retry-failed first.')
        return

    print('Loading output files ...')
    fieldnames = None
    all_rows = []
    for path in files:
        with open(path, encoding='utf-8-sig', newline='') as f:
            reader = csv.DictReader(f, delimiter=';')
            if fieldnames is None:
                fieldnames = reader.fieldnames
            all_rows.extend(reader)

    failed_rows = [r for r in all_rows if is_google_news_url(r['link'])]
    print(f'  {len(all_rows):,} total rows, {len(failed_rows):,} still have Google URLs.')

    if not failed_rows:
        print('All links are already resolved.')
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    pbar = tqdm(total=len(failed_rows), unit='row', smoothing=0.1)
    resolved_count = 0
    still_failed = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox'],
        )
        context = await browser.new_context(
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/125.0.0.0 Safari/537.36'
            ),
            viewport={'width': 1280, 'height': 800},
            locale='tr-TR',
        )

        id_to_row = {r['Id']: r for r in all_rows}
        tasks = [resolve_one(context, semaphore, r) for r in failed_rows]

        for coro in asyncio.as_completed(tasks):
            updated_row = await coro
            id_to_row[updated_row['Id']]['link'] = updated_row['link']
            if is_google_news_url(updated_row['link']):
                still_failed += 1
            else:
                resolved_count += 1
            pbar.update(1)
            pbar.set_postfix(resolved=resolved_count, failed=still_failed, refresh=False)

        await browser.close()

    pbar.close()

    print('Rewriting output files ...')
    for file_num, path in enumerate(files, 1):
        start = (file_num - 1) * ROWS_PER_FILE
        chunk = all_rows[start:start + ROWS_PER_FILE]
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(chunk)

    print(f'\nDone. {resolved_count:,} newly resolved, {still_failed:,} still failed.')


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if '--reset' in sys.argv:
        for path in all_output_files():
            os.remove(path)
            print(f'Deleted {path}.')

    file_range = parse_range_args()

    if '--retry-failed' in sys.argv:
        asyncio.run(run_retry_failed())
    else:
        asyncio.run(run_normal(file_range))


if __name__ == '__main__':
    main()