#!/usr/bin/env python3
"""
Wiki Speedrun - Full Pipeline
Downloads, extracts, and loads Wikipedia dumps into sqlite.
Optimized for high-RAM, multi-core machines.
"""

import os
import sys
import gzip
import time
import re
import csv
import sqlite3
import urllib.request
import urllib.error
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

csv.field_size_limit(10_000_000)

PAGELINKS_URL   = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pagelinks.sql.gz"
PAGE_URL        = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-page.sql.gz"
LINKTARGET_URL  = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-linktarget.sql.gz"
PAGELINKS_GZ    = "enwiki-latest-pagelinks.sql.gz"
PAGE_GZ         = "enwiki-latest-page.sql.gz"
LINKTARGET_GZ   = "enwiki-latest-linktarget.sql.gz"
PAGELINKS_TSV   = "wiki_pagelinks.tsv"
PAGE_TSV        = "wiki_page_ids.tsv"
LINKTARGET_TSV  = "wiki_linktarget.tsv"
DB_FILE         = "wiki.db"


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------

def download(url: str, dest: str, label: str) -> None:
    if Path(dest).exists():
        print(f"[skip] {label} already downloaded")
        return

    print(f"[download] {label}")
    start = time.time()
    last = 0

    def hook(blocks, block_size, total):
        nonlocal last
        done = blocks * block_size
        elapsed = time.time() - start
        speed = done / elapsed / 1024 / 1024 if elapsed else 0
        if done - last < 100 * 1024 * 1024:
            return
        last = done
        if total > 0:
            pct = min(done / total * 100, 100)
            bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
            print(f"\r  [{bar}] {pct:4.1f}%  {done/1e9:.2f}/{total/1e9:.2f} GB  {speed:.1f} MB/s", end="", flush=True)
        else:
            print(f"\r  {done/1e9:.2f} GB  {speed:.1f} MB/s", end="", flush=True)

    try:
        urllib.request.urlretrieve(url, dest, hook)
        print(f"\n[done] {label} in {time.time()-start:.0f}s")
    except urllib.error.URLError as e:
        print(f"\n[error] {label}: {e}")
        sys.exit(1)


def download_both():
    print("=" * 60)
    print("STEP 1: Download dumps")
    print("=" * 60)
    with ThreadPoolExecutor(max_workers=3) as ex:
        f1 = ex.submit(download, PAGELINKS_URL, PAGELINKS_GZ, "pagelinks")
        f2 = ex.submit(download, PAGE_URL, PAGE_GZ, "page table")
        f3 = ex.submit(download, LINKTARGET_URL, LINKTARGET_GZ, "linktarget")
        f1.result()
        f2.result()
        f3.result()


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------

def extract_pagelinks():
    if Path(PAGELINKS_TSV).exists():
        print(f"[skip] {PAGELINKS_TSV} already exists")
        return

    print(f"\n[extract] pagelinks → {PAGELINKS_TSV}")
    row_re = re.compile(r"\((\d+),(\d+),(\d+)\)")
    start = time.time()
    written = 0

    with gzip.open(PAGELINKS_GZ, "rt", encoding="utf-8", errors="replace") as f, \
         open(PAGELINKS_TSV, "w", encoding="utf-8", buffering=8*1024*1024) as out:

        out.write("source_page_id\ttarget_page_id\n")

        for line in f:
            if not line.startswith("INSERT INTO `pagelinks`"):
                continue
            for m in row_re.finditer(line):
                pl_from, pl_from_ns, pl_target = m.groups()
                if pl_from_ns == "0":
                    out.write(f"{pl_from}\t{pl_target}\n")
                    written += 1

            if written % 5_000_000 < 1000 and written > 0:
                elapsed = time.time() - start
                mb = os.path.getsize(PAGELINKS_TSV) / 1024 / 1024
                print(f"  {written:>12,} written | {elapsed/60:.1f} min | {written/elapsed:,.0f} rows/s | {mb:.0f} MB")

    elapsed = time.time() - start
    print(f"[done] {written:,} pagelinks in {elapsed/60:.1f} min")


def extract_pages():
    if Path(PAGE_TSV).exists():
        print(f"[skip] {PAGE_TSV} already exists")
        return

    print(f"\n[extract] page table → {PAGE_TSV}")
    # page table columns: page_id, page_namespace, page_title, ...
    row_re = re.compile(r"\((\d+),(\d+),'((?:[^'\\]|\\.)*)'")
    start = time.time()
    written = 0

    with gzip.open(PAGE_GZ, "rt", encoding="utf-8", errors="replace") as f, \
         open(PAGE_TSV, "w", encoding="utf-8", buffering=4*1024*1024) as out:

        out.write("page_id\tpage_title\n")

        for line in f:
            if not line.startswith("INSERT INTO `page`"):
                continue
            for m in row_re.finditer(line):
                page_id, page_ns, page_title = m.groups()
                if page_ns == "0":
                    out.write(f"{page_id}\t{page_title}\n")
                    written += 1

    elapsed = time.time() - start
    print(f"[done] {written:,} pages in {elapsed:.0f}s")


def extract_linktarget():
    if Path(LINKTARGET_TSV).exists():
        print(f"[skip] {LINKTARGET_TSV} already exists")
        return

    print(f"\n[extract] linktarget → {LINKTARGET_TSV}")
    # schema: (lt_id, lt_namespace, lt_title)
    row_re = re.compile(r"\((\d+),(\d+),'((?:[^'\\]|\\.)*)'\)")
    start = time.time()
    written = 0

    with gzip.open(LINKTARGET_GZ, "rt", encoding="utf-8", errors="replace") as f, \
         open(LINKTARGET_TSV, "w", encoding="utf-8", buffering=4*1024*1024) as out:

        out.write("lt_id\tlt_title\n")

        for line in f:
            if not line.startswith("INSERT INTO `linktarget`"):
                continue
            for m in row_re.finditer(line):
                lt_id, lt_ns, lt_title = m.groups()
                if lt_ns == "0":
                    out.write(f"{lt_id}\t{lt_title}\n")
                    written += 1

    elapsed = time.time() - start
    print(f"[done] {written:,} linktargets in {elapsed:.0f}s")


def extract_both():
    print("\n" + "=" * 60)
    print("STEP 2: Extract dumps")
    print("=" * 60)
    with ThreadPoolExecutor(max_workers=3) as ex:
        f1 = ex.submit(extract_pagelinks)
        f2 = ex.submit(extract_pages)
        f3 = ex.submit(extract_linktarget)
        f1.result()
        f2.result()
        f3.result()


# ---------------------------------------------------------------------------
# load into sqlite
# ---------------------------------------------------------------------------

def load_db():
    print("\n" + "=" * 60)
    print("STEP 3: Load into SQLite")
    print("=" * 60)

    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()

    # performance pragmas — use lots of ram since we have it
    cur.executescript("""
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA cache_size = -4000000;
        PRAGMA temp_store = MEMORY;
        PRAGMA mmap_size = 30000000000;
    """)

    # linktarget
    print(f"\n[load] linktarget → {DB_FILE}")
    cur.execute("drop table if exists linktarget")
    cur.execute("create table linktarget (lt_id integer primary key, lt_title text)")
    start = time.time()
    with open(LINKTARGET_TSV, encoding="utf-8") as f:
        next(f)
        cur.executemany("insert into linktarget values (?,?)", csv.reader(f, delimiter="\t"))
    con.commit()
    print(f"[done] linktarget in {time.time()-start:.0f}s")

    # page_ids
    print(f"\n[load] page_ids → {DB_FILE}")
    cur.execute("drop table if exists page_ids")
    cur.execute("create table page_ids (page_id integer primary key, page_title text)")
    start = time.time()
    with open(PAGE_TSV, encoding="utf-8") as f:
        next(f)
        cur.executemany("insert into page_ids values (?,?)", csv.reader(f, delimiter="\t"))
    con.commit()
    print(f"[done] page_ids in {time.time()-start:.0f}s")

    # pagelinks
    print(f"\n[load] pagelinks → {DB_FILE} (this will take a while)")
    cur.execute("drop table if exists pagelinks")
    cur.execute("create table pagelinks (source_page_id integer, target_page_id integer)")
    start = time.time()
    CHUNK = 500_000
    count = 0
    with open(PAGELINKS_TSV, encoding="utf-8") as f:
        next(f)
        reader = csv.reader(f, delimiter="\t")
        while True:
            chunk = []
            try:
                for _ in range(CHUNK):
                    chunk.append(next(reader))
            except StopIteration:
                pass
            if not chunk:
                break
            cur.executemany("insert into pagelinks values (?,?)", chunk)
            con.commit()
            count += len(chunk)
            elapsed = time.time() - start
            print(f"  {count:>12,} inserted | {elapsed/60:.1f} min | {count/elapsed:,.0f} rows/s")
            if len(chunk) < CHUNK:
                break

    print(f"[done] {count:,} pagelinks inserted in {(time.time()-start)/60:.1f} min")

    # indexes
    print("\n[index] building indexes (10-20 min)...")
    start = time.time()
    cur.execute("create index idx_pl_source on pagelinks(source_page_id)")
    cur.execute("create index idx_pl_target on pagelinks(target_page_id)")
    cur.execute("create index idx_page_title on page_ids(page_title)")
    cur.execute("create index idx_lt_title on linktarget(lt_title)")
    con.commit()
    con.close()
    print(f"[done] indexes in {(time.time()-start)/60:.1f} min")

    db_gb = os.path.getsize(DB_FILE) / 1024 / 1024 / 1024
    print(f"\n[done] {DB_FILE} is {db_gb:.1f} GB")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    total_start = time.time()
    download_both()
    extract_both()
    load_db()
    total = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"ALL DONE in {total/60:.0f} min — {DB_FILE} is ready")
    print(f"{'='*60}")