#!/usr/bin/env python3
"""
Run the ETL for a range of date folders under XMLS_COL.

Usage:
  python -m tools.etl.load_by_range --start 20250707 --end 20250812 [--only AR|CSL] [--limit-per-day N] [--quiet]

Behavior:
  - Iterates inclusively from --start to --end
  - If a day's folder doesn't exist, prints a note and continues
  - Passes through --only/--quiet/--limit-per-day to load_by_date
  - Honors COMMIT_EVERY env var for batching commits
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import subprocess
import sys

from .config import Settings


def _parse_yyyymmdd(s: str) -> _dt.date:
    if len(s) != 8 or not s.isdigit():
        raise ValueError("Date must be in YYYYMMDD format")
    return _dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="Start date YYYYMMDD")
    ap.add_argument("--end", required=True, help="End date YYYYMMDD (inclusive)")
    ap.add_argument("--only", choices=["AR", "CSL"], help="Process only AR or only CSL")
    ap.add_argument("--limit-per-day", type=int, help="Max files per day to process")
    ap.add_argument("--quiet", action="store_true", help="Suppress fallback logs from ETL")
    args = ap.parse_args(argv[1:])

    start = _parse_yyyymmdd(args.start)
    end = _parse_yyyymmdd(args.end)
    if end < start:
        print("End date must be >= start date", file=sys.stderr)
        return 2

    commit_every = os.environ.get("COMMIT_EVERY", "5")
    env = os.environ.copy()
    env["COMMIT_EVERY"] = commit_every

    processed_days = 0
    cur = start
    while cur <= end:
        d = cur.strftime("%Y%m%d")
        print(f"=== Running {d}", flush=True)
        cmd = [sys.executable, "-m", "tools.etl.load_by_date", "--date", d]
        if args.only:
            cmd += ["--only", args.only]
        if args.limit_per_day:
            cmd += ["--limit", str(args.limit_per_day)]
        if args.quiet:
            cmd += ["--quiet"]
        try:
            r = subprocess.run(cmd, env=env)
            print(f"=== Done {d} exit {r.returncode}", flush=True)
        except KeyboardInterrupt:
            print("Interrupted by user", flush=True)
            return 130
        except Exception as e:
            print(f"Error running day {d}: {e}", flush=True)
        processed_days += 1
        cur += _dt.timedelta(days=1)

    print(f"=== All done: {processed_days} day(s)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
