#!/usr/bin/env python3
"""
Initialize the Dwh2 schema in Azure SQL by executing the DDL and DimDate SP.

Usage:
  python tools/etl/init_db.py [--dates]
    --dates  Also create the DimDate stored procedure script.
"""
from __future__ import annotations

import argparse
import os
import sys

from .exec_sql import run_sql_file

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--dates", action="store_true", help="Also create/load the DimDate SP script")
    args = parser.parse_args(argv[1:])

    ddl = os.path.join(ROOT, "tools", "sql", "dwh2_ddl.sql")
    if not os.path.isfile(ddl):
        print(f"DDL not found: {ddl}")
        return 1
    print(f"Executing DDL: {ddl}")
    run_sql_file(ddl)
    print("DDL executed.")

    if args.dates:
        sp = os.path.join(ROOT, "tools", "sql", "dwh2_load_dimdate.sql")
        if os.path.isfile(sp):
            print(f"Executing DimDate SP: {sp}")
            run_sql_file(sp)
            print("DimDate SP created.")
        else:
            print(f"DimDate SP script not found: {sp}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
