#!/usr/bin/env python3
"""
Execute a SQL file against Azure SQL using pyodbc and env-based connection.

Usage:
  python tools/etl/exec_sql.py path/to/script.sql
"""
from __future__ import annotations

import os
import sys
from typing import List

import pyodbc

from .config import build_connection_string


def run_sql_file(path: str) -> None:
    conn_str = build_connection_string()
    if not conn_str:
        raise SystemExit("Missing DB settings. Set AZURE_SQL_CONNECTION_STRING or AZURE_SQL_SERVER/AZURE_SQL_DATABASE (and auth).")
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    # Split on GO batches safely (start of line, optional spaces)
    batches = []
    current: List[str] = []
    for line in sql.splitlines():
        if line.strip().upper() == 'GO':
            batches.append('\n'.join(current))
            current = []
        else:
            current.append(line)
    if current:
        batches.append('\n'.join(current))

    cnxn = pyodbc.connect(conn_str)
    try:
        cur = cnxn.cursor()
        for b in batches:
            if not b.strip():
                continue
            cur.execute(b)
        cnxn.commit()
    finally:
        cnxn.close()


def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Usage: exec_sql.py <sql-file>")
        return 2
    path = argv[1]
    if not os.path.isfile(path):
        print(f"File not found: {path}")
        return 1
    run_sql_file(path)
    print(f"Executed: {path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
