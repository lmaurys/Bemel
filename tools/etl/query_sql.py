#!/usr/bin/env python3
"""
Quick ad-hoc SQL query runner using the existing connection config.

Usage:
  python -m tools.etl.query_sql "<SQL query>"

Prints rows to stdout.
"""
from __future__ import annotations

import sys
import pyodbc

from .config import build_connection_string


def main(argv):
    if len(argv) < 2:
        print("Usage: query_sql.py <SQL>")
        return 2
    sql = argv[1]
    conn_str = build_connection_string()
    if not conn_str:
        print("Missing DB settings. Set AZURE_SQL_CONNECTION_STRING or AZURE_SQL_SERVER/AZURE_SQL_DATABASE.")
        return 2
    cn = pyodbc.connect(conn_str)
    try:
        cur = cn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        print("\t".join(cols))
        for row in cur.fetchall():
            print("\t".join(["" if v is None else str(v) for v in row]))
    finally:
        cn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
