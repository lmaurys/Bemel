#!/usr/bin/env python3
"""
SFTP fetch utility to pull XML files from a remote SFTP folder into a local staging directory.

Usage:
    # Download files
    python -m tools.etl.sftp_fetch --remote-dir /path/on/sftp --local-dir XMLS_COL/20250707 --patterns "AR_*.xml" "CSL*.xml" [--move-remote-to /archive]

    # Just test the connection (no download)
    python -m tools.etl.sftp_fetch --test-connection

Environment variables:
  SFTP_HOST          required, hostname or IP
  SFTP_PORT          optional, default 22
  SFTP_USER          required, username
  SFTP_PASSWORD      optional, if using password auth
  SFTP_KEY_FILE      optional, path to private key file (OpenSSH PEM)
  SFTP_KEY_PASSPHRASE optional, passphrase for key file

Notes:
- If both password and key file are provided, key-based auth is attempted first.
- Files are downloaded only if not present locally or remote size differs.
- Optional post-download move of remote files to an archive folder keeps the drop clean.
"""
from __future__ import annotations

import argparse
import fnmatch
import os
import posixpath
import stat
from typing import Iterable, List

from .config import _load_env_files as _load_env


def _connect():
    # Local import to avoid hard dependency at module import time
    import paramiko  # noqa: WPS433 (local import by design)
    # Ensure .env files are loaded so SFTP_* variables are available
    _load_env()
    host = os.getenv("SFTP_HOST")
    if not host:
        raise RuntimeError("SFTP_HOST is required")
    port = int(os.getenv("SFTP_PORT", "22"))
    user = os.getenv("SFTP_USER")
    if not user:
        raise RuntimeError("SFTP_USER is required")
    password = os.getenv("SFTP_PASSWORD")
    key_file = os.getenv("SFTP_KEY_FILE")
    key_pass = os.getenv("SFTP_KEY_PASSPHRASE") or None

    transport = paramiko.Transport((host, port))
    try:
        if key_file and os.path.isfile(key_file):
            pkey = None
            try:
                pkey = paramiko.RSAKey.from_private_key_file(key_file, password=key_pass)
            except paramiko.PasswordRequiredException:
                raise
            except Exception:
                # Try Ed25519 and ECDSA variants
                try:
                    pkey = paramiko.Ed25519Key.from_private_key_file(key_file, password=key_pass)
                except Exception:
                    pkey = paramiko.ECDSAKey.from_private_key_file(key_file, password=key_pass)
            transport.connect(username=user, pkey=pkey)
        elif password:
            transport.connect(username=user, password=password)
        else:
            raise RuntimeError("Provide SFTP_PASSWORD or SFTP_KEY_FILE")
        return paramiko.SFTPClient.from_transport(transport)
    except Exception:
        transport.close()
        raise


def _list_files(sftp, remote_dir: str) -> List:
    files: List = []
    for entry in sftp.listdir_attr(remote_dir):
        if stat.S_ISREG(entry.st_mode):
            # Record full path in .filename for convenience
            entry.filename = posixpath.join(remote_dir, entry.filename)
            files.append(entry)
    return files


def _match(files: Iterable, patterns: List[str]):
    for f in files:
        base = posixpath.basename(f.filename)
        if any(fnmatch.fnmatch(base, pat) for pat in patterns):
            yield f


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def fetch(remote_dir: str, local_dir: str, patterns: List[str], move_remote_to: str | None = None) -> int:
    sftp = _connect()
    try:
        _ensure_dir(local_dir)
        files = list(_match(_list_files(sftp, remote_dir), patterns))
        downloaded = 0
        for attr in files:
            rpath = attr.filename
            lpath = os.path.join(local_dir, posixpath.basename(rpath))
            needs = True
            if os.path.exists(lpath):
                try:
                    if os.path.getsize(lpath) == attr.st_size:
                        needs = False
                except Exception:
                    needs = True
            if needs:
                sftp.get(rpath, lpath)
                downloaded += 1
            if move_remote_to:
                # Ensure archive folder exists
                try:
                    sftp.listdir(move_remote_to)
                except IOError:
                    sftp.mkdir(move_remote_to)
                target = posixpath.join(move_remote_to, posixpath.basename(rpath))
                sftp.rename(rpath, target)
        return downloaded
    finally:
        try:
            sftp.close()
        except Exception:
            pass


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--remote-dir", required=False, help="Remote SFTP folder to read from")
    ap.add_argument("--local-dir", required=False, help="Local directory to write files into")
    ap.add_argument("--patterns", nargs="+", default=["AR_*.xml", "CSL*.xml"], help="Glob patterns to select files")
    ap.add_argument("--move-remote-to", default=None, help="Optional remote archive folder to move files after download")
    ap.add_argument("--test-connection", action="store_true", help="Only test that the SFTP credentials work and print current directory")
    args = ap.parse_args(argv[1:])

    if args.test_connection:
        sftp = _connect()
        try:
            # Print normalized current directory to confirm connectivity
            try:
                cwd = sftp.normalize(".")
            except Exception:
                cwd = "(unknown)"
            host = os.getenv("SFTP_HOST")
            port = os.getenv("SFTP_PORT", "22")
            user = os.getenv("SFTP_USER")
            print(f"SFTP connection OK: {user}@{host}:{port} cwd={cwd}")
        finally:
            try:
                sftp.close()
            except Exception:
                pass
        return 0

    if not args.remote_dir or not args.local_dir:
        ap.error("--remote-dir and --local-dir are required unless --test-connection is used")

    count = fetch(args.remote_dir, args.local_dir, args.patterns, args.move_remote_to)
    print(f"Downloaded {count} file(s) to {args.local_dir}")
    return 0


if __name__ == "__main__":
    import sys
    raise SystemExit(main(sys.argv))
