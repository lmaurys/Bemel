#!/usr/bin/env python3
"""
Validate XML files under XMLS_COL/** against a provided XSD.

Defaults to AR_*.xml and XSD/UniversalTransaction.xsd, but you can pass
an XSD path and optional prefix to target other families (e.g., CSL).
"""
from __future__ import annotations

import sys
import os
from glob import iglob
from typing import List, Tuple

from lxml import etree

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
XML_ROOT = os.path.join(ROOT, "XMLS_COL")
XSD_PATH = os.path.join(ROOT, "XSD", "UniversalTransaction.xsd")


def load_schema(xsd_path: str) -> etree.XMLSchema:
    with open(xsd_path, "rb") as f:
        schema_doc = etree.parse(f)
    return etree.XMLSchema(schema_doc)


def find_xml_files(root: str, prefix: str) -> List[str]:
    pattern = os.path.join(root, "**", f"{prefix}*.xml")
    return sorted(iglob(pattern, recursive=True))


def validate_file(xml_path: str, schema: etree.XMLSchema) -> Tuple[bool, str]:
    try:
        parser = etree.XMLParser(remove_blank_text=False, ns_clean=True, recover=False)
        doc = etree.parse(xml_path, parser)
        schema.assertValid(doc)
        return True, "OK"
    except etree.DocumentInvalid as e:
        return False, str(e.error_log).strip()
    except Exception as e:
        return False, f"ParseError: {e}"


def main(argv: List[str]) -> int:
    # Usage: validate_ar_xml.py [XSD_PATH] [PREFIX]
    xsd = XSD_PATH
    prefix = "AR_"
    if len(argv) > 1:
        xsd = argv[1]
    if len(argv) > 2:
        prefix = argv[2]
    if not os.path.isfile(xsd):
        print(f"XSD not found: {xsd}")
        return 2
    if not os.path.isdir(XML_ROOT):
        print(f"XML root not found: {XML_ROOT}")
        return 2

    schema = load_schema(xsd)
    files = find_xml_files(XML_ROOT, prefix)
    if not files:
        print(f"No {prefix}*.xml files found.")
        return 1

    total = 0
    ok = 0
    failures: List[Tuple[str, str]] = []

    for path in files:
        total += 1
        valid, msg = validate_file(path, schema)
        if valid:
            ok += 1
        else:
            failures.append((path, msg))

    print(f"Validated: {ok}/{total} passed")
    if failures:
        print("Failures:")
        for path, msg in failures[:20]:  # limit output
            print(f"- {path}: {msg.splitlines()[0] if msg else 'Invalid'}")
        if len(failures) > 20:
            print(f"... and {len(failures) - 20} more")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
