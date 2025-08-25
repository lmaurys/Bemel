#!/usr/bin/env python3
"""
Infer a stricter XSD from sample XML files (CSL*.xml) under XMLS_COL/**.

This produces XSD/UniversalShipment.strict.xsd with local, context-specific
element declarations to avoid collisions for common names like "Type".

Approach mirrors gen_xsd_from_xmls.py used for AR files, but targets
UniversalShipment documents and CSL*.xml patterns.
"""
from __future__ import annotations

import os
import re
import sys
from collections import OrderedDict, defaultdict
from glob import iglob
from typing import Dict, List, Optional, Set, Tuple

from lxml import etree

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
XML_ROOT = os.path.join(ROOT, "XMLS_COL")
XSD_OUTPUT = os.path.join(ROOT, "XSD", "UniversalShipment.strict.xsd")
TARGET_NS = "http://www.cargowise.com/Schemas/Universal/2011/11"


num_re = re.compile(r"^[+-]?\d+$")
dec_re = re.compile(r"^[+-]?(?:\d+\.\d+|\d+)$")
date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
datetime_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$")


def guess_type(text: str) -> str:
    t = text.strip()
    if not t:
        return "string"
    if t in ("true", "false"):
        return "boolean"
    if datetime_re.match(t):
        return "dateTime"
    if date_re.match(t):
        return "date"
    if num_re.match(t):
        return "integer"
    if dec_re.match(t):
        return "decimal"
    return "string"


def localname(tag: str) -> str:
    if tag is None:
        return ""
    return tag.split('}')[-1] if '}' in tag else tag


class ChildEdge:
    __slots__ = ("node", "presence_count", "repeated")

    def __init__(self, node: "Node"):
        self.node = node
        self.presence_count: int = 0
        self.repeated: bool = False


class Node:
    def __init__(self, name: str, path: Tuple[str, ...]):
        self.name = name
        self.path = path
        self.attrs: Set[str] = set()
        self.children: "OrderedDict[str, ChildEdge]" = OrderedDict()
        self.instances: int = 0
        self.text_types: Set[str] = set()

    def child_edge(self, child_name: str, child_node: "Node") -> ChildEdge:
        if child_name not in self.children:
            self.children[child_name] = ChildEdge(child_node)
        return self.children[child_name]


def walk(elem: etree._Element, parent: Optional[Node], nodes_by_path: Dict[Tuple[str, ...], Node]):
    name = localname(elem.tag)
    path = (*(() if parent is None else parent.path), name)
    node = nodes_by_path.get(path)
    if node is None:
        node = Node(name, path)
        nodes_by_path[path] = node
    node.instances += 1
    for k in elem.attrib:
        node.attrs.add(localname(k))
    children = [c for c in elem if isinstance(c.tag, str)]
    if children:
        counts: Dict[str, int] = defaultdict(int)
        for c in children:
            counts[localname(c.tag)] += 1
        seen_names_in_this_instance: Set[str] = set()
        for c in children:
            cname = localname(c.tag)
            cpath = (*path, cname)
            cnode = nodes_by_path.get(cpath)
            if cnode is None:
                cnode = Node(cname, cpath)
                nodes_by_path[cpath] = cnode
            edge = node.child_edge(cname, cnode)
            if cname not in seen_names_in_this_instance:
                edge.presence_count += 1
                seen_names_in_this_instance.add(cname)
            if counts[cname] > 1:
                edge.repeated = True
            walk(c, node, nodes_by_path)
    else:
        if elem.text and elem.text.strip():
            node.text_types.add(guess_type(elem.text))


def find_csl_xml_files(root: str) -> List[str]:
    # CSL files are named like CSL040000630_...xml
    return sorted(iglob(os.path.join(root, "**", "CSL*.xml"), recursive=True))


def build_tree(files: List[str]) -> Node:
    nodes_by_path: Dict[Tuple[str, ...], Node] = {}
    parser = etree.XMLParser(remove_blank_text=False, ns_clean=True, recover=False)
    for path in files:
        doc = etree.parse(path, parser)
        root = doc.getroot()
        walk(root, None, nodes_by_path)
    root_node = nodes_by_path.get(("UniversalShipment",))
    if root_node is None:
        raise RuntimeError("Root UniversalShipment not found in samples")
    return root_node


def choose_simple_type(types: Set[str]) -> str:
    return "string"


def write_xsd(root_node: Node, output: str):
    lines: List[str] = []
    ap = lines.append
    ap('<?xml version="1.0" encoding="UTF-8"?>')
    ap('<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"')
    ap(f'           targetNamespace="{TARGET_NS}"')
    ap(f'           xmlns="{TARGET_NS}"')
    ap(f'           xmlns:tns="{TARGET_NS}"')
    ap('           elementFormDefault="qualified"')
    ap('           attributeFormDefault="unqualified">')
    ap('')
    ap('  <xs:include schemaLocation="CommonTypes.xsd"/>')
    ap("")
    ap('  <xs:element name="UniversalShipment">')
    ap('    <xs:complexType>')

    def emit_local_element(node: Node, indent: int) -> None:
        pad = " " * indent
        if node.children:
            ap(f"{pad}<xs:complexType>")
            rep_exists = any(edge.repeated for edge in node.children.values())
            if rep_exists:
                ap(f"{pad}  <xs:sequence>")
                for cname, edge in node.children.items():
                    mino = 1 if edge.presence_count == node.instances else 0
                    maxo = "unbounded" if edge.repeated else 1
                    max_attr = '' if maxo == 1 else ' maxOccurs="unbounded"'
                    ap(f"{pad}    <xs:element name=\"{cname}\" minOccurs=\"{mino}\"{max_attr}>")
                    emit_local_element(edge.node, indent + 6)
                    ap(f"{pad}    </xs:element>")
                ap(f"{pad}  </xs:sequence>")
            else:
                ap(f"{pad}  <xs:all>")
                for cname, edge in node.children.items():
                    mino = 1 if edge.presence_count == node.instances else 0
                    ap(f"{pad}    <xs:element name=\"{cname}\" minOccurs=\"{mino}\">")
                    emit_local_element(edge.node, indent + 6)
                    ap(f"{pad}    </xs:element>")
                ap(f"{pad}  </xs:all>")
            for attr in sorted(node.attrs):
                ap(f"{pad}  <xs:attribute name=\"{attr}\" type=\"xs:string\" use=\"optional\"/>")
            ap(f"{pad}</xs:complexType>")
        else:
            stype = choose_simple_type(node.text_types)
            ap(f"{pad}<xs:complexType>")
            ap(f"{pad}  <xs:simpleContent>")
            ap(f"{pad}    <xs:extension base=\"xs:{stype}\">")
            for attr in sorted(node.attrs):
                ap(f"{pad}      <xs:attribute name=\"{attr}\" type=\"xs:string\" use=\"optional\"/>")
            ap(f"{pad}    </xs:extension>")
            ap(f"{pad}  </xs:simpleContent>")
            ap(f"{pad}</xs:complexType>")

    rep_exists_root = any(edge.repeated for edge in root_node.children.values())
    if rep_exists_root:
        ap('      <xs:sequence>')
        for cname, edge in root_node.children.items():
            mino = 1 if edge.presence_count == root_node.instances else 0
            maxo = "unbounded" if edge.repeated else 1
            max_attr = '' if maxo == 1 else ' maxOccurs="unbounded"'
            ap(f'        <xs:element name="{cname}" minOccurs="{mino}"{max_attr}>')
            emit_local_element(edge.node, indent=10)
            ap('        </xs:element>')
        ap('      </xs:sequence>')
    else:
        ap('      <xs:all>')
        for cname, edge in root_node.children.items():
            mino = 1 if edge.presence_count == root_node.instances else 0
            ap(f'        <xs:element name="{cname}" minOccurs="{mino}">')
            emit_local_element(edge.node, indent=10)
            ap('        </xs:element>')
        ap('      </xs:all>')

    ap('      <xs:attribute name="version" type="xs:string" use="optional"/>')
    ap('    </xs:complexType>')
    ap('  </xs:element>')
    ap('</xs:schema>')

    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main(argv: List[str]) -> int:
    files = find_csl_xml_files(XML_ROOT)
    if not files:
        print("No CSL*.xml files found.")
        return 1
    root = build_tree(files)
    write_xsd(root, XSD_OUTPUT)
    print(f"Generated: {XSD_OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
