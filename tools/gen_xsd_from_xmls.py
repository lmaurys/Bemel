#!/usr/bin/env python3
"""
Infer a stricter XSD from sample XML files (AR_*.xml) under XMLS_COL/**.

This produces XSD/UniversalTransaction.strict.xsd with local, context-specific
element declarations to avoid collisions for common names like "Type".

Approach:
- Build a tree of nodes keyed by path from the root, aggregating attributes,
    child order, and cardinalities observed across files.
- For each parent node, track how many times it appears; for each child edge,
    track how many parent instances contained that child (presence_count), and if
    any instance had multiple occurrences (repeated=True).
- Emit XSD with local declarations: complexType with xs:sequence, children in
    first-seen order, minOccurs=1 only if present in all parent instances, else 0;
    maxOccurs=unbounded when repeated.
- Leaf nodes (no children) are modeled as simpleContent with base type inferred
    from observed text values (boolean, integer, decimal, dateTime, date, string).

Limitations:
- Order is enforced as first seen; if real order varies, validation may fail.
- Attribute types are xs:string and optional.
- Mixed content is not modeled; if an element sometimes has text and children,
    it will be treated as complex with children (text ignored).
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
XSD_OUTPUT = os.path.join(ROOT, "XSD", "UniversalTransaction.strict.xsd")
TARGET_NS = "http://www.cargowise.com/Schemas/Universal/2011/11"


num_re = re.compile(r"^[+-]?\d+$")
dec_re = re.compile(r"^[+-]?(?:\d+\.\d+|\d+)$")
date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
datetime_re = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?$")


def guess_type(text: str) -> str:
    t = text.strip()
    if not t:
        return "string"
    if t in ("true", "false"):  # lowercase boolean forms seen in XML
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
        self.instances: int = 0  # how many times this node appeared
        self.text_types: Set[str] = set()  # only considered when no children

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
    # attrs
    for k in elem.attrib:
        node.attrs.add(localname(k))
    # children handling
    children = [c for c in elem if isinstance(c.tag, str)]
    if children:
        # count children by name for multiplicity and presence
        counts: Dict[str, int] = defaultdict(int)
        for c in children:
            counts[localname(c.tag)] += 1
        # ensure order is first-seen: iterate in document order
        seen_names_in_this_instance: Set[str] = set()
        for c in children:
            cname = localname(c.tag)
            cpath = (*path, cname)
            cnode = nodes_by_path.get(cpath)
            if cnode is None:
                cnode = Node(cname, cpath)
                nodes_by_path[cpath] = cnode
            edge = node.child_edge(cname, cnode)
            # presence count increments once per instance when child exists
            if cname not in seen_names_in_this_instance:
                edge.presence_count += 1
                seen_names_in_this_instance.add(cname)
            if counts[cname] > 1:
                edge.repeated = True
            # recurse
            walk(c, node, nodes_by_path)
    else:
        # leaf: note text type
        if elem.text and elem.text.strip():
            node.text_types.add(guess_type(elem.text))


def find_ar_xml_files(root: str) -> List[str]:
    return sorted(iglob(os.path.join(root, "**", "AR_*.xml"), recursive=True))


def build_tree(files: List[str]) -> Node:
    nodes_by_path: Dict[Tuple[str, ...], Node] = {}
    parser = etree.XMLParser(remove_blank_text=False, ns_clean=True, recover=False)
    for path in files:
        doc = etree.parse(path, parser)
        root = doc.getroot()
        walk(root, None, nodes_by_path)
    root_node = nodes_by_path.get(("UniversalTransaction",))
    if root_node is None:
        raise RuntimeError("Root UniversalTransaction not found in samples")
    return root_node


def choose_simple_type(types: Set[str]) -> str:
    # Be conservative: use string to avoid over-constraining IDs like NIT with hyphens
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
    # include common reusable types
    ap(f'  <xs:include schemaLocation="CommonTypes.xsd"/>')
    ap("")
    # Declare root with local type
    ap('  <xs:element name="UniversalTransaction">')
    ap('    <xs:complexType>')

    def emit_local_element(node: Node, indent: int) -> None:
        pad = " " * indent
        if node.children:
            ap(f"{pad}<xs:complexType>")
            # Decide model: if any child repeats, use sequence; otherwise, use all
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
            # attributes for complex content
            for attr in sorted(node.attrs):
                ap(f"{pad}  <xs:attribute name=\"{attr}\" type=\"xs:string\" use=\"optional\"/>")
            ap(f"{pad}</xs:complexType>")
        else:
            # simple content leaf
            stype = choose_simple_type(node.text_types)
            ap(f"{pad}<xs:complexType>")
            ap(f"{pad}  <xs:simpleContent>")
            ap(f"{pad}    <xs:extension base=\"xs:{stype}\">")
            for attr in sorted(node.attrs):
                ap(f"{pad}      <xs:attribute name=\"{attr}\" type=\"xs:string\" use=\"optional\"/>")
            ap(f"{pad}    </xs:extension>")
            ap(f"{pad}  </xs:simpleContent>")
            ap(f"{pad}</xs:complexType>")

    # Root content model: decide by repetition of direct children
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
    # root attributes (version)
    ap('      <xs:attribute name="version" type="xs:string" use="optional"/>')
    ap('    </xs:complexType>')
    ap('  </xs:element>')
    ap('</xs:schema>')

    os.makedirs(os.path.dirname(output), exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main(argv: List[str]) -> int:
    files = find_ar_xml_files(XML_ROOT)
    if not files:
        print("No AR_*.xml files found.")
        return 1
    root = build_tree(files)
    write_xsd(root, XSD_OUTPUT)
    print(f"Generated: {XSD_OUTPUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
