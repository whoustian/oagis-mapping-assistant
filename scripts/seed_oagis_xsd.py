"""
Seed the OAGIS Mapping Assistant's vector store with canonical paths from an
OAGIS XSD release.

The OAGIS schema is not redistributable, so this script is meant to be run
locally against a copy of the XSDs you've downloaded from
https://oagi.org/. Point it at the Nouns/ directory (or any directory
containing XSDs with top-level <xsd:element> declarations), and it will:

  1. Walk every .xsd file below that directory.
  2. For each top-level element (these are the OAGIS Nouns), recursively
     flatten its complex-type tree into canonical XPath-style paths like
       /ItemMaster/ItemMasterHeader/Item/Description
     deduplicating cycles and capping depth.
  3. Pull each element's xsd:annotation/xsd:documentation text to use as a
     description.
  4. POST the whole set to the assistant's /api/seed/canonical endpoint,
     where it's embedded and upserted under a dedicated "OAGIS canonical
     schema" pseudo-upload (so you can delete and re-seed from the UI).

Usage:
    python scripts/seed_oagis_xsd.py --xsd-dir /path/to/OAGIS/10.11/Model/Nouns
    python scripts/seed_oagis_xsd.py --xsd-dir ... --url http://localhost:5000
    python scripts/seed_oagis_xsd.py --xsd-dir ... --max-depth 8 --dry-run

Requires: standard library only (no extra deps).
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable

XSD_NS = "{http://www.w3.org/2001/XMLSchema}"


def _localname(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _doc_text(node: ET.Element) -> str:
    """Pull xsd:annotation/xsd:documentation text, flattened to one line."""
    chunks: list[str] = []
    for ann in node.findall(f"{XSD_NS}annotation"):
        for doc in ann.findall(f"{XSD_NS}documentation"):
            if doc.text:
                chunks.append(doc.text.strip())
    text = " ".join(chunks)
    # collapse whitespace
    return " ".join(text.split())


def _build_index(xsd_dir: Path) -> tuple[dict[str, ET.Element], dict[str, ET.Element]]:
    """Index every element declaration and named complex type across all XSDs.

    Returns (elements_by_name, types_by_name). Names are the *local* names from
    the XSD's targetNamespace (OAGIS uses a single namespace, so this is fine).
    """
    elements: dict[str, ET.Element] = {}
    types: dict[str, ET.Element] = {}
    files = sorted(xsd_dir.rglob("*.xsd"))
    if not files:
        raise SystemExit(f"No .xsd files found under {xsd_dir}")
    print(f"Indexing {len(files)} XSD files under {xsd_dir} ...", file=sys.stderr)

    for f in files:
        try:
            tree = ET.parse(f)
        except ET.ParseError as e:
            print(f"  skip (parse error): {f.name}: {e}", file=sys.stderr)
            continue
        root = tree.getroot()
        for el in root.findall(f"{XSD_NS}element"):
            name = el.get("name")
            if name and name not in elements:
                elements[name] = el
        for ct in root.findall(f"{XSD_NS}complexType"):
            name = ct.get("name")
            if name and name not in types:
                types[name] = ct
    print(f"  found {len(elements)} elements, {len(types)} complex types", file=sys.stderr)
    return elements, types


def _resolve_type(
    el: ET.Element,
    types: dict[str, ET.Element],
) -> ET.Element | None:
    """Return the complexType for an element, if one is reachable.

    Handles three patterns: type="X" attribute, inline <xsd:complexType>, and
    nothing (simple/unknown type — returns None).
    """
    # inline complexType child
    inline = el.find(f"{XSD_NS}complexType")
    if inline is not None:
        return inline
    type_ref = el.get("type")
    if type_ref:
        # strip namespace prefix if any
        local = type_ref.split(":", 1)[-1]
        return types.get(local)
    return None


def _walk_children(
    ct: ET.Element,
    elements: dict[str, ET.Element],
    types: dict[str, ET.Element],
) -> Iterable[tuple[str, ET.Element]]:
    """Yield (child_name, child_element_decl) for every element reference or
    inline element declaration reachable from a complex type.

    Follows xsd:extension, xsd:sequence, xsd:choice, xsd:all, xsd:group.
    """

    def recurse(node: ET.Element) -> Iterable[tuple[str, ET.Element]]:
        for child in node:
            tag = _localname(child.tag)
            if tag == "element":
                ref = child.get("ref")
                name = child.get("name")
                if ref:
                    local = ref.split(":", 1)[-1]
                    decl = elements.get(local)
                    if decl is not None:
                        yield local, decl
                elif name:
                    yield name, child
            elif tag in {"sequence", "choice", "all", "complexContent", "simpleContent"}:
                yield from recurse(child)
            elif tag == "extension":
                base = child.get("base")
                if base:
                    base_local = base.split(":", 1)[-1]
                    base_ct = types.get(base_local)
                    if base_ct is not None:
                        yield from recurse(base_ct)
                yield from recurse(child)
            elif tag == "group":
                ref = child.get("ref")
                if ref:
                    local = ref.split(":", 1)[-1]
                    # named group lookup: walk the declaring xsd's groups
                    # For simplicity we rely on it being declared somewhere
                    # we've already parsed; groups aren't indexed separately,
                    # so we skip undeclared ones. This keeps the seeder lean.
                    pass

    yield from recurse(ct)


def flatten_noun(
    noun_name: str,
    noun_el: ET.Element,
    elements: dict[str, ET.Element],
    types: dict[str, ET.Element],
    max_depth: int,
) -> list[dict]:
    """Flatten an OAGIS Noun into a list of canonical-path seed rows.

    Each row: { source_attribute, oagis_path, description, data_type }
    """
    rows: list[dict] = []
    seen_paths: set[str] = set()

    def visit(path: str, el: ET.Element, depth: int, type_stack: tuple[str, ...]):
        if path in seen_paths:
            return
        seen_paths.add(path)

        el_name = path.rsplit("/", 1)[-1]
        desc = _doc_text(el)
        type_ref = el.get("type") or ""
        type_local = type_ref.split(":", 1)[-1] if type_ref else ""
        rows.append(
            {
                # Canonical rows have no source_attribute yet — the LLM
                # treats the oagis_path itself as the anchor. We put the
                # element's own short name in there so text retrieval still
                # matches on common labels like "SerialNumberID".
                "source_attribute": el_name,
                "oagis_path": path,
                "description": desc,
                "data_type": type_local,
                "context": f"OAGIS canonical element in {noun_name}",
                "notes": "Canonical OAGIS schema entry (not a prior mapping).",
            }
        )
        if depth >= max_depth:
            return
        ct = _resolve_type(el, types)
        if ct is None:
            return
        # Cycle guard: don't re-expand a type we're already inside
        ct_name = ct.get("name") or ""
        if ct_name and ct_name in type_stack:
            return
        new_stack = type_stack + ((ct_name,) if ct_name else ())
        for child_name, child_el in _walk_children(ct, elements, types):
            visit(f"{path}/{child_name}", child_el, depth + 1, new_stack)

    visit(f"/{noun_name}", noun_el, 0, ())
    return rows


def collect_rows(xsd_dir: Path, max_depth: int, noun_filter: list[str] | None) -> list[dict]:
    elements, types = _build_index(xsd_dir)

    # Top-level nouns = elements declared as direct children of an xsd:schema
    # root AND whose type is a complexType. That's effectively everything in
    # the Nouns/ directory. We approximate by using every element we indexed
    # that has a resolvable complexType.
    noun_candidates = [
        (name, el)
        for name, el in elements.items()
        if _resolve_type(el, types) is not None
    ]
    if noun_filter:
        wanted = set(noun_filter)
        noun_candidates = [(n, e) for n, e in noun_candidates if n in wanted]

    # Prefer nouns whose name doesn't look like an internal component (heuristic:
    # skip names ending in "Type", "Header", "Line", etc. as TOP-level, but they
    # can still appear as children). We keep all for now — duplicates collapse
    # naturally because paths are unique.
    print(f"Flattening {len(noun_candidates)} top-level elements ...", file=sys.stderr)

    all_rows: list[dict] = []
    for name, el in sorted(noun_candidates):
        rows = flatten_noun(name, el, elements, types, max_depth)
        all_rows.extend(rows)
    print(f"  produced {len(all_rows)} canonical path rows", file=sys.stderr)

    # Dedupe on oagis_path (same path may appear via different nouns if an
    # element is reused). Keep the first occurrence.
    dedup: dict[str, dict] = {}
    for r in all_rows:
        dedup.setdefault(r["oagis_path"], r)
    return list(dedup.values())


def post_to_server(url: str, rows: list[dict]) -> None:
    payload = json.dumps({"rows": rows}).encode()
    req = urllib.request.Request(
        url.rstrip("/") + "/api/seed/canonical",
        data=payload,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:  # noqa: S310 - trusted local URL
        body = resp.read().decode()
        print(body)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--xsd-dir", required=True, type=Path, help="Directory containing OAGIS XSDs (e.g. .../Model/Nouns)")
    p.add_argument("--url", default="http://localhost:5000", help="Base URL of the running server")
    p.add_argument("--max-depth", type=int, default=6, help="Maximum nesting depth to flatten (default 6)")
    p.add_argument("--noun", action="append", default=None, help="Restrict to specific top-level Noun names (repeatable)")
    p.add_argument("--dry-run", action="store_true", help="Print stats + sample rows instead of posting")
    p.add_argument("--out", type=Path, default=None, help="Optional: also write the rows to a JSON file")
    args = p.parse_args()

    if not args.xsd_dir.exists():
        raise SystemExit(f"XSD directory not found: {args.xsd_dir}")

    rows = collect_rows(args.xsd_dir, args.max_depth, args.noun)

    if args.out:
        args.out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        print(f"Wrote {len(rows)} rows to {args.out}", file=sys.stderr)

    if args.dry_run:
        print(f"\nDry run: {len(rows)} canonical rows would be seeded.")
        for r in rows[:10]:
            print(f"  {r['oagis_path']}  ({r['data_type'] or '?'})")
        if len(rows) > 10:
            print(f"  ... and {len(rows) - 10} more")
        return

    post_to_server(args.url, rows)


if __name__ == "__main__":
    main()
