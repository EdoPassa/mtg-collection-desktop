from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ImportLine:
    raw: str
    qty: int
    name: str
    scryfall_id: str | None = None


_QTY_PREFIX = re.compile(r"^\s*(?P<qty>\d+)\s*(x)?\s+(?P<name>.+?)\s*$", re.IGNORECASE)
_QTY_SUFFIX = re.compile(r"^\s*(?P<name>.+?)\s*(x)?\s*(?P<qty>\d+)\s*$", re.IGNORECASE)


def parse_txt(text: str) -> tuple[list[ImportLine], list[str]]:
    parsed: list[ImportLine] = []
    unresolved: list[str] = []

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue

        qty: int | None = None
        name: str | None = None

        m = _QTY_PREFIX.match(line)
        if m:
            qty = int(m.group("qty"))
            name = m.group("name").strip()
        else:
            m = _QTY_SUFFIX.match(line)
            if m:
                qty = int(m.group("qty"))
                name = m.group("name").strip()

        if qty is None or name is None or qty <= 0 or not name:
            unresolved.append(raw)
            continue

        parsed.append(ImportLine(raw=raw, qty=qty, name=name, scryfall_id=None))

    return parsed, unresolved


def parse_csv_bytes(data: bytes, encoding: str = "utf-8") -> tuple[list[ImportLine], list[str]]:
    text = data.decode(encoding, errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    parsed: list[ImportLine] = []
    unresolved: list[str] = []

    if reader.fieldnames is None:
        return [], ["CSV has no header row"]

    def norm(h: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", h.strip().casefold())

    normalized: dict[str, str] = {norm(h): h for h in reader.fieldnames if isinstance(h, str)}

    # Common export formats:
    # - "name, quantity"
    # - "Card Name, Quantity, Scryfall ID" (this repo's sample)
    name_key = normalized.get("name") or normalized.get("cardname")
    qty_key = normalized.get("quantity") or normalized.get("qty")
    scryfall_id_key = normalized.get("scryfallid")

    if not name_key or not qty_key:
        return [], [
            "CSV must include columns for card name and quantity "
            f"(expected one of: name/card name + quantity/qty; found: {reader.fieldnames})"
        ]

    for row in reader:
        raw_name = (row.get(name_key) or "").strip()
        raw_qty = (row.get(qty_key) or "").strip()
        raw_scryfall_id = ((row.get(scryfall_id_key) if scryfall_id_key else None) or "").strip()
        if not raw_name or not raw_qty:
            unresolved.append(str(row))
            continue
        try:
            qty = int(raw_qty)
        except ValueError:
            unresolved.append(str(row))
            continue
        if qty <= 0:
            unresolved.append(str(row))
            continue
        parsed.append(
            ImportLine(
                raw=str(row),
                qty=qty,
                name=raw_name,
                scryfall_id=raw_scryfall_id or None,
            )
        )

    return parsed, unresolved

