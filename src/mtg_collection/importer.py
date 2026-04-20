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

        parsed.append(ImportLine(raw=raw, qty=qty, name=name))

    return parsed, unresolved


def parse_csv_bytes(data: bytes, encoding: str = "utf-8") -> tuple[list[ImportLine], list[str]]:
    text = data.decode(encoding, errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    parsed: list[ImportLine] = []
    unresolved: list[str] = []

    if reader.fieldnames is None:
        return [], ["CSV has no header row"]

    lowered = {h.lower(): h for h in reader.fieldnames if isinstance(h, str)}
    name_key = lowered.get("name")
    qty_key = lowered.get("quantity") or lowered.get("qty")

    if not name_key or not qty_key:
        return [], [f"CSV must include columns: name, quantity (found: {reader.fieldnames})"]

    for row in reader:
        raw_name = (row.get(name_key) or "").strip()
        raw_qty = (row.get(qty_key) or "").strip()
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
        parsed.append(ImportLine(raw=str(row), qty=qty, name=raw_name))

    return parsed, unresolved

