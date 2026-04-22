from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

from mtg_collection.scryfall import ScryfallCard, ScryfallError


SCRYFALL_BULK_META_URL = "https://api.scryfall.com/bulk-data"


@dataclass(frozen=True)
class BulkDataInfo:
    bulk_type: str
    download_uri: str
    updated_at: str
    content_type: str | None
    content_encoding: str | None


@dataclass(frozen=True)
class BulkCachePaths:
    root_dir: Path
    data_path: Path
    meta_path: Path


def default_bulk_cache_paths() -> BulkCachePaths:
    root = Path("data") / "scryfall"
    return BulkCachePaths(
        root_dir=root,
        # Default to .json; we may upgrade to .json.gz if the download is gzip.
        data_path=root / "oracle_cards.json",
        meta_path=root / "oracle_cards.meta.json",
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_read_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        v = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return v if isinstance(v, dict) else None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def fetch_oracle_bulk_info(session: requests.Session | None = None, timeout_s: float = 30.0) -> BulkDataInfo:
    s = session or requests.Session()
    r = s.get(SCRYFALL_BULK_META_URL, timeout=timeout_s)
    if r.status_code >= 400:
        raise ScryfallError(f"Scryfall bulk-data HTTP {r.status_code}: {r.text[:2000]}")
    payload = r.json()
    if not isinstance(payload, dict):
        raise ScryfallError("Unexpected Scryfall bulk-data response type")
    data = payload.get("data")
    if not isinstance(data, list):
        raise ScryfallError("Unexpected Scryfall bulk-data payload: missing data[]")

    for entry in data:
        if not isinstance(entry, dict):
            continue
        if entry.get("type") != "oracle_cards":
            continue
        download_uri = entry.get("download_uri")
        updated_at = entry.get("updated_at")
        if not isinstance(download_uri, str) or not download_uri:
            raise ScryfallError("Unexpected Scryfall bulk-data payload: missing download_uri")
        if not isinstance(updated_at, str) or not updated_at:
            raise ScryfallError("Unexpected Scryfall bulk-data payload: missing updated_at")
        content_type = entry.get("content_type")
        content_encoding = entry.get("content_encoding")
        return BulkDataInfo(
            bulk_type="oracle_cards",
            download_uri=download_uri,
            updated_at=updated_at,
            content_type=content_type if isinstance(content_type, str) else None,
            content_encoding=content_encoding if isinstance(content_encoding, str) else None,
        )

    raise ScryfallError("Scryfall bulk-data did not include oracle_cards")


def ensure_oracle_bulk_downloaded(
    *,
    paths: BulkCachePaths | None = None,
    session: requests.Session | None = None,
    timeout_s: float = 60.0,
    refresh_after: timedelta = timedelta(days=7),
) -> BulkCachePaths:
    """
    Ensures Scryfall Oracle Cards bulk data exists on disk.

    This is intentionally conservative about re-downloading:
    - If metadata is missing/corrupt, we download.
    - If metadata is present and recent, we skip.
    - If metadata is old, we check `/bulk-data` and download only if updated_at changed.
    """
    s = session or requests.Session()
    p = paths or default_bulk_cache_paths()
    p.root_dir.mkdir(parents=True, exist_ok=True)

    meta = _safe_read_json(p.meta_path) or {}
    meta_data_path = meta.get("data_path")
    if isinstance(meta_data_path, str) and meta_data_path:
        try:
            dp = Path(meta_data_path)
        except Exception:
            dp = None
        if isinstance(dp, Path):
            p = BulkCachePaths(root_dir=p.root_dir, data_path=dp, meta_path=p.meta_path)
    last_checked_iso = meta.get("last_checked_at")
    last_updated_at = meta.get("bulk_updated_at")

    # If we have a file and we checked recently, don't touch the network.
    if p.data_path.exists() and isinstance(last_checked_iso, str):
        try:
            last_checked = datetime.fromisoformat(last_checked_iso)
        except ValueError:
            last_checked = None
        if isinstance(last_checked, datetime) and (_utc_now() - last_checked) < refresh_after:
            return p

    info = fetch_oracle_bulk_info(session=s, timeout_s=timeout_s)
    should_download = (not p.data_path.exists()) or (not isinstance(last_updated_at, str)) or (last_updated_at != info.updated_at)
    if should_download:
        p = _download_to_cache_paths(s, info.download_uri, root_dir=p.root_dir, timeout_s=timeout_s)

    _write_json(
        p.meta_path,
        {
            "bulk_type": info.bulk_type,
            "download_uri": info.download_uri,
            "bulk_updated_at": info.updated_at,
            "content_type": info.content_type,
            "content_encoding": info.content_encoding,
            "last_checked_at": _utc_now().isoformat(),
            "data_path": str(p.data_path),
        },
    )
    return p


def _download_to_cache_paths(session: requests.Session, url: str, *, root_dir: Path, timeout_s: float) -> BulkCachePaths:
    """
    Downloads the bulk file and stores it as either `.json` or `.json.gz` depending on the content.
    """
    tmp = root_dir / "oracle_cards.download.tmp"
    with session.get(url, stream=True, timeout=timeout_s) as r:
        if r.status_code >= 400:
            raise ScryfallError(f"Scryfall bulk download HTTP {r.status_code}: {r.text[:2000]}")
        root_dir.mkdir(parents=True, exist_ok=True)
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    # Detect gzip by magic bytes (1F 8B). Don't trust filename/metadata alone.
    try:
        head = tmp.read_bytes()[:2]
    except OSError as e:
        raise ScryfallError(f"Failed reading downloaded bulk file: {e}") from e

    is_gzip = head == b"\x1f\x8b"
    final_path = root_dir / ("oracle_cards.json.gz" if is_gzip else "oracle_cards.json")
    tmp.replace(final_path)
    return BulkCachePaths(root_dir=root_dir, data_path=final_path, meta_path=root_dir / "oracle_cards.meta.json")


def open_bulk_json(path: Path):
    """
    Opens the bulk file as a text-mode stream, supporting `.json` and `.json.gz`.
    """
    # Be resilient to misnamed caches: detect gzip by magic bytes, not extension.
    try:
        with path.open("rb") as f:
            head = f.read(2)
    except OSError as e:
        raise ScryfallError(f"Failed to open bulk file: {e}") from e

    is_gzip = head == b"\x1f\x8b"
    if is_gzip:
        return gzip.open(path, "rt", encoding="utf-8")

    # Auto-heal: if the file is named `.gz` but isn't gzip, rename it to `.json`.
    if path.suffix.lower() == ".gz":
        repaired = path.with_suffix("")  # drop .gz => ... .json
        try:
            if not repaired.exists():
                path.replace(repaired)
                path = repaired
        except OSError:
            # If rename fails (file locked, permissions), just read as plain text.
            pass
    return path.open("rt", encoding="utf-8")


def iter_bulk_cards_minimal(path: Path) -> Iterable[ScryfallCard]:
    """
    Iterates Scryfall cards from a bulk file.

    Uses `ijson` if available (streaming, low memory). Falls back to full `json.load`.
    """
    try:
        import ijson  # type: ignore
    except Exception:
        ijson = None  # type: ignore

    with open_bulk_json(path) as f:
        if ijson is not None:
            items_iter = ijson.items(f, "item")
            for obj in items_iter:
                if not isinstance(obj, dict):
                    continue
                yield _bulk_obj_to_card(obj)
            return

        data = json.load(f)
        if not isinstance(data, list):
            raise ScryfallError("Unexpected bulk JSON: expected a list")
        for obj in data:
            if not isinstance(obj, dict):
                continue
            yield _bulk_obj_to_card(obj)


def iter_bulk_cards_identity(path: Path) -> Iterable[tuple[ScryfallCard, str | None]]:
    """
    Like `iter_bulk_cards_minimal`, but also yields the Scryfall card UUID (`id`) when present.
    """
    try:
        import ijson  # type: ignore
    except Exception:
        ijson = None  # type: ignore

    with open_bulk_json(path) as f:
        if ijson is not None:
            # Pre-bind methods for faster lookup in tight loop
            items_iter = ijson.items(f, "item")
            for obj in items_iter:
                if not isinstance(obj, dict):
                    continue
                scryfall_id = obj.get("id") if isinstance(obj.get("id"), str) else None
                yield _bulk_obj_to_card(obj), scryfall_id
            return

        data = json.load(f)
        if not isinstance(data, list):
            raise ScryfallError("Unexpected bulk JSON: expected a list")
        for obj in data:
            if not isinstance(obj, dict):
                continue
            scryfall_id = obj.get("id") if isinstance(obj.get("id"), str) else None
            yield _bulk_obj_to_card(obj), scryfall_id


def _bulk_obj_to_card(obj: dict[str, Any]) -> ScryfallCard:
    # Keep this minimal; the app only needs identity fields.
    oracle_id = obj.get("oracle_id")
    name = obj.get("name")
    scryfall_uri = obj.get("scryfall_uri")
    if not isinstance(oracle_id, str) or not oracle_id:
        raise ScryfallError("Unexpected bulk card payload: missing oracle_id")
    if not isinstance(name, str) or not name:
        raise ScryfallError("Unexpected bulk card payload: missing name")
    if not isinstance(scryfall_uri, str) or not scryfall_uri:
        raise ScryfallError("Unexpected bulk card payload: missing scryfall_uri")
    return ScryfallCard(oracle_id=oracle_id, name=name, scryfall_uri=scryfall_uri)

