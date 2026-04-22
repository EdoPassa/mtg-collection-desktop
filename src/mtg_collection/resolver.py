from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from mtg_collection.importer import ImportLine
from mtg_collection.scryfall import ScryfallCard, ScryfallClient, ScryfallError
from mtg_collection.scryfall_bulk import BulkCachePaths, ensure_oracle_bulk_downloaded, iter_bulk_cards_identity


_WS_RE = re.compile(r"\s+")


def normalize_card_name(name: str) -> str:
    # Keep this conservative: Scryfall is tolerant, but bulk lookup needs stable keys.
    # - Trim
    # - Collapse internal whitespace
    # - Casefold for unicode-safe case-insensitivity
    return _WS_RE.sub(" ", name.strip()).casefold()


@dataclass(frozen=True)
class ResolveResult:
    card: ScryfallCard
    source: str  # "bulk" | "api"


class CardResolver:
    def resolve_line(self, line: ImportLine) -> ResolveResult:
        raise NotImplementedError

    def resolve_name(self, name: str) -> ResolveResult:
        raise NotImplementedError

    def resolve_scryfall_id(self, scryfall_id: str) -> ResolveResult:
        raise NotImplementedError


class BulkOracleIndex:
    def __init__(self, by_name: dict[str, ScryfallCard], by_scryfall_id: dict[str, ScryfallCard]):
        self._by_name = by_name
        self._by_scryfall_id = by_scryfall_id

    @classmethod
    def build_from_bulk_file(cls, path: Path) -> "BulkOracleIndex":
        by_name: dict[str, ScryfallCard] = {}
        by_scryfall_id: dict[str, ScryfallCard] = {}

        # Pre-bind methods for faster lookup in tight loop
        normalize = normalize_card_name
        setdefault_name = by_name.setdefault
        setdefault_id = by_scryfall_id.setdefault

        for card, scryfall_id in iter_bulk_cards_identity(path):
            setdefault_name(normalize(card.name), card)
            if scryfall_id:
                setdefault_id(scryfall_id.strip().casefold(), card)

        for card, scryfall_id in iter_bulk_cards_identity(path):
            by_name.setdefault(normalize_card_name(card.name), card)
            if scryfall_id:
                by_scryfall_id.setdefault(scryfall_id.strip().casefold(), card)

        return cls(by_name=by_name, by_scryfall_id=by_scryfall_id)

    def lookup_name(self, name: str) -> ScryfallCard | None:
        key = normalize_card_name(name)
        if not key:
            return None
        return self._by_name.get(key)

    def lookup_scryfall_id(self, scryfall_id: str) -> ScryfallCard | None:
        key = scryfall_id.strip().casefold()
        if not key:
            return None
        return self._by_scryfall_id.get(key)


class BulkFirstResolver(CardResolver):
    def __init__(self, *, bulk_index: BulkOracleIndex, api: ScryfallClient):
        self._bulk = bulk_index
        self._api = api

    def resolve_line(self, line: ImportLine) -> ResolveResult:
        if line.scryfall_id:
            return self.resolve_scryfall_id(line.scryfall_id)
        return self.resolve_name(line.name)

    def resolve_name(self, name: str) -> ResolveResult:
        card = self._bulk.lookup_name(name)
        if card is not None:
            return ResolveResult(card=card, source="bulk")
        return ResolveResult(card=self._api.lookup_named(name), source="api")

    def resolve_scryfall_id(self, scryfall_id: str) -> ResolveResult:
        card = self._bulk.lookup_scryfall_id(scryfall_id)
        if card is not None:
            return ResolveResult(card=card, source="bulk")
        return ResolveResult(card=self._api.lookup_scryfall_id(scryfall_id), source="api")


class ApiOnlyResolver(CardResolver):
    def __init__(self, api: ScryfallClient):
        self._api = api

    def resolve_line(self, line: ImportLine) -> ResolveResult:
        if line.scryfall_id:
            return self.resolve_scryfall_id(line.scryfall_id)
        return self.resolve_name(line.name)

    def resolve_name(self, name: str) -> ResolveResult:
        return ResolveResult(card=self._api.lookup_named(name), source="api")

    def resolve_scryfall_id(self, scryfall_id: str) -> ResolveResult:
        return ResolveResult(card=self._api.lookup_scryfall_id(scryfall_id), source="api")


def build_default_bulk_first_resolver(api: ScryfallClient) -> tuple[BulkCachePaths, BulkOracleIndex, BulkFirstResolver]:
    paths = ensure_oracle_bulk_downloaded()
    idx = BulkOracleIndex.build_from_bulk_file(paths.data_path)
    return paths, idx, BulkFirstResolver(bulk_index=idx, api=api)

