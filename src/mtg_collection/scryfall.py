from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


class ScryfallError(RuntimeError):
    pass


@dataclass(frozen=True)
class ScryfallCard:
    oracle_id: str
    name: str
    scryfall_uri: str


def _pick(d: dict[str, Any], key: str) -> str:
    v = d.get(key)
    if not isinstance(v, str) or not v:
        raise ScryfallError(f"Unexpected Scryfall payload: missing {key}")
    return v


class ScryfallClient:
    def __init__(self, session: requests.Session | None = None, timeout_s: float = 10.0):
        self._session = session or requests.Session()
        self._timeout_s = timeout_s

    def lookup_named(self, name: str) -> ScryfallCard:
        name = name.strip()
        if not name:
            raise ScryfallError("Empty card name")

        # Try exact match first
        exact = self._get_json("https://api.scryfall.com/cards/named", params={"exact": name})
        if exact is not None:
            return self._to_card(exact)

        # Fallback to fuzzy
        fuzzy = self._get_json("https://api.scryfall.com/cards/named", params={"fuzzy": name})
        if fuzzy is not None:
            return self._to_card(fuzzy)

        raise ScryfallError(f"No match for: {name}")

    def _get_json(self, url: str, params: dict[str, str]) -> dict[str, Any] | None:
        r = self._session.get(url, params=params, timeout=self._timeout_s)
        # Scryfall returns 404 with a JSON error when no match is found.
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise ScryfallError(f"Scryfall HTTP {r.status_code}: {r.text[:2000]}")
        data = r.json()
        if not isinstance(data, dict):
            raise ScryfallError("Unexpected Scryfall response type")
        if data.get("object") == "error":
            # For safety; shouldn't happen for non-404.
            return None
        return data

    def _to_card(self, data: dict[str, Any]) -> ScryfallCard:
        return ScryfallCard(
            oracle_id=_pick(data, "oracle_id"),
            name=_pick(data, "name"),
            scryfall_uri=_pick(data, "scryfall_uri"),
        )

