from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

import random
import threading
import time


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
    def __init__(
        self,
        session: requests.Session | None = None,
        timeout_s: float = 10.0,
        min_interval_s: float = 0.12,
        max_attempts: int = 5,
    ):
        self._session = session or requests.Session()
        self._timeout_s = timeout_s
        self._min_interval_s = float(min_interval_s)
        self._max_attempts = int(max_attempts)
        self._lock = threading.Lock()
        self._last_request_t = 0.0

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

    def lookup_scryfall_id(self, scryfall_id: str) -> ScryfallCard:
        scryfall_id = scryfall_id.strip()
        if not scryfall_id:
            raise ScryfallError("Empty Scryfall ID")

        data = self._get_json_required(f"https://api.scryfall.com/cards/{scryfall_id}", params={})
        return self._to_card(data)

    def _get_json(self, url: str, params: dict[str, str]) -> dict[str, Any] | None:
        return self._request_json(url, params=params, allow_404=True)

    def _get_json_required(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        data = self._request_json(url, params=params, allow_404=False)
        if data is None:
            raise ScryfallError("Unexpected missing payload from Scryfall")
        return data

    def _request_json(self, url: str, params: dict[str, str], *, allow_404: bool) -> dict[str, Any] | None:
        attempt = 0
        while True:
            attempt += 1
            self._throttle()
            try:
                r = self._session.get(url, params=params, timeout=self._timeout_s)
            except requests.RequestException as e:
                if attempt >= self._max_attempts:
                    raise ScryfallError(f"Scryfall request failed: {e}") from e
                self._sleep_backoff(attempt)
                continue

            # Named endpoint returns 404 with JSON error when no match is found.
            if r.status_code == 404 and allow_404:
                return None

            if r.status_code == 429:
                if attempt >= self._max_attempts:
                    raise ScryfallError(f"Scryfall HTTP 429: {r.text[:2000]}")
                self._sleep_retry_after(r)
                continue

            if 500 <= r.status_code < 600:
                if attempt >= self._max_attempts:
                    raise ScryfallError(f"Scryfall HTTP {r.status_code}: {r.text[:2000]}")
                self._sleep_backoff(attempt)
                continue

            if r.status_code >= 400:
                raise ScryfallError(f"Scryfall HTTP {r.status_code}: {r.text[:2000]}")

            data = r.json()
            if not isinstance(data, dict):
                raise ScryfallError("Unexpected Scryfall response type")
            if data.get("object") == "error":
                if allow_404:
                    return None
                raise ScryfallError(_pick(data, "details"))
            return data

    def _throttle(self) -> None:
        if self._min_interval_s <= 0:
            return
        with self._lock:
            now = time.monotonic()
            delta = now - self._last_request_t
            if delta < self._min_interval_s:
                time.sleep(self._min_interval_s - delta)
                now = time.monotonic()
            self._last_request_t = now

    def _sleep_retry_after(self, r: requests.Response) -> None:
        h = r.headers.get("Retry-After")
        delay_s: float | None = None
        if isinstance(h, str) and h.strip():
            try:
                delay_s = float(h.strip())
            except ValueError:
                delay_s = None
        # Fallback: small exponential-ish delay with jitter.
        if delay_s is None:
            delay_s = 1.0 + random.random()
        time.sleep(min(60.0, max(0.1, delay_s)))

    def _sleep_backoff(self, attempt: int) -> None:
        # Exponential backoff with jitter, capped.
        base = min(8.0, 0.5 * (2 ** max(0, attempt - 1)))
        time.sleep(base + random.random() * 0.25)

    def _to_card(self, data: dict[str, Any]) -> ScryfallCard:
        return ScryfallCard(
            oracle_id=_pick(data, "oracle_id"),
            name=_pick(data, "name"),
            scryfall_uri=_pick(data, "scryfall_uri"),
        )

