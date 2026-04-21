from __future__ import annotations

import sys
from collections import Counter

from mtg_collection.resolver import build_default_bulk_first_resolver
from mtg_collection.scryfall import ScryfallClient


def main(argv: list[str]) -> int:
    names = argv[1:] or [
        "Lightning Bolt",
        "Opt",
        "Counterspell",
        "Sol Ring",
        "Brainstorm",
    ]

    api = ScryfallClient()
    _, _, resolver = build_default_bulk_first_resolver(api)

    # Simulate a medium import (repeat names to ensure dedupe/index behavior).
    test_names = (names * 250)[:1000]

    sources = Counter()
    failures: list[str] = []
    for n in test_names:
        try:
            res = resolver.resolve_name(n)
        except Exception as e:
            failures.append(f"{n}: {e}")
            continue
        sources[res.source] += 1

    print("Resolved counts by source:")
    for k, v in sources.most_common():
        print(f"- {k}: {v}")

    if failures:
        print("\nFailures:")
        for f in failures[:20]:
            print(f"- {f}")
        if len(failures) > 20:
            print(f"... {len(failures) - 20} more")
        return 2

    # Expect mostly bulk hits after the first successful bulk prep.
    if sources.get("bulk", 0) == 0:
        print("\nWARNING: No bulk hits; are you offline or did the bulk download fail?")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

