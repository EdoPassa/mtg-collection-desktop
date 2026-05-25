import { describe, expect, it, vi } from "vitest";
import { createBackendApi, type WailsBindings } from "./backend";

function fakeBindings(): WailsBindings {
  return {
    ResolverStatus: vi.fn().mockResolvedValue("api-only"),
    ListCollection: vi.fn().mockResolvedValue([]),
    PreviewTextImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    PreviewCSVImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    CommitImport: vi.fn().mockResolvedValue(undefined),
    CompareDeck: vi.fn().mockResolvedValue({ rows: [], unresolved: [], repairs: [], hasUnresolved: false }),
    BuildDeckFromCompare: vi.fn().mockResolvedValue(1),
    ListDecks: vi.fn().mockResolvedValue([]),
    ListDeckCards: vi.fn().mockResolvedValue([]),
    DeleteDeck: vi.fn().mockResolvedValue(undefined),
    RenameDeck: vi.fn().mockResolvedValue(undefined),
    SetDeckCardQuantity: vi.fn().mockResolvedValue(undefined),
    AddCardToDeckByName: vi.fn().mockResolvedValue(undefined),
    LendCard: vi.fn().mockResolvedValue(undefined),
    ListLentCards: vi.fn().mockResolvedValue([]),
    ReturnCard: vi.fn().mockResolvedValue(undefined),
    RepairCompareMismatches: vi.fn().mockResolvedValue(undefined),
    FormatMissingDecklist: vi.fn().mockResolvedValue("")
  };
}

describe("createBackendApi", () => {
  it("adapts generated Wails bindings to the app API surface", async () => {
    const bindings = fakeBindings();
    const api = createBackendApi(bindings);

    await expect(api.ResolverStatus()).resolves.toBe("api-only");
    await api.BuildDeckFromCompare({ Name: "Burn", ReplaceDeckID: 0, Rows: [] });

    expect(bindings.ResolverStatus).toHaveBeenCalledOnce();
    expect(bindings.BuildDeckFromCompare).toHaveBeenCalledWith({ Name: "Burn", ReplaceDeckID: 0, Rows: [] });
  });
});
