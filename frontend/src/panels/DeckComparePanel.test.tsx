import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { createBackendApi } from "../backend";
import { DeckComparePanel } from "./DeckComparePanel";

function mockApi() {
  const formatMissingDecklist = vi.fn().mockResolvedValue("2 Lightning Bolt");
  const api = createBackendApi({
    ResolverStatus: vi.fn().mockResolvedValue("bulk-first"),
    ListCollection: vi.fn().mockResolvedValue([]),
    PreviewTextImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    PreviewCSVImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    CommitImport: vi.fn().mockResolvedValue(undefined),
    CompareDeck: vi.fn().mockResolvedValue({
      rows: [
        {
          board: "main",
          card: { oracleId: "bolt", name: "Lightning Bolt", scryfallURI: "" },
          needed: 4,
          owned: 2,
          missing: 2
        }
      ],
      unresolved: [],
      repairs: [],
      hasUnresolved: false
    }),
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
    FormatMissingDecklist: formatMissingDecklist
  });
  return { api, formatMissingDecklist };
}

describe("DeckComparePanel", () => {
  it("exports missing cards as txt after compare", async () => {
    const { api, formatMissingDecklist } = mockApi();
    const setMessage = vi.fn();
    const createElement = vi.spyOn(document, "createElement");

    render(<DeckComparePanel api={api} setMessage={setMessage} />);

    await userEvent.click(screen.getByRole("button", { name: "Run compare" }));
    await userEvent.click(screen.getByRole("button", { name: "Export missing (.txt)" }));

    expect(formatMissingDecklist).toHaveBeenCalledWith([
      expect.objectContaining({ missing: 2, card: expect.objectContaining({ name: "Lightning Bolt" }) })
    ]);
    expect(createElement).toHaveBeenCalledWith("a");
    expect(setMessage).toHaveBeenCalledWith("Exported missing cards.");

    createElement.mockRestore();
  });
});
