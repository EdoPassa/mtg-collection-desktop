import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BackendApi } from "../backend";
import { DeckAnalysisPanel } from "./DeckAnalysisPanel";

afterEach(() => cleanup());

function fakeApi(): BackendApi {
  return {
    ResolverStatus: vi.fn().mockResolvedValue("api-only"),
    ListCollection: vi.fn().mockResolvedValue([]),
    PreviewTextImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    PreviewCSVImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    CommitImport: vi.fn().mockResolvedValue(undefined),
    CompareDeck: vi.fn().mockResolvedValue({ rows: [], unresolved: [], repairs: [], hasUnresolved: false }),
    BuildDeckFromCompare: vi.fn().mockResolvedValue(1),
    ListDecks: vi.fn().mockResolvedValue([{ id: 1, name: "Burn" }]),
    ListDeckCards: vi.fn().mockResolvedValue([
      {
        card: { oracleId: "oracle-bolt", name: "Lightning Bolt", scryfallUri: "https://example.test/bolt", typeLine: "Instant" },
        quantity: 4
      },
      {
        card: { oracleId: "oracle-mountain", name: "Mountain", scryfallUri: "https://example.test/mountain", typeLine: "Basic Land — Mountain" },
        quantity: 20
      }
    ]),
    DeleteDeck: vi.fn().mockResolvedValue(undefined),
    RenameDeck: vi.fn().mockResolvedValue(undefined),
    SetDeckCardQuantity: vi.fn().mockResolvedValue(undefined),
    AddCardToDeckByName: vi.fn().mockResolvedValue(undefined),
    LendCard: vi.fn().mockResolvedValue(undefined),
    ListLentCards: vi.fn().mockResolvedValue([]),
    ReturnCard: vi.fn().mockResolvedValue(undefined),
    RepairCompareMismatches: vi.fn().mockResolvedValue(undefined)
  };
}

describe("DeckAnalysisPanel", () => {
  it("shows draw odds after selecting a deck and card", async () => {
    render(<DeckAnalysisPanel api={fakeApi()} setMessage={vi.fn()} />);

    await userEvent.click(await screen.findByRole("button", { name: "Burn" }));
    const cardResult = await screen.findByText(/Chance to draw at least 1/);
    expect(cardResult.querySelector("strong")?.textContent).toMatch(/\d+\.\d%|<0\.1%|99\.9%/);
  });

  it("auto-fills land count from type_line and computes land odds", async () => {
    render(<DeckAnalysisPanel api={fakeApi()} setMessage={vi.fn()} />);

    await userEvent.click(await screen.findByRole("button", { name: "Burn" }));

    expect(await screen.findByText("Lands detected")).toBeInTheDocument();
    expect(screen.getByLabelText("Lands in deck")).toHaveValue(20);

    const landResult = await screen.findByText(/Chance for at least 2 land/);
    expect(landResult.querySelector("strong")?.textContent).toMatch(/\d+\.\d%|<0\.1%|99\.9%/);
  });
});
