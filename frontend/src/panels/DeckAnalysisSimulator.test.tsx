import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BackendApi } from "../backend";
import { DeckAnalysisSimulator } from "./DeckAnalysisSimulator";

afterEach(() => cleanup());

const mainboard = [
  {
    card: {
      oracleId: "oracle-bolt",
      name: "Lightning Bolt",
      scryfallUri: "https://example.test/bolt",
      typeLine: "Instant"
    },
    quantity: 4
  }
];

function fakeApi(overrides: Partial<BackendApi> = {}): BackendApi {
  return {
    ResolverStatus: vi.fn(),
    ListCollection: vi.fn(),
    PreviewTextImport: vi.fn(),
    PreviewCSVImport: vi.fn(),
    CommitImport: vi.fn(),
    CompareDeck: vi.fn(),
    BuildDeckFromCompare: vi.fn(),
    ListDecks: vi.fn(),
    ListDeckCards: vi.fn(),
    DeleteDeck: vi.fn(),
    RenameDeck: vi.fn(),
    SetDeckCardQuantity: vi.fn(),
    AddCardToDeckByName: vi.fn(),
    LendCard: vi.fn(),
    ListLentCards: vi.fn(),
    ReturnCard: vi.fn(),
    RepairCompareMismatches: vi.fn(),
    FormatMissingDecklist: vi.fn(),
    Hypergeometric: vi.fn(),
    AnalyzeDeckDraw: vi.fn(),
    StartDeckSimulation: vi.fn().mockResolvedValue({
      sessionId: "sim-1",
      phase: "awaiting_bottom",
      hand: [{ slotId: "s1", oracleId: "oracle-bolt", name: "Lightning Bolt", isLand: false }],
      libraryCount: 53,
      mulliganCount: 1,
      canMulligan: false,
      canDraw: false,
      stats: {
        landsInHand: 0,
        libraryRemaining: 53,
        nextDrawLandProb: 0.4,
        nextDrawLandProbFormatted: "40.0%",
        nextDrawCardProb: 0,
        nextDrawCardProbFormatted: "0.0%",
        afterOneDrawLandsProb: 0.5,
        afterOneDrawLandsProbFormatted: "50.0%",
        minLandsThreshold: 2
      },
      deckId: 1,
      formatTarget: "standard"
    }),
    SimNewOpening: vi.fn(),
    SimMulligan: vi.fn(),
    SimPutOnBottom: vi.fn().mockResolvedValue({
      sessionId: "sim-1",
      phase: "playing",
      hand: [],
      libraryCount: 53,
      mulliganCount: 1,
      canMulligan: true,
      canDraw: true,
      stats: {
        landsInHand: 0,
        libraryRemaining: 53,
        nextDrawLandProb: 0.4,
        nextDrawLandProbFormatted: "40.0%",
        nextDrawCardProb: 0,
        nextDrawCardProbFormatted: "0.0%",
        afterOneDrawLandsProb: 0.5,
        afterOneDrawLandsProbFormatted: "50.0%",
        minLandsThreshold: 2
      },
      deckId: 1,
      formatTarget: "standard"
    }),
    SimDrawCard: vi.fn(),
    SimSetOracleFocus: vi.fn().mockImplementation((_id: string) =>
      Promise.resolve({
        sessionId: "sim-1",
        phase: "awaiting_bottom",
        hand: [{ slotId: "s1", oracleId: "oracle-bolt", name: "Lightning Bolt", isLand: false }],
        libraryCount: 53,
        mulliganCount: 1,
        canMulligan: false,
        canDraw: false,
        stats: {
          landsInHand: 0,
          libraryRemaining: 53,
          nextDrawLandProb: 0.4,
          nextDrawLandProbFormatted: "40.0%",
          nextDrawCardProb: 0,
          nextDrawCardProbFormatted: "0.0%",
          oracleIdUsed: "oracle-bolt",
          afterOneDrawLandsProb: 0.5,
          afterOneDrawLandsProbFormatted: "50.0%",
          minLandsThreshold: 2
        },
        deckId: 1,
        formatTarget: "standard"
      })
    ),
    EndDeckSimulation: vi.fn().mockResolvedValue(undefined),
    ...overrides
  };
}

describe("DeckAnalysisSimulator", () => {
  it("puts a card on bottom when awaiting bottom phase", async () => {
    const api = fakeApi();
    render(
      <DeckAnalysisSimulator
        api={api}
        setMessage={vi.fn()}
        deckId={1}
        formatTarget="standard"
        mainboardCards={mainboard}
        selectedOracleId="oracle-bolt"
        onOracleChange={vi.fn()}
      />
    );

    await screen.findByText(/put it on the bottom/i);
    const bottomButton = await screen.findByRole("button", { name: /Put Lightning Bolt on bottom/i });
    await waitFor(() => expect(bottomButton).not.toBeDisabled());
    await userEvent.click(bottomButton);

    await waitFor(() => expect(api.SimPutOnBottom).toHaveBeenCalledWith("sim-1", "s1"));
  });
});
