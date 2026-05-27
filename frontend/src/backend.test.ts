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
    FormatMissingDecklist: vi.fn().mockResolvedValue(""),
    Hypergeometric: vi.fn().mockResolvedValue({ probability: 0.5, probabilityFormatted: "50.0%" }),
    AnalyzeDeckDraw: vi.fn().mockResolvedValue({
      populationN: 60,
      deckTotal: 24,
      targetSize: 60,
      detectedLands: 20,
      effectiveLandsK: 20,
      effectiveSampleSize: 7,
      cardProbability: 0.5,
      cardProbabilityFormatted: "50.0%",
      landProbability: 0.8,
      landProbabilityFormatted: "80.0%"
    }),
    StartDeckSimulation: vi.fn().mockResolvedValue({
      sessionId: "sim-1",
      phase: "playing",
      hand: [],
      libraryCount: 53,
      mulliganCount: 0,
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
    SimNewOpening: vi.fn(),
    SimMulligan: vi.fn(),
    SimPutOnBottom: vi.fn(),
    SimDrawCard: vi.fn(),
    SimSetOracleFocus: vi.fn(),
    EndDeckSimulation: vi.fn().mockResolvedValue(undefined)
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
