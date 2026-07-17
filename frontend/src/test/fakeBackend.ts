import { vi } from "vitest";
import { createBackendApi, type BackendApi, type SimulationState, type WailsBindings } from "../backend";

const defaultFormatTargets = [
  { id: "standard", label: "60-card", size: 60 },
  { id: "commander", label: "100-card", size: 100 }
] as const;

const defaultSimulationState: SimulationState = {
  sessionId: "sim-test",
  phase: "playing",
  hand: [],
  libraryCount: 53,
  mulliganCount: 0,
  canMulligan: true,
  canDraw: true,
  stats: {
    landsInHand: 0,
    libraryRemaining: 53,
    nextDrawLandProb: 0,
    nextDrawLandProbFormatted: "—",
    nextDrawCardProb: 0,
    nextDrawCardProbFormatted: "—",
    nextDrawTagProb: 0,
    nextDrawTagProbFormatted: "—",
    afterOneDrawLandsProb: 0,
    afterOneDrawLandsProbFormatted: "—",
    minLandsThreshold: 2
  },
  deckId: 1,
  formatTarget: "standard"
};

/** Default Wails binding overrides for Vitest. Merge with createBackendApi for typed panel tests. */
export function defaultWailsOverrides(): Partial<WailsBindings> {
  return {
    ResolverStatus: vi.fn().mockResolvedValue("api-only"),
    ListFormatTargets: vi.fn().mockResolvedValue([...defaultFormatTargets]),
    ListCollection: vi.fn().mockResolvedValue([]),
    ListCollectionFolders: vi.fn().mockResolvedValue([]),
    CreateCollectionFolder: vi.fn().mockResolvedValue(1),
    RenameCollectionFolder: vi.fn().mockResolvedValue(undefined),
    MoveCollectionFolder: vi.fn().mockResolvedValue(undefined),
    DeleteCollectionFolder: vi.fn().mockResolvedValue(undefined),
    ListCollectionInFolder: vi.fn().mockResolvedValue([]),
    MoveCollectionCopies: vi.fn().mockResolvedValue(undefined),
    ListCollectionTags: vi.fn().mockResolvedValue([]),
    CreateCollectionTag: vi.fn().mockResolvedValue(1),
    RenameCollectionTag: vi.fn().mockResolvedValue(undefined),
    UpdateCollectionTagColor: vi.fn().mockResolvedValue(undefined),
    DeleteCollectionTag: vi.fn().mockResolvedValue(undefined),
    SetCardTags: vi.fn().mockResolvedValue(undefined),
    AddTagsToCards: vi.fn().mockResolvedValue(undefined),
    RemoveTagsFromCards: vi.fn().mockResolvedValue(undefined),
    DeleteCollectionCards: vi.fn().mockResolvedValue(undefined),
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
    Hypergeometric: vi.fn().mockResolvedValue({ probability: 0, probabilityFormatted: "0.0%" }),
    AnalyzeDeckDraw: vi.fn().mockResolvedValue({
      populationN: 60,
      deckTotal: 0,
      targetSize: 60,
      detectedLands: 0,
      effectiveLandsK: 0,
      effectiveSampleSize: 7,
      cardProbability: 0,
      cardProbabilityFormatted: "—",
      landProbability: 0,
      landProbabilityFormatted: "—"
    }),
    AnalyzeDeckTags: vi.fn().mockResolvedValue({
      populationN: 60,
      deckTotal: 0,
      tags: []
    }),
    StartDeckSimulation: vi.fn().mockResolvedValue(defaultSimulationState),
    SimNewOpening: vi.fn().mockResolvedValue(defaultSimulationState),
    SimMulligan: vi.fn().mockResolvedValue({ ...defaultSimulationState, phase: "awaiting_bottom" }),
    SimPutOnBottom: vi.fn().mockResolvedValue(defaultSimulationState),
    SimDrawCard: vi.fn().mockResolvedValue(defaultSimulationState),
    SimSetOracleFocus: vi.fn().mockResolvedValue(defaultSimulationState),
    SimSetTagFocus: vi.fn().mockResolvedValue(defaultSimulationState),
    EndDeckSimulation: vi.fn().mockResolvedValue(undefined)
  };
}

export function createFakeBindings(overrides: Partial<WailsBindings> = {}): WailsBindings {
  return { ...defaultWailsOverrides(), ...overrides } as WailsBindings;
}

export function createFakeBackendApi(overrides: Partial<WailsBindings> = {}): BackendApi {
  return createBackendApi(createFakeBindings(overrides));
}

/** API plus underlying Wails mocks for assertions (vi.mocked(bindings.ListDecks)). */
export function createFakeBackendTestKit(overrides: Partial<WailsBindings> = {}) {
  const bindings = createFakeBindings(overrides);
  return { api: createBackendApi(bindings), bindings };
}
