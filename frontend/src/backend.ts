import * as wailsApi from "../wailsjs/go/app/App";
import type { analysis, cards, collection, storage } from "../wailsjs/go/models";
import { EventsOn } from "../wailsjs/runtime/runtime";

// Plain strips Go method stubs from Wails-generated model types for use in the frontend.
type Plain<T> = T extends Array<infer U>
  ? Plain<U>[]
  : T extends object
    ? { [K in keyof T as T[K] extends (...args: never[]) => unknown ? never : K]: Plain<T[K]> }
    : T;

export type CollectionItem = Plain<cards.CollectionItem>;
export type ImportPreview = Plain<collection.ImportPreview>;
export type ResolvedLine = Plain<collection.ResolvedLine>;
export type DeckCompareRow = Plain<collection.DeckCompareRow>;
export type DeckCompareResult = Plain<collection.DeckCompareResult>;
export type RepairCandidate = Plain<collection.RepairCandidate>;
export type BuildDeckInput = Plain<collection.BuildDeckInput>;
export type LendInput = Plain<storage.LendInput>;
export type LentCard = Plain<cards.LentCard>;
export type Deck = Plain<cards.Deck>;
export type DeckCard = Plain<cards.DeckCard>;
export type CollectionFolder = Plain<cards.CollectionFolder>;
export type FolderCard = Plain<cards.FolderCard>;
export type CollectionTag = Plain<cards.CollectionTag>;

/** Sentinel folder ID for unallocated copies (not a DB row). */
export const UnsortedFolderID = 0;

export type FormatTarget = Plain<analysis.FormatTarget>;
export type HypergeometricRequest = Plain<analysis.HypergeometricRequest>;
export type HypergeometricResult = Plain<analysis.HypergeometricResult>;
export type DeckDrawAnalysisRequest = Plain<analysis.DeckDrawAnalysisRequest>;
export type DeckDrawAnalysisResult = Plain<analysis.DeckDrawAnalysisResult>;
export type SimulationState = Plain<analysis.SimulationState>;
export type SimulationCard = Plain<analysis.SimulationCard>;
export type DrawStats = Plain<analysis.DrawStats>;

/** Matches app.ImportProgressEvent in Go. */
export const ImportProgressEvent = "import:progress";

export type WailsBindings = typeof wailsApi;

export type BackendApi = {
  ResolverStatus(): Promise<string>;
  ListFormatTargets(): Promise<FormatTarget[]>;
  ListCollection(): Promise<CollectionItem[]>;
  ListCollectionFolders(): Promise<CollectionFolder[]>;
  CreateCollectionFolder(parentID: number | null, name: string): Promise<number>;
  RenameCollectionFolder(folderID: number, name: string): Promise<void>;
  MoveCollectionFolder(folderID: number, newParentID: number | null): Promise<void>;
  DeleteCollectionFolder(folderID: number): Promise<void>;
  ListCollectionInFolder(folderID: number): Promise<FolderCard[]>;
  MoveCollectionCopies(oracleID: string, fromFolderID: number, toFolderID: number, quantity: number): Promise<void>;
  ListCollectionTags(): Promise<CollectionTag[]>;
  CreateCollectionTag(name: string, color: string): Promise<number>;
  RenameCollectionTag(tagID: number, name: string): Promise<void>;
  UpdateCollectionTagColor(tagID: number, color: string): Promise<void>;
  DeleteCollectionTag(tagID: number): Promise<void>;
  SetCardTags(oracleID: string, tagIDs: number[]): Promise<void>;
  PreviewTextImport(text: string): Promise<ImportPreview>;
  PreviewCSVImport(data: number[]): Promise<ImportPreview>;
  CommitImport(rows: ResolvedLine[]): Promise<void>;
  CompareDeck(text: string): Promise<DeckCompareResult>;
  BuildDeckFromCompare(input: BuildDeckInput): Promise<number>;
  ListDecks(): Promise<Deck[]>;
  ListDeckCards(deckID: number): Promise<DeckCard[]>;
  DeleteDeck(deckID: number): Promise<void>;
  RenameDeck(deckID: number, name: string): Promise<void>;
  SetDeckCardQuantity(deckID: number, oracleID: string, board: string, qty: number): Promise<void>;
  AddCardToDeckByName(deckID: number, name: string, qty: number): Promise<void>;
  LendCard(input: LendInput): Promise<void>;
  ListLentCards(includeReturned: boolean): Promise<LentCard[]>;
  ReturnCard(id: number, returnDate: string): Promise<void>;
  RepairCompareMismatches(repairs: RepairCandidate[]): Promise<void>;
  FormatMissingDecklist(rows: DeckCompareRow[]): Promise<string>;
  Hypergeometric(req: HypergeometricRequest): Promise<HypergeometricResult>;
  AnalyzeDeckDraw(req: DeckDrawAnalysisRequest): Promise<DeckDrawAnalysisResult>;
  StartDeckSimulation(
    deckID: number,
    formatTarget: string,
    oracleFocus: string,
    minLands: number
  ): Promise<SimulationState>;
  SimNewOpening(sessionID: string): Promise<SimulationState>;
  SimMulligan(sessionID: string): Promise<SimulationState>;
  SimPutOnBottom(sessionID: string, slotID: string): Promise<SimulationState>;
  SimDrawCard(sessionID: string): Promise<SimulationState>;
  SimSetOracleFocus(sessionID: string, oracleID: string): Promise<SimulationState>;
  EndDeckSimulation(sessionID: string): Promise<void>;
};

type WailsRuntime = {
  EventsOnMultiple?: (eventName: string, callback: (...data: unknown[]) => void, maxCallbacks: number) => () => void;
};

function wailsRuntime(): WailsRuntime | undefined {
  if (typeof window === "undefined") {
    return undefined;
  }
  return (window as Window & { runtime?: WailsRuntime }).runtime;
}

/** Subscribe to a Wails runtime event; no-op outside the desktop shell (e.g. Vitest). */
export function subscribeWailsEvent<T>(eventName: string, callback: (payload: T) => void): () => void {
  const runtime = wailsRuntime();
  if (!runtime?.EventsOnMultiple) {
    return () => {};
  }
  return EventsOn(eventName, (payload) => callback(payload as T));
}

export function createBackendApi(overrides: Partial<WailsBindings> = {}): BackendApi {
  const bindings = { ...wailsApi, ...overrides } as WailsBindings;
  return {
    ResolverStatus: bindings.ResolverStatus,
    ListFormatTargets: () => bindings.ListFormatTargets() as Promise<FormatTarget[]>,
    ListCollection: () => bindings.ListCollection() as Promise<CollectionItem[]>,
    ListCollectionFolders: () => bindings.ListCollectionFolders() as Promise<CollectionFolder[]>,
    CreateCollectionFolder: (parentID, name) => bindings.CreateCollectionFolder(parentID, name),
    RenameCollectionFolder: bindings.RenameCollectionFolder,
    MoveCollectionFolder: (folderID, newParentID) => bindings.MoveCollectionFolder(folderID, newParentID),
    DeleteCollectionFolder: bindings.DeleteCollectionFolder,
    ListCollectionInFolder: (folderID) => bindings.ListCollectionInFolder(folderID) as Promise<FolderCard[]>,
    MoveCollectionCopies: bindings.MoveCollectionCopies,
    ListCollectionTags: () => bindings.ListCollectionTags() as Promise<CollectionTag[]>,
    CreateCollectionTag: (name, color) => bindings.CreateCollectionTag(name, color),
    RenameCollectionTag: bindings.RenameCollectionTag,
    UpdateCollectionTagColor: bindings.UpdateCollectionTagColor,
    DeleteCollectionTag: bindings.DeleteCollectionTag,
    SetCardTags: (oracleID, tagIDs) => bindings.SetCardTags(oracleID, tagIDs),
    PreviewTextImport: (text) => bindings.PreviewTextImport(text) as Promise<ImportPreview>,
    PreviewCSVImport: (data) => bindings.PreviewCSVImport(data) as Promise<ImportPreview>,
    CommitImport: (rows) => bindings.CommitImport(rows as collection.ResolvedLine[]),
    CompareDeck: (text) => bindings.CompareDeck(text) as Promise<DeckCompareResult>,
    BuildDeckFromCompare: (input) => bindings.BuildDeckFromCompare(input as collection.BuildDeckInput),
    ListDecks: () => bindings.ListDecks() as Promise<Deck[]>,
    ListDeckCards: (deckID) => bindings.ListDeckCards(deckID) as Promise<DeckCard[]>,
    DeleteDeck: bindings.DeleteDeck,
    RenameDeck: bindings.RenameDeck,
    SetDeckCardQuantity: bindings.SetDeckCardQuantity,
    AddCardToDeckByName: bindings.AddCardToDeckByName,
    LendCard: (input) => bindings.LendCard(input as storage.LendInput),
    ListLentCards: (includeReturned) => bindings.ListLentCards(includeReturned) as Promise<LentCard[]>,
    ReturnCard: bindings.ReturnCard,
    RepairCompareMismatches: (repairs) => bindings.RepairCompareMismatches(repairs as collection.RepairCandidate[]),
    FormatMissingDecklist: (rows) => bindings.FormatMissingDecklist(rows as collection.DeckCompareRow[]),
    Hypergeometric: (req) => bindings.Hypergeometric(req as analysis.HypergeometricRequest) as Promise<HypergeometricResult>,
    AnalyzeDeckDraw: (req) =>
      bindings.AnalyzeDeckDraw(req as analysis.DeckDrawAnalysisRequest) as Promise<DeckDrawAnalysisResult>,
    StartDeckSimulation: (deckID, formatTarget, oracleFocus, minLands) =>
      bindings.StartDeckSimulation(deckID, formatTarget, oracleFocus, minLands) as Promise<SimulationState>,
    SimNewOpening: (sessionID) => bindings.SimNewOpening(sessionID) as Promise<SimulationState>,
    SimMulligan: (sessionID) => bindings.SimMulligan(sessionID) as Promise<SimulationState>,
    SimPutOnBottom: (sessionID, slotID) => bindings.SimPutOnBottom(sessionID, slotID) as Promise<SimulationState>,
    SimDrawCard: (sessionID) => bindings.SimDrawCard(sessionID) as Promise<SimulationState>,
    SimSetOracleFocus: (sessionID, oracleID) =>
      bindings.SimSetOracleFocus(sessionID, oracleID) as Promise<SimulationState>,
    EndDeckSimulation: bindings.EndDeckSimulation
  };
}
