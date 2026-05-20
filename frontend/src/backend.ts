import * as wailsApi from "../wailsjs/go/app/App";
import type { cards, collection, storage } from "../wailsjs/go/models";

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

export type WailsBindings = typeof wailsApi;

export type BackendApi = {
  ResolverStatus(): Promise<string>;
  ListCollection(): Promise<CollectionItem[]>;
  PreviewTextImport(text: string): Promise<ImportPreview>;
  PreviewCSVImport(data: number[]): Promise<ImportPreview>;
  CommitImport(rows: ResolvedLine[]): Promise<void>;
  CompareDeck(text: string): Promise<DeckCompareResult>;
  BuildDeckFromCompare(input: BuildDeckInput): Promise<number>;
  ListDecks(): Promise<Deck[]>;
  LendCard(input: LendInput): Promise<void>;
  ListLentCards(includeReturned: boolean): Promise<LentCard[]>;
  ReturnCard(id: number, returnDate: string): Promise<void>;
  RepairCompareMismatches(repairs: RepairCandidate[]): Promise<void>;
};

export function createBackendApi(bindings: WailsBindings = wailsApi): BackendApi {
  return {
    ResolverStatus: bindings.ResolverStatus,
    ListCollection: () => bindings.ListCollection() as Promise<CollectionItem[]>,
    PreviewTextImport: (text) => bindings.PreviewTextImport(text) as Promise<ImportPreview>,
    PreviewCSVImport: (data) => bindings.PreviewCSVImport(data) as Promise<ImportPreview>,
    CommitImport: (rows) => bindings.CommitImport(rows as collection.ResolvedLine[]),
    CompareDeck: (text) => bindings.CompareDeck(text) as Promise<DeckCompareResult>,
    BuildDeckFromCompare: (input) => bindings.BuildDeckFromCompare(input as collection.BuildDeckInput),
    ListDecks: () => bindings.ListDecks() as Promise<Deck[]>,
    LendCard: (input) => bindings.LendCard(input as storage.LendInput),
    ListLentCards: (includeReturned) => bindings.ListLentCards(includeReturned) as Promise<LentCard[]>,
    ReturnCard: bindings.ReturnCard,
    RepairCompareMismatches: (repairs) => bindings.RepairCompareMismatches(repairs as collection.RepairCandidate[])
  };
}
