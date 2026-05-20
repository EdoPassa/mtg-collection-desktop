package app

import (
	"context"

	"mtgcollection/internal/cards"
	"mtgcollection/internal/collection"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/storage"
)

type App struct {
	service        *collection.Service
	resolverStatus string
}

func New(store collection.Store, cardResolver resolver.Resolver, resolverStatus string) *App {
	return &App{service: collection.New(store, cardResolver), resolverStatus: resolverStatus}
}

func (a *App) ResolverStatus() string {
	return a.resolverStatus
}

func (a *App) PreviewTextImport(text string) (collection.ImportPreview, error) {
	return a.service.PreviewTextImport(context.Background(), text)
}

func (a *App) PreviewCSVImport(data []byte) (collection.ImportPreview, error) {
	return a.service.PreviewCSVImport(context.Background(), data)
}

func (a *App) CommitImport(rows []collection.ResolvedLine) error {
	return a.service.CommitImport(context.Background(), rows)
}

func (a *App) ListCollection() ([]cards.CollectionItem, error) {
	return a.service.ListCollection(context.Background())
}

func (a *App) CompareDeck(deckText string) (collection.DeckCompareResult, error) {
	return a.service.CompareDeck(context.Background(), deckText)
}

func (a *App) RepairCompareMismatches(repairs []collection.RepairCandidate) error {
	return a.service.RepairCompareMismatches(context.Background(), repairs)
}

func (a *App) BuildDeckFromCompare(input collection.BuildDeckInput) (int64, error) {
	return a.service.BuildDeckFromCompare(context.Background(), input)
}

func (a *App) ListDecks() ([]cards.Deck, error) {
	return a.service.ListDecks(context.Background())
}

func (a *App) ListDeckCards(deckID int64) ([]cards.DeckCard, error) {
	return a.service.ListDeckCards(context.Background(), deckID)
}

func (a *App) DeleteDeck(deckID int64) error {
	return a.service.DeleteDeck(context.Background(), deckID)
}

func (a *App) RenameDeck(deckID int64, name string) error {
	return a.service.RenameDeck(context.Background(), deckID, name)
}

func (a *App) SetDeckCardQuantity(deckID int64, oracleID string, qty int) error {
	return a.service.SetDeckCardQuantity(context.Background(), deckID, oracleID, qty)
}

func (a *App) AddCardToDeckByName(deckID int64, name string, qty int) error {
	return a.service.AddCardToDeckByName(context.Background(), deckID, name, qty)
}

func (a *App) LendCard(input storage.LendInput) error {
	return a.service.LendCard(context.Background(), input)
}

func (a *App) ReturnCard(lentID int64, returnDate string) error {
	return a.service.ReturnCard(context.Background(), lentID, returnDate)
}

func (a *App) ListLentCards(includeReturned bool) ([]cards.LentCard, error) {
	return a.service.ListLentCards(context.Background(), includeReturned)
}
