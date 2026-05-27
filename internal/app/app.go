// Package app exposes collection operations to the Wails frontend.
// Each method delegates to collection.Service with a background context.
package app

import (
	"context"

	"github.com/wailsapp/wails/v2/pkg/runtime"

	"mtgcollection/internal/analysis"
	"mtgcollection/internal/cards"
	"mtgcollection/internal/collection"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/storage"
)

// ImportProgressEvent is emitted during PreviewTextImport / PreviewCSVImport validate.
const ImportProgressEvent = "import:progress"

type App struct {
	ctx            context.Context
	service        *collection.Service
	resolverStatus string
	sessions       *analysis.SessionStore
}

// Startup stores the Wails context for runtime APIs such as EventsEmit.
func (a *App) Startup(ctx context.Context) {
	a.ctx = ctx
}

func New(store collection.Store, cardResolver resolver.Resolver, resolverStatus string, oracleIndex resolver.BulkOracleIndex) *App {
	return &App{service: collection.New(store, cardResolver, oracleIndex), resolverStatus: resolverStatus}
}

func (a *App) ResolverStatus() string {
	return a.resolverStatus
}

func (a *App) PreviewTextImport(text string) (collection.ImportPreview, error) {
	return a.service.PreviewTextImport(context.Background(), text, a.emitImportProgress)
}

func (a *App) PreviewCSVImport(data []byte) (collection.ImportPreview, error) {
	return a.service.PreviewCSVImport(context.Background(), data, a.emitImportProgress)
}

func (a *App) emitImportProgress(p collection.ImportProgress) {
	if a.ctx == nil {
		return
	}
	runtime.EventsEmit(a.ctx, ImportProgressEvent, p)
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

func (a *App) SetDeckCardQuantity(deckID int64, oracleID string, board string, qty int) error {
	return a.service.SetDeckCardQuantity(context.Background(), deckID, oracleID, board, qty)
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

func (a *App) FormatMissingDecklist(rows []collection.DeckCompareRow) string {
	return collection.FormatMissingDecklist(rows)
}
