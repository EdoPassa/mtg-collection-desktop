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

func (a *App) ListCollectionFolders() ([]cards.CollectionFolder, error) {
	return a.service.ListCollectionFolders(context.Background())
}

func (a *App) CreateCollectionFolder(parentID *int64, name string) (int64, error) {
	return a.service.CreateCollectionFolder(context.Background(), parentID, name)
}

func (a *App) RenameCollectionFolder(folderID int64, name string) error {
	return a.service.RenameCollectionFolder(context.Background(), folderID, name)
}

func (a *App) MoveCollectionFolder(folderID int64, newParentID *int64) error {
	return a.service.MoveCollectionFolder(context.Background(), folderID, newParentID)
}

func (a *App) DeleteCollectionFolder(folderID int64) error {
	return a.service.DeleteCollectionFolder(context.Background(), folderID)
}

func (a *App) ListCollectionInFolder(folderID int64) ([]cards.FolderCard, error) {
	return a.service.ListCollectionInFolder(context.Background(), folderID)
}

func (a *App) MoveCollectionCopies(oracleID string, fromFolderID, toFolderID int64, quantity int) error {
	return a.service.MoveCollectionCopies(context.Background(), oracleID, fromFolderID, toFolderID, quantity)
}

func (a *App) ListCollectionTags() ([]cards.CollectionTag, error) {
	return a.service.ListCollectionTags(context.Background())
}

func (a *App) CreateCollectionTag(name, color string) (int64, error) {
	return a.service.CreateCollectionTag(context.Background(), name, color)
}

func (a *App) RenameCollectionTag(tagID int64, name string) error {
	return a.service.RenameCollectionTag(context.Background(), tagID, name)
}

func (a *App) UpdateCollectionTagColor(tagID int64, color string) error {
	return a.service.UpdateCollectionTagColor(context.Background(), tagID, color)
}

func (a *App) DeleteCollectionTag(tagID int64) error {
	return a.service.DeleteCollectionTag(context.Background(), tagID)
}

func (a *App) SetCardTags(oracleID string, tagIDs []int64) error {
	return a.service.SetCardTags(context.Background(), oracleID, tagIDs)
}
