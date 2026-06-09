package collection

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"mtgcollection/internal/cards"
	"mtgcollection/internal/importer"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/scryfall"
	"mtgcollection/internal/storage"
)

type fakeResolver struct {
	cards map[string]scryfall.Card
}

func (f fakeResolver) ResolveLine(ctx context.Context, line importer.ImportLine) (resolver.Result, error) {
	if line.ScryfallID != "" {
		if card, ok := f.cards[line.ScryfallID]; ok {
			return resolver.Result{Card: card, Source: "bulk"}, nil
		}
	}
	return f.ResolveName(ctx, line.Name)
}

func (f fakeResolver) ResolveName(_ context.Context, name string) (resolver.Result, error) {
	card := f.cards[name]
	return resolver.Result{Card: card, Source: "bulk"}, nil
}

func (f fakeResolver) ResolveScryfallID(_ context.Context, id string) (resolver.Result, error) {
	card := f.cards[id]
	return resolver.Result{Card: card, Source: "bulk"}, nil
}

func TestImportPreviewJSONNeverUsesNullSlices(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "not a valid line\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Validated == nil || preview.Unresolved == nil {
		t.Fatalf("preview slices must be non-nil: %#v", preview)
	}
	data, err := json.Marshal(preview)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"validated":null`) || strings.Contains(string(data), `"unresolved":null`) {
		t.Fatalf("import preview JSON must use empty arrays, got %s", data)
	}
}

func TestPreviewTextImportReportsMonotonicProgress(t *testing.T) {
	service := newTestService(t)
	var progress []ImportProgress
	_, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\n2 Counterspell\n4 Lightning Bolt\n", func(p ImportProgress) {
		progress = append(progress, p)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != 3 {
		t.Fatalf("progress callbacks = %d, want 3 (one per line)", len(progress))
	}
	for i, p := range progress {
		wantCurrent := i + 1
		if p.Current != wantCurrent || p.Total != 3 {
			t.Fatalf("progress[%d] = %#v, want current %d total 3", i, p, wantCurrent)
		}
		if p.Current < (i) {
			t.Fatalf("progress not monotonic at index %d: %#v", i, p)
		}
	}
	if progress[0].Label != "Lightning Bolt" {
		t.Fatalf("first label = %q, want Lightning Bolt", progress[0].Label)
	}
}

func TestPreviewCSVImportReturnsNonNilSlices(t *testing.T) {
	service := newTestService(t)
	csv := []byte("name,quantity\nLightning Bolt,4\n")
	preview, err := service.PreviewCSVImport(t.Context(), csv, nil)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Validated == nil || preview.Unresolved == nil {
		t.Fatalf("csv preview slices must be non-nil: %#v", preview)
	}
	if len(preview.Validated) != 1 {
		t.Fatalf("validated = %#v, want one row", preview.Validated)
	}
}

func TestPreviewAndCommitImportIncrementCollection(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\nBad line\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Validated) != 1 || len(preview.Unresolved) != 1 {
		t.Fatalf("preview = %#v, want one valid and one unresolved", preview)
	}

	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}
	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Quantity != 4 || rows[0].Card.Name != "Lightning Bolt" {
		t.Fatalf("collection = %#v, want 4 Lightning Bolt", rows)
	}
}

func TestListCollectionReturnsEmptySliceForEmptyCollection(t *testing.T) {
	service := newTestService(t)

	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil {
		t.Fatal("ListCollection returned nil, want empty slice for frontend arrays")
	}
	if len(rows) != 0 {
		t.Fatalf("collection = %#v, want empty collection", rows)
	}
}

func TestCompareBuildDeckAndLendingUseCases(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\n2 Counterspell\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}

	compare, err := service.CompareDeck(t.Context(), "4 Lightning Bolt\n1 Counterspell\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(compare.Rows) != 2 || compare.HasUnresolved || compare.Rows[0].Missing != 0 {
		t.Fatalf("compare = %#v, want complete deck", compare)
	}
	deckID, err := service.BuildDeckFromCompare(t.Context(), BuildDeckInput{Name: "Burn", Rows: compare.Rows})
	if err != nil {
		t.Fatal(err)
	}
	decks, err := service.ListDecks(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(decks) != 1 || decks[0].ID != deckID {
		t.Fatalf("decks = %#v, want built deck", decks)
	}
	if err := service.LendCard(t.Context(), storage.LendInput{OracleID: "oracle-bolt", Quantity: 1, BorrowerName: "Alice", LentDate: "2026-05-20"}); err != nil {
		t.Fatal(err)
	}
	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Available != rows[0].Quantity-rows[0].LentQty {
		t.Fatalf("availability not derived from lending: %#v", rows[0])
	}
}

func TestAddCardToDeckByName(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}
	compare, err := service.CompareDeck(t.Context(), "4 Lightning Bolt\n")
	if err != nil {
		t.Fatal(err)
	}
	deckID, err := service.BuildDeckFromCompare(t.Context(), BuildDeckInput{Name: "Burn", Rows: compare.Rows})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AddCardToDeckByName(t.Context(), deckID, "Counterspell", 2); err != nil {
		t.Fatal(err)
	}
	deckCards, err := service.ListDeckCards(t.Context(), deckID)
	if err != nil {
		t.Fatal(err)
	}
	if len(deckCards) != 2 {
		t.Fatalf("deck cards = %#v, want bolt and counterspell", deckCards)
	}
	var counterQty int
	for _, row := range deckCards {
		if row.Card.Name == "Counterspell" {
			counterQty = row.Quantity
		}
	}
	if counterQty != 2 {
		t.Fatalf("counterspell qty = %d, want 2", counterQty)
	}
}

func TestListCollectionDoesNotReportNegativeAvailability(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "1 Lightning Bolt\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}
	if err := service.LendCard(t.Context(), storage.LendInput{OracleID: "oracle-bolt", Quantity: 2, BorrowerName: "Alice", LentDate: "2026-05-20"}); err != nil {
		t.Fatal(err)
	}

	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("collection = %#v, want one row", rows)
	}
	if rows[0].Available != 0 {
		t.Fatalf("available = %d, want capped zero when lent exceeds owned", rows[0].Available)
	}
}

func TestCompareDeckDoesNotUseAmbiguousNameFallbackAsOwnedQuantity(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	oldOne := cards.CardIdentity{OracleID: "old-shock-one", Name: "Shock", ScryfallURI: "https://example.test/old-one"}
	oldTwo := cards.CardIdentity{OracleID: "old-shock-two", Name: "Shock", ScryfallURI: "https://example.test/old-two"}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{oldOne, oldTwo}); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollectionBatch(t.Context(), []storage.QuantityChange{
		{OracleID: oldOne.OracleID, Quantity: 2},
		{OracleID: oldTwo.OracleID, Quantity: 1},
	}); err != nil {
		t.Fatal(err)
	}
	service := New(store, fakeResolver{cards: map[string]scryfall.Card{
		"Shock": {OracleID: "new-shock", Name: "Shock", ScryfallURI: "https://example.test/new"},
	}}, resolver.BulkOracleIndex{})

	compare, err := service.CompareDeck(t.Context(), "1 Shock")
	if err != nil {
		t.Fatal(err)
	}
	if len(compare.Rows) != 1 {
		t.Fatalf("compare rows = %#v, want one row", compare.Rows)
	}
	row := compare.Rows[0]
	if row.Owned != 0 || row.Missing != 1 {
		t.Fatalf("ambiguous fallback row = %#v, want owned 0 and missing 1", row)
	}
	if !compare.HasUnresolved || len(compare.Unresolved) == 0 {
		t.Fatalf("compare = %#v, want unresolved ambiguous fallback warning", compare)
	}
	if len(compare.Repairs) != 0 {
		t.Fatalf("repairs = %#v, want no automatic repair for ambiguous fallback", compare.Repairs)
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	res := fakeResolver{cards: map[string]scryfall.Card{
		"Lightning Bolt": {OracleID: "oracle-bolt", Name: "Lightning Bolt", ScryfallURI: "https://example.test/bolt"},
		"Counterspell":   {OracleID: "oracle-counterspell", Name: "Counterspell", ScryfallURI: "https://example.test/counterspell"},
	}}
	_ = cards.CardIdentity{}
	return New(store, res, resolver.BulkOracleIndex{})
}

func TestCompareDeckPreservesSideboardRows(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\n2 Counterspell\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}

	compare, err := service.CompareDeck(t.Context(), "4 Lightning Bolt\n\nSideboard\n2 Counterspell\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(compare.Rows) != 2 {
		t.Fatalf("compare rows = %#v, want main bolt and side counterspell", compare.Rows)
	}
	byBoard := map[string]DeckCompareRow{}
	for _, row := range compare.Rows {
		byBoard[row.Board] = row
	}
	if byBoard[cards.BoardMain].Needed != 4 || byBoard[cards.BoardSide].Needed != 2 {
		t.Fatalf("compare by board = %#v", byBoard)
	}
	deckID, err := service.BuildDeckFromCompare(t.Context(), BuildDeckInput{Name: "Burn", Rows: compare.Rows})
	if err != nil {
		t.Fatal(err)
	}
	deckCards, err := service.ListDeckCards(t.Context(), deckID)
	if err != nil {
		t.Fatal(err)
	}
	var mainQty, sideQty int
	for _, row := range deckCards {
		if row.Card.Name == "Lightning Bolt" && row.Board == cards.BoardMain {
			mainQty = row.Quantity
		}
		if row.Card.Name == "Counterspell" && row.Board == cards.BoardSide {
			sideQty = row.Quantity
		}
	}
	if mainQty != 4 || sideQty != 2 {
		t.Fatalf("deck cards = %#v, want 4 bolt main and 2 counterspell side", deckCards)
	}
}

func TestListDeckCardsEnrichesTypeLineFromBulkIndex(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	index, err := resolver.BuildBulkOracleIndex(filepath.Join("..", "..", "testdata", "scryfall", "oracle_cards.json"))
	if err != nil {
		t.Fatal(err)
	}
	res := fakeResolver{cards: map[string]scryfall.Card{
		"Lightning Bolt": {OracleID: "oracle-lightning-bolt", Name: "Lightning Bolt", ScryfallURI: "https://example.test/bolt"},
		"Mountain":       {OracleID: "oracle-mountain", Name: "Mountain", ScryfallURI: "https://example.test/mountain"},
	}}
	service := New(store, res, index)

	deckID, err := store.CreateDeck(t.Context(), "Burn")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddCardToDeck(t.Context(), deckID, cards.CardIdentity{OracleID: "oracle-lightning-bolt", Name: "Lightning Bolt", ScryfallURI: "https://example.test/bolt"}, cards.BoardMain, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.AddCardToDeck(t.Context(), deckID, cards.CardIdentity{OracleID: "oracle-mountain", Name: "Mountain", ScryfallURI: "https://example.test/mountain"}, cards.BoardMain, 20); err != nil {
		t.Fatal(err)
	}

	rows, err := service.ListDeckCards(t.Context(), deckID)
	if err != nil {
		t.Fatal(err)
	}
	byOracle := map[string]cards.DeckCard{}
	for _, row := range rows {
		byOracle[row.Card.OracleID] = row
	}
	if byOracle["oracle-lightning-bolt"].Card.TypeLine != "Instant" {
		t.Fatalf("bolt type_line = %q, want Instant", byOracle["oracle-lightning-bolt"].Card.TypeLine)
	}
	if byOracle["oracle-mountain"].Card.TypeLine != "Basic Land — Mountain" {
		t.Fatalf("mountain type_line = %q, want Basic Land — Mountain", byOracle["oracle-mountain"].Card.TypeLine)
	}
}

// The Collection view leans on bulk-indexed image URIs, mana cost, and color identity
// to render the gallery toggle and color filter. Persisted rows only carry oracle ID,
// name, and scryfall URI, so enrichment has to happen at read time.
func TestListCollectionEnrichesImagesAndColorFromBulkIndex(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	index, err := resolver.BuildBulkOracleIndex(filepath.Join("..", "..", "testdata", "scryfall", "oracle_cards.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertCards(t.Context(), []cards.CardIdentity{
		{OracleID: "oracle-lightning-bolt", Name: "Lightning Bolt", ScryfallURI: "https://scryfall.test/bolt"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollectionBatch(t.Context(), []storage.QuantityChange{
		{OracleID: "oracle-lightning-bolt", Quantity: 4},
	}); err != nil {
		t.Fatal(err)
	}
	service := New(store, fakeResolver{}, index)

	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %#v, want one row", rows)
	}
	got := rows[0].Card
	if got.TypeLine != "Instant" || got.ManaCost != "{R}" {
		t.Fatalf("enriched card = %#v, want type Instant cost {R}", got)
	}
	if len(got.ColorIdentity) != 1 || got.ColorIdentity[0] != "R" {
		t.Fatalf("color_identity = %#v, want [R]", got.ColorIdentity)
	}
	if got.ImageSmall == "" || got.ImageNormal == "" {
		t.Fatalf("image URIs not enriched: %#v", got)
	}
}

// API-only mode (no bulk index) must still return collection rows. The new metadata
// fields are simply left empty and the frontend degrades gracefully.
func TestListCollectionWithoutBulkIndexLeavesEnrichmentFieldsEmpty(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "1 Lightning Bolt\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}
	rows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %#v, want one row", rows)
	}
	got := rows[0].Card
	if got.ManaCost != "" || got.ImageSmall != "" || got.ImageNormal != "" || len(got.ColorIdentity) != 0 {
		t.Fatalf("API-only enrichment leaked: %#v", got)
	}
}

func TestCollectionFolderWorkflow(t *testing.T) {
	service := newTestService(t)
	preview, err := service.PreviewTextImport(t.Context(), "4 Lightning Bolt\n", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.CommitImport(t.Context(), preview.Validated); err != nil {
		t.Fatal(err)
	}

	folderID, err := service.CreateCollectionFolder(t.Context(), nil, "Trade Binder")
	if err != nil {
		t.Fatal(err)
	}
	oracleID := preview.Validated[0].OracleID
	if err := service.MoveCollectionCopies(t.Context(), oracleID, UnsortedFolderID, folderID, 2); err != nil {
		t.Fatal(err)
	}

	allRows, err := service.ListCollection(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if allRows[0].Quantity != 4 || allRows[0].AllocatedQty != 2 || allRows[0].UnassignedQty != 2 {
		t.Fatalf("all collection = %#v, want qty 4 allocated 2 unassigned 2", allRows[0])
	}

	folderRows, err := service.ListCollectionInFolder(t.Context(), folderID)
	if err != nil {
		t.Fatal(err)
	}
	if len(folderRows) != 1 || folderRows[0].Quantity != 2 {
		t.Fatalf("folder rows = %#v, want one row qty 2", folderRows)
	}

	unsortedRows, err := service.ListCollectionInFolder(t.Context(), UnsortedFolderID)
	if err != nil {
		t.Fatal(err)
	}
	if len(unsortedRows) != 1 || unsortedRows[0].Quantity != 2 {
		t.Fatalf("unsorted rows = %#v, want one row qty 2", unsortedRows)
	}

	folders, err := service.ListCollectionFolders(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if folders == nil {
		t.Fatal("ListCollectionFolders returned nil, want empty slice")
	}
	if len(folders) != 1 || folders[0].Name != "Trade Binder" {
		t.Fatalf("folders = %#v, want Trade Binder", folders)
	}
}
