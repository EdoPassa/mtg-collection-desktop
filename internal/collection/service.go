// Package collection implements import preview/commit, deck comparison, and lending workflows.
package collection

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"sort"
	"strings"

	"mtgcollection/internal/cards"
	"mtgcollection/internal/importer"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/storage"
)

type Store interface {
	UpsertCards(ctx context.Context, cardRows []cards.CardIdentity) error
	IncrementCollectionBatch(ctx context.Context, items []storage.QuantityChange) error
	ListCollection(ctx context.Context) ([]cards.CollectionItem, error)
	CreateDeck(ctx context.Context, name string) (int64, error)
	ListDecks(ctx context.Context) ([]cards.Deck, error)
	ListDeckCards(ctx context.Context, deckID int64) ([]cards.DeckCard, error)
	DeleteDeck(ctx context.Context, deckID int64) error
	RenameDeck(ctx context.Context, deckID int64, name string) error
	SetDeckCardQuantity(ctx context.Context, deckID int64, oracleID string, board string, qty int) error
	AddCardToDeck(ctx context.Context, deckID int64, card cards.CardIdentity, board string, qty int) error
	ReplaceDeckCards(ctx context.Context, deckID int64, rows []cards.DeckCard) error
	GetOwnedByOracleID(ctx context.Context) (map[string]storage.OwnedCard, error)
	GetOwnedByNormalizedName(ctx context.Context) (map[string][]storage.NameOwnedCard, error)
	MoveCollectionQuantity(ctx context.Context, fromOracleID string, toCard cards.CardIdentity) error
	LendCard(ctx context.Context, input storage.LendInput) error
	ReturnCard(ctx context.Context, lentID int64, returnDate string) error
	ListLentCards(ctx context.Context, includeReturned bool) ([]cards.LentCard, error)
	GetLentSummaryByOracleID(ctx context.Context) (map[string]storage.LentSummary, error)
	CreateFolder(ctx context.Context, parentID *int64, name string) (int64, error)
	ListFolders(ctx context.Context) ([]cards.CollectionFolder, error)
	RenameFolder(ctx context.Context, folderID int64, name string) error
	MoveFolder(ctx context.Context, folderID int64, newParentID *int64) error
	DeleteFolder(ctx context.Context, folderID int64) error
	ListFolderCards(ctx context.Context, folderID int64) ([]cards.FolderCard, error)
	ListUnsortedCards(ctx context.Context) ([]cards.FolderCard, error)
	GetAllocatedByOracleID(ctx context.Context) (map[string]int, error)
	MoveCopies(ctx context.Context, oracleID string, fromFolderID, toFolderID int64, qty int) error
}

type Service struct {
	store        Store
	resolver     resolver.Resolver
	oracleIndex  resolver.BulkOracleIndex
}

type ResolvedLine struct {
	Line        importer.ImportLine `json:"line"`
	OracleID    string              `json:"oracleId"`
	Name        string              `json:"name"`
	ScryfallURI string              `json:"scryfallUri"`
	Source      string              `json:"source"`
}

type ImportPreview struct {
	Validated  []ResolvedLine `json:"validated"`
	Unresolved []string       `json:"unresolved"`
}

// ImportProgress reports validate/preview resolution progress to the UI.
type ImportProgress struct {
	Current int    `json:"current"`
	Total   int    `json:"total"`
	Label   string `json:"label"`
}

// ImportProgressReporter is called once per import line during preview.
type ImportProgressReporter func(ImportProgress)

type DeckCompareRow struct {
	Board   string             `json:"board,omitempty"`
	Card    cards.CardIdentity `json:"card"`
	Needed  int                `json:"needed"`
	Owned   int                `json:"owned"`
	Missing int                `json:"missing"`
}

type RepairCandidate struct {
	FromOracleID string             `json:"fromOracleId"`
	ToCard       cards.CardIdentity `json:"toCard"`
}

type DeckCompareResult struct {
	Rows          []DeckCompareRow  `json:"rows"`
	Unresolved    []string          `json:"unresolved"`
	Repairs       []RepairCandidate `json:"repairs"`
	HasUnresolved bool              `json:"hasUnresolved"`
}

type BuildDeckInput struct {
	Name          string            `json:"name"`
	ReplaceDeckID int64             `json:"replaceDeckId"`
	Rows          []DeckCompareRow  `json:"rows"`
}

func New(store Store, cardResolver resolver.Resolver, oracleIndex resolver.BulkOracleIndex) *Service {
	return &Service{store: store, resolver: cardResolver, oracleIndex: oracleIndex}
}

func (s *Service) PreviewTextImport(ctx context.Context, text string, report ImportProgressReporter) (ImportPreview, error) {
	lines, unresolved := importer.ParseText(text)
	return s.preview(ctx, lines, unresolved, report)
}

func (s *Service) PreviewCSVImport(ctx context.Context, data []byte, report ImportProgressReporter) (ImportPreview, error) {
	lines, unresolved := importer.ParseCSVBytes(data)
	return s.preview(ctx, lines, unresolved, report)
}

func (s *Service) CommitImport(ctx context.Context, rows []ResolvedLine) error {
	if rows == nil {
		rows = []ResolvedLine{}
	}
	cardsToUpsert := make([]cards.CardIdentity, 0, len(rows))
	changes := make([]storage.QuantityChange, 0, len(rows))
	for _, row := range rows {
		cardsToUpsert = append(cardsToUpsert, cards.CardIdentity{OracleID: row.OracleID, Name: row.Name, ScryfallURI: row.ScryfallURI})
		changes = append(changes, storage.QuantityChange{OracleID: row.OracleID, Quantity: row.Line.Quantity})
	}
	if err := s.store.UpsertCards(ctx, cardsToUpsert); err != nil {
		return err
	}
	return s.store.IncrementCollectionBatch(ctx, changes)
}

func (s *Service) ListCollection(ctx context.Context) ([]cards.CollectionItem, error) {
	rows, err := s.store.ListCollection(ctx)
	if err != nil {
		return nil, err
	}
	// Available = owned quantity minus copies currently lent out (not yet returned).
	lent, err := s.store.GetLentSummaryByOracleID(ctx)
	if err != nil {
		return nil, err
	}
	allocated, err := s.store.GetAllocatedByOracleID(ctx)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		summary := lent[rows[i].Card.OracleID]
		rows[i].LentQty = summary.TotalQuantity
		rows[i].Available = rows[i].Quantity - rows[i].LentQty
		if rows[i].Available < 0 {
			rows[i].Available = 0
		}
		rows[i].AllocatedQty = allocated[rows[i].Card.OracleID]
		rows[i].UnassignedQty = rows[i].Quantity - rows[i].AllocatedQty
		if rows[i].UnassignedQty < 0 {
			rows[i].UnassignedQty = 0
		}
		s.enrichCardFromBulk(&rows[i].Card)
	}
	return rows, nil
}

// enrichCardFromBulk fills metadata fields (type line, mana cost, color identity, image URIs)
// from the local Scryfall oracle bulk index, leaving values empty when the index is unavailable
// or the card is not present (API-only mode degrades gracefully in the UI).
func (s *Service) enrichCardFromBulk(card *cards.CardIdentity) {
	bulk, ok := s.oracleIndex.LookupOracleID(card.OracleID)
	if !ok {
		return
	}
	if card.TypeLine == "" {
		card.TypeLine = bulk.TypeLine
	}
	if card.ManaCost == "" {
		card.ManaCost = bulk.ManaCost
	}
	if len(card.ColorIdentity) == 0 {
		card.ColorIdentity = bulk.ColorIdentity
	}
	if card.ImageSmall == "" {
		card.ImageSmall = bulk.ImageSmall
	}
	if card.ImageNormal == "" {
		card.ImageNormal = bulk.ImageNormal
	}
}

// CompareDeck resolves a pasted decklist against the owned collection.
// When oracle IDs differ but the normalized name matches exactly one owned copy,
// a RepairCandidate is suggested so the user can merge stale collection rows.
func (s *Service) CompareDeck(ctx context.Context, deckText string) (DeckCompareResult, error) {
	lines, unresolved := importer.ParseDeckText(deckText)
	wanted := map[string]DeckCompareRow{}
	for _, line := range lines {
		result, err := s.resolver.ResolveName(ctx, line.Name)
		if err != nil {
			unresolved = append(unresolved, line.Raw+"  ->  "+err.Error())
			continue
		}
		card := cards.CardIdentity{OracleID: result.Card.OracleID, Name: result.Card.Name, ScryfallURI: result.Card.ScryfallURI}
		board := cards.NormalizeBoard(line.Board)
		key := card.OracleID + ":" + board
		row := wanted[key]
		row.Board = board
		row.Card = card
		row.Needed += line.Quantity
		wanted[key] = row
	}
	var resolvedCards []cards.CardIdentity
	for _, row := range wanted {
		resolvedCards = append(resolvedCards, row.Card)
	}
	if len(resolvedCards) > 0 {
		if err := s.store.UpsertCards(ctx, resolvedCards); err != nil {
			return DeckCompareResult{}, err
		}
	}
	ownedByOracle, err := s.store.GetOwnedByOracleID(ctx)
	if err != nil {
		return DeckCompareResult{}, err
	}
	ownedByName, err := s.store.GetOwnedByNormalizedName(ctx)
	if err != nil {
		return DeckCompareResult{}, err
	}
	var rows []DeckCompareRow
	var repairs []RepairCandidate
	for _, row := range wanted {
		if owned, ok := ownedByOracle[row.Card.OracleID]; ok {
			row.Owned = owned.Quantity
		}
		// Fallback: match by normalized name when the Scryfall oracle ID changed
		// (e.g. after re-importing with updated bulk data).
		if row.Owned <= 0 {
			candidates := ownedByName[cards.NormalizeName(row.Card.Name)]
			var positiveCandidates []storage.NameOwnedCard
			for _, candidate := range candidates {
				if candidate.Quantity > 0 {
					positiveCandidates = append(positiveCandidates, candidate)
				}
			}
			if len(positiveCandidates) == 1 {
				candidate := positiveCandidates[0]
				row.Owned = candidate.Quantity
				if candidate.OracleID != row.Card.OracleID {
					repairs = append(repairs, RepairCandidate{FromOracleID: candidate.OracleID, ToCard: row.Card})
					unresolved = append(unresolved, "Oracle ID mismatch for "+candidate.Name)
				}
			} else if len(positiveCandidates) > 1 {
				unresolved = append(unresolved, "Ambiguous owned cards named "+row.Card.Name)
			}
		}
		if row.Needed > row.Owned {
			row.Missing = row.Needed - row.Owned
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		return cards.NormalizeName(rows[i].Card.Name) < cards.NormalizeName(rows[j].Card.Name)
	})
	return DeckCompareResult{
		Rows:          nonNilSlice(rows),
		Unresolved:    nonNilSlice(unresolved),
		Repairs:       nonNilSlice(repairs),
		HasUnresolved: len(unresolved) > 0,
	}, nil
}

func (s *Service) RepairCompareMismatches(ctx context.Context, repairs []RepairCandidate) error {
	seen := map[string]bool{}
	for _, repair := range repairs {
		key := repair.FromOracleID + "->" + repair.ToCard.OracleID
		if seen[key] || repair.FromOracleID == repair.ToCard.OracleID {
			continue
		}
		seen[key] = true
		if err := s.store.MoveCollectionQuantity(ctx, repair.FromOracleID, repair.ToCard); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) BuildDeckFromCompare(ctx context.Context, input BuildDeckInput) (int64, error) {
	if len(input.Rows) == 0 {
		return 0, errors.New("no compare rows to build")
	}
	deckRows := make([]cards.DeckCard, 0, len(input.Rows))
	for _, row := range input.Rows {
		if row.Missing > 0 {
			return 0, errors.New("cannot build deck with missing cards")
		}
		deckRows = append(deckRows, cards.DeckCard{Card: row.Card, Quantity: row.Needed, Board: cards.NormalizeBoard(row.Board)})
	}
	deckID := input.ReplaceDeckID
	var err error
	if deckID == 0 {
		deckID, err = s.store.CreateDeck(ctx, input.Name)
		if err != nil {
			return 0, err
		}
	}
	if err := s.store.ReplaceDeckCards(ctx, deckID, deckRows); err != nil {
		return 0, err
	}
	return deckID, nil
}

func (s *Service) ListDecks(ctx context.Context) ([]cards.Deck, error) {
	return s.store.ListDecks(ctx)
}

func (s *Service) ListDeckCards(ctx context.Context, deckID int64) ([]cards.DeckCard, error) {
	rows, err := nonNilDeckCards(s.store.ListDeckCards(ctx, deckID))
	if err != nil {
		return nil, err
	}
	for i := range rows {
		s.enrichCardFromBulk(&rows[i].Card)
	}
	return rows, nil
}

func (s *Service) DeleteDeck(ctx context.Context, deckID int64) error {
	return s.store.DeleteDeck(ctx, deckID)
}

func (s *Service) RenameDeck(ctx context.Context, deckID int64, name string) error {
	return s.store.RenameDeck(ctx, deckID, name)
}

func (s *Service) SetDeckCardQuantity(ctx context.Context, deckID int64, oracleID string, board string, qty int) error {
	return s.store.SetDeckCardQuantity(ctx, deckID, oracleID, cards.NormalizeBoard(board), qty)
}

func (s *Service) AddCardToDeckByName(ctx context.Context, deckID int64, name string, qty int) error {
	if qty <= 0 {
		return errors.New("quantity must be > 0")
	}
	result, err := s.resolver.ResolveName(ctx, name)
	if err != nil {
		return err
	}
	card := cards.CardIdentity{OracleID: result.Card.OracleID, Name: result.Card.Name, ScryfallURI: result.Card.ScryfallURI}
	return s.store.AddCardToDeck(ctx, deckID, card, cards.BoardMain, qty)
}

func (s *Service) LendCard(ctx context.Context, input storage.LendInput) error {
	return s.store.LendCard(ctx, input)
}

func (s *Service) ReturnCard(ctx context.Context, lentID int64, returnDate string) error {
	return s.store.ReturnCard(ctx, lentID, returnDate)
}

func (s *Service) ListLentCards(ctx context.Context, includeReturned bool) ([]cards.LentCard, error) {
	rows, err := s.store.ListLentCards(ctx, includeReturned)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		s.enrichCardFromBulk(&rows[i].Card)
	}
	return rows, nil
}

func (s *Service) ExportMissingCSV(writer io.Writer, rows []DeckCompareRow) error {
	csvWriter := csv.NewWriter(writer)
	if err := csvWriter.Write([]string{"Card", "Needed", "Owned", "Missing"}); err != nil {
		return err
	}
	for _, row := range rows {
		if row.Missing <= 0 {
			continue
		}
		if err := csvWriter.Write([]string{row.Card.Name, itoa(row.Needed), itoa(row.Owned), itoa(row.Missing)}); err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return csvWriter.Error()
}

func (s *Service) preview(ctx context.Context, lines []importer.ImportLine, unresolved []string, report ImportProgressReporter) (ImportPreview, error) {
	var validated []ResolvedLine
	total := len(lines)
	// Avoid duplicate Scryfall lookups when the same card appears on multiple lines.
	cache := map[string]resolver.Result{}
	for i, line := range lines {
		key := "name:" + line.Name
		if line.ScryfallID != "" {
			key = "id:" + line.ScryfallID
		}
		result, ok := cache[key]
		if !ok {
			var err error
			result, err = s.resolver.ResolveLine(ctx, line)
			if err != nil {
				unresolved = append(unresolved, line.Raw+"  ->  "+err.Error())
				if report != nil {
					report(ImportProgress{Current: i + 1, Total: total, Label: importProgressLabel(line)})
				}
				continue
			}
			cache[key] = result
		}
		validated = append(validated, ResolvedLine{
			Line:        line,
			OracleID:    result.Card.OracleID,
			Name:        result.Card.Name,
			ScryfallURI: result.Card.ScryfallURI,
			Source:      result.Source,
		})
		if report != nil {
			report(ImportProgress{Current: i + 1, Total: total, Label: importProgressLabel(line)})
		}
	}
	return ImportPreview{Validated: nonNilSlice(validated), Unresolved: nonNilSlice(unresolved)}, nil
}

func importProgressLabel(line importer.ImportLine) string {
	if strings.TrimSpace(line.Name) != "" {
		return line.Name
	}
	return strings.TrimSpace(line.Raw)
}

func nonNilSlice[T any](items []T) []T {
	if items == nil {
		return []T{}
	}
	return items
}

func nonNilDeckCards(items []cards.DeckCard, err error) ([]cards.DeckCard, error) {
	if err != nil {
		return nil, err
	}
	return nonNilSlice(items), nil
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	n := v
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
