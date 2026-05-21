// Package collection implements import preview/commit, deck comparison, and lending workflows.
package collection

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"sort"

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
	SetDeckCardQuantity(ctx context.Context, deckID int64, oracleID string, qty int) error
	AddCardToDeck(ctx context.Context, deckID int64, card cards.CardIdentity, qty int) error
	ReplaceDeckCards(ctx context.Context, deckID int64, rows []cards.DeckCard) error
	GetOwnedByOracleID(ctx context.Context) (map[string]storage.OwnedCard, error)
	GetOwnedByNormalizedName(ctx context.Context) (map[string][]storage.NameOwnedCard, error)
	MoveCollectionQuantity(ctx context.Context, fromOracleID string, toCard cards.CardIdentity) error
	LendCard(ctx context.Context, input storage.LendInput) error
	ReturnCard(ctx context.Context, lentID int64, returnDate string) error
	ListLentCards(ctx context.Context, includeReturned bool) ([]cards.LentCard, error)
	GetLentSummaryByOracleID(ctx context.Context) (map[string]storage.LentSummary, error)
}

type Service struct {
	store    Store
	resolver resolver.Resolver
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

type DeckCompareRow struct {
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
	Name          string
	ReplaceDeckID int64
	Rows          []DeckCompareRow
}

func New(store Store, cardResolver resolver.Resolver) *Service {
	return &Service{store: store, resolver: cardResolver}
}

func (s *Service) PreviewTextImport(ctx context.Context, text string) (ImportPreview, error) {
	lines, unresolved := importer.ParseText(text)
	return s.preview(ctx, lines, unresolved)
}

func (s *Service) PreviewCSVImport(ctx context.Context, data []byte) (ImportPreview, error) {
	lines, unresolved := importer.ParseCSVBytes(data)
	return s.preview(ctx, lines, unresolved)
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
	for i := range rows {
		summary := lent[rows[i].Card.OracleID]
		rows[i].LentQty = summary.TotalQuantity
		rows[i].Available = rows[i].Quantity - rows[i].LentQty
		if rows[i].Available < 0 {
			rows[i].Available = 0
		}
	}
	return rows, nil
}

// CompareDeck resolves a pasted decklist against the owned collection.
// When oracle IDs differ but the normalized name matches exactly one owned copy,
// a RepairCandidate is suggested so the user can merge stale collection rows.
func (s *Service) CompareDeck(ctx context.Context, deckText string) (DeckCompareResult, error) {
	lines, unresolved := importer.ParseText(deckText)
	wanted := map[string]DeckCompareRow{}
	for _, line := range lines {
		result, err := s.resolver.ResolveName(ctx, line.Name)
		if err != nil {
			unresolved = append(unresolved, line.Raw+"  ->  "+err.Error())
			continue
		}
		card := cards.CardIdentity{OracleID: result.Card.OracleID, Name: result.Card.Name, ScryfallURI: result.Card.ScryfallURI}
		row := wanted[card.OracleID]
		row.Card = card
		row.Needed += line.Quantity
		wanted[card.OracleID] = row
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
		deckRows = append(deckRows, cards.DeckCard{Card: row.Card, Quantity: row.Needed})
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
	return nonNilDeckCards(s.store.ListDeckCards(ctx, deckID))
}

func (s *Service) DeleteDeck(ctx context.Context, deckID int64) error {
	return s.store.DeleteDeck(ctx, deckID)
}

func (s *Service) RenameDeck(ctx context.Context, deckID int64, name string) error {
	return s.store.RenameDeck(ctx, deckID, name)
}

func (s *Service) SetDeckCardQuantity(ctx context.Context, deckID int64, oracleID string, qty int) error {
	return s.store.SetDeckCardQuantity(ctx, deckID, oracleID, qty)
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
	return s.store.AddCardToDeck(ctx, deckID, card, qty)
}

func (s *Service) LendCard(ctx context.Context, input storage.LendInput) error {
	return s.store.LendCard(ctx, input)
}

func (s *Service) ReturnCard(ctx context.Context, lentID int64, returnDate string) error {
	return s.store.ReturnCard(ctx, lentID, returnDate)
}

func (s *Service) ListLentCards(ctx context.Context, includeReturned bool) ([]cards.LentCard, error) {
	return s.store.ListLentCards(ctx, includeReturned)
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

func (s *Service) preview(ctx context.Context, lines []importer.ImportLine, unresolved []string) (ImportPreview, error) {
	var validated []ResolvedLine
	// Avoid duplicate Scryfall lookups when the same card appears on multiple lines.
	cache := map[string]resolver.Result{}
	for _, line := range lines {
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
	}
	return ImportPreview{Validated: nonNilSlice(validated), Unresolved: nonNilSlice(unresolved)}, nil
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
