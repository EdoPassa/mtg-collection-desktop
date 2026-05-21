// Package storage persists cards, collection quantities, decks, and lending records in SQLite.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"mtgcollection/internal/cards"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

type QuantityChange struct {
	OracleID string
	Quantity int
}

type OwnedCard struct {
	Card     cards.CardIdentity
	Quantity int
}

type NameOwnedCard struct {
	OracleID string
	Name     string
	Quantity int
}

type LendInput struct {
	OracleID     string
	Quantity     int
	BorrowerName string
	LentDate     string
	Notes        string
}

type LentSummary struct {
	TotalQuantity int
	Borrowers     []string
}

func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("database path cannot be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db, path: path}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Backup(ctx context.Context) (string, error) {
	if _, err := os.Stat(s.path); err != nil {
		return "", err
	}
	backup := fmt.Sprintf("%s.backup-%s", s.path, time.Now().UTC().Format("20060102T150405Z"))
	input, err := os.ReadFile(s.path)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(backup, input, 0o600); err != nil {
		return "", err
	}
	return backup, nil
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, schemaSQL)
	return err
}

func (s *Store) UpsertCards(ctx context.Context, cardRows []cards.CardIdentity) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cards (oracle_id, name, scryfall_uri)
		VALUES (?, ?, ?)
		ON CONFLICT(oracle_id) DO UPDATE SET
		  name = excluded.name,
		  scryfall_uri = excluded.scryfall_uri
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, card := range cardRows {
		if strings.TrimSpace(card.OracleID) == "" {
			return errors.New("oracle id cannot be empty")
		}
		if _, err := stmt.ExecContext(ctx, card.OracleID, card.Name, card.ScryfallURI); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) IncrementCollection(ctx context.Context, oracleID string, qty int) error {
	if qty <= 0 {
		return nil
	}
	return s.IncrementCollectionBatch(ctx, []QuantityChange{{OracleID: oracleID, Quantity: qty}})
}

func (s *Store) IncrementCollectionBatch(ctx context.Context, items []QuantityChange) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO collection_items (oracle_id, quantity)
		VALUES (?, ?)
		ON CONFLICT(oracle_id) DO UPDATE SET
		  quantity = quantity + excluded.quantity
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, item := range items {
		if item.Quantity <= 0 {
			continue
		}
		if _, err := stmt.ExecContext(ctx, item.OracleID, item.Quantity); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) SetCollectionQuantity(ctx context.Context, oracleID string, qty int) error {
	if qty < 0 {
		return errors.New("qty must be >= 0")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO collection_items (oracle_id, quantity)
		VALUES (?, ?)
		ON CONFLICT(oracle_id) DO UPDATE SET
		  quantity = excluded.quantity
	`, oracleID, qty)
	return err
}

func (s *Store) ListCollection(ctx context.Context) ([]cards.CollectionItem, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ci.oracle_id, c.name, ci.quantity, c.scryfall_uri,
		       CASE WHEN EXISTS (
		         SELECT 1 FROM deck_cards dc WHERE dc.oracle_id = ci.oracle_id
		       ) THEN 1 ELSE 0 END AS in_deck
		FROM collection_items ci
		JOIN cards c ON c.oracle_id = ci.oracle_id
		ORDER BY c.name COLLATE NOCASE
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []cards.CollectionItem{}
	for rows.Next() {
		var row cards.CollectionItem
		var inDeck int
		if err := rows.Scan(&row.Card.OracleID, &row.Card.Name, &row.Quantity, &row.Card.ScryfallURI, &inDeck); err != nil {
			return nil, err
		}
		row.InDeck = inDeck == 1
		row.Available = row.Quantity
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) CreateDeck(ctx context.Context, name string) (int64, error) {
	deckName := strings.TrimSpace(name)
	if deckName == "" {
		return 0, errors.New("deck name cannot be empty")
	}
	res, err := s.db.ExecContext(ctx, "INSERT INTO decks (name) VALUES (?)", deckName)
	if err != nil {
		return 0, fmt.Errorf("deck already exists: %s", deckName)
	}
	return res.LastInsertId()
}

func (s *Store) ListDecks(ctx context.Context) ([]cards.Deck, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT id, name FROM decks ORDER BY name COLLATE NOCASE")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cards.Deck
	for rows.Next() {
		var deck cards.Deck
		if err := rows.Scan(&deck.ID, &deck.Name); err != nil {
			return nil, err
		}
		out = append(out, deck)
	}
	return out, rows.Err()
}

func (s *Store) AddCardToDeck(ctx context.Context, deckID int64, card cards.CardIdentity, qty int) error {
	if qty <= 0 {
		return errors.New("quantity must be > 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := upsertCard(ctx, tx, card); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO deck_cards (deck_id, oracle_id, quantity)
		VALUES (?, ?, ?)
		ON CONFLICT(deck_id, oracle_id) DO UPDATE SET
		  quantity = quantity + excluded.quantity
	`, deckID, card.OracleID, qty); err != nil {
		return errors.New("deck does not exist")
	}
	return tx.Commit()
}

func (s *Store) ReplaceDeckCards(ctx context.Context, deckID int64, rows []cards.DeckCard) error {
	for _, row := range rows {
		if row.Quantity <= 0 {
			return errors.New("quantity must be > 0")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT 1 FROM decks WHERE id = ?", deckID).Scan(&exists); err != nil {
		return errors.New("deck does not exist")
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM deck_cards WHERE deck_id = ?", deckID); err != nil {
		return err
	}
	for _, row := range rows {
		if err := upsertCard(ctx, tx, row.Card); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO deck_cards (deck_id, oracle_id, quantity)
			VALUES (?, ?, ?)
		`, deckID, row.Card.OracleID, row.Quantity); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ListDeckCards(ctx context.Context, deckID int64) ([]cards.DeckCard, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT dc.oracle_id, c.name, c.scryfall_uri, dc.quantity
		FROM deck_cards dc
		JOIN cards c ON c.oracle_id = dc.oracle_id
		WHERE dc.deck_id = ?
		ORDER BY c.name COLLATE NOCASE
	`, deckID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cards.DeckCard
	for rows.Next() {
		var row cards.DeckCard
		if err := rows.Scan(&row.Card.OracleID, &row.Card.Name, &row.Card.ScryfallURI, &row.Quantity); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) DeleteDeck(ctx context.Context, deckID int64) error {
	res, err := s.db.ExecContext(ctx, "DELETE FROM decks WHERE id = ?", deckID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("deck does not exist")
	}
	return nil
}

func (s *Store) RenameDeck(ctx context.Context, deckID int64, name string) error {
	deckName := strings.TrimSpace(name)
	if deckName == "" {
		return errors.New("deck name cannot be empty")
	}
	res, err := s.db.ExecContext(ctx, "UPDATE decks SET name = ? WHERE id = ?", deckName, deckID)
	if err != nil {
		return fmt.Errorf("deck already exists: %s", deckName)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("deck does not exist")
	}
	return nil
}

// SetDeckCardQuantity sets qty to 0 to remove the card from the deck entirely.
func (s *Store) SetDeckCardQuantity(ctx context.Context, deckID int64, oracleID string, qty int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT 1 FROM decks WHERE id = ?", deckID).Scan(&exists); err != nil {
		return errors.New("deck does not exist")
	}
	if qty <= 0 {
		if _, err := tx.ExecContext(ctx, "DELETE FROM deck_cards WHERE deck_id = ? AND oracle_id = ?", deckID, oracleID); err != nil {
			return err
		}
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO deck_cards (deck_id, oracle_id, quantity)
		VALUES (?, ?, ?)
		ON CONFLICT(deck_id, oracle_id) DO UPDATE SET quantity = excluded.quantity
	`, deckID, oracleID, qty); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) GetOwnedByOracleID(ctx context.Context) (map[string]OwnedCard, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ci.oracle_id, c.name, c.scryfall_uri, ci.quantity
		FROM collection_items ci
		JOIN cards c ON c.oracle_id = ci.oracle_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]OwnedCard{}
	for rows.Next() {
		var row OwnedCard
		if err := rows.Scan(&row.Card.OracleID, &row.Card.Name, &row.Card.ScryfallURI, &row.Quantity); err != nil {
			return nil, err
		}
		out[row.Card.OracleID] = row
	}
	return out, rows.Err()
}

func (s *Store) GetOwnedByNormalizedName(ctx context.Context) (map[string][]NameOwnedCard, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ci.oracle_id, c.name, ci.quantity
		FROM collection_items ci
		JOIN cards c ON c.oracle_id = ci.oracle_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]NameOwnedCard{}
	for rows.Next() {
		var row NameOwnedCard
		if err := rows.Scan(&row.OracleID, &row.Name, &row.Quantity); err != nil {
			return nil, err
		}
		key := cards.NormalizeName(row.Name)
		if key != "" {
			out[key] = append(out[key], row)
		}
	}
	return out, rows.Err()
}

// MoveCollectionQuantity reassigns all copies from one oracle ID to another.
// Used when Scryfall oracle IDs change but the physical card is the same.
func (s *Store) MoveCollectionQuantity(ctx context.Context, fromOracleID string, toCard cards.CardIdentity) error {
	fromOracleID = strings.TrimSpace(fromOracleID)
	toCard.OracleID = strings.TrimSpace(toCard.OracleID)
	if fromOracleID == "" {
		return nil
	}
	if fromOracleID == toCard.OracleID {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if err := upsertCard(ctx, tx, toCard); err != nil {
		return err
	}
	var qty int
	err = tx.QueryRowContext(ctx, "SELECT quantity FROM collection_items WHERE oracle_id = ?", fromOracleID).Scan(&qty)
	if errors.Is(err, sql.ErrNoRows) {
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if qty > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO collection_items (oracle_id, quantity)
			VALUES (?, ?)
			ON CONFLICT(oracle_id) DO UPDATE SET
			  quantity = quantity + excluded.quantity
		`, toCard.OracleID, qty); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM collection_items WHERE oracle_id = ?", fromOracleID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) LendCard(ctx context.Context, input LendInput) error {
	if input.Quantity <= 0 {
		return errors.New("quantity must be > 0")
	}
	borrower := strings.TrimSpace(input.BorrowerName)
	if borrower == "" {
		return errors.New("borrower_name cannot be empty")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO lent_cards (oracle_id, quantity, borrower_name, lent_date, notes)
		VALUES (?, ?, ?, ?, ?)
	`, input.OracleID, input.Quantity, borrower, input.LentDate, input.Notes)
	return err
}

func (s *Store) ReturnCard(ctx context.Context, lentID int64, returnDate string) error {
	returnDate = strings.TrimSpace(returnDate)
	if returnDate == "" {
		return errors.New("return date cannot be empty")
	}
	res, err := s.db.ExecContext(ctx, "UPDATE lent_cards SET return_date = ? WHERE id = ?", returnDate, lentID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errors.New("lent card does not exist")
	}
	return nil
}

func (s *Store) ListLentCards(ctx context.Context, includeReturned bool) ([]cards.LentCard, error) {
	query := `
		SELECT lc.id, lc.oracle_id, c.name, c.scryfall_uri, lc.quantity, lc.borrower_name, lc.lent_date,
		       COALESCE(lc.return_date, ''), COALESCE(lc.notes, '')
		FROM lent_cards lc
		JOIN cards c ON c.oracle_id = lc.oracle_id
	`
	if !includeReturned {
		query += " WHERE lc.return_date IS NULL"
	}
	query += " ORDER BY lc.lent_date DESC"
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []cards.LentCard{}
	for rows.Next() {
		var row cards.LentCard
		if err := rows.Scan(&row.ID, &row.Card.OracleID, &row.Card.Name, &row.Card.ScryfallURI, &row.Quantity, &row.BorrowerName, &row.LentDate, &row.ReturnDate, &row.Notes); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GetLentSummaryByOracleID(ctx context.Context) (map[string]LentSummary, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT oracle_id, SUM(quantity) AS total_qty, COALESCE(GROUP_CONCAT(borrower_name, ', '), '') AS borrowers
		FROM lent_cards
		WHERE return_date IS NULL
		GROUP BY oracle_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]LentSummary{}
	for rows.Next() {
		var oracleID string
		var total int
		var borrowersRaw string
		if err := rows.Scan(&oracleID, &total, &borrowersRaw); err != nil {
			return nil, err
		}
		var borrowers []string
		for _, borrower := range strings.Split(borrowersRaw, ",") {
			if b := strings.TrimSpace(borrower); b != "" {
				borrowers = append(borrowers, b)
			}
		}
		out[oracleID] = LentSummary{TotalQuantity: total, Borrowers: borrowers}
	}
	return out, rows.Err()
}

func upsertCard(ctx context.Context, tx *sql.Tx, card cards.CardIdentity) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO cards (oracle_id, name, scryfall_uri)
		VALUES (?, ?, ?)
		ON CONFLICT(oracle_id) DO UPDATE SET
		  name = excluded.name,
		  scryfall_uri = excluded.scryfall_uri
	`, card.OracleID, card.Name, card.ScryfallURI)
	return err
}

func rollback(tx *sql.Tx) {
	_ = tx.Rollback()
}
