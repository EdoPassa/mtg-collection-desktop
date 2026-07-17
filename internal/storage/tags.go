package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"mtgcollection/internal/cards"
)

func (s *Store) migrateCollectionTags(ctx context.Context) error {
	var exists int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'tags'
	`).Scan(&exists); err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, migrateV4SQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO schema_migrations(version) VALUES (4)`); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateTag(ctx context.Context, name, color string) (int64, error) {
	tagName := strings.TrimSpace(name)
	if tagName == "" {
		return 0, errors.New("tag name cannot be empty")
	}
	tagColor := strings.TrimSpace(color)
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO tags (name, color)
		VALUES (?, NULLIF(?, ''))
	`, tagName, tagColor)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return 0, fmt.Errorf("tag already exists: %s", tagName)
		}
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) ListTags(ctx context.Context) ([]cards.CollectionTag, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.id, t.name, COALESCE(t.color, ''),
		       (SELECT COUNT(*) FROM card_tags ct WHERE ct.tag_id = t.id)
		FROM tags t
		ORDER BY t.name COLLATE NOCASE
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []cards.CollectionTag{}
	for rows.Next() {
		var tag cards.CollectionTag
		if err := rows.Scan(&tag.ID, &tag.Name, &tag.Color, &tag.CardCount); err != nil {
			return nil, err
		}
		out = append(out, tag)
	}
	return out, rows.Err()
}

func (s *Store) RenameTag(ctx context.Context, tagID int64, name string) error {
	tagName := strings.TrimSpace(name)
	if tagName == "" {
		return errors.New("tag name cannot be empty")
	}
	if err := s.ensureTagExists(ctx, tagID); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tags SET name = ? WHERE id = ?`, tagName, tagID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return fmt.Errorf("tag already exists: %s", tagName)
		}
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("tag not found")
	}
	return nil
}

func (s *Store) UpdateTagColor(ctx context.Context, tagID int64, color string) error {
	if err := s.ensureTagExists(ctx, tagID); err != nil {
		return err
	}
	tagColor := strings.TrimSpace(color)
	res, err := s.db.ExecContext(ctx, `UPDATE tags SET color = NULLIF(?, '') WHERE id = ?`, tagColor, tagID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("tag not found")
	}
	return nil
}

func (s *Store) DeleteTag(ctx context.Context, tagID int64) error {
	if err := s.ensureTagExists(ctx, tagID); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM tags WHERE id = ?`, tagID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("tag not found")
	}
	return nil
}

func (s *Store) GetTagsByOracleID(ctx context.Context) (map[string][]cards.CollectionTag, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ct.oracle_id, t.id, t.name, COALESCE(t.color, '')
		FROM card_tags ct
		JOIN tags t ON t.id = ct.tag_id
		ORDER BY ct.oracle_id, t.name COLLATE NOCASE
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]cards.CollectionTag{}
	for rows.Next() {
		var oracleID string
		var tag cards.CollectionTag
		if err := rows.Scan(&oracleID, &tag.ID, &tag.Name, &tag.Color); err != nil {
			return nil, err
		}
		out[oracleID] = append(out[oracleID], tag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) GetCardTagIDs(ctx context.Context, oracleID string) ([]int64, error) {
	oracleID = strings.TrimSpace(oracleID)
	if oracleID == "" {
		return nil, errors.New("oracle id cannot be empty")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT tag_id FROM card_tags WHERE oracle_id = ? ORDER BY tag_id
	`, oracleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) SetCardTags(ctx context.Context, oracleID string, tagIDs []int64) error {
	oracleID = strings.TrimSpace(oracleID)
	if oracleID == "" {
		return errors.New("oracle id cannot be empty")
	}
	if tagIDs == nil {
		tagIDs = []int64{}
	}
	if err := s.ensureCardInCollection(ctx, oracleID); err != nil {
		return err
	}
	seen := map[int64]struct{}{}
	for _, id := range tagIDs {
		if id <= 0 {
			return errors.New("invalid tag id")
		}
		if _, dup := seen[id]; dup {
			return errors.New("duplicate tag id")
		}
		seen[id] = struct{}{}
		if err := s.ensureTagExists(ctx, id); err != nil {
			return err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `DELETE FROM card_tags WHERE oracle_id = ?`, oracleID); err != nil {
		return err
	}
	for _, id := range tagIDs {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO card_tags (oracle_id, tag_id) VALUES (?, ?)
		`, oracleID, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AddTagsToCards assigns every tag to every card, ignoring pairs that already exist.
func (s *Store) AddTagsToCards(ctx context.Context, oracleIDs []string, tagIDs []int64) error {
	oracleIDs, tagIDs, err := s.validateCardTagBatch(ctx, oracleIDs, tagIDs)
	if err != nil {
		return err
	}
	if len(oracleIDs) == 0 || len(tagIDs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO card_tags (oracle_id, tag_id) VALUES (?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, oracleID := range oracleIDs {
		for _, tagID := range tagIDs {
			if _, err := stmt.ExecContext(ctx, oracleID, tagID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// RemoveTagsFromCards unassigns every tag from every card; missing pairs are ignored.
func (s *Store) RemoveTagsFromCards(ctx context.Context, oracleIDs []string, tagIDs []int64) error {
	oracleIDs, tagIDs, err := s.validateCardTagBatch(ctx, oracleIDs, tagIDs)
	if err != nil {
		return err
	}
	if len(oracleIDs) == 0 || len(tagIDs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	stmt, err := tx.PrepareContext(ctx, `
		DELETE FROM card_tags WHERE oracle_id = ? AND tag_id = ?
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, oracleID := range oracleIDs {
		for _, tagID := range tagIDs {
			if _, err := stmt.ExecContext(ctx, oracleID, tagID); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// validateCardTagBatch trims and deduplicates oracle IDs and tag IDs, verifying that
// each card is in the collection and each tag exists.
func (s *Store) validateCardTagBatch(ctx context.Context, oracleIDs []string, tagIDs []int64) ([]string, []int64, error) {
	seenOracle := map[string]struct{}{}
	cleanOracle := make([]string, 0, len(oracleIDs))
	for _, oracleID := range oracleIDs {
		oracleID = strings.TrimSpace(oracleID)
		if oracleID == "" {
			return nil, nil, errors.New("oracle id cannot be empty")
		}
		if _, dup := seenOracle[oracleID]; dup {
			continue
		}
		seenOracle[oracleID] = struct{}{}
		if err := s.ensureCardInCollection(ctx, oracleID); err != nil {
			return nil, nil, err
		}
		cleanOracle = append(cleanOracle, oracleID)
	}
	seenTag := map[int64]struct{}{}
	cleanTags := make([]int64, 0, len(tagIDs))
	for _, tagID := range tagIDs {
		if tagID <= 0 {
			return nil, nil, errors.New("invalid tag id")
		}
		if _, dup := seenTag[tagID]; dup {
			continue
		}
		seenTag[tagID] = struct{}{}
		if err := s.ensureTagExists(ctx, tagID); err != nil {
			return nil, nil, err
		}
		cleanTags = append(cleanTags, tagID)
	}
	return cleanOracle, cleanTags, nil
}

func (s *Store) ensureTagExists(ctx context.Context, tagID int64) error {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM tags WHERE id = ?`, tagID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("tag not found")
		}
		return err
	}
	return nil
}

func (s *Store) ensureCardInCollection(ctx context.Context, oracleID string) error {
	var qty int
	err := s.db.QueryRowContext(ctx, `
		SELECT quantity FROM collection_items WHERE oracle_id = ?
	`, oracleID).Scan(&qty)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("card is not in collection")
		}
		return err
	}
	return nil
}
