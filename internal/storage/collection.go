package storage

import (
	"context"
	"errors"
	"strings"
)

// DeleteCollectionCards removes ownership of the given cards: the collection rows,
// folder allocations, and tag assignments are deleted in one transaction.
// The cards table rows are kept so decks and lending history stay intact.
func (s *Store) DeleteCollectionCards(ctx context.Context, oracleIDs []string) error {
	seen := map[string]struct{}{}
	clean := make([]string, 0, len(oracleIDs))
	for _, oracleID := range oracleIDs {
		oracleID = strings.TrimSpace(oracleID)
		if oracleID == "" {
			return errors.New("oracle id cannot be empty")
		}
		if _, dup := seen[oracleID]; dup {
			continue
		}
		seen[oracleID] = struct{}{}
		clean = append(clean, oracleID)
	}
	if len(clean) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	for _, oracleID := range clean {
		if _, err := tx.ExecContext(ctx, `DELETE FROM card_tags WHERE oracle_id = ?`, oracleID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM collection_folder_items WHERE oracle_id = ?`, oracleID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM collection_items WHERE oracle_id = ?`, oracleID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
