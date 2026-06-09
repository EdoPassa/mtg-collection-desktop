package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"mtgcollection/internal/cards"
)

// UnsortedFolderID is the sentinel folder ID for unallocated copies (not stored in collection_folders).
const UnsortedFolderID int64 = 0

func (s *Store) migrateCollectionFolders(ctx context.Context) error {
	var exists int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'collection_folders'
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
	if _, err := tx.ExecContext(ctx, migrateV3SQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO schema_migrations(version) VALUES (3)`); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateFolder(ctx context.Context, parentID *int64, name string) (int64, error) {
	folderName := strings.TrimSpace(name)
	if folderName == "" {
		return 0, errors.New("folder name cannot be empty")
	}
	if parentID != nil {
		if err := s.ensureFolderExists(ctx, *parentID); err != nil {
			return 0, err
		}
	}
	if err := s.ensureSiblingFolderNameFree(ctx, parentID, folderName, 0); err != nil {
		return 0, err
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO collection_folders (name, parent_id)
		VALUES (?, ?)
	`, folderName, nullableInt64(parentID))
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return 0, fmt.Errorf("folder already exists: %s", folderName)
		}
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) ListFolders(ctx context.Context) ([]cards.CollectionFolder, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, parent_id
		FROM collection_folders
		ORDER BY COALESCE(parent_id, 0), sort_order, name COLLATE NOCASE
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []cards.CollectionFolder{}
	for rows.Next() {
		var folder cards.CollectionFolder
		var parent sql.NullInt64
		if err := rows.Scan(&folder.ID, &folder.Name, &parent); err != nil {
			return nil, err
		}
		if parent.Valid {
			folder.ParentID = &parent.Int64
		}
		out = append(out, folder)
	}
	return out, rows.Err()
}

func (s *Store) RenameFolder(ctx context.Context, folderID int64, name string) error {
	folderName := strings.TrimSpace(name)
	if folderName == "" {
		return errors.New("folder name cannot be empty")
	}
	parentID, err := s.folderParentID(ctx, folderID)
	if err != nil {
		return err
	}
	if err := s.ensureSiblingFolderNameFree(ctx, parentID, folderName, folderID); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, "UPDATE collection_folders SET name = ? WHERE id = ?", folderName, folderID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return fmt.Errorf("folder already exists: %s", folderName)
		}
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("folder does not exist")
	}
	return nil
}

func (s *Store) MoveFolder(ctx context.Context, folderID int64, newParentID *int64) error {
	if newParentID != nil && *newParentID == folderID {
		return errors.New("folder cannot be its own parent")
	}
	if err := s.ensureFolderExists(ctx, folderID); err != nil {
		return err
	}
	if newParentID != nil {
		if err := s.ensureFolderExists(ctx, *newParentID); err != nil {
			return err
		}
		if err := s.ensureNotFolderAncestor(ctx, folderID, *newParentID); err != nil {
			return err
		}
	}
	folder, err := s.getFolderByID(ctx, folderID)
	if err != nil {
		return err
	}
	if err := s.ensureSiblingFolderNameFree(ctx, newParentID, folder.Name, folderID); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, "UPDATE collection_folders SET parent_id = ? WHERE id = ?", nullableInt64(newParentID), folderID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE") {
			return fmt.Errorf("folder already exists: %s", folder.Name)
		}
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("folder does not exist")
	}
	return nil
}

func (s *Store) DeleteFolder(ctx context.Context, folderID int64) error {
	if err := s.ensureFolderExists(ctx, folderID); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, `
		WITH RECURSIVE subtree AS (
		  SELECT id FROM collection_folders WHERE id = ?
		  UNION ALL
		  SELECT f.id FROM collection_folders f JOIN subtree s ON f.parent_id = s.id
		)
		DELETE FROM collection_folder_items WHERE folder_id IN (SELECT id FROM subtree)
	`, folderID); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, "DELETE FROM collection_folders WHERE id = ?", folderID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("folder does not exist")
	}
	return tx.Commit()
}

func (s *Store) ListFolderCards(ctx context.Context, folderID int64) ([]cards.FolderCard, error) {
	if err := s.ensureFolderExists(ctx, folderID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT cfi.oracle_id, c.name, c.scryfall_uri, cfi.quantity,
		       CASE WHEN EXISTS (
		         SELECT 1 FROM deck_cards dc WHERE dc.oracle_id = cfi.oracle_id
		       ) THEN 1 ELSE 0 END AS in_deck
		FROM collection_folder_items cfi
		JOIN cards c ON c.oracle_id = cfi.oracle_id
		WHERE cfi.folder_id = ?
		ORDER BY c.name COLLATE NOCASE
	`, folderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFolderCards(rows)
}

func (s *Store) ListUnsortedCards(ctx context.Context) ([]cards.FolderCard, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ci.oracle_id, c.name, c.scryfall_uri,
		       ci.quantity - COALESCE(alloc.total_qty, 0) AS unassigned_qty,
		       CASE WHEN EXISTS (
		         SELECT 1 FROM deck_cards dc WHERE dc.oracle_id = ci.oracle_id
		       ) THEN 1 ELSE 0 END AS in_deck
		FROM collection_items ci
		JOIN cards c ON c.oracle_id = ci.oracle_id
		LEFT JOIN (
		  SELECT oracle_id, SUM(quantity) AS total_qty
		  FROM collection_folder_items
		  GROUP BY oracle_id
		) alloc ON alloc.oracle_id = ci.oracle_id
		WHERE ci.quantity > COALESCE(alloc.total_qty, 0)
		ORDER BY c.name COLLATE NOCASE
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []cards.FolderCard{}
	for rows.Next() {
		var row cards.FolderCard
		var inDeck int
		if err := rows.Scan(&row.Card.OracleID, &row.Card.Name, &row.Card.ScryfallURI, &row.Quantity, &inDeck); err != nil {
			return nil, err
		}
		row.InDeck = inDeck == 1
		row.Available = row.Quantity
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) GetAllocatedByOracleID(ctx context.Context) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT oracle_id, SUM(quantity) AS total_qty
		FROM collection_folder_items
		GROUP BY oracle_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var oracleID string
		var total int
		if err := rows.Scan(&oracleID, &total); err != nil {
			return nil, err
		}
		out[oracleID] = total
	}
	return out, rows.Err()
}

func (s *Store) MoveCopies(ctx context.Context, oracleID string, fromFolderID, toFolderID int64, qty int) error {
	oracleID = strings.TrimSpace(oracleID)
	if oracleID == "" {
		return errors.New("oracle id cannot be empty")
	}
	if qty <= 0 {
		return errors.New("quantity must be > 0")
	}
	if fromFolderID == toFolderID {
		return errors.New("source and destination folders must differ")
	}
	if fromFolderID != UnsortedFolderID {
		if err := s.ensureFolderExists(ctx, fromFolderID); err != nil {
			return err
		}
	}
	if toFolderID != UnsortedFolderID {
		if err := s.ensureFolderExists(ctx, toFolderID); err != nil {
			return err
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	var owned int
	err = tx.QueryRowContext(ctx, "SELECT quantity FROM collection_items WHERE oracle_id = ?", oracleID).Scan(&owned)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("card is not in collection")
	}
	if err != nil {
		return err
	}

	var allocated int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(quantity), 0) FROM collection_folder_items WHERE oracle_id = ?
	`, oracleID).Scan(&allocated); err != nil {
		return err
	}
	unassigned := owned - allocated
	if unassigned < 0 {
		unassigned = 0
	}

	var fromQty int
	if fromFolderID == UnsortedFolderID {
		fromQty = unassigned
	} else {
		err = tx.QueryRowContext(ctx, `
			SELECT quantity FROM collection_folder_items WHERE folder_id = ? AND oracle_id = ?
		`, fromFolderID, oracleID).Scan(&fromQty)
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("not enough copies in source folder")
		}
		if err != nil {
			return err
		}
	}
	if fromQty < qty {
		return errors.New("not enough copies in source folder")
	}

	if fromFolderID != UnsortedFolderID {
		newFromQty := fromQty - qty
		if newFromQty <= 0 {
			if _, err := tx.ExecContext(ctx, `
				DELETE FROM collection_folder_items WHERE folder_id = ? AND oracle_id = ?
			`, fromFolderID, oracleID); err != nil {
				return err
			}
		} else {
			if _, err := tx.ExecContext(ctx, `
				UPDATE collection_folder_items SET quantity = ? WHERE folder_id = ? AND oracle_id = ?
			`, newFromQty, fromFolderID, oracleID); err != nil {
				return err
			}
		}
	}

	if toFolderID != UnsortedFolderID {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO collection_folder_items (folder_id, oracle_id, quantity)
			VALUES (?, ?, ?)
			ON CONFLICT(folder_id, oracle_id) DO UPDATE SET
			  quantity = quantity + excluded.quantity
		`, toFolderID, oracleID, qty); err != nil {
			return err
		}
	}

	var allocatedAfter int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(quantity), 0) FROM collection_folder_items WHERE oracle_id = ?
	`, oracleID).Scan(&allocatedAfter); err != nil {
		return err
	}
	if allocatedAfter > owned {
		return errors.New("allocated quantity exceeds owned total")
	}
	return tx.Commit()
}

func (s *Store) ensureFolderExists(ctx context.Context, folderID int64) error {
	var exists int
	if err := s.db.QueryRowContext(ctx, "SELECT 1 FROM collection_folders WHERE id = ?", folderID).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("folder does not exist")
		}
		return err
	}
	return nil
}

func (s *Store) getFolderByID(ctx context.Context, folderID int64) (cards.CollectionFolder, error) {
	var folder cards.CollectionFolder
	var parent sql.NullInt64
	err := s.db.QueryRowContext(ctx, "SELECT id, name, parent_id FROM collection_folders WHERE id = ?", folderID).
		Scan(&folder.ID, &folder.Name, &parent)
	if errors.Is(err, sql.ErrNoRows) {
		return folder, errors.New("folder does not exist")
	}
	if err != nil {
		return folder, err
	}
	if parent.Valid {
		folder.ParentID = &parent.Int64
	}
	return folder, nil
}

func (s *Store) folderParentID(ctx context.Context, folderID int64) (*int64, error) {
	var parent sql.NullInt64
	err := s.db.QueryRowContext(ctx, "SELECT parent_id FROM collection_folders WHERE id = ?", folderID).Scan(&parent)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("folder does not exist")
	}
	if err != nil {
		return nil, err
	}
	if !parent.Valid {
		return nil, nil
	}
	return &parent.Int64, nil
}

func (s *Store) ensureSiblingFolderNameFree(ctx context.Context, parentID *int64, name string, excludeID int64) error {
	var count int
	var err error
	if parentID == nil {
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM collection_folders
			WHERE parent_id IS NULL AND name = ? AND id != ?
		`, name, excludeID).Scan(&count)
	} else {
		err = s.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM collection_folders
			WHERE parent_id = ? AND name = ? AND id != ?
		`, *parentID, name, excludeID).Scan(&count)
	}
	if err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("folder already exists: %s", name)
	}
	return nil
}

func (s *Store) ensureNotFolderAncestor(ctx context.Context, folderID, candidateParentID int64) error {
	current := candidateParentID
	for current != 0 {
		if current == folderID {
			return errors.New("folder cannot be moved into its own descendant")
		}
		var parent sql.NullInt64
		err := s.db.QueryRowContext(ctx, "SELECT parent_id FROM collection_folders WHERE id = ?", current).Scan(&parent)
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("folder does not exist")
		}
		if err != nil {
			return err
		}
		if !parent.Valid {
			break
		}
		current = parent.Int64
	}
	return nil
}

func scanFolderCards(rows *sql.Rows) ([]cards.FolderCard, error) {
	out := []cards.FolderCard{}
	for rows.Next() {
		var row cards.FolderCard
		var inDeck int
		if err := rows.Scan(&row.Card.OracleID, &row.Card.Name, &row.Card.ScryfallURI, &row.Quantity, &inDeck); err != nil {
			return nil, err
		}
		row.InDeck = inDeck == 1
		row.Available = row.Quantity
		out = append(out, row)
	}
	return out, rows.Err()
}

func nullableInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}
