package collection

import (
	"context"
	"errors"
	"strings"

	"mtgcollection/internal/cards"
	"mtgcollection/internal/storage"
)

// UnsortedFolderID is the sentinel folder ID for unallocated copies.
const UnsortedFolderID = storage.UnsortedFolderID

func (s *Service) ListCollectionFolders(ctx context.Context) ([]cards.CollectionFolder, error) {
	rows, err := s.store.ListFolders(ctx)
	if err != nil {
		return nil, err
	}
	return nonNilSlice(rows), nil
}

func (s *Service) CreateCollectionFolder(ctx context.Context, parentID *int64, name string) (int64, error) {
	return s.store.CreateFolder(ctx, parentID, name)
}

func (s *Service) RenameCollectionFolder(ctx context.Context, folderID int64, name string) error {
	return s.store.RenameFolder(ctx, folderID, name)
}

func (s *Service) MoveCollectionFolder(ctx context.Context, folderID int64, newParentID *int64) error {
	return s.store.MoveFolder(ctx, folderID, newParentID)
}

func (s *Service) DeleteCollectionFolder(ctx context.Context, folderID int64) error {
	return s.store.DeleteFolder(ctx, folderID)
}

func (s *Service) ListCollectionInFolder(ctx context.Context, folderID int64) ([]cards.FolderCard, error) {
	var rows []cards.FolderCard
	var err error
	if folderID == UnsortedFolderID {
		rows, err = s.store.ListUnsortedCards(ctx)
	} else {
		rows, err = s.store.ListFolderCards(ctx, folderID)
	}
	if err != nil {
		return nil, err
	}
	lent, err := s.store.GetLentSummaryByOracleID(ctx)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		summary := lent[rows[i].Card.OracleID]
		rows[i].LentQty = summary.TotalQuantity
		rows[i].Available = rows[i].Quantity
		s.enrichCardFromBulk(&rows[i].Card)
	}
	if err := s.attachTagsToFolderCards(ctx, rows); err != nil {
		return nil, err
	}
	return nonNilSlice(rows), nil
}

func (s *Service) MoveCollectionCopies(ctx context.Context, oracleID string, fromFolderID, toFolderID int64, quantity int) error {
	oracleID = strings.TrimSpace(oracleID)
	if oracleID == "" {
		return errors.New("oracle id cannot be empty")
	}
	if quantity <= 0 {
		return errors.New("quantity must be > 0")
	}
	if fromFolderID == toFolderID {
		return errors.New("source and destination folders must differ")
	}
	return s.store.MoveCopies(ctx, oracleID, fromFolderID, toFolderID, quantity)
}
