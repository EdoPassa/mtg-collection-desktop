package collection

import (
	"context"
	"strings"

	"mtgcollection/internal/cards"
)

func (s *Service) ListCollectionTags(ctx context.Context) ([]cards.CollectionTag, error) {
	rows, err := s.store.ListTags(ctx)
	if err != nil {
		return nil, err
	}
	return nonNilSlice(rows), nil
}

func (s *Service) CreateCollectionTag(ctx context.Context, name, color string) (int64, error) {
	return s.store.CreateTag(ctx, strings.TrimSpace(name), strings.TrimSpace(color))
}

func (s *Service) RenameCollectionTag(ctx context.Context, tagID int64, name string) error {
	return s.store.RenameTag(ctx, tagID, strings.TrimSpace(name))
}

func (s *Service) UpdateCollectionTagColor(ctx context.Context, tagID int64, color string) error {
	return s.store.UpdateTagColor(ctx, tagID, strings.TrimSpace(color))
}

func (s *Service) DeleteCollectionTag(ctx context.Context, tagID int64) error {
	return s.store.DeleteTag(ctx, tagID)
}

func (s *Service) SetCardTags(ctx context.Context, oracleID string, tagIDs []int64) error {
	return s.store.SetCardTags(ctx, strings.TrimSpace(oracleID), tagIDs)
}

func (s *Service) attachTagsToCollectionItems(ctx context.Context, rows []cards.CollectionItem) error {
	if len(rows) == 0 {
		return nil
	}
	byOracle, err := s.store.GetTagsByOracleID(ctx)
	if err != nil {
		return err
	}
	for i := range rows {
		rows[i].Tags = nonNilSlice(byOracle[rows[i].Card.OracleID])
	}
	return nil
}

func (s *Service) attachTagsToFolderCards(ctx context.Context, rows []cards.FolderCard) error {
	if len(rows) == 0 {
		return nil
	}
	byOracle, err := s.store.GetTagsByOracleID(ctx)
	if err != nil {
		return err
	}
	for i := range rows {
		rows[i].Tags = nonNilSlice(byOracle[rows[i].Card.OracleID])
	}
	return nil
}
