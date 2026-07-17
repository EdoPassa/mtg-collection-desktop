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

// AddTagsToCards assigns every tag to every selected card (idempotent per pair).
func (s *Service) AddTagsToCards(ctx context.Context, oracleIDs []string, tagIDs []int64) error {
	oracleIDs = trimNonEmpty(oracleIDs)
	if len(oracleIDs) == 0 || len(tagIDs) == 0 {
		return nil
	}
	return s.store.AddTagsToCards(ctx, oracleIDs, tagIDs)
}

// RemoveTagsFromCards unassigns every tag from every selected card.
func (s *Service) RemoveTagsFromCards(ctx context.Context, oracleIDs []string, tagIDs []int64) error {
	oracleIDs = trimNonEmpty(oracleIDs)
	if len(oracleIDs) == 0 || len(tagIDs) == 0 {
		return nil
	}
	return s.store.RemoveTagsFromCards(ctx, oracleIDs, tagIDs)
}

// DeleteCollectionCards removes the selected cards from the collection
// (ownership, folder allocations, tag assignments); decks and lending history are kept.
func (s *Service) DeleteCollectionCards(ctx context.Context, oracleIDs []string) error {
	oracleIDs = trimNonEmpty(oracleIDs)
	if len(oracleIDs) == 0 {
		return nil
	}
	return s.store.DeleteCollectionCards(ctx, oracleIDs)
}

func trimNonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// GetTagsByOracleID returns all card tags keyed by oracle_id.
func (s *Service) GetTagsByOracleID(ctx context.Context) (map[string][]cards.CollectionTag, error) {
	return s.store.GetTagsByOracleID(ctx)
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
