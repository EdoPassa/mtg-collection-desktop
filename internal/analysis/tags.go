package analysis

import (
	"sort"

	"mtgcollection/internal/cards"
)

// TagRefsForDeck filters a global oracle→tags map to deck mainboard oracles.
func TagRefsForDeck(rows []cards.DeckCard, all map[string][]cards.CollectionTag) map[string][]TagRef {
	oracles := make(map[string]struct{})
	for _, row := range filterMainboard(rows) {
		if row.Card.OracleID != "" {
			oracles[row.Card.OracleID] = struct{}{}
		}
	}
	out := make(map[string][]TagRef)
	for oracleID := range oracles {
		for _, tag := range all[oracleID] {
			out[oracleID] = append(out[oracleID], TagRef{
				ID:    tag.ID,
				Name:  tag.Name,
				Color: tag.Color,
			})
		}
	}
	return out
}

// AnalyzeDeckTags computes hypergeometric odds for every tag present in the deck.
func AnalyzeDeckTags(rows []cards.DeckCard, tagsByOracle map[string][]TagRef, req DeckTagAnalysisRequest) (DeckTagAnalysisResult, error) {
	pool := BuildDeckPool(rows, req.FormatTarget, tagsByOracle)
	if pool.PopulationN <= 0 {
		return DeckTagAnalysisResult{}, invalidAnalysisInput("deck has no cards")
	}

	n := req.SampleSize
	if n > pool.PopulationN {
		n = pool.PopulationN
	}
	if n < 1 {
		n = 1
	}

	minTag := req.MinTagCards
	if minTag < 1 {
		minTag = 1
	}

	result := DeckTagAnalysisResult{
		PopulationN: pool.PopulationN,
		DeckTotal:   pool.DeckTotal,
		SizeWarning: pool.SizeWarning(),
		Tags:        []TagDeckStat{},
	}

	tagIDs := sortedTagIDs(pool)
	for _, tagID := range tagIDs {
		k := pool.TagCounts[tagID]
		if k <= 0 {
			continue
		}
		stat := tagDeckStatFromPool(pool, tagID, k, n, minTag, 0)
		result.Tags = append(result.Tags, stat)
		if req.TagFocus > 0 && tagID == req.TagFocus {
			focus := stat
			result.Focus = &focus
		}
	}

	if req.TagFocus > 0 && result.Focus == nil {
		if k := pool.TagCounts[req.TagFocus]; k > 0 {
			stat := tagDeckStatFromPool(pool, req.TagFocus, k, n, minTag, 0)
			result.Focus = &stat
		}
	}

	return result, nil
}

func sortedTagIDs(pool DeckPool) []int64 {
	ids := make([]int64, 0, len(pool.TagCounts))
	for id := range pool.TagCounts {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ni, nj := pool.TagMeta[ids[i]].Name, pool.TagMeta[ids[j]].Name
		if ni != nj {
			return ni < nj
		}
		return ids[i] < ids[j]
	})
	return ids
}

func tagDeckStatFromPool(pool DeckPool, tagID int64, copiesInDeck, sampleSize, minTag, copiesInLibrary int) TagDeckStat {
	meta := pool.TagMeta[tagID]
	minK := minTag
	if minK > copiesInDeck {
		minK = copiesInDeck
	}
	if minK > sampleSize {
		minK = sampleSize
	}
	sampleProb := AtLeast(pool.PopulationN, copiesInDeck, sampleSize, minK)

	stat := TagDeckStat{
		TagID:               tagID,
		Name:                meta.Name,
		Color:               meta.Color,
		CopiesInDeck:        copiesInDeck,
		CopiesInLibrary:     copiesInLibrary,
		SampleProb:          sampleProb,
		SampleProbFormatted: FormatProbability(sampleProb),
	}
	if copiesInLibrary > 0 && pool.PopulationN > 0 {
		// next-draw only when library context provided
	}
	return stat
}

func tagStatsFromRemaining(pool DeckPool, rem RemainingCounts, sampleSize, minTag int) []TagDeckStat {
	if len(pool.TagCounts) == 0 {
		return []TagDeckStat{}
	}
	tagIDs := make([]int64, 0, len(pool.TagCounts))
	for id := range pool.TagCounts {
		tagIDs = append(tagIDs, id)
	}
	sort.Slice(tagIDs, func(i, j int) bool {
		ni, nj := pool.TagMeta[tagIDs[i]].Name, pool.TagMeta[tagIDs[j]].Name
		if ni != nj {
			return ni < nj
		}
		return tagIDs[i] < tagIDs[j]
	})

	out := make([]TagDeckStat, 0, len(tagIDs))
	for _, tagID := range tagIDs {
		inDeck := pool.TagCounts[tagID]
		if inDeck <= 0 {
			continue
		}
		inLib := rem.ByTag[tagID]
		nextProb := NextDrawTagProb(rem, tagID)
		stat := tagDeckStatFromPool(pool, tagID, inDeck, sampleSize, minTag, inLib)
		stat.NextDrawProb = nextProb
		stat.NextDrawProbFormatted = FormatProbability(nextProb)
		out = append(out, stat)
	}
	return out
}
