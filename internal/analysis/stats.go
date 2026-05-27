package analysis

import (
	"errors"
	"fmt"

	"mtgcollection/internal/cards"
)

var errAnalysisInvalid = errors.New("invalid analysis input")

func invalidAnalysisInput(msg string) error {
	return fmt.Errorf("%w: %s", errAnalysisInvalid, msg)
}

// RemainingCounts tracks library composition after cards are in hand or on bottom.
type RemainingCounts struct {
	Total       int
	Lands       int
	ByOracle    map[string]int
}

func countRemaining(pool DeckPool, hand []SimulationCard, bottom []SimulationCard) RemainingCounts {
	inPlay := make(map[string]int)
	landsInPlay := 0
	blankInPlay := 0

	countSlot := func(slotID string, isLand bool, oracleID string, isBlank bool) {
		inPlay[slotID]++
		if isBlank {
			blankInPlay++
			return
		}
		if isLand {
			landsInPlay++
		}
	}

	for _, c := range hand {
		if c.SlotID != "" {
			countSlot(c.SlotID, c.IsLand, c.OracleID, c.OracleID == "" && c.Name == "—")
		}
	}
	for _, c := range bottom {
		if c.SlotID != "" {
			countSlot(c.SlotID, c.IsLand, c.OracleID, c.OracleID == "" && c.Name == "—")
		}
	}

	rem := RemainingCounts{ByOracle: make(map[string]int)}
	for _, s := range pool.Slots {
		if inPlay[s.slotID] > 0 {
			inPlay[s.slotID]--
			continue
		}
		rem.Total++
		if s.isBlank {
			continue
		}
		if s.isLand {
			rem.Lands++
		}
		if s.oracleID != "" {
			rem.ByOracle[s.oracleID]++
		}
	}
	return rem
}

// NextDrawLandProb is P(next card is a land) from remaining library.
func NextDrawLandProb(rem RemainingCounts) float64 {
	if rem.Total <= 0 {
		return 0
	}
	return float64(rem.Lands) / float64(rem.Total)
}

// NextDrawCardProb is P(next card is a specific oracle) from remaining library.
func NextDrawCardProb(rem RemainingCounts, oracleID string) float64 {
	if rem.Total <= 0 || oracleID == "" {
		return 0
	}
	return float64(rem.ByOracle[oracleID]) / float64(rem.Total)
}

// ProbAtLeastLandsInHandAfterDraws returns P(total lands in hand >= minLands) after drawing `draws` more cards.
func ProbAtLeastLandsInHandAfterDraws(rem RemainingCounts, landsInHand, minLands, draws int) float64 {
	if minLands <= landsInHand {
		return 1
	}
	need := minLands - landsInHand
	if need > draws {
		return 0
	}
	return AtLeast(rem.Total, rem.Lands, draws, need)
}

func computeDrawStats(pool DeckPool, hand, bottom []SimulationCard, oracleID string, minLands int) DrawStats {
	rem := countRemaining(pool, hand, bottom)
	landsInHand := 0
	for _, c := range hand {
		if c.IsLand {
			landsInHand++
		}
	}

	nextLand := NextDrawLandProb(rem)
	nextCard := NextDrawCardProb(rem, oracleID)
	if oracleID == "" {
		nextCard = 0
	}

	afterOne := ProbAtLeastLandsInHandAfterDraws(rem, landsInHand, minLands, 1)

	return DrawStats{
		LandsInHand:                    landsInHand,
		LibraryRemaining:               rem.Total,
		NextDrawLandProb:               nextLand,
		NextDrawLandProbFormatted:      FormatProbability(nextLand),
		NextDrawCardProb:               nextCard,
		NextDrawCardProbFormatted:      FormatProbability(nextCard),
		OracleIDUsed:                   oracleID,
		AfterOneDrawLandsProb:          afterOne,
		AfterOneDrawLandsProbFormatted: FormatProbability(afterOne),
		MinLandsThreshold:              minLands,
	}
}

// ComputeHypergeometric evaluates the generic calculator request.
func ComputeHypergeometric(req HypergeometricRequest) (HypergeometricResult, error) {
	if req.Population <= 0 || req.SuccessesInPopulation < 0 || req.SuccessesInPopulation > req.Population {
		return HypergeometricResult{}, invalidAnalysisInput("invalid population or successes")
	}
	if req.SampleSize < 0 || req.SampleSize > req.Population {
		return HypergeometricResult{}, invalidAnalysisInput("invalid sample size")
	}
	k := min(req.MinSuccessesInSample, req.SuccessesInPopulation, req.SampleSize)
	var prob float64
	if req.Mode == "exactly" {
		prob = PMF(req.Population, req.SuccessesInPopulation, req.SampleSize, k)
	} else {
		prob = AtLeast(req.Population, req.SuccessesInPopulation, req.SampleSize, k)
	}
	return HypergeometricResult{
		Probability:          prob,
		ProbabilityFormatted: FormatProbability(prob),
	}, nil
}

// AnalyzeDeckDraw computes deck-aware hypergeometric odds.
func AnalyzeDeckDraw(rows []cards.DeckCard, req DeckDrawAnalysisRequest) (DeckDrawAnalysisResult, error) {
	pool := BuildDeckPool(rows, req.FormatTarget)
	if pool.PopulationN <= 0 {
		return DeckDrawAnalysisResult{}, invalidAnalysisInput("deck has no cards")
	}

	n := req.SampleSize
	if n > pool.PopulationN {
		n = pool.PopulationN
	}
	if n < 1 {
		n = 1
	}

	landsK := req.LandsInDeck
	if landsK <= 0 {
		landsK = pool.DetectedLands
	}
	if landsK > pool.PopulationN {
		landsK = pool.PopulationN
	}

	result := DeckDrawAnalysisResult{
		PopulationN:         pool.PopulationN,
		DeckTotal:           pool.DeckTotal,
		TargetSize:          pool.TargetSize,
		DetectedLands:       pool.DetectedLands,
		EffectiveLandsK:     landsK,
		EffectiveSampleSize: n,
		SizeWarning:         pool.SizeWarning(),
	}

	minLands := req.MinLands
	if minLands < 0 {
		minLands = 0
	}
	if minLands > landsK {
		minLands = landsK
	}
	if minLands > n {
		minLands = n
	}
	landProb := AtLeast(pool.PopulationN, landsK, n, minLands)
	result.LandProbability = landProb
	result.LandProbabilityFormatted = FormatProbability(landProb)

	if req.OracleID != "" {
		cardK := pool.OracleCounts[req.OracleID]
		if cardK > pool.PopulationN {
			cardK = pool.PopulationN
		}
		minCopies := req.MinCardCopies
		if minCopies < 1 {
			minCopies = 1
		}
		if minCopies > cardK {
			minCopies = cardK
		}
		if minCopies > n {
			minCopies = n
		}
		if cardK > 0 {
			cardProb := AtLeast(pool.PopulationN, cardK, n, minCopies)
			result.CardProbability = cardProb
			result.CardProbabilityFormatted = FormatProbability(cardProb)
		}
	}

	return result, nil
}
