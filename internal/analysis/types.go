package analysis

// HypergeometricRequest is input for the generic calculator.
type HypergeometricRequest struct {
	Population            int    `json:"population"`
	SuccessesInPopulation int    `json:"successesInPopulation"`
	SampleSize            int    `json:"sampleSize"`
	MinSuccessesInSample  int    `json:"minSuccessesInSample"`
	Mode                  string `json:"mode"` // "at-least" or "exactly"
}

// HypergeometricResult is the generic calculator output.
type HypergeometricResult struct {
	Probability         float64 `json:"probability"`
	ProbabilityFormatted string `json:"probabilityFormatted"`
}

// DeckDrawAnalysisRequest is input for deck-aware draw odds.
type DeckDrawAnalysisRequest struct {
	DeckID           int64  `json:"deckId"`
	FormatTarget     string `json:"formatTarget"` // "standard" or "commander"
	SampleSize       int    `json:"sampleSize"`
	OracleID         string `json:"oracleId"`
	MinCardCopies    int    `json:"minCardCopies"`
	MinLands         int    `json:"minLands"`
	LandsInDeck      int    `json:"landsInDeck"` // 0 = auto-detect from type_line
}

// DeckDrawAnalysisResult is deck-aware calculator output.
type DeckDrawAnalysisResult struct {
	PopulationN          int    `json:"populationN"`
	DeckTotal            int    `json:"deckTotal"`
	TargetSize           int    `json:"targetSize"`
	DetectedLands        int    `json:"detectedLands"`
	EffectiveLandsK      int    `json:"effectiveLandsK"`
	EffectiveSampleSize  int    `json:"effectiveSampleSize"`
	CardProbability      float64 `json:"cardProbability"`
	CardProbabilityFormatted string `json:"cardProbabilityFormatted"`
	LandProbability      float64 `json:"landProbability"`
	LandProbabilityFormatted string `json:"landProbabilityFormatted"`
	SizeWarning          string `json:"sizeWarning,omitempty"`
}

// SimulationCard is one physical card slot in hand or library.
type SimulationCard struct {
	SlotID        string `json:"slotId"`
	OracleID      string `json:"oracleId"`
	Name          string `json:"name"`
	IsLand        bool   `json:"isLand"`
	TypeLine      string `json:"typeLine,omitempty"`
	ImageSmall    string `json:"imageSmall,omitempty"`
	ImageNormal   string `json:"imageNormal,omitempty"`
	ColorIdentity []string `json:"colorIdentity,omitempty"`
}

// DrawStats are conditional probabilities for the current simulation state.
type DrawStats struct {
	LandsInHand                    int     `json:"landsInHand"`
	LibraryRemaining               int     `json:"libraryRemaining"`
	NextDrawLandProb               float64 `json:"nextDrawLandProb"`
	NextDrawLandProbFormatted      string  `json:"nextDrawLandProbFormatted"`
	NextDrawCardProb               float64 `json:"nextDrawCardProb"`
	NextDrawCardProbFormatted      string  `json:"nextDrawCardProbFormatted"`
	OracleIDUsed                   string  `json:"oracleIdUsed,omitempty"`
	AfterOneDrawLandsProb          float64 `json:"afterOneDrawLandsProb"`
	AfterOneDrawLandsProbFormatted string  `json:"afterOneDrawLandsProbFormatted"`
	MinLandsThreshold              int     `json:"minLandsThreshold"`
}

// SimulationState is returned after each simulation action.
type SimulationState struct {
	SessionID      string           `json:"sessionId"`
	Phase          string           `json:"phase"` // "playing" or "awaiting_bottom"
	Hand           []SimulationCard `json:"hand"`
	LibraryCount   int              `json:"libraryCount"`
	MulliganCount  int              `json:"mulliganCount"`
	CanMulligan    bool             `json:"canMulligan"`
	CanDraw        bool             `json:"canDraw"`
	Stats          DrawStats        `json:"stats"`
	DeckID         int64            `json:"deckId"`
	FormatTarget   string           `json:"formatTarget"`
}
