package analysis

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	mathrand "math/rand"
	"sync"

	"mtgcollection/internal/cards"
)

const (
	PhasePlaying        = "playing"
	PhaseAwaitingBottom = "awaiting_bottom"
	openingHandSize     = 7
)

// Simulation holds mutable deck simulation state.
type Simulation struct {
	DeckID        int64
	FormatTarget  string
	Pool          DeckPool
	Library       []poolSlot
	Hand          []poolSlot
	Bottom        []poolSlot
	Phase         string
	MulliganCount int
	OracleFocus   string
	TagFocus      int64
	MinLands      int
	rng           *mathrand.Rand
}

// SessionStore holds in-memory simulation sessions.
type SessionStore struct {
	mu       sync.Mutex
	sessions map[string]*Simulation
	seq      uint64
}

// NewSessionStore creates an empty session store.
func NewSessionStore() *SessionStore {
	return &SessionStore{sessions: make(map[string]*Simulation)}
}

// Create registers a simulation and returns its session id.
func (st *SessionStore) Create(sim *Simulation) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.seq++
	id := fmt.Sprintf("sim-%d", st.seq)
	st.sessions[id] = sim
	return id
}

// Get returns a session by id.
func (st *SessionStore) Get(id string) (*Simulation, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	s, ok := st.sessions[id]
	return s, ok
}

// Delete removes a session.
func (st *SessionStore) Delete(id string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.sessions, id)
}

// NewSimulation builds a simulation from deck rows.
func NewSimulation(deckID int64, formatTarget string, rows []cards.DeckCard, tagsByOracle map[string][]TagRef, oracleFocus string, tagFocus int64, minLands int) *Simulation {
	pool := BuildDeckPool(rows, formatTarget, tagsByOracle)
	lib := append([]poolSlot(nil), pool.Slots...)
	sim := &Simulation{
		DeckID:       deckID,
		FormatTarget: formatTarget,
		Pool:         pool,
		Library:      lib,
		Phase:        PhasePlaying,
		OracleFocus:  oracleFocus,
		TagFocus:     tagFocus,
		MinLands:     minLands,
		rng:          mathrand.New(mathrand.NewSource(randSource())),
	}
	if minLands < 2 {
		sim.MinLands = 2
	}
	return sim
}

// NewSimulationWithRNG is for tests with a fixed seed.
func NewSimulationWithRNG(deckID int64, formatTarget string, rows []cards.DeckCard, seed int64) *Simulation {
	sim := NewSimulation(deckID, formatTarget, rows, nil, "", 0, 2)
	sim.rng = mathrand.New(mathrand.NewSource(seed))
	return sim
}

func randSource() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return mathrand.Int63()
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

func (s *Simulation) allLibrarySlots() []poolSlot {
	out := make([]poolSlot, 0, len(s.Library)+len(s.Bottom))
	out = append(out, s.Library...)
	out = append(out, s.Bottom...)
	return out
}

func (s *Simulation) shuffleLibrary() {
	combined := s.allLibrarySlots()
	for i := len(combined) - 1; i > 0; i-- {
		j := s.rng.Intn(i + 1)
		combined[i], combined[j] = combined[j], combined[i]
	}
	s.Library = combined
	s.Bottom = nil
}

func (s *Simulation) drawN(n int) error {
	if n > len(s.Library) {
		return fmt.Errorf("cannot draw %d cards from library of %d", n, len(s.Library))
	}
	drawn := s.Library[:n]
	s.Library = s.Library[n:]
	s.Hand = append(s.Hand, drawn...)
	return nil
}

// NewOpening shuffles and draws 7.
func (s *Simulation) NewOpening() error {
	s.Hand = nil
	s.Bottom = nil
	s.Library = append([]poolSlot(nil), s.Pool.Slots...)
	s.Phase = PhasePlaying
	s.shuffleLibrary()
	return s.drawN(openingHandSize)
}

// Mulligan shuffles hand back, draws 7, awaits bottom.
func (s *Simulation) Mulligan() error {
	total := len(s.Library) + len(s.Hand) + len(s.Bottom)
	if total < openingHandSize {
		return fmt.Errorf("not enough cards to mulligan")
	}
	s.Library = append(s.Library, s.Hand...)
	s.Hand = nil
	s.shuffleLibrary()
	if err := s.drawN(openingHandSize); err != nil {
		return err
	}
	s.Phase = PhaseAwaitingBottom
	s.MulliganCount++
	return nil
}

// PutOnBottom moves one hand card to the bottom pile.
func (s *Simulation) PutOnBottom(slotID string) error {
	if s.Phase != PhaseAwaitingBottom {
		return fmt.Errorf("not awaiting bottom selection")
	}
	idx := -1
	for i, slot := range s.Hand {
		if slot.slotID == slotID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("card not in hand")
	}
	card := s.Hand[idx]
	s.Hand = append(s.Hand[:idx], s.Hand[idx+1:]...)
	s.Bottom = append(s.Bottom, card)
	s.Phase = PhasePlaying
	return nil
}

// DrawOne draws a single card from library top.
func (s *Simulation) DrawOne() error {
	if s.Phase != PhasePlaying {
		return fmt.Errorf("cannot draw while awaiting bottom selection")
	}
	if len(s.Library) == 0 {
		return fmt.Errorf("library is empty")
	}
	return s.drawN(1)
}

func (s *Simulation) handCards() []SimulationCard {
	out := make([]SimulationCard, len(s.Hand))
	for i, slot := range s.Hand {
		out[i] = slotToSimulationCard(slot)
	}
	return out
}

func (s *Simulation) bottomCards() []SimulationCard {
	out := make([]SimulationCard, len(s.Bottom))
	for i, slot := range s.Bottom {
		out[i] = slotToSimulationCard(slot)
	}
	return out
}

// State builds the API response.
func (s *Simulation) State(sessionID string) SimulationState {
	hand := s.handCards()
	bottom := s.bottomCards()
	stats := computeDrawStats(s.Pool, hand, bottom, s.OracleFocus, s.TagFocus, s.MinLands)

	totalCards := len(s.Library) + len(s.Hand) + len(s.Bottom)
	canMulligan := s.Phase == PhasePlaying && totalCards >= openingHandSize
	canDraw := s.Phase == PhasePlaying && len(s.Library) > 0

	return SimulationState{
		SessionID:     sessionID,
		Phase:         s.Phase,
		Hand:          hand,
		LibraryCount:  len(s.Library),
		MulliganCount: s.MulliganCount,
		CanMulligan:   canMulligan,
		CanDraw:       canDraw,
		Stats:         stats,
		DeckID:        s.DeckID,
		FormatTarget:  s.FormatTarget,
	}
}

// SetOracleFocus updates the card used for next-draw stats.
func (s *Simulation) SetOracleFocus(oracleID string) {
	s.OracleFocus = oracleID
}

// SetTagFocus updates which tag next-draw stats use.
func (s *Simulation) SetTagFocus(tagID int64) {
	s.TagFocus = tagID
}
