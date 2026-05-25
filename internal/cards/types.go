package cards

type CardIdentity struct {
	OracleID      string   `json:"oracleId"`
	Name          string   `json:"name"`
	ScryfallURI   string   `json:"scryfallUri"`
	TypeLine      string   `json:"typeLine,omitempty"`
	ManaCost      string   `json:"manaCost,omitempty"`
	ColorIdentity []string `json:"colorIdentity,omitempty"`
	ImageSmall    string   `json:"imageSmall,omitempty"`
	ImageNormal   string   `json:"imageNormal,omitempty"`
}

type CollectionItem struct {
	Card      CardIdentity `json:"card"`
	Quantity  int          `json:"quantity"`
	LentQty   int          `json:"lentQty"`
	InDeck    bool         `json:"inDeck"`
	Available int          `json:"available"`
}

type Deck struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

type DeckCard struct {
	Card     CardIdentity `json:"card"`
	Quantity int          `json:"quantity"`
	Board    string       `json:"board,omitempty"`
}

type LentCard struct {
	ID           int64        `json:"id"`
	Card         CardIdentity `json:"card"`
	Quantity     int          `json:"quantity"`
	BorrowerName string       `json:"borrowerName"`
	LentDate     string       `json:"lentDate"`
	ReturnDate   string       `json:"returnDate,omitempty"`
	Notes        string       `json:"notes,omitempty"`
}
