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

type CollectionTag struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Color     string `json:"color,omitempty"`
	CardCount int    `json:"cardCount,omitempty"`
}

type CollectionItem struct {
	Card          CardIdentity    `json:"card"`
	Quantity      int             `json:"quantity"`
	LentQty       int             `json:"lentQty"`
	InDeck        bool            `json:"inDeck"`
	Available     int             `json:"available"`
	AllocatedQty  int             `json:"allocatedQty,omitempty"`
	UnassignedQty int             `json:"unassignedQty,omitempty"`
	Tags          []CollectionTag `json:"tags,omitempty"`
}

type CollectionFolder struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	ParentID *int64 `json:"parentId,omitempty"`
}

type FolderCard struct {
	Card      CardIdentity    `json:"card"`
	Quantity  int             `json:"quantity"`
	LentQty   int             `json:"lentQty,omitempty"`
	InDeck    bool            `json:"inDeck,omitempty"`
	Available int             `json:"available,omitempty"`
	Tags      []CollectionTag `json:"tags,omitempty"`
}

type FolderAllocation struct {
	FolderID   int64  `json:"folderId"`
	FolderName string `json:"folderName"`
	Quantity   int    `json:"quantity"`
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
