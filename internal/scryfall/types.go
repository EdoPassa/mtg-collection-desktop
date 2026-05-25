package scryfall

type Card struct {
	OracleID      string   `json:"oracleId"`
	Name          string   `json:"name"`
	ScryfallURI   string   `json:"scryfallUri"`
	TypeLine      string   `json:"typeLine,omitempty"`
	ManaCost      string   `json:"manaCost,omitempty"`
	ColorIdentity []string `json:"colorIdentity,omitempty"`
	ImageSmall    string   `json:"imageSmall,omitempty"`
	ImageNormal   string   `json:"imageNormal,omitempty"`
}

type Error struct {
	Message string
}

func (e Error) Error() string {
	return e.Message
}
