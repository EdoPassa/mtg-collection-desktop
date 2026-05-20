package scryfall

type Card struct {
	OracleID    string `json:"oracleId"`
	Name        string `json:"name"`
	ScryfallURI string `json:"scryfallUri"`
}

type Error struct {
	Message string
}

func (e Error) Error() string {
	return e.Message
}
