package scryfall

import (
	"fmt"
	"net/http"

	"mtgcollection/internal/version"
)

func DefaultUserAgent() string {
	return fmt.Sprintf("MTGCollectionDesktop/%s", version.Version)
}

func applyAPIHeaders(req *http.Request, userAgent string) {
	if userAgent == "" {
		userAgent = DefaultUserAgent()
	}
	req.Header.Set("Accept", "application/json;q=0.9,*/*;q=0.8")
	req.Header.Set("User-Agent", userAgent)
}
