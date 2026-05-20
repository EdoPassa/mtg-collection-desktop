package scryfall

import "net/http"

const DefaultUserAgent = "MTGCollectionDesktop/1.0"

func applyAPIHeaders(req *http.Request, userAgent string) {
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	req.Header.Set("Accept", "application/json;q=0.9,*/*;q=0.8")
	req.Header.Set("User-Agent", userAgent)
}
