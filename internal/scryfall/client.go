package scryfall

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Options struct {
	BaseURL     string
	HTTPClient  *http.Client
	Timeout     time.Duration
	MinInterval time.Duration
	MaxAttempts int
}

type Client struct {
	baseURL     string
	httpClient  *http.Client
	minInterval time.Duration
	maxAttempts int
	mu          sync.Mutex
	lastRequest time.Time
}

func NewClient(opts Options) *Client {
	baseURL := strings.TrimRight(opts.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.scryfall.com"
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	}
	minInterval := opts.MinInterval
	if minInterval == 0 {
		minInterval = 120 * time.Millisecond
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}
	return &Client{baseURL: baseURL, httpClient: httpClient, minInterval: minInterval, maxAttempts: maxAttempts}
}

func (c *Client) LookupNamed(ctx context.Context, name string) (Card, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Card{}, Error{Message: "Empty card name"}
	}
	exact, found, err := c.getCard(ctx, "/cards/named", url.Values{"exact": {name}}, true)
	if err != nil {
		return Card{}, err
	}
	if found {
		return exact, nil
	}
	fuzzy, found, err := c.getCard(ctx, "/cards/named", url.Values{"fuzzy": {name}}, true)
	if err != nil {
		return Card{}, err
	}
	if found {
		return fuzzy, nil
	}
	return Card{}, Error{Message: fmt.Sprintf("No match for: %s", name)}
}

func (c *Client) LookupScryfallID(ctx context.Context, scryfallID string) (Card, error) {
	scryfallID = strings.TrimSpace(scryfallID)
	if scryfallID == "" {
		return Card{}, Error{Message: "Empty Scryfall ID"}
	}
	card, found, err := c.getCard(ctx, "/cards/"+url.PathEscape(scryfallID), nil, false)
	if err != nil {
		return Card{}, err
	}
	if !found {
		return Card{}, Error{Message: "Unexpected missing payload from Scryfall"}
	}
	return card, nil
}

func (c *Client) getCard(ctx context.Context, path string, values url.Values, allow404 bool) (Card, bool, error) {
	payload, found, err := c.requestJSON(ctx, path, values, allow404)
	if err != nil || !found {
		return Card{}, found, err
	}
	return toCard(payload)
}

func (c *Client) requestJSON(ctx context.Context, path string, values url.Values, allow404 bool) (map[string]any, bool, error) {
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if err := c.throttle(ctx); err != nil {
			return nil, false, err
		}
		reqURL := c.baseURL + path
		if len(values) > 0 {
			reqURL += "?" + values.Encode()
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, false, err
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.maxAttempts {
				_ = sleepContext(ctx, backoff(attempt))
				continue
			}
			break
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, false, readErr
		}
		if resp.StatusCode == http.StatusNotFound && allow404 {
			return nil, false, nil
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = Error{Message: fmt.Sprintf("Scryfall HTTP 429: %s", first(body, 2000))}
			if attempt < c.maxAttempts {
				_ = sleepContext(ctx, retryAfter(resp.Header.Get("Retry-After")))
				continue
			}
			break
		}
		if resp.StatusCode >= 500 {
			lastErr = Error{Message: fmt.Sprintf("Scryfall HTTP %d: %s", resp.StatusCode, first(body, 2000))}
			if attempt < c.maxAttempts {
				_ = sleepContext(ctx, backoff(attempt))
				continue
			}
			break
		}
		if resp.StatusCode >= 400 {
			return nil, false, Error{Message: fmt.Sprintf("Scryfall HTTP %d: %s", resp.StatusCode, first(body, 2000))}
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, false, err
		}
		if payload["object"] == "error" {
			if allow404 {
				return nil, false, nil
			}
			return nil, false, Error{Message: stringField(payload, "details")}
		}
		return payload, true, nil
	}
	if lastErr == nil {
		lastErr = Error{Message: "Scryfall request failed"}
	}
	return nil, false, lastErr
}

func (c *Client) throttle(ctx context.Context) error {
	if c.minInterval <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	wait := c.minInterval - time.Since(c.lastRequest)
	if wait > 0 {
		if err := sleepContext(ctx, wait); err != nil {
			return err
		}
	}
	c.lastRequest = time.Now()
	return nil
}

func toCard(payload map[string]any) (Card, bool, error) {
	card := Card{
		OracleID:    stringField(payload, "oracle_id"),
		Name:        stringField(payload, "name"),
		ScryfallURI: stringField(payload, "scryfall_uri"),
	}
	if card.OracleID == "" || card.Name == "" || card.ScryfallURI == "" {
		return Card{}, false, Error{Message: "Unexpected Scryfall payload: missing card identity"}
	}
	return card, true, nil
}

func stringField(payload map[string]any, key string) string {
	value, _ := payload[key].(string)
	return value
}

func retryAfter(header string) time.Duration {
	if header != "" {
		if seconds, err := time.ParseDuration(header + "s"); err == nil {
			return clampDelay(seconds)
		}
	}
	return time.Second + time.Duration(rand.Intn(1000))*time.Millisecond
}

func backoff(attempt int) time.Duration {
	base := 500 * time.Millisecond * (1 << max(0, attempt-1))
	if base > 8*time.Second {
		base = 8 * time.Second
	}
	return base + time.Duration(rand.Intn(250))*time.Millisecond
}

func clampDelay(delay time.Duration) time.Duration {
	if delay < 100*time.Millisecond {
		return delay
	}
	if delay > 60*time.Second {
		return 60 * time.Second
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func first(body []byte, limit int) string {
	if len(body) > limit {
		body = body[:limit]
	}
	return string(body)
}
