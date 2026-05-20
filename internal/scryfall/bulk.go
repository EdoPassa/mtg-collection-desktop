package scryfall

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const DefaultBulkMetadataURL = "https://api.scryfall.com/bulk-data"

type BulkCachePaths struct {
	RootDir  string
	DataPath string
	MetaPath string
}

type BulkOptions struct {
	Paths        BulkCachePaths
	MetadataURL  string
	HTTPClient   *http.Client
	RefreshAfter time.Duration
}

type BulkIdentity struct {
	Card       Card
	ScryfallID string
}

type bulkInfo struct {
	BulkType        string `json:"bulk_type"`
	DownloadURI     string `json:"download_uri"`
	UpdatedAt       string `json:"updated_at"`
	ContentType     string `json:"content_type,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
}

func DefaultBulkCachePaths() BulkCachePaths {
	root := filepath.Join("data", "scryfall")
	return BulkCachePaths{
		RootDir:  root,
		DataPath: filepath.Join(root, "oracle_cards.json"),
		MetaPath: filepath.Join(root, "oracle_cards.meta.json"),
	}
}

func EnsureOracleBulkDownloaded(ctx context.Context, opts BulkOptions) (BulkCachePaths, error) {
	paths := opts.Paths
	if paths.RootDir == "" {
		paths = DefaultBulkCachePaths()
	}
	if paths.DataPath == "" {
		paths.DataPath = filepath.Join(paths.RootDir, "oracle_cards.json")
	}
	if paths.MetaPath == "" {
		paths.MetaPath = filepath.Join(paths.RootDir, "oracle_cards.meta.json")
	}
	if opts.MetadataURL == "" {
		opts.MetadataURL = DefaultBulkMetadataURL
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	refreshAfter := opts.RefreshAfter
	if refreshAfter == 0 {
		refreshAfter = 7 * 24 * time.Hour
	}
	if err := os.MkdirAll(paths.RootDir, 0o755); err != nil {
		return paths, err
	}
	if isFresh(paths.DataPath, paths.MetaPath, refreshAfter) {
		return paths, nil
	}

	info, err := fetchBulkInfo(ctx, client, opts.MetadataURL)
	if err != nil {
		return paths, err
	}
	if err := downloadBulk(ctx, client, info.DownloadURI, &paths); err != nil {
		return paths, err
	}
	meta := map[string]any{
		"bulk_type":        info.BulkType,
		"download_uri":     info.DownloadURI,
		"bulk_updated_at":  info.UpdatedAt,
		"content_type":     info.ContentType,
		"content_encoding": info.ContentEncoding,
		"last_checked_at":  time.Now().UTC().Format(time.RFC3339Nano),
		"data_path":        paths.DataPath,
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return paths, err
	}
	return paths, os.WriteFile(paths.MetaPath, append(data, '\n'), 0o644)
}

func ReadBulkIdentity(path string) ([]BulkIdentity, error) {
	reader, err := openBulkJSON(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	decoder := json.NewDecoder(reader)
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, Error{Message: "Unexpected bulk JSON: expected a list"}
	}
	var out []BulkIdentity
	for decoder.More() {
		var raw map[string]any
		if err := decoder.Decode(&raw); err != nil {
			return nil, err
		}
		card, _, err := toCard(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, BulkIdentity{Card: card, ScryfallID: stringField(raw, "id")})
	}
	return out, nil
}

func fetchBulkInfo(ctx context.Context, client *http.Client, metadataURL string) (bulkInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL, nil)
	if err != nil {
		return bulkInfo{}, err
	}
	applyAPIHeaders(req, DefaultUserAgent)
	resp, err := client.Do(req)
	if err != nil {
		return bulkInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return bulkInfo{}, Error{Message: "Scryfall bulk-data HTTP " + resp.Status + ": " + first(body, 2000)}
	}
	var payload struct {
		Data []struct {
			Type            string `json:"type"`
			DownloadURI     string `json:"download_uri"`
			UpdatedAt       string `json:"updated_at"`
			ContentType     string `json:"content_type"`
			ContentEncoding string `json:"content_encoding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return bulkInfo{}, err
	}
	for _, entry := range payload.Data {
		if entry.Type == "oracle_cards" {
			return bulkInfo{BulkType: entry.Type, DownloadURI: entry.DownloadURI, UpdatedAt: entry.UpdatedAt, ContentType: entry.ContentType, ContentEncoding: entry.ContentEncoding}, nil
		}
	}
	return bulkInfo{}, Error{Message: "Scryfall bulk-data did not include oracle_cards"}
}

func downloadBulk(ctx context.Context, client *http.Client, uri string, paths *BulkCachePaths) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return err
	}
	applyAPIHeaders(req, DefaultUserAgent)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return Error{Message: "Scryfall bulk download HTTP " + resp.Status + ": " + first(body, 2000)}
	}
	tmp := filepath.Join(paths.RootDir, "oracle_cards.download.tmp")
	file, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, resp.Body); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	isGzip, err := fileIsGzip(tmp)
	if err != nil {
		return err
	}
	paths.DataPath = filepath.Join(paths.RootDir, "oracle_cards.json")
	if isGzip {
		paths.DataPath += ".gz"
	}
	return os.Rename(tmp, paths.DataPath)
}

func openBulkJSON(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	head := make([]byte, 2)
	n, _ := file.Read(head)
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	if n == 2 && head[0] == 0x1f && head[1] == 0x8b {
		gz, err := gzip.NewReader(file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return struct {
			io.Reader
			io.Closer
		}{Reader: gz, Closer: multiCloser{gz, file}}, nil
	}
	if strings.HasSuffix(strings.ToLower(path), ".gz") {
		repaired := strings.TrimSuffix(path, ".gz")
		if _, err := os.Stat(repaired); os.IsNotExist(err) {
			_ = file.Close()
			if err := os.Rename(path, repaired); err == nil {
				return os.Open(repaired)
			}
			return os.Open(path)
		}
	}
	return file, nil
}

func isFresh(dataPath string, metaPath string, refreshAfter time.Duration) bool {
	if _, err := os.Stat(dataPath); err != nil {
		return false
	}
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return false
	}
	var meta map[string]any
	if err := json.Unmarshal(data, &meta); err != nil {
		return false
	}
	lastChecked, _ := meta["last_checked_at"].(string)
	checkedAt, err := time.Parse(time.RFC3339Nano, lastChecked)
	if err != nil {
		return false
	}
	return time.Since(checkedAt) < refreshAfter
}

func fileIsGzip(path string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	head := make([]byte, 2)
	n, err := file.Read(head)
	if err != nil && err != io.EOF {
		return false, err
	}
	return n == 2 && head[0] == 0x1f && head[1] == 0x8b, nil
}

type multiCloser []io.Closer

func (m multiCloser) Close() error {
	var err error
	for _, closer := range m {
		if closeErr := closer.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}
