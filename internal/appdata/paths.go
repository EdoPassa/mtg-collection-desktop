package appdata

import (
	"io"
	"os"
	"path/filepath"
)

type Mode string

const (
	Development Mode = "development"
	Packaged    Mode = "packaged"
)

type Config struct {
	Mode        Mode
	WorkingDir  string
	AppDataRoot string
}

type Paths struct {
	DatabasePath string
	ScryfallDir  string
}

type MigrationResult struct {
	DatabaseCopied bool
	ScryfallCopied bool
}

func ResolvePaths(config Config) Paths {
	workingDir := config.WorkingDir
	if workingDir == "" {
		workingDir, _ = os.Getwd()
	}
	if config.Mode == "" || config.Mode == Development {
		return Paths{
			DatabasePath: filepath.Join(workingDir, "data", "collection.sqlite3"),
			ScryfallDir:  filepath.Join(workingDir, "data", "scryfall"),
		}
	}
	root := config.AppDataRoot
	if root == "" {
		if data, err := os.UserConfigDir(); err == nil {
			root = data
		} else {
			root = workingDir
		}
	}
	appRoot := filepath.Join(root, "MTG Collection")
	return Paths{
		DatabasePath: filepath.Join(appRoot, "collection.sqlite3"),
		ScryfallDir:  filepath.Join(appRoot, "scryfall"),
	}
}

func MigrateRepoData(repoRoot string, destinationRoot string) (MigrationResult, error) {
	var result MigrationResult
	if err := os.MkdirAll(destinationRoot, 0o755); err != nil {
		return result, err
	}
	sourceDB := filepath.Join(repoRoot, "data", "collection.sqlite3")
	destinationDB := filepath.Join(destinationRoot, "collection.sqlite3")
	if fileExists(sourceDB) && !fileExists(destinationDB) {
		if err := copyFile(sourceDB, destinationDB); err != nil {
			return result, err
		}
		result.DatabaseCopied = true
	}
	sourceCache := filepath.Join(repoRoot, "data", "scryfall")
	destinationCache := filepath.Join(destinationRoot, "scryfall")
	if fileExists(sourceCache) && !fileExists(destinationCache) {
		if err := copyDir(sourceCache, destinationCache); err != nil {
			return result, err
		}
		result.ScryfallCopied = true
	}
	return result, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func copyDir(source string, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		return copyFile(path, target)
	})
}

func copyFile(source string, destination string) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer output.Close()
	_, err = io.Copy(output, input)
	return err
}
