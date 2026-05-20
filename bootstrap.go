package main

import (
	"context"

	appsvc "mtgcollection/internal/app"
	"mtgcollection/internal/appdata"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/scryfall"
	"mtgcollection/internal/storage"
)

func bootstrapConfig() appdata.Config {
	return appdata.Config{Mode: appdata.Packaged}
}

func bootstrap() (*appsvc.App, func(), error) {
	paths := appdata.ResolvePaths(bootstrapConfig())
	store, err := storage.Open(paths.DatabasePath)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() { _ = store.Close() }
	api := scryfall.NewClient(scryfall.Options{})
	var cardResolver resolver.Resolver = resolver.NewAPIOnly(api)
	status := "api-only"
	bulkPaths, err := scryfall.EnsureOracleBulkDownloaded(context.Background(), scryfall.BulkOptions{
		Paths: scryfall.BulkCachePaths{RootDir: paths.ScryfallDir},
	})
	if err == nil {
		if index, idxErr := resolver.BuildBulkOracleIndex(bulkPaths.DataPath); idxErr == nil {
			cardResolver = resolver.NewBulkFirst(index, api)
			status = "bulk-first"
		}
	}
	return appsvc.New(store, cardResolver, status), cleanup, nil
}
