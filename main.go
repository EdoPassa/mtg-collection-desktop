package main

import (
	"embed"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:cmd/mtg-collection/assets
var assets embed.FS

func main() {
	app, cleanup, err := bootstrap()
	if err != nil {
		panic(err)
	}
	defer cleanup()

	err = wails.Run(&options.App{
		Title:  "MTG Collection",
		Width:  1200,
		Height: 800,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		OnStartup: app.Startup,
		Bind:      []interface{}{app},
	})
	if err != nil {
		panic(err)
	}
}
