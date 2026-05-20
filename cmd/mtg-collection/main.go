//go:build !wails

package main

import (
	"fmt"
	"os"
)

func main() {
	app, cleanup, err := bootstrap()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cleanup()
	fmt.Printf("MTG Collection Go backend ready (resolver=%s)\n", app.ResolverStatus())
}
