package main

import (
	"os"

	"github.com/outpostos/OutpostNet/cmd/outpostnet-sidecar/app"
)

func main() {
	command := app.NewSidecarCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
