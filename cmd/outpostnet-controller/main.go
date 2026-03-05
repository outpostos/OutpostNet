// Package main is the entrypoint for outpostnet-controller.
package main

import (
	"os"

	"github.com/outpostos/OutpostNet/cmd/outpostnet-controller/app"
)

func main() {
	command := app.NewControllerCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
