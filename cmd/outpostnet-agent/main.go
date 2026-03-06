package main

import (
	"os"

	"github.com/outpostos/OutpostNet/cmd/outpostnet-agent/app"
)

func main() {
	command := app.NewAgentCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
