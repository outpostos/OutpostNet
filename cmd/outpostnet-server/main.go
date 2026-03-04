package main

import (
	"os"

	"github.com/outpostos/OutpostNet/cmd/outpostnet-server/app"
)

func main() {
	command := app.NewServerCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
