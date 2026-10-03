package main

import (
	"os"

	"github.com/outpostos/OutpostNet/cmd/outpostnet-service/app"
)

func main() {
	if err := app.NewServiceCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
