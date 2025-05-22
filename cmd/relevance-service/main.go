package main

import (
	"agregator/relevance/internal/pkg/app"
	"log/slog"
)

func main() {
	app.New().Run(slog.Default())
}
