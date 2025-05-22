package app

import (
	"agregator/relevance/internal/interfaces"
	"agregator/relevance/internal/service/db"
	"agregator/relevance/internal/service/relevance"
	"context"
	"log"
	"sync"
)

type App struct {
}

func New() *App {
	return &App{}
}

func (a *App) Run(logger interfaces.Logger) {
	db, err := db.New(5, logger)
	if err != nil {
		log.Fatal(err)
	}
	relevanceService := relevance.New(logger)
	input := relevanceService.Input()
	output := relevanceService.Output()
	wg := sync.WaitGroup{}
	wg.Add(3)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		db.StartReading(input, ctx)
	}()
	go func() {
		defer wg.Done()
		db.StartUpdating(output, ctx)
	}()
	go func() {
		defer wg.Done()
		relevanceService.Run(ctx)
	}()
	wg.Wait()
	cancel()
}
