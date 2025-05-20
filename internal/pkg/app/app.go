package app

import (
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

func (a *App) Run() {
	db, err := db.New(5)
	if err != nil {
		log.Fatal(err)
	}
	relevanceService := relevance.New()
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
		db.StartUpdating(output)
	}()
	go func() {
		defer wg.Done()
		relevanceService.Run()
	}()
	wg.Wait()
	cancel()
}
