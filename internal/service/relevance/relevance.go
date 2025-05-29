package relevance

import (
	config "agregator/relevance/internal/config"
	"agregator/relevance/internal/interfaces"
	model "agregator/relevance/internal/model/db"
	"context"
	"time"
)

type RelevanceService struct {
	config *config.Config
	input  chan (*model.GroupRelevanceMetrics)
	output chan (*model.GroupRelevanceMetrics)
	logger interfaces.Logger
}

func New(logger interfaces.Logger) *RelevanceService {
	return &RelevanceService{
		config: config.New(),
		input:  make(chan *model.GroupRelevanceMetrics, 1000),
		output: make(chan *model.GroupRelevanceMetrics, 1000),
		logger: logger,
	}
}

func (s *RelevanceService) Calculate(metrics *model.GroupRelevanceMetrics) {
	age_decay := s.timeNormalization(metrics.GroupAgeSeconds / time.Hour.Seconds())

	relevance_score := (s.config.W1()*s.logarithmicNirmalixation(float64(metrics.ArticleCount)) + s.config.W2()*s.logarithmicNirmalixation(float64(metrics.DistinctSourceCount)) +
		s.config.W3()*s.logarithmicNirmalixation(float64(metrics.RecentArticleCount)) + s.config.W4()*s.logarithmicNirmalixation(metrics.AverageSourceRelevance) + s.config.W5()*s.logarithmicNirmalixation(float64(metrics.Views))) * age_decay

	metrics.CalculatedRelevanceScore = relevance_score
}

func (s *RelevanceService) Input() chan<- *model.GroupRelevanceMetrics {
	return s.input
}

func (s *RelevanceService) Output() <-chan *model.GroupRelevanceMetrics {
	return s.output
}

func (s *RelevanceService) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Relevance service stopped due to context cancellation")
			return
		case metrics, ok := <-s.input:
			if !ok {
				return
			}
			if metrics.CalculatedRelevanceScore == -1 {
				s.Calculate(metrics)
			}
			s.output <- metrics

		}
	}
}
