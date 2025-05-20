package relevance

import (
	config "agregator/relevance/internal/config"
	model "agregator/relevance/internal/model/db"
	"context"
	"time"
)

type RelevanceService struct {
	config *config.Config
	input  chan (*model.GroupRelevanceMetrics)
	output chan (*model.GroupRelevanceMetrics)
}

func New() *RelevanceService {
	return &RelevanceService{
		config: config.New(),
		input:  make(chan *model.GroupRelevanceMetrics, 1000),
		output: make(chan *model.GroupRelevanceMetrics, 1000),
	}
}

func (s *RelevanceService) Calculate(metrics *model.GroupRelevanceMetrics) {
	article_velocity := float64(metrics.RecentArticleCount) / time.Hour.Seconds()

	group_momentum := s.timeNormalization(article_velocity, metrics.TimeSinceLastArticleSeconds)

	age_decay := s.timeNormalization(1, metrics.GroupAgeSeconds)

	relevance_score := (s.config.W1()*s.logarithmicNirmalixation(float64(metrics.ArticleCount)) + s.config.W2()*s.logarithmicNirmalixation(float64(metrics.DistinctSourceCount)) +
		s.config.W3()*s.logarithmicNirmalixation(group_momentum) + s.config.W4()*s.logarithmicNirmalixation(metrics.AverageSourceRelevance)) * age_decay

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
