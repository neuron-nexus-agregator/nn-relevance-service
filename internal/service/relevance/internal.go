package relevance

import "math"

func (s *RelevanceService) logarithmicNirmalixation(x float64) float64 {
	return math.Log(1 + x)
}

func (s *RelevanceService) timeNormalization(x float64, time float64) float64 {
	return x * math.Exp(-s.config.Phy()*time)
}
