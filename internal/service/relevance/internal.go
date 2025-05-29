package relevance

import "math"

func (s *RelevanceService) logarithmicNirmalixation(x float64) float64 {
	if x == 0 {
		return 0
	}
	return math.Log(1 + x)
}

func (s *RelevanceService) timeNormalization(time float64) float64 {
	if time == 0 {
		return 1
	}
	return math.Exp(-s.config.Phy() * time)
}
