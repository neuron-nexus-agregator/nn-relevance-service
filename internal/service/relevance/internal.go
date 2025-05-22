package relevance

import "math"

func (s *RelevanceService) logarithmicNirmalixation(x float64) float64 {
	if x == 0 {
		return 0
	}
	return math.Log(1 + x)
}

func (s *RelevanceService) timeNormalization(x float64, time float64) float64 {
	if x == 0 {
		return 0
	}
	if time == 0 {
		return x
	}
	return x * math.Exp(-s.config.Phy()*time)
}
