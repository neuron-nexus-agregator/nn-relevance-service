package db

type GroupRelevanceMetrics struct {
	GroupID                     int     `db:"group_id"`                        // ID группы
	ArticleCount                int     `db:"article_count"`                   // Общее количество статей в группе
	DistinctSourceCount         int     `db:"distinct_source_count"`           // Количество уникальных источников
	RecentArticleCount          int     `db:"recent_article_count"`            // Количество статей за последний час
	AverageSourceRelevance      float64 `db:"average_source_relevance"`        // Средняя релевантность источников
	TimeSinceLastArticleSeconds float64 `db:"time_since_last_article_seconds"` // Время с момента последней статьи в секундах
	GroupAgeSeconds             float64 `db:"group_age_seconds"`               // Возраст группы в секундах
	Views                       int     `db:"views"`
	CalculatedRelevanceScore    float64 `db:"calculated_relevance_score"` // Рассчитанная оценка актуальности (может быть 0 для старых групп)
}
