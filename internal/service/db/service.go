package db

import (
	"context"
	"fmt"
	"os"
	"time"

	model "agregator/relevance/internal/model/db"

	"agregator/relevance/internal/interfaces"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DB struct {
	db     *sqlx.DB
	logger interfaces.Logger
}

func New(logger interfaces.Logger) (*DB, error) {
	connectionData := fmt.Sprintf(
		"user=%s dbname=%s sslmode=disable password=%s host=%s port=%s",
		os.Getenv("DB_LOGIN"),
		"newagregator",
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
	)

	conn, err := sqlx.Connect("postgres", connectionData)
	if err != nil {
		logger.Error("Failed to connect to database", "error", err)
		return nil, err
	}

	conn.SetMaxOpenConns(4)
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(5 * time.Minute)

	return &DB{
		db:     conn,
		logger: logger,
	}, nil
}

func (db *DB) GetRelevanceMetrics() ([]*model.GroupRelevanceMetrics, error) {
	db.logger.Info("Getting relevance metrics")
	query := `
	SELECT
    	g.id AS group_id,
    	g.views AS views,
    	COALESCE(COUNT(c.feed_id), 0) AS article_count,
    	COALESCE(COUNT(DISTINCT f.source_name), 0) AS distinct_source_count,
    	COALESCE(COUNT(c.feed_id) FILTER (WHERE f.time >= NOW() - INTERVAL '1 hour'), 0) AS recent_article_count,
    	COALESCE(AVG(s.relevance), 0.0) AS average_source_relevance,
    	COALESCE(EXTRACT(EPOCH FROM (NOW() - MAX(f.time))), 0.0) AS time_since_last_article_seconds,
    	COALESCE(EXTRACT(EPOCH FROM (NOW() - g.time)), 0.0) AS group_age_seconds,
    	CASE
        	WHEN COALESCE(EXTRACT(EPOCH FROM (NOW() - g.time)), 0.0) >= 24 * 3600 THEN 0.0
        	ELSE -1.0
    	END AS calculated_relevance_score
	FROM Groups g
	LEFT JOIN Compares c ON g.id = c.group_id
	LEFT JOIN Feed f ON c.feed_id = f.id
	LEFT JOIN Sources s ON f.source_name = s.name
	WHERE
    	g.time >= NOW() - INTERVAL '24 hour'
	GROUP BY g.id, g.views, g.time -- Добавлено g.time в GROUP BY
	HAVING COALESCE(COUNT(c.feed_id), 0) > 1;
	`

	// Выполнение запроса
	rows, err := db.db.Query(query)
	if err != nil {
		// Логируем ошибку и возвращаем ее
		db.logger.Error("Ошибка выполнения запроса метрик актуальности", "error", err)
		return nil, fmt.Errorf("ошибка выполнения запроса метрик актуальности: %w", err)
	}
	defer rows.Close() // Обязательно закрываем rows после использования

	var metrics []*model.GroupRelevanceMetrics

	// Итерация по результатам запроса
	for rows.Next() {
		var m model.GroupRelevanceMetrics
		// Сканирование колонок текущей строки в поля структуры
		err := rows.Scan(
			&m.GroupID,
			&m.Views,
			&m.ArticleCount,
			&m.DistinctSourceCount,
			&m.RecentArticleCount,
			&m.AverageSourceRelevance,
			&m.TimeSinceLastArticleSeconds,
			&m.GroupAgeSeconds,
			&m.CalculatedRelevanceScore, // Сканируем новое поле
		)
		if err != nil {
			// Логируем ошибку сканирования и возвращаем ее
			db.logger.Error("Ошибка сканирования строки результатов", "error", err)
			return nil, fmt.Errorf("ошибка сканирования строки результатов: %w", err)
		}
		metrics = append(metrics, &m) // Добавляем полученную структуру в слайс
	}

	// Проверка на ошибки после завершения итерации
	if err = rows.Err(); err != nil {
		// Логируем ошибку итерации и возвращаем ее
		db.logger.Error("Ошибка при итерации по результатам запроса", "error", err)
		return nil, fmt.Errorf("ошибка при итерации по результатам запроса: %w", err)
	}
	db.logger.Info("Получены метрики актуальности", "metrics", len(metrics))
	return metrics, nil // Возвращаем полученные метрики
}

func (db *DB) UpdateRelevance(metrics *model.GroupRelevanceMetrics) error {
	db.logger.Info("Обновление метрик актуальности", "id", metrics.GroupID, "relevance", metrics.CalculatedRelevanceScore, "metrcis", metrics)
	query := `
	UPDATE groups
	SET
		relevance_score = $1
	WHERE
		id = $2
	`
	_, err := db.db.Exec(query, metrics.CalculatedRelevanceScore, metrics.GroupID)
	if err != nil {
		db.logger.Error("Ошибка обновления метрик актуальности", "error", err)
		return err
	}
	return nil
}

func (db *DB) MakeRelevanceZero() error {
	query := `
	UPDATE groups
	SET relevance_score = 0
	WHERE time < NOW() - INTERVAL '24 hour'`
	_, err := db.db.Exec(query)
	if err != nil {
		db.logger.Error("Ошибка обнуления метрик актуальности", "error", err)
		return err
	}
	return nil
}

func (db *DB) UpdateRelevanceBatch(metrics []*model.GroupRelevanceMetrics) error {
	if len(metrics) == 0 {
		return nil
	}
	for _, met := range metrics {
		err := db.UpdateRelevance(met)
		if err != nil {
			db.logger.Error("Ошибка обновления метрик актуальности", "error", err)
			return err
		}
	}
	return nil
}

func (db *DB) update(input chan<- *model.GroupRelevanceMetrics, ctx context.Context) {
	metrics, err := db.GetRelevanceMetrics()
	if err != nil {
		db.logger.Error("Ошибка получения метрик актуальности", "error", err)
		return
	}
	for _, met := range metrics {
		select {
		case input <- met:
		case <-ctx.Done():
			db.logger.Info("Stopped due to context done")
			return
		}

	}
}

func (db *DB) StartReading(input chan<- *model.GroupRelevanceMetrics, ctx context.Context) {
	db.update(input, ctx)
	ticker := time.NewTicker(10 * time.Minute)
	day := time.NewTicker(getDurationUntilNextDayMidnight())

	defer ticker.Stop()
	defer day.Stop()

	for {
		select {
		case <-ticker.C:
			db.update(input, ctx)
		case <-day.C:
			err := db.MakeRelevanceZero()
			if err != nil {
				db.logger.Error("Ошибка обнуления метрик", "error", err)
				continue
			}
			day.Reset(getDurationUntilNextDayMidnight())
		case <-ctx.Done():
			db.logger.Info("Stopped due to context done")
			return
		}
	}
}

func (db *DB) StartUpdating(input <-chan *model.GroupRelevanceMetrics, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			db.logger.Info("Stopped due to context done")
			return
		case met, ok := <-input:
			if !ok {
				return
			}
			err := db.UpdateRelevance(met)
			if err != nil {
				db.logger.Error("Ошибка обновления метрик", "error", err)
				continue
			}
		}
	}
}

func getDurationUntilNextDayMidnight() time.Duration {
	now := time.Now()
	tomorrow := now.Add(24 * time.Hour).Truncate(24 * time.Hour)
	return tomorrow.Sub(now)
}
