package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	model "agregator/relevance/internal/model/db"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DB struct {
	db *sqlx.DB
}

func New(maxConnections int) (*DB, error) {
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
		return nil, err
	}

	conn.SetMaxOpenConns(maxConnections)
	conn.SetMaxIdleConns(maxConnections / 2)
	conn.SetConnMaxLifetime(5 * time.Minute)

	return &DB{db: conn}, nil
}

func (db *DB) GetRelevanceMetrics() ([]*model.GroupRelevanceMetrics, error) {
	query := `
	WITH GroupMetrics AS (
    SELECT
        c.group_id,
        COUNT(c.feed_id) AS article_count,
        COUNT(DISTINCT f.source_name) AS distinct_source_count,
        COUNT(c.feed_id) FILTER (WHERE f.time >= NOW() - INTERVAL '1 hour') AS recent_article_count,
        AVG(COALESCE(s.relevance, 0.0)) AS average_source_relevance,
        MAX(f.time) AS last_article_time
    FROM Compares c
    JOIN Feed f ON c.feed_id = f.id
    LEFT JOIN Sources s ON f.source_name = s.name
    GROUP BY c.group_id
	HAVING COUNT(c.feed_id) > 1
	),
	GroupAges AS (
    	SELECT
        	id AS group_id,
        	COALESCE(EXTRACT(EPOCH FROM (NOW() - g.time)), 0.0) AS group_age_seconds
    	FROM Groups g
	)
	SELECT
    	g.id AS group_id,
    	g.views AS views,
    	COALESCE(gm.article_count, 0) AS article_count,
    	COALESCE(gm.distinct_source_count, 0) AS distinct_source_count,
    	COALESCE(gm.recent_article_count, 0) AS recent_article_count,
    	COALESCE(gm.average_source_relevance, 0.0) AS average_source_relevance,
    	COALESCE(EXTRACT(EPOCH FROM (NOW() - gm.last_article_time)), 0.0) AS time_since_last_article_seconds,
    	ga.group_age_seconds,
    	CASE
    	    WHEN ga.group_age_seconds >= 24 * 3600 THEN 0.0
    	    ELSE -1.0
    	END AS calculated_relevance_score
	FROM Groups g
	LEFT JOIN GroupMetrics gm ON g.id = gm.group_id
	JOIN GroupAges ga ON g.id = ga.group_id
	WHERE
    	COALESCE(gm.article_count, 0) > 1
    	AND g.time >= NOW() - INTERVAL '24 hour'
	`

	// Выполнение запроса
	rows, err := db.db.Query(query)
	if err != nil {
		// Логируем ошибку и возвращаем ее
		log.Printf("Ошибка выполнения запроса метрик актуальности: %v", err)
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
			log.Printf("Ошибка сканирования строки результатов: %v", err)
			return nil, fmt.Errorf("ошибка сканирования строки результатов: %w", err)
		}
		metrics = append(metrics, &m) // Добавляем полученную структуру в слайс
	}

	// Проверка на ошибки после завершения итерации
	if err = rows.Err(); err != nil {
		// Логируем ошибку итерации и возвращаем ее
		log.Printf("Ошибка при итерации по результатам запроса: %v", err)
		return nil, fmt.Errorf("ошибка при итерации по результатам запроса: %w", err)
	}

	return metrics, nil // Возвращаем полученные метрики
}

func (db *DB) UpdateRelevance(metrics *model.GroupRelevanceMetrics) error {
	query := `
	UPDATE groups
	SET
		relevance_scrore = $1
	WHERE
		id = $2
	`
	_, err := db.db.Exec(query, metrics.CalculatedRelevanceScore, metrics.GroupID)
	return err
}

func (db *DB) MakeRelevanceZero() error {
	query := `
	UPDATE groups
	SET relevance_score = 0
	WHERE time < NOW() - INTERVAL '24 hour'`
	_, err := db.db.Exec(query)
	return err
}

func (db *DB) UpdateRelevanceBatch(metrics []*model.GroupRelevanceMetrics) error {
	if len(metrics) == 0 {
		return nil
	}
	for _, met := range metrics {
		err := db.UpdateRelevance(met)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) StartReading(input chan<- *model.GroupRelevanceMetrics, ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	day := time.NewTicker(getDurationUntilNextDayMidnight())

	defer ticker.Stop()
	defer day.Stop()

	for {
		select {
		case <-ticker.C:
			metrics, err := db.GetRelevanceMetrics()
			if err != nil {
				log.Printf("Ошибка получения метрик: %v", err)
				continue
			}
			for _, met := range metrics {
				select {
				case input <- met:
				case <-ctx.Done():
					return
				}

			}
		case <-day.C:
			err := db.MakeRelevanceZero()
			if err != nil {
				log.Printf("Ошибка обнуления метрик: %v", err)
				continue
			}
			day.Reset(getDurationUntilNextDayMidnight())
		case <-ctx.Done():
			return
		}
	}
}

func (db *DB) StartUpdating(input <-chan *model.GroupRelevanceMetrics, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case met, ok := <-input:
			if !ok {
				return
			}
			err := db.UpdateRelevance(met)
			if err != nil {
				log.Printf("Ошибка обновления метрик: %v", err)
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
