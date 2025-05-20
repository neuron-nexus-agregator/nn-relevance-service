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
	WITH ArticleCounts AS (
		-- CTE для подсчета общего количества статей и уникальных источников
		SELECT
			group_id,
			COUNT(feed_id) AS article_count,
			COUNT(DISTINCT f.source_name) AS distinct_source_count
		FROM Compares c
		JOIN Feed f ON c.feed_id = f.id
		GROUP BY group_id
	),
	RecentArticleCounts AS (
		-- CTE для подсчета количества статей за последний час
		SELECT
			group_id,
			COUNT(feed_id) AS recent_article_count
		FROM Compares c
		JOIN Feed f ON c.feed_id = f.id
		WHERE f.time >= NOW() - INTERVAL '1 hour' -- Фильтр по времени
		GROUP BY group_id
	),
	SourceRelevance AS (
		-- CTE для вычисления среднего рейтинга источников (используем поле relevance)
		SELECT
			group_id,
			AVG(s.relevance) AS average_source_relevance -- Использование поля relevance
		FROM Compares c
		JOIN Feed f ON c.feed_id = f.id
		JOIN sources s ON f.source_name = s.name
		GROUP BY group_id
	),
	LastArticleTime AS (
		-- CTE для нахождения времени последней статьи в каждой группе
		SELECT
			group_id,
			MAX(f.time) AS last_article_time
		FROM Compares c
		JOIN Feed f ON c.feed_id = f.id
		GROUP BY group_id
	),
	GroupAges AS (
		-- CTE для расчета возраста группы в секундах
		SELECT
			id AS group_id,
			COALESCE(EXTRACT(EPOCH FROM (NOW() - g.time)), 0.0) AS group_age_seconds
		FROM Groups g
	)
	-- Финальный SELECT, объединяющий результаты из всех CTE, добавляющий возраст группы
	-- и рассчитывающий calculated_relevance_score с учетом возраста
	SELECT
		g.id AS group_id,
		COALESCE(ac.article_count, 0) AS article_count, -- Получаем article_count из CTE, COALESCE для групп без статей
		COALESCE(ac.distinct_source_count, 0) AS distinct_source_count, -- Получаем distinct_source_count из CTE
		COALESCE(rac.recent_article_count, 0) AS recent_article_count, -- Получаем recent_article_count из CTE, COALESCE для групп без недавних статей
		COALESCE(sr.average_source_relevance, 0.0) AS average_source_relevance, -- Получаем average_source_relevance из CTE, COALESCE для групп без источников
		-- Время с момента последней статьи в секундах
		COALESCE(EXTRACT(EPOCH FROM (NOW() - lat.last_article_time)), 0.0) AS time_since_last_article_seconds,
		-- Возраст группы в секундах
		ga.group_age_seconds,
		-- Рассчитываем calculated_relevance_score: если возраст >= 24 часов, то 0, иначе -1 (для последующего расчета в Golang)
		CASE
			WHEN ga.group_age_seconds >= 24 * 3600 THEN 0.0
			ELSE -1.0 -- Используем -1.0 как маркер для групп, которым нужно рассчитать реальную оценку в Golang
		END AS calculated_relevance_score
	FROM Groups g
	LEFT JOIN ArticleCounts ac ON g.id = ac.group_id -- Присоединяем метрики общего количества статей
	LEFT JOIN RecentArticleCounts rac ON g.id = rac.group_id -- Присоединяем метрики недавних статей
	LEFT JOIN SourceRelevance sr ON g.id = sr.group_id -- Присоединяем метрики релевантности источников
	LEFT JOIN LastArticleTime lat ON g.id = lat.group_id -- Присоединяем время последней статьи
	JOIN GroupAges ga ON g.id = ga.group_id -- Присоединяем возраст группы
	WHERE
		-- Фильтр: количество статей в группе больше 1
		COALESCE(ac.article_count, 0) > 1
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
	UPDATE Groups
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
