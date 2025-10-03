-- name: pie_genres_since_2000
-- Вопрос: Какие жанры чаще всего получают оценки для фильмов, вышедших с 2000 года?
SELECT
  genre,
  COUNT(DISTINCT m.movieid) AS movie_count
FROM movies m
JOIN ratings r ON m.movieid = r.movieid
JOIN links l ON l.movieid = m.movieid            -- второй JOIN (требование min 2 JOIN)
CROSS JOIN LATERAL unnest(string_to_array(m.genres, '|')) AS genre
WHERE title ~ '\(\d{4}\)' 
  AND regexp_replace(title, '.*\((\d{4})\).*', '\1')::INT >= 2000
GROUP BY genre
ORDER BY movie_count DESC;

-- name: bar_top_movies_with_genome_tags
-- Вопрос: Какие 10 фильмов получили наибольшее внимание зрителей (больше 1000 оценок)?
-- Также показываем топовые genome-теги для каждого фильма.
WITH movie_ratings AS (
    SELECT
      m.movieid,
      m.title,
      COUNT(r.rating) AS total_ratings,
      ROUND(AVG(r.rating),2) AS avg_rating
    FROM ratings r
    JOIN movies m ON r.movieid = m.movieid
    GROUP BY m.movieid, m.title
    HAVING COUNT(r.rating) > 1000
),
tag_ranked AS (
    SELECT
      gs.movieid,
      gt.tag,
      ROW_NUMBER() OVER (PARTITION BY gs.movieid ORDER BY gs.relevance DESC) AS rn
    FROM genome_scores gs
    JOIN genome_tags gt ON gs.tagid = gt.tagid
)
SELECT
    mr.movieid,
    mr.title,
    mr.total_ratings,
    mr.avg_rating,
    STRING_AGG(tr.tag, ', ') AS top_genome_tags
FROM movie_ratings mr
LEFT JOIN tag_ranked tr ON mr.movieid = tr.movieid AND tr.rn <= 5   -- топ-5 тегов
GROUP BY mr.movieid, mr.title, mr.total_ratings, mr.avg_rating
ORDER BY mr.total_ratings DESC
LIMIT 10;

-- name: barh_genres_avg_rating_with_tagstats
-- Вопрос: Какие жанры получают наивысший средний рейтинг? (только жанры с >50 фильмов)
-- Также считаем среднее количество пользовательских тегов на фильм в жанре.

WITH movie_stats AS (
    -- 1. Вычисляем средний рейтинг и количество оценок для КАЖДОГО фильма.
    SELECT
        movieid,
        ROUND(AVG(rating), 2) AS movie_avg_rating,
        COUNT(rating) AS rating_count
    FROM ratings
    GROUP BY movieid
),
movie_tag_stats AS (
    -- 2. Вычисляем количество пользовательских тегов для КАЖДОГО фильма (CTE с ранней агрегацией)
    SELECT movieid, COUNT(*) AS user_tag_count
    FROM tags
    GROUP BY movieid
)
SELECT
    genre,
    -- Средний рейтинг жанра: усредняем средние рейтинги фильмов в жанре
    ROUND(AVG(ms.movie_avg_rating)::numeric, 2) AS avg_rating,
    COUNT(m.movieid) AS num_movies,
    -- Среднее количество тегов на фильм: усредняем количество тегов фильмов в жанре
    ROUND(AVG(COALESCE(mts.user_tag_count, 0))::numeric, 2) AS avg_user_tags_per_movie
FROM movies m
-- Денормализация жанров
CROSS JOIN LATERAL unnest(string_to_array(m.genres, '|')) AS genre
-- Присоединяем статистику рейтинга
JOIN movie_stats ms ON m.movieid = ms.movieid
-- Присоединяем статистику тегов (используем LEFT JOIN, если у фильма нет тегов)
LEFT JOIN movie_tag_stats mts ON m.movieid = mts.movieid
GROUP BY genre
HAVING COUNT(m.movieid) > 50
ORDER BY avg_rating DESC;

-- name: line_ratings_per_year_with_links
-- Вопрос: Как менялась активность зрителей (число оценок) по годам для фильмов, у которых есть внешние ссылки (tmdb/imdb)?
SELECT
  date_part('year', to_timestamp(r.timestamp))::INT AS year,
  COUNT(*) AS ratings_count,
  COUNT(DISTINCT m.movieid) AS distinct_movies_rated
FROM ratings r
JOIN movies m ON r.movieid = m.movieid
JOIN links l ON l.movieid = m.movieid   -- второй JOIN
WHERE date_part('year', to_timestamp(r.timestamp)) >= 1995
GROUP BY year
ORDER BY year;

-- name: hist_avg_movie_ratings_with_genome
-- Вопрос: Как распределяются средние рейтинги фильмов (берём фильмы с >=5 оценками) и какая у них средняя "genome relevance"?
WITH movie_ratings AS (
    SELECT
        movieid,
        ROUND(AVG(rating),2) AS avg_rating,
        COUNT(*) AS rating_count
    FROM ratings
    GROUP BY movieid
    HAVING COUNT(*) >= 5
),
movie_genomes AS (
    SELECT
        movieid,
        ROUND(AVG(relevance),3) AS avg_genome_relevance
    FROM genome_scores
    GROUP BY movieid
)
SELECT
    m.movieid,
    m.title,
    r.avg_rating,
    g.avg_genome_relevance
FROM movies m
JOIN movie_ratings r ON m.movieid = r.movieid
LEFT JOIN movie_genomes g ON m.movieid = g.movieid;


-- name: scatter_popularity_quality_with_tagcount
-- Вопрос: Есть ли связь между популярностью (кол-во оценок), средней оценкой и количеством пользовательских тегов?

WITH movie_ratings AS (
    -- 1. Агрегируем данные по рейтингам для каждого фильма.
    SELECT
        movieid,
        COUNT(rating) AS total_ratings,
        ROUND(AVG(rating), 2) AS avg_rating
    FROM ratings
    GROUP BY movieid
    HAVING COUNT(rating) > 200
),
movie_tags AS (
    -- 2. Агрегируем количество тегов для каждого фильма.
    SELECT
        movieid,
        COUNT(DISTINCT tag_id) AS num_user_tags
    FROM tags
    GROUP BY movieid
)
-- 3. Соединяем агрегированные данные с таблицей movies.
SELECT
    m.movieid,
    m.title,
    mr.total_ratings,
    mr.avg_rating,
    COALESCE(mt.num_user_tags, 0) AS num_user_tags
FROM movie_ratings mr
JOIN movies m ON mr.movieid = m.movieid
LEFT JOIN movie_tags mt ON mr.movieid = mt.movieid
ORDER BY mr.total_ratings DESC;

-- name: plotly_genre_year_trend
-- Вопрос (для анимации): как по годам менялся интерес к жанрам (количество оценок по жанрам и годам)
SELECT
  date_part('year', to_timestamp(r.timestamp))::INT AS year,
  genre,
  COUNT(*) AS ratings_count,
  ROUND(AVG(r.rating),2) AS avg_rating
FROM ratings r
JOIN movies m ON r.movieid = m.movieid
JOIN links l ON l.movieid = m.movieid    -- второй JOIN
CROSS JOIN LATERAL unnest(string_to_array(m.genres, '|')) AS genre
WHERE date_part('year', to_timestamp(r.timestamp)) BETWEEN 1995 AND 2015
GROUP BY year, genre
ORDER BY year, ratings_count DESC;
