-- 1. Средний рейтинг фильмов по каждому году выпуска
SELECT 
  CASE 
    WHEN title ~ '\(\d{4}\)' 
    THEN regexp_replace(title, '.*\((\d{4})\).*', '\1')::INT 
  END AS release_year,
  AVG(rating) AS avg_rating,
  COUNT(*) AS num_ratings
FROM ratings r
JOIN movies m ON r.movieid = m.movieid
GROUP BY release_year
ORDER BY release_year;
-- считает средний рейтинг по годам выпуска

-- 2. Топ-10 фильмов по количеству оценок
SELECT 
  m.title,
  COUNT(r.rating) AS total_ratings,
  ROUND(AVG(r.rating),2) AS avg_rating
FROM ratings r
JOIN movies m ON r.movieid = m.movieid
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;
-- выводит 10 фильмов с наибольшим числом оценок

-- 3. Самые популярные жанры (парсинг genres)
SELECT 
  unnest(string_to_array(m.genres, '|')) AS genre,
  COUNT(r.rating) AS total_ratings
FROM ratings r
JOIN movies m ON r.movieid = m.movieid
GROUP BY genre
ORDER BY total_ratings DESC;
-- считает количество оценок по каждому жанру

-- 4. Среднее количество тегов на фильм
SELECT 
  AVG(tags_count) AS avg_tags_per_movie
FROM (
  SELECT movieid, COUNT(*) AS tags_count
  FROM tags
  GROUP BY movieid
) t;
-- среднее количество тегов на фильм

-- 5. Пользователи с наибольшей активностью (по ratings)
SELECT 
  userid,
  COUNT(*) AS ratings_count,
  ROUND(AVG(rating),2) AS avg_rating
FROM ratings
GROUP BY userid
ORDER BY ratings_count DESC
LIMIT 10;
-- 10 самых активных пользователей по количеству оценок

-- 6. Топ-теги по релевантности для конкретного фильма (пример: movieId=1)
SELECT 
  gt.tag,
  gs.relevance
FROM genome_scores gs
JOIN genome_tags gt ON gs.tagid = gt.tagid
WHERE gs.movieid = 1
ORDER BY gs.relevance DESC
LIMIT 10;
-- 10 тегов с наибольшей релевантностью для фильма с movieId=1

-- 7. Средняя релевантность тегов по жанрам
SELECT 
  genre,
  ROUND(AVG(gs.relevance),3) AS avg_relevance
FROM genome_scores gs
JOIN genome_tags gt ON gs.tagid = gt.tagid
JOIN movies m ON gs.movieid = m.movieid
CROSS JOIN LATERAL unnest(string_to_array(m.genres, '|')) AS genre
GROUP BY genre
ORDER BY avg_relevance DESC;
-- средняя релевантность тегов для каждого жанра

-- 8. Средняя длина названия фильма по жанрам
SELECT 
    genre,
    AVG(LENGTH(title)) AS avg_title_length
FROM movies
CROSS JOIN LATERAL unnest(string_to_array(genres, '|')) AS genre
GROUP BY genre
ORDER BY avg_title_length DESC;
-- показывает среднюю длину названия фильмов по жанрам

-- 9. Динамика количества оценок по годам
SELECT 
  date_part('year', to_timestamp(r.timestamp))::INT AS year,
  COUNT(*) AS ratings_count
FROM ratings r
GROUP BY year
ORDER BY year;
-- количество оценок по годам (по timestamp из ratings)

-- 10. Как изменяется средняя оценка одного пользователя по времени (пример: userid=1)
SELECT 
  date_trunc('month', to_timestamp(r.timestamp)) AS month,
  ROUND(AVG(r.rating),2) AS avg_rating
FROM ratings r
WHERE userid = 1
GROUP BY month
ORDER BY month;
-- динамика средней оценки пользователя (помесячно)