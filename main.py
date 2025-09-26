import psycopg2
import pandas as pd

# Настройки подключения — замени на свои при необходимости
conn_params = {
    "dbname": "movies_db",
    "user": "amanbelgibay",   # твой PostgreSQL пользователь
    "password": "0909",
    "host": "localhost",
    "port": 5432
}

# SQL-запросы (берём 2 примера: средний рейтинг по годам и топ-10 фильмов)
queries = {
    "avg_rating_by_year": """
        SELECT 
          regexp_replace(title, '.*\\((\\d{4})\\).*', '\\1')::INT AS release_year,
          AVG(rating) AS avg_rating,
          COUNT(*) AS num_ratings
        FROM ratings r
        JOIN movies m ON r.movieid = m.movieid
        WHERE title ~ '\\(\\d{4}\\)'
        GROUP BY release_year
        ORDER BY release_year;
    """,
    "top10_movies_by_ratings_count": """
        SELECT m.title, COUNT(r.rating) AS num_ratings
        FROM ratings r
        JOIN movies m ON r.movieid = m.movieid
        GROUP BY m.title
        ORDER BY num_ratings DESC
        LIMIT 10;
    """
}

def run_query(conn, sql):
    """Выполняет SQL-запрос и возвращает DataFrame"""
    return pd.read_sql(sql, conn)

def main():
    # Подключение к БД
    try:
        conn = psycopg2.connect(**conn_params)
        print("✅ Успешное подключение к БД")

        # Выполняем запросы
        for name, sql in queries.items():
            print(f"\n🔹 Результат запроса: {name}")
            df = run_query(conn, sql)
            print(df.head())  # выводим первые строки в терминал
            # Сохраняем в CSV
            csv_filename = f"{name}.csv"
            df.to_csv(csv_filename, index=False)
            print(f"💾 Сохранено в {csv_filename}")

    except Exception as e:
        print("Ошибка:", e)
    finally:
        if 'conn' in locals():
            conn.close()
            print("🔒 Соединение закрыто")

if __name__ == "__main__":
    main()