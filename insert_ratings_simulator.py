# insert_ratings_simulator.py
import time, random, datetime
from sqlalchemy import text
import config

engine = config.get_engine()

def get_random_user_and_movie(conn):
    # если нет таблицы users, используем произвольные userid > max(userid)
    res = conn.execute(text("SELECT min(userid) as min_u, max(userid) as max_u FROM ratings")).fetchone()
    userid = 283228
    movie = conn.execute(text("SELECT movieid FROM movies ORDER BY random() LIMIT 1")).fetchone()
    return userid, int(movie[0])

def insert_rating(conn, userid, movieid, rating):
    ts = int(time.time())
    # Создаём datetime-объект в UTC (без таймзоны — соответствует timestamp without time zone)
    dt = datetime.datetime.utcnow()

    conn.execute(
        text("""
            INSERT INTO ratings (userid, movieid, rating, timestamp, rating_datetime)
            VALUES (:u, :m, :r, :t, :dt)
        """),
        {"u": userid, "m": movieid, "r": rating, "t": ts, "dt": dt}
    )

def main(interval=10, max_inserts=None):
    print("Connecting to DB...")
    with engine.begin() as conn:
        i = 0
        while True:
            with engine.begin() as conn:
                try:
                    # 1. Получаем данные в рамках транзакции
                    userid, movieid = get_random_user_and_movie(conn)
                
                    # 2. Формируем рейтинг и вставляем
                    rating = random.choice([0.5,1.0,1.5,2.0,2.5,3.0,3.5,4.0,4.5,5.0])
                    insert_rating(conn, userid, movieid, rating)
                
                    # 3. Транзакция фиксируется автоматически при выходе из блока 'with'
                    i += 1
                    print(f"Inserted #{i}: userid={userid}, movieid={movieid}, rating={rating}")
                
                except Exception as e:
                    print(f"An error occurred: {e}. Retrying after interval.")

            if max_inserts and i >= max_inserts:
                break
            time.sleep(interval)

if __name__ == "__main__":
    import sys
    inter = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    max_i = int(sys.argv[2]) if len(sys.argv) > 2 else None
    main(interval=inter, max_inserts=max_i)
