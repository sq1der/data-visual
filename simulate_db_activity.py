import psycopg2
import random, time

print("Simulating PostgreSQL activity...")

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="0909",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

while True:
    try:
        # Простые SELECT-запросы (нагрузка на чтение)
        for _ in range(random.randint(5, 15)):
            cur.execute("SELECT pg_sleep(0.1);")  # короткие паузы
            cur.execute("SELECT random();")

        # Имитация записи данных
        cur.execute("CREATE TABLE IF NOT EXISTS load_test (id serial PRIMARY KEY, value float);")
        cur.execute("INSERT INTO load_test (value) VALUES (%s)", (random.random(),))
        conn.commit()

        print("✅ Inserted data & executed queries")

        # Ждём немного, затем повторяем
        time.sleep(random.uniform(5, 15))

    except Exception as e:
        print("DB error:", e)
        conn.rollback()
