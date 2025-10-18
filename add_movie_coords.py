# add_movie_coords.py
import hashlib, struct
from sqlalchemy import text
import os, math
import config

engine = config.get_engine()

def movie_hash_to_latlon(movieid):
    h = hashlib.md5(str(movieid).encode()).digest()
    # получаем 2 int32 из хеша
    a, b = struct.unpack_from('>ii', h)
    # масштабируем в диапазон широт/долгот
    lat = (a % 1800000) / 10000.0 - 90.0    # -90..+90
    lon = (b % 3600000) / 10000.0 - 180.0   # -180..+180
    return round(lat, 6), round(lon, 6)

with engine.begin() as conn:
    conn.execute(text("ALTER TABLE movies ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION"))
    conn.execute(text("ALTER TABLE movies ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION"))
    rows = conn.execute(text("SELECT movieid FROM movies WHERE latitude IS NULL OR longitude IS NULL")).fetchall()
    for r in rows:
        m = r[0]
        lat, lon = movie_hash_to_latlon(m)
        conn.execute(text("UPDATE movies SET latitude = :lat, longitude = :lon WHERE movieid = :m"),
                     {"lat": lat, "lon": lon, "m": m})
    print("Filled coords for", len(rows), "movies")
