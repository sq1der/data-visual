import os
from sqlalchemy import create_engine

def get_database_url():
    user = "postgres"
    password = "0909"
    host = "localhost"
    port = "5432"
    db = "movies_db"
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"

def get_engine():
    url = get_database_url()
    return create_engine(url, pool_pre_ping=True)
