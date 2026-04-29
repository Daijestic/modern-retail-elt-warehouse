# ingestion/db.py
from sqlalchemy import create_engine
from config import DB_CONFIG

def get_engine():
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:"
        f"{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/"
        f"{DB_CONFIG['database']}"
    )
    return create_engine(conn_str)