import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "database": os.getenv("POSTGRES_DB", "retail_dw"),
    "user": os.getenv("POSTGRES_USER", "retail_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail_password"),
}