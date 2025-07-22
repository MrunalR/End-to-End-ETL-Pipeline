# supabase_config.py
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

SUPABASE_CONFIG = {
    "host": os.getenv("SUPABASE_HOST"),
    "dbname": os.getenv("SUPABASE_DB"),
    "user": os.getenv("SUPABASE_USER"),
    "password": os.getenv("SUPABASE_PASSWORD"),
    "port": os.getenv("SUPABASE_PORT"),
    "sslmode": os.getenv("SUPABASE_SSLMODE")
}

def get_supabase_connection():
    return psycopg2.connect(**SUPABASE_CONFIG)
