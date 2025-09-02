# server/connection.py
"""
Database connection utility using psycopg2.
Central place for handling DB connections.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load env variables from .env
load_dotenv()

def get_connection():
    """
    Create a new PostgreSQL connection.
    Returns a connection object with RealDictCursor (rows as dicts).
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        raise RuntimeError(f"❌ Failed to connect to database: {e}")
