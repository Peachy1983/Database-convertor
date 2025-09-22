import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Try to read a DATABASE_URL (env var or Streamlit secrets)
db_url = os.environ.get("DATABASE_URL")
try:
    import streamlit as st
    if not db_url:
        db_url = st.secrets.get("DATABASE_URL", None)
except Exception:
    # running outside Streamlit or st not available -> ignore
    pass

def try_connect(url):
    """Try to create an engine and test a simple query. Return engine or None."""
    if not url:
        return None
    try:
        # create engine (psycopg2 URL or similar)
        e = create_engine(url, pool_pre_ping=True)
        # test connection right away
        with e.connect() as conn:
            conn.execute(text("SELECT 1"))
        return e
    except Exception as exc:
        # fail quietly; caller will fallback to sqlite
        print("DB connect failed:", exc, file=sys.stderr)
        return None

# 1) prefer real DB (Supabase) if reachable
engine = try_connect(db_url)

# 2) If not reachable, fall back to local sqlite (keeps the app working locally)
if engine is None:
    sqlite_url = "sqlite:///company_data.db"
    engine = create_engine(sqlite_url, connect_args={"check_same_thread": False}, pool_pre_ping=True)
    print("Using local sqlite database at company_data.db", file=sys.stderr)

def init_db():
    """Create a minimal companies table compatible with both sqlite and Postgres."""
    dialect = engine.dialect.name
    if dialect == "sqlite":
        create_sql = """
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT UNIQUE,
            name TEXT,
            domain TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    else:
        create_sql = """
        CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            company_number TEXT UNIQUE,
            name TEXT,
            domain TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

# Initialise DB when imported
init_db()
