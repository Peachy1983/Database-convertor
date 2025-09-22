import os
from sqlalchemy import create_engine, text

# Prefer environment var DATABASE_URL, else try Streamlit secrets, else local sqlite
db_url = os.environ.get("DATABASE_URL")
try:
    import streamlit as st
    if not db_url:
        db_url = st.secrets.get("DATABASE_URL", None)
except Exception:
    pass

if not db_url:
    db_url = "sqlite:///company_data.db"

connect_args = {"check_same_thread": False} if db_url.startswith("sqlite") else {}
engine = create_engine(db_url, connect_args=connect_args, pool_pre_ping=True)

def init_db():
    # create a simple companies table that works on sqlite and Postgres
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

# run on import so DB exists automatically
init_db()
