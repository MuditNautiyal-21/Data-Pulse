"""
DataPulse Storage — saves results to SQLite database.
"""
import sqlite3
import json
from datetime import datetime

DB_PATH = "datapulse.db"


def init_db():
    """Create the database and tables if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS check_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            check_name TEXT,
            check_type TEXT,
            source TEXT,
            severity TEXT,
            passed BOOLEAN,
            details TEXT,
            ran_at TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            profile_data TEXT,
            profiled_at TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    print("Database ready: datapulse.db")


def save_check_results(results, source):
    """Save check results to database."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for r in results:
        c.execute(
            """INSERT INTO check_results
            (check_name, check_type, source, severity, passed, details, ran_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (r["name"], r.get("type",""), source,
             r["severity"], r["passed"], r["details"],
             r.get("ran_at", datetime.now().isoformat())),
        )

    conn.commit()
    conn.close()
    print(f"Saved {len(results)} results to database.")


def save_profile(source, profile):
    """Save a profile snapshot."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO profiles (source, profile_data, profiled_at) VALUES (?,?,?)",
        (source, json.dumps(profile), datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def get_recent_results(source=None, limit=50):
    """Get recent check results."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    if source:
        c.execute(
            "SELECT * FROM check_results WHERE source=? ORDER BY ran_at DESC LIMIT ?",
            (source, limit),
        )
    else:
        c.execute(
            "SELECT * FROM check_results ORDER BY ran_at DESC LIMIT ?",
            (limit,),
        )

    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_check_history(check_name, limit=30):
    """Get history for one specific check."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        "SELECT * FROM check_results WHERE check_name=? ORDER BY ran_at DESC LIMIT ?",
        (check_name, limit),
    )
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully!")