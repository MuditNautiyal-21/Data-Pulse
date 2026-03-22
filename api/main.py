"""
Data-Pulse API: It serves the check results as web data.
Run: uvicorn api.main:app -- reload
Open: http: http://localhost:8000/docs
"""


from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pathlib import Path
from engine.storage import init_db, get_recent_results, get_check_history
import sqlite3

app = FastAPI(title = "Data-Pulse API", version = "1.0.1")
init_db()

@app.get("/")
def home():
    return {"status": "running", "app": "Data-Pulse"}

@app.get("/api/health")
def health():
    """Overall health score = passed / total * 100."""
    if not results:
        return {"health_score": 0, "total": 0, "message": "No results yet. Run: python run.py"}
    
    # Only look at the latest result per check
    latest = {}
    for r in results:
        key = r["check_name"] + r["source"]
        if key not in latest:
            latest[key] = r
        
    checks = list(latest.values())
    passed = sum(1 for c in checks if c["passed"])
    total = len(checks)

    return {
        "health_score": round(passed / total * 100, 1),
        "passed": passed,
        "failed": total - passed,
        "total": total,
        "critical_failures": sum(
            1 for c in checks
            if not c["passed"] and c["severity"] == "critical"
        ),
    }

@app.get("/api/sources")
def sources():
    """Get all monitored sources."""
    conn = sqlite3.connect("datapulse.db")
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT source,
            COUNT(*) as total,
            SUM(CASE WHEN passed=1 THEN 1 ELSE 0 END) as passed,
            MAX(ran_at) as last_run
        FROM check_results GROUP BY source
              """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return {"sources": rows}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """Serve the dashboard page."""
    return Path("templates/dashboards.html").read_text()