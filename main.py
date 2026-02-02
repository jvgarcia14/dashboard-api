import os
from datetime import datetime, time, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Header, HTTPException

PH_TZ = ZoneInfo("Asia/Manila")

SALES_DB = os.getenv("SALES_DATABASE_URL_RO")
ATTEND_DB = os.getenv("ATTEND_DATABASE_URL_RO")
INTERNAL_KEY = os.getenv("INTERNAL_KEY")

if not SALES_DB:
    raise RuntimeError("SALES_DATABASE_URL_RO not set")
if not ATTEND_DB:
    raise RuntimeError("ATTEND_DATABASE_URL_RO not set")
if not INTERNAL_KEY:
    raise RuntimeError("INTERNAL_KEY not set")

app = FastAPI(title="Sales + Attendance Dashboard API")

def sales_conn():
    return psycopg2.connect(SALES_DB, sslmode="require")

def attend_conn():
    return psycopg2.connect(ATTEND_DB, sslmode="require")

def require_internal_key(x_internal_key: Optional[str] = Header(None)):
    if x_internal_key != INTERNAL_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def now_ph() -> datetime:
    return datetime.now(PH_TZ)

def shift_start_ph(dt: datetime) -> datetime:
    d = dt.date()
    h = dt.hour
    if 8 <= h < 16:
        return datetime(d.year, d.month, d.day, 8, 0, tzinfo=PH_TZ)
    if 16 <= h < 24:
        return datetime(d.year, d.month, d.day, 16, 0, tzinfo=PH_TZ)
    return datetime(d.year, d.month, d.day, 0, 0, tzinfo=PH_TZ)

def attendance_day_for(ph_dt: datetime) -> date:
    if ph_dt.time() < time(6, 0):
        return ph_dt.date() - timedelta(days=1)
    return ph_dt.date()

@app.get("/health")
def health():
    return {"ok": True}

# ---------------- SALES (Sales DB) ----------------
@app.get("/teams")
def teams(x_internal_key: Optional[str] = Header(None)):
    require_internal_key(x_internal_key)
    with sales_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT DISTINCT name FROM teams ORDER BY name ASC;")
        return [r["name"] for r in cur.fetchall()]

@app.get("/sales/shift")
def sales_shift(team: str, x_internal_key: Optional[str] = Header(None)):
    require_internal_key(x_internal_key)

    now = now_ph()
    start = shift_start_ph(now)

    with sales_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT page, COALESCE(SUM(amount),0)::float AS total
            FROM sales
            WHERE team=%s AND ts >= %s
            GROUP BY page
            ORDER BY total DESC;
            """,
            (team, start.astimezone(ZoneInfo("UTC"))),
        )
        rows = cur.fetchall()

    return {
        "team": team,
        "shift_start_ph": start.isoformat(),
        "updated_ph": now.isoformat(),
        "rows": rows
    }

# ---------------- ATTENDANCE (Attendance DB) ----------------
@app.get("/attendance/today")
def attendance_today(x_internal_key: Optional[str] = Header(None)):
    require_internal_key(x_internal_key)

    now = now_ph()
    att_day = attendance_day_for(now)

    with attend_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT shift, page_key, user_name, is_cover, ph_ts
            FROM attendance_clockins
            WHERE attendance_day=%s
            ORDER BY shift, page_key, is_cover, ph_ts;
            """,
            (att_day,),
        )
        rows = cur.fetchall()

    grouped: Dict[str, Dict[str, Any]] = {"prime": {}, "midshift": {}, "closing": {}}
    for r in rows:
        shift = r["shift"]
        page_key = r["page_key"]

        grouped.setdefault(shift, {})
        grouped[shift].setdefault(page_key, {"users": [], "covers": []})

        entry = {"name": r["user_name"], "ph_ts": r["ph_ts"].astimezone(PH_TZ).isoformat()}
        if r["is_cover"]:
            grouped[shift][page_key]["covers"].append(entry)
        else:
            grouped[shift][page_key]["users"].append(entry)

    return {"attendance_day": att_day.isoformat(), "updated_ph": now.isoformat(), "data": grouped}
