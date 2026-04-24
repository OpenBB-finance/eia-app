import asyncio
import json
import logging
import queue
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import duckdb
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings
from .steo_tables import STEO_TABLE_MAP, STEO_TABLE_NAMES
from .wpsr import WPSR_CATEGORY_LABELS, WPSR_TABLE_MAP

log = logging.getLogger(__name__)

WIDGETS_FILE = Path(__file__).parent.parent / "widgets.json"
APPS_FILE = Path(__file__).parent.parent / "apps.json"
STATIC_DIR = Path(__file__).parent
PUBLIC_STATIC_FILES: dict[str, tuple[str, str]] = {
    "openbb-logo.svg": ("openbb-logo.svg", "image/svg+xml"),
    "openbb-eia-app-screenshot1.png": ("openbb-eia-app-screenshot1.png", "image/png"),
    "openbb-eia-app-screenshot2.png": ("openbb-eia-app-screenshot2.png", "image/png"),
    "openbb-eia-app-screenshot3.png": ("openbb-eia-app-screenshot3.png", "image/png"),
}

_pool: queue.Queue[duckdb.DuckDBPyConnection] | None = None
_ingest_task: asyncio.Task | None = None
_wpsr_task: asyncio.Task | None = None
_startup_task: asyncio.Task | None = None
POOL_SIZE = 8


def _open_read_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(settings.eia_db_path, read_only=True)
    try:
        con.execute("INSTALL fts; LOAD fts;")
    except Exception:
        pass
    return con


def _init_pool():
    global _pool
    _pool = queue.Queue(maxsize=POOL_SIZE)
    for _ in range(POOL_SIZE):
        _pool.put(_open_read_connection())


def _drain_pool():
    global _pool
    if _pool is None:
        return
    while not _pool.empty():
        try:
            _pool.get_nowait().close()
        except queue.Empty:
            break
    _pool = None


@contextmanager
def _get_connection():
    con = _pool.get()
    try:
        yield con
    finally:
        _pool.put(con)


def _seconds_until_next_refresh() -> float:
    from zoneinfo import ZoneInfo
    from datetime import datetime as dt, timedelta

    et = ZoneInfo("America/New_York")
    now = dt.now(et)
    next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    wait = (next_run - now).total_seconds()
    return max(wait, 60)


def _seconds_until_next_wpsr() -> float:
    from zoneinfo import ZoneInfo
    from datetime import datetime as dt, timedelta

    et = ZoneInfo("America/New_York")
    now = dt.now(et)
    wed = 2
    days_ahead = (wed - now.weekday()) % 7
    if days_ahead == 0 and (now.hour > 11 or (now.hour == 11 and now.minute > 0)):
        days_ahead = 7
    next_wed = (now + timedelta(days=days_ahead)).replace(
        hour=11, minute=0, second=0, microsecond=0
    )
    wait = (next_wed - now).total_seconds()
    return max(wait, 60)


async def _wait_for_write_lock(max_wait: float = 600.0, interval: float = 5.0) -> bool:
    from .db import get_connection

    waited = 0.0
    while waited < max_wait:
        try:
            con = get_connection(read_only=False)
            con.close()
            return True
        except Exception as e:
            msg = str(e).lower()
            transient = (
                "lock" in msg
                or "different configuration" in msg
                or "already open" in msg
            )
            if not transient:
                log.exception("Unexpected error acquiring write lock")
                return False
            log.info(
                "DB write lock unavailable (%s), waiting %.0fs (waited %.0fs)",
                e.__class__.__name__,
                interval,
                waited,
            )
            await asyncio.sleep(interval)
            waited += interval
    log.error("Timed out waiting for DB write lock after %.0fs", max_wait)
    return False


async def _startup_ingest():
    from .ingest import run_ingest
    from .wpsr import ingest_wpsr
    from .db import get_connection

    log.info("Startup ingest: draining read pool and waiting for DB write lock")
    _drain_pool()

    if not await _wait_for_write_lock():
        log.error("Startup ingest skipped: could not acquire DB write lock")
        _init_pool()
        return

    try:
        log.info("Startup bulk ingest starting")
        await run_ingest()
        log.info("Startup bulk ingest complete")
    except Exception:
        log.exception("Startup bulk ingest failed")

    try:
        log.info("Startup WPSR ingest starting")
        con = get_connection(read_only=False)
        try:
            stats = ingest_wpsr(con)
        finally:
            con.close()
        log.info("Startup WPSR ingest complete: %s", stats)
    except Exception:
        log.exception("Startup WPSR ingest failed")

    _init_pool()
    log.info("Startup ingest complete, connection pool refreshed")


async def _ingest_loop():
    from .ingest import run_ingest

    while True:
        wait = _seconds_until_next_refresh()
        log.info(
            "Next ingest scheduled in %.1f hours (nightly at 2:00 AM ET)", wait / 3600
        )
        await asyncio.sleep(wait)
        try:
            log.info("Scheduled ingest starting")
            _drain_pool()
            await run_ingest()
            _init_pool()
            log.info("Scheduled ingest complete, connection pool refreshed")
        except Exception:
            log.exception("Scheduled ingest failed")
            if _pool is None:
                _init_pool()


async def _wpsr_loop():
    from .wpsr import ingest_wpsr
    from .db import get_connection

    while True:
        wait = _seconds_until_next_wpsr()
        log.info(
            "Next WPSR update scheduled in %.1f hours (Wednesday 11:00 AM ET)",
            wait / 3600,
        )
        await asyncio.sleep(wait)
        try:
            log.info("WPSR update starting")
            con = get_connection(read_only=False)
            stats = ingest_wpsr(con)
            con.close()
            log.info("WPSR update complete: %s", stats)
        except Exception:
            log.exception("WPSR update failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ingest_task, _wpsr_task, _startup_task
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    _init_pool()
    log.info(
        "Connection pool initialized (%d connections), scheduling background refresh",
        POOL_SIZE,
    )
    _startup_task = asyncio.create_task(_startup_ingest())
    _ingest_task = asyncio.create_task(_ingest_loop())
    _wpsr_task = asyncio.create_task(_wpsr_loop())
    yield
    if _startup_task:
        _startup_task.cancel()
    if _ingest_task:
        _ingest_task.cancel()
    if _wpsr_task:
        _wpsr_task.cancel()
    _drain_pool()


app = FastAPI(title="EIA Energy Data Explorer", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://pro.openbb.co",
        "https://pro.openbb.dev",
        "http://localhost:1420",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CacheControlMiddleware(BaseHTTPMiddleware):
    _NO_CACHE_PATHS = {"/health", "/"}

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        if (
            request.method == "GET"
            and response.status_code == 200
            and request.url.path not in self._NO_CACHE_PATHS
        ):
            response.headers["Cache-Control"] = "public, max-age=21600"
        return response


class RequireOpenBBUserMiddleware(BaseHTTPMiddleware):
    _EXEMPT_PATHS = {"/health", "/"}

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if (
            request.method == "OPTIONS"
            or path in self._EXEMPT_PATHS
            or (
                path.startswith("/static/")
                and path[len("/static/") :] in PUBLIC_STATIC_FILES
            )
        ):
            return await call_next(request)
        if not request.headers.get("x-openbb-user"):
            return JSONResponse(
                status_code=403, content={"detail": "Missing required header."}
            )
        return await call_next(request)


app.add_middleware(RequireOpenBBUserMiddleware)
app.add_middleware(CacheControlMiddleware)


@app.get("/static/{filename}")
def serve_static(filename: str):
    entry = PUBLIC_STATIC_FILES.get(filename)
    if entry is None:
        return JSONResponse({"detail": "Not found"}, status_code=404)
    name, media_type = entry
    return FileResponse(STATIC_DIR / name, media_type=media_type)


def _query(sql: str, params: list | None = None) -> list[dict]:
    if _pool is None:
        return []
    with _get_connection() as con:
        result = con.execute(sql, params or [])
        cols = [d[0] for d in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]


def _short_labels(names: list[str]) -> dict[str, str]:
    if len(names) <= 1:
        return {n: n for n in names}
    word_lists = [n.split() for n in names]
    min_len = min(len(w) for w in word_lists)
    prefix_words = 0
    for i in range(min_len):
        if len(set(wl[i] for wl in word_lists)) == 1:
            prefix_words += 1
        else:
            break
    suffix_words = 0
    for i in range(1, min_len - prefix_words + 1):
        if len(set(wl[-i] for wl in word_lists)) == 1:
            suffix_words += 1
        else:
            break
    if suffix_words:
        sample = word_lists[0]
        kept = sample[prefix_words : len(sample) - suffix_words]
        stripped = sample[len(sample) - suffix_words :]
        if any("(" in w for w in kept) and not any(")" in w for w in kept):
            paren_start = next(i for i, w in enumerate(stripped) if ")" in w)
            suffix_words -= paren_start + 1
            if suffix_words < 0:
                suffix_words = 0
    mapping = {}
    for n in names:
        words = n.split()
        end = len(words) - suffix_words if suffix_words else len(words)
        short = " ".join(words[prefix_words:end]).strip(" ,:-")
        mapping[n] = short if short else n
    dupes: dict[str, int] = {}
    final = {}
    for orig in names:
        label = mapping[orig]
        dupes[label] = dupes.get(label, 0) + 1
        if dupes[label] > 1:
            final[orig] = f"{label} ({dupes[label]})"
        else:
            final[orig] = label
    return final


def _pivot_rows(
    rows: list[dict], index_col: str, name_col: str, value_col: str
) -> list[dict]:
    from collections import OrderedDict

    all_names = list(dict.fromkeys(row[name_col] for row in rows))
    labels = _short_labels(all_names)
    all_labels = list(labels.values())
    pivoted: OrderedDict[str, dict] = OrderedDict()
    for row in rows:
        idx = row[index_col]
        if idx not in pivoted:
            pivoted[idx] = {index_col: idx}
        pivoted[idx][labels[row[name_col]]] = row[value_col]
    for rec in pivoted.values():
        for label in all_labels:
            if label not in rec:
                rec[label] = None
    return list(pivoted.values())


# --- OpenBB config endpoints ---


@app.get("/widgets.json")
def get_widgets():
    with open(WIDGETS_FILE) as f:
        return json.load(f)


@app.get("/apps.json")
def get_apps():
    with open(APPS_FILE) as f:
        return json.load(f)


# --- Health ---


@app.get("/health")
def health():
    try:
        rows = _query("SELECT COUNT(*) AS c FROM datasets")
        return {"status": "ok", "datasets_loaded": rows[0]["c"] if rows else 0}
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=503)


# --- Option endpoints (for dynamic dropdowns) ---


@app.get("/dataset_options")
def dataset_options():
    rows = _query("SELECT dataset_id, name FROM datasets ORDER BY dataset_id")
    return [
        {"label": f"{r['dataset_id']} — {r['name']}", "value": r["dataset_id"]}
        for r in rows
    ]


@app.get("/category_options")
def category_options(dataset_id: str = Query("")):
    if not dataset_id:
        rows = _query("""
            SELECT DISTINCT c.category_id, c.name, c.dataset_id
            FROM categories c
            WHERE NOT EXISTS (
                SELECT 1 FROM categories p
                WHERE p.category_id = c.parent_category_id AND p.dataset_id = c.dataset_id
            )
            ORDER BY c.dataset_id, c.name
            LIMIT 200
        """)
        return [
            {"label": f"{r['dataset_id']}: {r['name']}", "value": str(r["category_id"])}
            for r in rows
        ]
    rows = _query(
        """
        SELECT c.category_id, c.name
        FROM categories c
        WHERE c.dataset_id = ?
          AND NOT EXISTS (
              SELECT 1 FROM categories p
              WHERE p.category_id = c.parent_category_id AND p.dataset_id = c.dataset_id
          )
        ORDER BY c.name
    """,
        [dataset_id],
    )
    return [{"label": r["name"], "value": str(r["category_id"])} for r in rows]


@app.get("/subcategory_options")
def subcategory_options(dataset_id: str = Query(""), category_id: str = Query("")):
    if not category_id:
        return []
    conditions = ["c.parent_category_id = CAST(? AS INTEGER)"]
    params: list = [category_id]
    if dataset_id:
        conditions.append("c.dataset_id = ?")
        params.append(dataset_id)
    where = " AND ".join(conditions)
    rows = _query(
        f"""
        SELECT c.category_id, c.name
        FROM categories c
        WHERE {where}
        ORDER BY c.name
    """,
        params,
    )
    return [{"label": r["name"], "value": str(r["category_id"])} for r in rows]


@app.get("/frequency_options")
def frequency_options(dataset_id: str = Query("")):
    conditions = ["1=1"]
    params: list = []
    if dataset_id:
        conditions.append("dataset_id = ?")
        params.append(dataset_id)
    where = " AND ".join(conditions)
    rows = _query(
        f"SELECT DISTINCT frequency FROM series WHERE {where} ORDER BY frequency",
        params,
    )
    freq_labels = {
        "A": "Annual",
        "Q": "Quarterly",
        "M": "Monthly",
        "W": "Weekly",
        "D": "Daily",
    }
    return [
        {
            "label": freq_labels.get(r["frequency"], r["frequency"]),
            "value": r["frequency"],
        }
        for r in rows
    ]


@app.get("/geography_options")
def geography_options(dataset_id: str = Query("")):
    conditions = ["geography != ''", "geography NOT LIKE '%+%'"]
    params: list = []
    if dataset_id:
        conditions.append("dataset_id = ?")
        params.append(dataset_id)
    where = " AND ".join(conditions)
    rows = _query(
        f"""
        SELECT DISTINCT geography
        FROM series
        WHERE {where}
        ORDER BY geography
        LIMIT 200
    """,
        params,
    )
    return [{"label": r["geography"], "value": r["geography"]} for r in rows]


@app.get("/series_options")
def series_options(
    dataset_id: str = Query(""),
    category_id: str = Query(""),
    subcategory_id: str = Query(""),
    frequency: str = Query(""),
    geography: str = Query(""),
):
    conditions: list[str] = []
    params: list = []

    if dataset_id:
        conditions.append("s.dataset_id = ?")
        params.append(dataset_id)
    if frequency:
        conditions.append("s.frequency = ?")
        params.append(frequency)
    if geography:
        conditions.append("s.geography = ?")
        params.append(geography)

    cat_filter = subcategory_id or category_id
    if cat_filter:
        cat_ids = [cat_filter]
        children = _query(
            "SELECT category_id FROM categories WHERE parent_category_id = CAST(? AS INTEGER) AND dataset_id = ?",
            [cat_filter, dataset_id or ""],
        )
        cat_ids.extend(str(c["category_id"]) for c in children)
        placeholders = ",".join("?" for _ in cat_ids)
        conditions.append(
            f"s.series_id IN (SELECT series_id FROM category_series WHERE category_id IN ({placeholders}))"
        )
        params.extend(cat_ids)

    where = " AND ".join(conditions) if conditions else "1=1"
    rows = _query(
        f"""SELECT s.series_id, s.name, s.units, s.frequency, s.geography
           FROM series s WHERE {where} ORDER BY s.name LIMIT 500""",
        params,
    )
    return [
        {
            "label": r["name"][:80],
            "value": r["series_id"],
            "extraInfo": {
                "description": f"{r['units']} | {r['frequency']}",
                "rightOfDescription": r["geography"] or "",
            },
        }
        for r in rows
    ]


@app.get("/fuel_type_options")
def fuel_type_options():
    return [
        {"label": "All", "value": ""},
        {"label": "Petroleum", "value": "petroleum"},
        {"label": "Natural Gas", "value": "natural_gas"},
        {"label": "Coal", "value": "coal"},
        {"label": "Electricity", "value": "electricity"},
        {"label": "Nuclear", "value": "nuclear"},
        {"label": "Renewable", "value": "renewable"},
        {"label": "Total / Mixed", "value": "total"},
    ]


# --- Data endpoints ---


@app.get("/dataset_overview")
def dataset_overview():
    return _query("""
        SELECT
            d.dataset_id,
            d.name,
            d.temporal,
            d.spatial,
            d.last_updated,
            COALESCE(s.cnt, 0) AS series_count
        FROM datasets d
        LEFT JOIN (SELECT dataset_id, COUNT(*) AS cnt FROM series GROUP BY dataset_id) s
            ON d.dataset_id = s.dataset_id
        ORDER BY d.dataset_id
    """)


@app.get("/dataset_metrics")
def dataset_metrics():
    rows = _query("SELECT COUNT(*) AS c FROM datasets")
    ds = rows[0]["c"] if rows else 0
    rows = _query("SELECT COUNT(*) AS c FROM series")
    sr = rows[0]["c"] if rows else 0
    rows = _query("SELECT COUNT(*) AS c FROM observations")
    obs = rows[0]["c"] if rows else 0
    rows = _query("SELECT COUNT(*) AS c FROM wpsr_data")
    wpsr = rows[0]["c"] if rows else 0
    rows = _query("SELECT MAX(date) AS d FROM observations")
    latest = rows[0]["d"] if rows and rows[0]["d"] else "—"
    rows = _query("SELECT MAX(fetched_at) AS f FROM wpsr_releases")
    wpsr_updated = rows[0]["f"][:10] if rows and rows[0]["f"] else "—"
    return [
        {"label": "Datasets Loaded", "value": str(ds)},
        {"label": "Total Series", "value": f"{sr:,}"},
        {"label": "Observations", "value": f"{obs:,}"},
        {"label": "WPSR Records", "value": f"{wpsr:,}"},
        {"label": "Latest Data", "value": latest},
        {"label": "WPSR Updated", "value": wpsr_updated},
    ]


@app.get("/series_search")
def series_search(
    q: str = Query(""),
    dataset_id: str = Query(""),
    fuel_type: str = Query(""),
    measure_type: str = Query(""),
    frequency: str = Query(""),
    limit: int = Query(100, ge=1, le=5000),
):
    conditions = []
    params = []

    if dataset_id:
        conditions.append("s.dataset_id = ?")
        params.append(dataset_id)
    if fuel_type:
        conditions.append("s.fuel_type = ?")
        params.append(fuel_type)
    if measure_type:
        conditions.append("s.measure_type = ?")
        params.append(measure_type)
    if frequency:
        conditions.append("s.frequency = ?")
        params.append(frequency)

    if q:
        try:
            _query(
                "SELECT * FROM fts_main_series.match_bm25(series_id, 'test') LIMIT 1"
            )
            fts_available = True
        except Exception:
            fts_available = False

        if fts_available:
            conditions.append(
                "s.series_id IN (SELECT series_id FROM fts_main_series.match_bm25(series_id, ?) LIMIT ?)"
            )
            params.append(q)
            params.append(limit * 5)
        else:
            like = f"%{q}%"
            conditions.append(
                "(s.name ILIKE ? OR s.series_id ILIKE ? OR s.description ILIKE ?)"
            )
            params.extend([like, like, like])

    where = " AND ".join(conditions) if conditions else "1=1"
    params.append(limit)

    return _query(
        f"""
        SELECT series_id, dataset_id, name, units, frequency,
               geography, fuel_type, measure_type
        FROM series s
        WHERE {where}
        ORDER BY series_id
        LIMIT ?
        """,
        params,
    )


@app.get("/series_detail")
def series_detail(series_id: str = Query("")):
    if not series_id:
        return "Select a series to view details."
    rows = _query("SELECT * FROM series WHERE series_id = ?", [series_id])
    if not rows:
        return f"Series `{series_id}` not found."
    s = rows[0]
    cats = _query(
        """SELECT c.name FROM categories c
           JOIN category_series cs ON c.category_id = cs.category_id AND c.dataset_id = cs.dataset_id
           WHERE cs.series_id = ? ORDER BY c.name""",
        [series_id],
    )
    cat_list = ", ".join(c["name"] for c in cats) if cats else "—"
    geo_str = s["geography"] or s.get("iso3166", "") or "—"
    geo_type = s.get("geography_type", "")
    loc = ""
    if s.get("lat") and s.get("lon"):
        loc = f" ({s['lat']}, {s['lon']})"
    return (
        f"## {s['name']}\n\n"
        f"**Series ID:** `{s['series_id']}`\n\n"
        f"**Dataset:** {s['dataset_id']}\n\n"
        f"**Description:** {s.get('description', '') or '—'}\n\n"
        f"**Units:** {s['units']}"
        + (f" ({s.get('unitsshort', '')})" if s.get("unitsshort") else "")
        + "\n\n"
        f"**Frequency:** {s['frequency']}\n\n"
        f"**Geography:** {geo_str} ({geo_type}){loc}\n\n"
        f"**Period:** {s['start_period']} → {s['end_period']}\n\n"
        f"**Fuel Type:** {s['fuel_type']}\n\n"
        f"**Measure:** {s['measure_type']}\n\n"
        f"**Categories:** {cat_list}\n\n"
        f"**Source:** {s.get('source', '') or '—'}\n"
        + (f"\n**Copyright:** {s['copyright']}\n" if s.get("copyright") else "")
    )


@app.get("/time_series_chart")
def time_series_chart(
    series_id: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
):
    if not series_id:
        return []

    conditions = ["o.series_id = ?"]
    params: list = [series_id]
    if start_date:
        conditions.append("o.date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("o.date <= ?")
        params.append(end_date)

    where = " AND ".join(conditions)
    return _query(
        f"""SELECT o.date, o.value, s.name, s.units
           FROM observations o
           JOIN series s ON o.series_id = s.series_id
           WHERE {where} ORDER BY o.date""",
        params,
    )


@app.get("/time_series_table")
def time_series_table(
    series_id: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
):
    if not series_id:
        return []

    conditions = ["o.series_id = ?"]
    params: list = [series_id]
    if start_date:
        conditions.append("o.date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("o.date <= ?")
        params.append(end_date)

    where = " AND ".join(conditions)
    return _query(
        f"""SELECT o.date, o.value, s.name, s.units
           FROM observations o
           JOIN series s ON o.series_id = s.series_id
           WHERE {where} ORDER BY o.date""",
        params,
    )


@app.get("/category_breakdown")
def category_breakdown(dataset_id: str = Query("")):
    if dataset_id:
        return _query(
            """
            SELECT c.name AS category_name, COUNT(DISTINCT cs.series_id) AS series_count
            FROM categories c
            JOIN category_series cs ON c.category_id = cs.category_id AND c.dataset_id = cs.dataset_id
            WHERE c.dataset_id = ? AND c.parent_category_id != 0
            GROUP BY c.name
            ORDER BY series_count DESC
            LIMIT 200
        """,
            [dataset_id],
        )
    return _query("""
        SELECT c.name AS category_name, c.dataset_id, COUNT(DISTINCT cs.series_id) AS series_count
        FROM categories c
        JOIN category_series cs ON c.category_id = cs.category_id AND c.dataset_id = cs.dataset_id
        WHERE c.parent_category_id != 0
        GROUP BY c.name, c.dataset_id
        ORDER BY series_count DESC
        LIMIT 200
    """)


@app.get("/multi_series_chart")
def multi_series_chart(
    series_ids: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
):
    ids = [s.strip() for s in series_ids.split(",") if s.strip()]
    if not ids:
        return []

    all_data = []
    for sid in ids[:10]:
        conditions = ["o.series_id = ?"]
        params: list = [sid]
        if start_date:
            conditions.append("o.date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("o.date <= ?")
            params.append(end_date)
        where = " AND ".join(conditions)
        rows = _query(
            f"""SELECT o.date, s.name || ' (' || s.units || ')' AS name, o.value
               FROM observations o
               JOIN series s ON o.series_id = s.series_id
               WHERE {where} ORDER BY o.date""",
            params,
        )
        all_data.extend(rows)

    return _pivot_rows(all_data, "date", "name", "value")


# --- WPSR endpoints ---


@app.get("/wpsr_category_options")
def wpsr_category_options():
    return [{"label": v, "value": k} for k, v in WPSR_CATEGORY_LABELS.items()]


@app.get("/wpsr_table_options")
def wpsr_table_options(category: str = Query("")):
    if not category:
        category = "balance_sheet"
    tables = WPSR_TABLE_MAP.get(category, {})
    return [{"label": t.replace("_", " ").title(), "value": t} for t in tables]


@app.get("/wpsr_data")
def wpsr_data(
    category: str = Query(""),
    table: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
):
    if not category:
        category = "balance_sheet"
    conditions = ["category = ?"]
    params: list = [category]
    if table:
        conditions.append("table_name = ?")
        params.append(table)
    else:
        tables = WPSR_TABLE_MAP.get(category, {})
        if tables:
            first_table = next(iter(tables.keys()))
            conditions.append("table_name = ?")
            params.append(first_table)
    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)
    where = " AND ".join(conditions)
    rows = _query(
        f"SELECT date, REPLACE(REPLACE(title, 'Weekly U.S. ', ''), 'Weekly U.S.', '') AS title, value FROM wpsr_data WHERE {where} ORDER BY date, \"order\"",
        params,
    )
    return _pivot_rows(rows, "date", "title", "value")


# --- STEO table endpoints ---


@app.get("/steo_table_options")
def steo_table_options():
    return [
        {"label": f"Table {k}: {v}", "value": k} for k, v in STEO_TABLE_NAMES.items()
    ]


@app.get("/steo_table")
def steo_table(
    table_id: str = Query(""),
    start_date: str = Query(""),
    end_date: str = Query(""),
):
    if not table_id:
        table_id = "01"
    series_ids = STEO_TABLE_MAP.get(table_id, [])
    if not series_ids:
        return []

    expanded = [f"STEO.{sid.upper()}.M" for sid in series_ids]

    placeholders = ",".join("?" for _ in expanded)
    conditions = [f"o.series_id IN ({placeholders})"]
    params: list = list(expanded)
    if start_date:
        conditions.append("o.date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("o.date <= ?")
        params.append(end_date)

    where = " AND ".join(conditions)
    rows = _query(
        f"""SELECT o.date, s.name || ' (' || s.units || ')' AS name, o.value
           FROM observations o
           JOIN series s ON o.series_id = s.series_id
           WHERE {where}
           ORDER BY o.date""",
        params,
    )
    return _pivot_rows(rows, "date", "name", "value")
