import duckdb
from pathlib import Path

from .config import settings

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    name TEXT,
    description TEXT,
    keywords TEXT,
    temporal TEXT,
    spatial TEXT,
    last_updated TEXT,
    loaded_at TEXT
);

CREATE TABLE IF NOT EXISTS categories (
    category_id INTEGER,
    dataset_id TEXT NOT NULL,
    name TEXT,
    parent_category_id INTEGER,
    notes TEXT,
    PRIMARY KEY (dataset_id, category_id)
);

CREATE TABLE IF NOT EXISTS category_series (
    category_id INTEGER NOT NULL,
    dataset_id TEXT NOT NULL,
    series_id TEXT NOT NULL,
    PRIMARY KEY (dataset_id, category_id, series_id)
);

CREATE TABLE IF NOT EXISTS series (
    series_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    name TEXT,
    description TEXT,
    units TEXT,
    unitsshort TEXT,
    frequency TEXT,
    geography TEXT,
    geography_type TEXT,
    iso3166 TEXT,
    lat DOUBLE,
    lon DOUBLE,
    geoset_id TEXT,
    source TEXT,
    copyright TEXT,
    start_period TEXT,
    end_period TEXT,
    last_updated TEXT,
    last_historical_period TEXT,
    fuel_type TEXT,
    measure_type TEXT,
    unit_multiplier DOUBLE DEFAULT 1.0,
    unit_label TEXT
);

CREATE TABLE IF NOT EXISTS observations (
    series_id TEXT NOT NULL,
    date TEXT NOT NULL,
    value DOUBLE NOT NULL
);

CREATE TABLE IF NOT EXISTS wpsr_releases (
    category TEXT NOT NULL,
    table_name TEXT NOT NULL,
    sheet_name TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (category, table_name)
);

CREATE TABLE IF NOT EXISTS wpsr_data (
    category TEXT NOT NULL,
    table_name TEXT NOT NULL,
    table_label TEXT,
    symbol TEXT NOT NULL,
    title TEXT,
    unit TEXT,
    date TEXT NOT NULL,
    value DOUBLE,
    "order" INTEGER
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_obs_series ON observations(series_id);
CREATE INDEX IF NOT EXISTS idx_obs_series_date ON observations(series_id, date);
CREATE INDEX IF NOT EXISTS idx_series_dataset ON series(dataset_id);
CREATE INDEX IF NOT EXISTS idx_series_fuel ON series(fuel_type);
CREATE INDEX IF NOT EXISTS idx_series_measure ON series(measure_type);
CREATE INDEX IF NOT EXISTS idx_series_freq ON series(frequency);
CREATE INDEX IF NOT EXISTS idx_series_geo ON series(geography_type);
CREATE INDEX IF NOT EXISTS idx_cat_parent ON categories(dataset_id, parent_category_id);
CREATE INDEX IF NOT EXISTS idx_catseries_series ON category_series(series_id);
CREATE INDEX IF NOT EXISTS idx_catseries_cat ON category_series(dataset_id, category_id);
CREATE INDEX IF NOT EXISTS idx_wpsr_cat_table ON wpsr_data(category, table_name);
CREATE INDEX IF NOT EXISTS idx_wpsr_date ON wpsr_data(date);
"""


def get_connection(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    db_path = Path(settings.eia_db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def _exec_multi(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    _exec_multi(con, SCHEMA_SQL)
    _ensure_obs_unique_index(con)


def _ensure_obs_unique_index(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_pk ON observations(series_id, date)"
        )
    except Exception:
        con.execute("""
            CREATE TABLE _obs_deduped AS
                SELECT series_id, date, MAX(value) AS value
                FROM observations
                GROUP BY series_id, date
        """)
        con.execute("DROP TABLE observations")
        con.execute("ALTER TABLE _obs_deduped RENAME TO observations")
        con.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_pk ON observations(series_id, date)"
        )


def ensure_indexes(con: duckdb.DuckDBPyConnection) -> None:
    _exec_multi(con, INDEX_SQL)


def build_fts_index(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL fts; LOAD fts;")
    con.execute("DROP INDEX IF EXISTS series_fts_idx;")
    con.execute(
        "PRAGMA create_fts_index('series', 'series_id', 'name', 'description', 'units', 'geography', overwrite=1);"
    )
