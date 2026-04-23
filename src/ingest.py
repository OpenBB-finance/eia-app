import asyncio
import calendar
import io
import json
import logging
import re as _re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from tqdm import tqdm

from .categorize import (
    infer_fuel_type,
    infer_geography_type,
    infer_measure_type,
    parse_units,
)
from .config import settings
from .db import build_fts_index, ensure_indexes, get_connection, init_schema
from .wpsr import ingest_wpsr

log = logging.getLogger(__name__)

MANIFEST_URL = settings.eia_manifest_url
MAX_RETRIES = 3
BACKOFF_BASE = 2.0

SENTINEL_VALUES = frozenset({"NA", "W", "-", "--", "- -", "(s)", "NM", "ie", ""})

_RE_QUARTERLY = _re.compile(r"^(\d{4})Q([1-4])$")
_QUARTER_END = {"1": (3, 31), "2": (6, 30), "3": (9, 30), "4": (12, 31)}


def _parse_date(raw: str) -> str:
    s = raw.strip()
    if len(s) == 4 and s.isdigit():
        return f"{s}-12-31"
    m = _RE_QUARTERLY.match(s)
    if m:
        yr, q = m.group(1), m.group(2)
        mo, dy = _QUARTER_END[q]
        return f"{yr}-{mo:02d}-{dy:02d}"
    if len(s) == 6 and s.isdigit():
        yr, mo = int(s[:4]), int(s[4:6])
        _, last_day = calendar.monthrange(yr, mo)
        return f"{yr:04d}-{mo:02d}-{last_day:02d}"
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


SERIES_COLS = [
    "series_id",
    "dataset_id",
    "name",
    "description",
    "units",
    "unitsshort",
    "frequency",
    "geography",
    "geography_type",
    "iso3166",
    "lat",
    "lon",
    "geoset_id",
    "source",
    "copyright",
    "start_period",
    "end_period",
    "last_updated",
    "last_historical_period",
    "fuel_type",
    "measure_type",
    "unit_multiplier",
    "unit_label",
]


async def fetch_manifest(client: httpx.AsyncClient) -> dict:
    resp = await client.get(MANIFEST_URL, timeout=60)
    resp.raise_for_status()
    raw = resp.json()
    return raw.get("dataset", raw)


async def download_zip(client: httpx.AsyncClient, url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with client.stream(
                "GET", url, timeout=600, follow_redirects=True
            ) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                with (
                    open(dest, "wb") as f,
                    tqdm(
                        total=total,
                        unit="B",
                        unit_scale=True,
                        desc=dest.name,
                        disable=total == 0,
                    ) as pbar,
                ):
                    async for chunk in resp.aiter_bytes(65536):
                        f.write(chunk)
                        pbar.update(len(chunk))
            return dest
        except (httpx.HTTPError, httpx.StreamError) as e:
            if attempt == MAX_RETRIES:
                raise
            wait = BACKOFF_BASE**attempt
            log.warning("Retry %d for %s: %s (wait %.1fs)", attempt, url, e, wait)
            await asyncio.sleep(wait)
    return dest


def _iter_records(zip_path: Path):
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not any(name.endswith(ext) for ext in (".txt", ".jsonl", ".json")):
                continue
            with zf.open(name) as f:
                for line in io.TextIOWrapper(f, encoding="utf-8", errors="replace"):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(obj, dict):
                        yield obj


def _parse_value(raw):
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if s in SENTINEL_VALUES:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _parse_lat_lon(obj: dict, key: str) -> float | None:
    val = obj.get(key)
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _build_series_record(obj: dict, dataset_id: str) -> dict:
    sid = obj["series_id"]
    name = obj.get("name", "")
    geo = obj.get("geography", obj.get("geography2", ""))
    iso = obj.get("iso3166", "")
    units = obj.get("units", "")
    multiplier, label = parse_units(units)

    return {
        "series_id": sid,
        "dataset_id": dataset_id,
        "name": name,
        "description": obj.get("description", ""),
        "units": units,
        "unitsshort": obj.get("unitsshort", ""),
        "frequency": obj.get("f", obj.get("frequency", "")),
        "geography": geo,
        "geography_type": infer_geography_type(geo, iso),
        "iso3166": iso,
        "lat": _parse_lat_lon(obj, "lat"),
        "lon": _parse_lat_lon(obj, "lon"),
        "geoset_id": obj.get("geoset_id", ""),
        "source": obj.get("source", ""),
        "copyright": obj.get("copyright") or "",
        "start_period": obj.get("start", ""),
        "end_period": obj.get("end", ""),
        "last_updated": obj.get("last_updated", obj.get("updated", "")),
        "last_historical_period": obj.get("lastHistoricalPeriod", ""),
        "fuel_type": infer_fuel_type(dataset_id, sid, name),
        "measure_type": infer_measure_type(name),
        "unit_multiplier": multiplier,
        "unit_label": label,
    }


def _flush_obs(con, rows: list[tuple]) -> None:
    df = pd.DataFrame(rows, columns=["series_id", "date", "value"])
    con.execute("INSERT OR IGNORE INTO observations SELECT * FROM df")


def load_dataset(dataset_id: str, zip_path: Path, con) -> dict:
    OBS_CHUNK = 500_000

    cat_rows: list[tuple] = []
    cat_series_rows: list[tuple] = []
    series_rows: list[dict] = []
    obs_rows: list[tuple] = []
    total_obs = 0
    skipped_series = 0
    n_cat = 0

    for obj in tqdm(_iter_records(zip_path), desc=f"  {dataset_id}", unit=" rec"):
        if "series_id" in obj:
            sid = obj["series_id"]
            series_obs = []
            for entry in obj.get("data", []):
                if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                    continue
                val = _parse_value(entry[1])
                if val is not None:
                    series_obs.append((sid, _parse_date(str(entry[0])), val))

            if not series_obs:
                skipped_series += 1
                continue

            series_rows.append(_build_series_record(obj, dataset_id))
            obs_rows.extend(series_obs)

            if len(obs_rows) >= OBS_CHUNK:
                _flush_obs(con, obs_rows)
                total_obs += len(obs_rows)
                obs_rows = []

        elif "category_id" in obj:
            cid = obj["category_id"]
            cat_rows.append(
                (
                    cid,
                    dataset_id,
                    obj.get("name", ""),
                    obj.get("parent_category_id"),
                    obj.get("notes", ""),
                )
            )
            n_cat += 1
            for sid in obj.get("childseries", []):
                if sid:
                    cat_series_rows.append((cid, dataset_id, sid))

    n_series = len(series_rows)

    if series_rows:
        df_s = pd.DataFrame(series_rows)[SERIES_COLS]
        con.execute("INSERT OR REPLACE INTO series SELECT * FROM df_s")

    if obs_rows:
        _flush_obs(con, obs_rows)
        total_obs += len(obs_rows)

    if cat_rows:
        df_cat = pd.DataFrame(
            cat_rows,
            columns=[
                "category_id",
                "dataset_id",
                "name",
                "parent_category_id",
                "notes",
            ],
        )
        con.execute("INSERT OR IGNORE INTO categories SELECT * FROM df_cat")

    if cat_series_rows:
        df_cs = pd.DataFrame(
            cat_series_rows, columns=["category_id", "dataset_id", "series_id"]
        )
        con.execute("INSERT OR IGNORE INTO category_series SELECT * FROM df_cs")

    log.info(
        "  %s: %d series (%d skipped), %d obs, %d categories, %d cat-series links",
        dataset_id,
        n_series,
        skipped_series,
        total_obs,
        n_cat,
        len(cat_series_rows),
    )

    return {
        "series": n_series,
        "obs": total_obs,
        "categories": n_cat,
        "cat_links": len(cat_series_rows),
        "skipped": skipped_series,
    }


async def run_ingest(dataset_codes: list[str] | None = None, force: bool = False):
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    con = get_connection(read_only=False)
    init_schema(con)

    async with httpx.AsyncClient() as client:
        log.info("Fetching manifest from %s", MANIFEST_URL)
        manifest = await fetch_manifest(client)
        log.info("Manifest: %d datasets available", len(manifest))

        codes = dataset_codes or settings.dataset_list
        if codes:
            selected = {k: v for k, v in manifest.items() if k in codes}
            missing = set(codes) - set(selected.keys())
            if missing:
                log.warning("Not found in manifest: %s", missing)
        else:
            selected = manifest

        if not selected:
            log.error("No matching datasets. Available: %s", list(manifest.keys()))
            return

        log.info("Processing %d datasets: %s", len(selected), list(selected.keys()))

        data_dir = Path(settings.eia_data_dir)
        zip_dir = data_dir / "zips"
        zip_dir.mkdir(parents=True, exist_ok=True)

        grand = {"series": 0, "obs": 0, "categories": 0, "cat_links": 0}

        for ds_key, ds_meta in selected.items():
            ds_id = ds_key
            access_url = ds_meta.get("accessURL", "")
            manifest_updated = ds_meta.get("last_updated", ds_meta.get("modified", ""))

            if not access_url:
                log.warning("No accessURL for %s, skipping", ds_id)
                continue

            if not force and not settings.eia_force_reload:
                row = con.execute(
                    "SELECT loaded_at, last_updated FROM datasets WHERE dataset_id = ?",
                    [ds_id],
                ).fetchone()
                if row and row[1] and str(row[1]) >= str(manifest_updated):
                    log.info("Skipping %s (unchanged since %s)", ds_id, row[1])
                    continue

            zip_name = access_url.split("/")[-1]
            zip_path = zip_dir / zip_name

            log.info("Downloading %s (%s)", ds_id, access_url)
            await download_zip(client, access_url, zip_path)

            log.info("Loading %s into DuckDB (incremental)", ds_id)

            stats = load_dataset(ds_id, zip_path, con)
            zip_path.unlink(missing_ok=True)
            for k in grand:
                grand[k] += stats.get(k, 0)

            now = datetime.now(timezone.utc).isoformat()
            con.execute(
                "INSERT OR REPLACE INTO datasets VALUES (?,?,?,?,?,?,?,?)",
                (
                    ds_id,
                    ds_meta.get("name", ""),
                    ds_meta.get("description", ""),
                    ds_meta.get("keyword", ""),
                    ds_meta.get("temporal", ""),
                    ds_meta.get("spatial", ""),
                    manifest_updated,
                    now,
                ),
            )
            log.info("Done: %s", ds_id)

    log.info("Building indexes")
    ensure_indexes(con)

    log.info("Building FTS index")
    try:
        build_fts_index(con)
    except Exception:
        log.warning("FTS index build failed (non-fatal)", exc_info=True)

    log.info("Ingesting WPSR data")
    try:
        wpsr_stats = ingest_wpsr(con)
        log.info("WPSR ingest: %s", wpsr_stats)
    except Exception:
        log.warning("WPSR ingest failed (non-fatal)", exc_info=True)

    con.close()
    log.info(
        "Ingest complete: %d series, %d obs, %d categories, %d cat-series links",
        grand["series"],
        grand["obs"],
        grand["categories"],
        grand["cat_links"],
    )


def main():
    import argparse

    parser = argparse.ArgumentParser(description="EIA Bulk Data Ingest")
    parser.add_argument(
        "--datasets",
        type=str,
        default=None,
        help="Comma-separated dataset codes or ALL",
    )
    parser.add_argument(
        "--force", action="store_true", help="Force reload even if unchanged"
    )
    args = parser.parse_args()

    codes = None
    if args.datasets:
        if args.datasets.upper() == "ALL":
            codes = []
        else:
            codes = [d.strip() for d in args.datasets.split(",")]

    asyncio.run(run_ingest(dataset_codes=codes, force=args.force))


if __name__ == "__main__":
    main()
