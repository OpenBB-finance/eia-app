import io
import logging
from datetime import datetime, timezone, timedelta

import httpx
import pandas as pd

log = logging.getLogger(__name__)

WPSR_FILE_MAP = {
    "balance_sheet": "https://ir.eia.gov/wpsr/psw01.xls",
    "inputs_and_production": "https://ir.eia.gov/wpsr/psw02.xls",
    "refiner_blender_net_production": "https://ir.eia.gov/wpsr/psw03.xls",
    "crude_petroleum_stocks": "https://ir.eia.gov/wpsr/psw04.xls",
    "gasoline_fuel_stocks": "https://ir.eia.gov/wpsr/psw05.xls",
    "total_gasoline_by_sub_padd": "https://ir.eia.gov/wpsr/psw05a.xls",
    "distillate_fuel_oil_stocks": "https://ir.eia.gov/wpsr/psw06.xls",
    "imports": "https://ir.eia.gov/wpsr/psw07.xls",
    "imports_by_country": "https://ir.eia.gov/wpsr/psw08.xls",
    "weekly_estimates": "https://ir.eia.gov/wpsr/psw09.xls",
    "spot_prices_crude_gas_heating": "https://ir.eia.gov/wpsr/psw11.xls",
    "spot_prices_diesel_jet_fuel_propane": "https://ir.eia.gov/wpsr/psw12.xls",
    "retail_prices": "https://ir.eia.gov/wpsr/psw14.xls",
}

WPSR_TABLE_MAP = {
    "balance_sheet": {
        "stocks": "Data 1",
        "supply": "Data 2",
        "supply_avg": "Data 3",
    },
    "inputs_and_production": {
        "product_by_region": "Data 1",
        "product_by_region_avg": "Data 2",
    },
    "refiner_blender_net_production": {
        "net_production": "Data 1",
        "net_production_avg": "Data 2",
    },
    "crude_petroleum_stocks": {"stocks": "Data 1"},
    "gasoline_fuel_stocks": {"stocks": "Data 1"},
    "total_gasoline_by_sub_padd": {"stocks": "Data 1"},
    "distillate_fuel_oil_stocks": {"stocks": "Data 1"},
    "imports": {
        "imports": "Data 1",
        "imports_avg": "Data 2",
    },
    "imports_by_country": {
        "imports_by_country": "Data 1",
        "imports_by_country_avg": "Data 2",
    },
    "weekly_estimates": {
        "crude_production": "Data 1",
        "inputs_and_utilization": "Data 2",
        "refiner_blender_net_production": "Data 3",
        "net_production_by_product": "Data 4",
        "ethanol_plant_production": "Data 5",
        "stocks": "Data 6",
        "imports": "Data 7",
        "exports": "Data 8",
        "net_imports_incl_spr": "Data 9",
        "product_supplied": "Data 10",
        "ulta_low_sulfur_distillate_reclassification": "Data 11",
        "crude_production_avg": "Data 12",
        "inputs_utilization_avg": "Data 13",
        "refiner_blender_net_production_avg": "Data 14",
        "net_production_by_production_avg": "Data 15",
        "ethanol_plant_production_avg": "Data 16",
        "imports_avg": "Data 17",
        "exports_avg": "Data 18",
        "net_imports_inc_spr_avg": "Data 19",
        "product_supplied_avg": "Data 20",
        "ulta_low_sulfur_distillate_reclassification_avg": "Data 21",
    },
    "spot_prices_crude_gas_heating": {
        "crude": "Data 1",
        "conventional_gas": "Data 2",
        "rbob": "Data 3",
        "heating_oil": "Data 4",
    },
    "spot_prices_diesel_jet_fuel_propane": {
        "diesel": "Data 1",
        "jet_fuel": "Data 2",
        "propane": "Data 3",
    },
    "retail_prices": {
        "weekly": "Data 1",
        "monthly": "Data 2",
    },
}

WPSR_CATEGORY_LABELS = {
    "balance_sheet": "Balance Sheet",
    "inputs_and_production": "Inputs & Production",
    "refiner_blender_net_production": "Refiner/Blender Net Production",
    "crude_petroleum_stocks": "Crude & Petroleum Stocks",
    "gasoline_fuel_stocks": "Gasoline & Fuel Stocks",
    "total_gasoline_by_sub_padd": "Total Gasoline by Sub-PADD",
    "distillate_fuel_oil_stocks": "Distillate Fuel Oil Stocks",
    "imports": "Imports",
    "imports_by_country": "Imports by Country",
    "weekly_estimates": "Weekly Estimates",
    "spot_prices_crude_gas_heating": "Spot Prices: Crude, Gas, Heating",
    "spot_prices_diesel_jet_fuel_propane": "Spot Prices: Diesel, Jet, Propane",
    "retail_prices": "Retail Prices",
}


def _read_sheet(xls: pd.ExcelFile, category: str, table_name: str, sheet_name: str) -> pd.DataFrame | None:
    import re

    try:
        table_label = pd.read_excel(xls, sheet_name, header=None, nrows=1).iloc[0, 1]
    except Exception:
        log.exception("WPSR: failed to read table label %s/%s", category, table_name)
        return None

    pattern = r"Data (\d):"
    table_label = re.sub(pattern, lambda m: f"Data 0{m.group(1)}:", str(table_label))

    try:
        hdr = pd.read_excel(xls, sheet_name, header=[1, 2], nrows=3)
    except Exception:
        log.exception("WPSR: failed to read headers %s/%s", category, table_name)
        return None

    symbols = hdr.columns.get_level_values(0).tolist()
    titles = [d.replace(".1", "") for d in hdr.columns.get_level_values(1).tolist()]
    title_map = dict(zip(symbols, titles))

    try:
        df = pd.read_excel(xls, sheet_name, header=None, skiprows=3)
    except Exception:
        log.exception("WPSR: failed to read data %s/%s", category, table_name)
        return None

    df.columns = [d.replace("Sourcekey", "date") for d in symbols]

    value_cols = [c for c in df.columns if c != "date"]
    df = df.melt(
        id_vars="date",
        value_vars=value_cols,
        var_name="symbol",
    ).dropna()
    df = df.reset_index(drop=True)

    df["title"] = df["symbol"].map(title_map)
    df["unit"] = df["title"].map(lambda x: x.split(" (")[-1].split(")")[0] if " (" in str(x) else "")
    units = [f"({d})" for d in df["unit"].unique().tolist() if d]
    for u in units:
        df["title"] = df["title"].str.replace(u, "", regex=False).str.strip()

    df["table_label"] = table_label
    df["order"] = df.groupby("date").cumcount() + 1
    df["date"] = df["date"].apply(
        lambda v: v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v).strip()
    )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    df["category"] = category
    df["table_name"] = table_name

    return df[["category", "table_name", "table_label", "symbol", "title", "unit", "date", "value", "order"]]


def _read_sheets(raw_bytes: bytes, category: str, tables: dict[str, str]) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(raw_bytes), engine="calamine")
    all_dfs = []
    for table_name, sheet_name in tables.items():
        df = _read_sheet(xls, category, table_name, sheet_name)
        if df is not None and not df.empty:
            all_dfs.append(df)
    if not all_dfs:
        return pd.DataFrame(columns=["category", "table_name", "table_label", "symbol", "title", "unit", "date", "value", "order"])
    return pd.concat(all_dfs, ignore_index=True)


def _flush_wpsr(con, df: pd.DataFrame) -> None:
    con.execute("INSERT INTO wpsr_data SELECT * FROM df")


def _last_wpsr_release() -> datetime:
    """Return the most recent WPSR release datetime (Wednesday 10:30 AM ET)."""
    from zoneinfo import ZoneInfo
    et = ZoneInfo("America/New_York")
    now_et = datetime.now(et)
    release_time = now_et.replace(hour=10, minute=30, second=0, microsecond=0)
    days_since_wed = (now_et.weekday() - 2) % 7
    last_wed = release_time - timedelta(days=days_since_wed)
    if days_since_wed == 0 and now_et < release_time:
        last_wed -= timedelta(days=7)
    return last_wed.astimezone(timezone.utc)


def ingest_wpsr(con) -> dict:
    now = datetime.now(timezone.utc)
    latest_release = _last_wpsr_release()

    try:
        row = con.execute("SELECT MAX(fetched_at) FROM wpsr_releases").fetchone()
        if row and row[0]:
            last_fetched = datetime.fromisoformat(row[0])
            if last_fetched >= latest_release:
                log.info("WPSR: already have data from latest release (%s), skipping",
                         latest_release.strftime("%Y-%m-%d %H:%M %Z"))
                return {"tables": 0, "rows": 0, "skipped": True}
    except Exception:
        pass

    now_iso = now.isoformat()
    total_rows = 0
    total_tables = 0

    con.execute("DELETE FROM wpsr_data")
    con.execute("DELETE FROM wpsr_releases")

    for category, url in WPSR_FILE_MAP.items():
        tables = WPSR_TABLE_MAP.get(category, {})
        if not tables:
            continue

        log.info("WPSR: downloading %s", category)
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
        except Exception:
            log.exception("WPSR: failed to download %s", category)
            continue

        df = _read_sheets(resp.content, category, tables)
        if not df.empty:
            _flush_wpsr(con, df)
            total_rows += len(df)

        for table_name, sheet_name in tables.items():
            con.execute(
                "INSERT OR REPLACE INTO wpsr_releases VALUES (?, ?, ?, ?)",
                (category, table_name, sheet_name, now_iso),
            )
            total_tables += 1

        log.info("WPSR: %s — %d tables, %d rows", category, len(tables), len(df))

    log.info("WPSR ingest complete: %d tables, %d data rows", total_tables, total_rows)
    return {"tables": total_tables, "rows": total_rows}
