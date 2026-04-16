import re

MULTIPLIER_PREFIXES = [
    (re.compile(r"\bQuadrillion\b", re.I), 1e15),
    (re.compile(r"\bTrillion\b", re.I), 1e12),
    (re.compile(r"\bBillion\b", re.I), 1e9),
    (re.compile(r"\bMillion\b", re.I), 1e6),
    (re.compile(r"\bThousands? of\b", re.I), 1e3),
    (re.compile(r"\bThousand\b", re.I), 1e3),
    (re.compile(r"\bHundred\b", re.I), 1e2),
]

_STRIP_PREFIX = re.compile(
    r"^(Quadrillion|Trillion|Billion|Million|Thousands? of|Thousand|Hundred)\s+",
    re.I,
)


def parse_units(units: str) -> tuple[float, str]:
    if not units:
        return 1.0, units
    multiplier = 1.0
    for pat, mult in MULTIPLIER_PREFIXES:
        if pat.search(units):
            multiplier = mult
            break
    label = _STRIP_PREFIX.sub("", units).strip()
    if label:
        label = label[0].upper() + label[1:]
    return multiplier, label or units

DATASET_FUEL_MAP = {
    "PET": "petroleum",
    "PET_IMPORTS": "petroleum",
    "NG": "natural_gas",
    "COAL": "coal",
    "ELEC": "electricity",
    "NUC_STATUS": "nuclear",
    "EBA": "electricity",
}

MULTI_FUEL_DATASETS = {"TOTAL", "SEDS", "STEO", "INTL", "EMISS", "IEO", "AEO"}

NAME_FUEL_PATTERNS = [
    (re.compile(r"\bcrude oil\b|\bpetroleum\b|\bgasoline\b|\bdiesel\b|\bdistillate\b|\bresidual fuel\b|\bjet fuel\b|\bkerosene\b|\bpropane\b|\bLPG\b|\bNGL\b|\bfuel oil\b|\basphalt\b|\blubricant\b|\bnaphtha\b|\bwax\b|\bHGL\b", re.I), "petroleum"),
    (re.compile(r"\bnatural gas\b|\bNG\b", re.I), "natural_gas"),
    (re.compile(r"\bcoal\b|\bcoke\b", re.I), "coal"),
    (re.compile(r"\bnuclear\b|\buranium\b", re.I), "nuclear"),
    (re.compile(r"\bsolar\b|\bphotovoltaic\b|\bPV\b", re.I), "solar"),
    (re.compile(r"\bwind\b", re.I), "wind"),
    (re.compile(r"\bhydroelectric\b|\bhydro\b|\bpumped storage\b", re.I), "hydroelectric"),
    (re.compile(r"\bbiomass\b|\bwood\b|\bwaste\b|\blandfill\b|\bbiofuel\b|\bethanol\b|\bbiodiesel\b|\brenewable diesel\b", re.I), "biomass"),
    (re.compile(r"\bgeothermal\b", re.I), "geothermal"),
    (re.compile(r"\brenewable\b", re.I), "renewable"),
    (re.compile(r"\belectricity\b|\belectric\b|\bgenerat\b", re.I), "electricity"),
]

MEASURE_PATTERNS = [
    (re.compile(r"price|cost|revenue|tariff|rate", re.I), "price"),
    (re.compile(r"product|generat|output|supply", re.I), "production"),
    (re.compile(r"consum|demand|sales|use|deliveri", re.I), "consumption"),
    (re.compile(r"import", re.I), "imports"),
    (re.compile(r"export", re.I), "exports"),
    (re.compile(r"stock|inventor|storage", re.I), "stocks"),
    (re.compile(r"reserv|proved", re.I), "reserves"),
    (re.compile(r"emiss|co2|carbon", re.I), "emissions"),
    (re.compile(r"capacit", re.I), "capacity"),
]

GEO_PATTERNS = [
    (re.compile(r"^US-[A-Z]{2}$"), "state"),
    (re.compile(r"PADD", re.I), "padd"),
    (re.compile(r"^USA?$"), "national"),
]


def infer_fuel_type(dataset_id: str, series_id: str, series_name: str = "") -> str:
    base = dataset_id.split(".")[0]
    if base in DATASET_FUEL_MAP:
        return DATASET_FUEL_MAP[base]

    if base in MULTI_FUEL_DATASETS and series_name:
        for pattern, fuel in NAME_FUEL_PATTERNS:
            if pattern.search(series_name):
                return fuel

    sid = series_id.upper()
    if "PET" in sid or "CRUDE" in sid:
        return "petroleum"
    if sid.startswith("NG") or "NATGAS" in sid:
        return "natural_gas"
    if "COAL" in sid:
        return "coal"
    if "ELEC" in sid or "EBA" in sid:
        return "electricity"
    if "NUC" in sid:
        return "nuclear"
    if any(k in sid for k in ("WIND", "SOLAR", "HYDRO", "RENEW", "GEO", "BIOMASS")):
        return "renewable"
    return "total"


def infer_measure_type(series_name: str) -> str:
    for pattern, measure in MEASURE_PATTERNS:
        if pattern.search(series_name):
            return measure
    return "other"


def infer_geography_type(geography: str | None, iso3166: str | None = None) -> str:
    geo = (geography or "").strip()
    iso = (iso3166 or "").strip()

    if not geo and not iso:
        return "national"

    combined = iso or geo

    if re.match(r"^USA?$", combined, re.I):
        return "national"
    if re.match(r"^USA-[A-Z]{2}$", combined, re.I):
        return "state"
    if re.match(r"^[A-Z]{2}$", geo):
        return "state"
    if re.match(r"PADD", combined, re.I):
        return "padd"
    if "+" in combined:
        return "region"
    if re.match(r"^[A-Z]{3}$", combined):
        return "country"
    if len(combined) <= 5 and combined.isalpha():
        return "country"
    return "other"
