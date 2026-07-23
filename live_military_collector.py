#!/usr/bin/env python3
"""
live_military_collector.py

Monitor publicznie widocznych lotów wojskowych nad Polską.

Działanie:
- co uruchomienie pobiera ADSB.lol /v2/mil;
- akceptuje wyłącznie pozycje znajdujące się wewnątrz wielokąta Polski;
- odrzuca rekordy bez pozycji albo ze zbyt starą pozycją;
- zapisuje próbki do SQLite;
- po pierwszym uruchomieniu w nowej godzinie tworzy raport za poprzednią;
- raport jest gotowy do skopiowania do social media;
- gdy raport zawiera loty, Discord dostaje embed oraz plik .md do pobrania.

Wymagania:
    pip install requests pandas

Zmienne środowiskowe:
    ADSB_API_URL              domyślnie https://api.adsb.lol/v2/mil
    DATABASE_PATH             domyślnie data/military_flights.sqlite3
    REPORTS_DIR               domyślnie reports/hourly
    DISCORD_WEBHOOK_URL       opcjonalny sekret GitHub Actions
    RETENTION_DAYS            domyślnie 14
    MAX_SEEN_SECONDS          domyślnie 120
    MIN_ALTITUDE_FT           domyślnie 0
    MIN_SAMPLES_PER_FLIGHT    domyślnie 1
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests


# =============================================================================
# Konfiguracja
# =============================================================================

UTC = timezone.utc
POLAND_TZ = ZoneInfo("Europe/Warsaw")

ADSB_API_URL = os.getenv(
    "ADSB_API_URL",
    "https://api.adsb.lol/v2/mil",
).strip()

DATABASE_PATH = Path(
    os.getenv("DATABASE_PATH", "data/military_flights.sqlite3")
)
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports/hourly"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 5

RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "14"))
MAX_SEEN_SECONDS = int(os.getenv("MAX_SEEN_SECONDS", "120"))
MIN_ALTITUDE_FT = int(os.getenv("MIN_ALTITUDE_FT", "0"))
MIN_SAMPLES_PER_FLIGHT = int(os.getenv("MIN_SAMPLES_PER_FLIGHT", "1"))

# Szybki bbox Polski — tylko wstępne odrzucenie odległych obiektów.
POLAND_BOUNDS = {
    "lat_min": 48.70,
    "lat_max": 55.20,
    "lon_min": 13.70,
    "lon_max": 24.40,
}

# Uproszczony wielokąt granic Polski.
# Format punktu: (longitude, latitude).
# Cel: odrzucać obiekty z Czech, Niemiec, Słowacji, Białorusi i Bałtyku,
# które trafiały do raportów przy samym bbox.
POLAND_POLYGON: tuple[tuple[float, float], ...] = (
    (14.12, 53.92),
    (14.32, 54.18),
    (14.70, 54.46),
    (15.20, 54.63),
    (16.20, 54.84),
    (17.20, 54.84),
    (18.20, 54.75),
    (19.10, 54.60),
    (19.70, 54.48),
    (20.30, 54.42),
    (21.00, 54.35),
    (21.70, 54.27),
    (22.30, 54.18),
    (23.00, 54.10),
    (23.50, 54.00),
    (23.70, 53.65),
    (23.85, 53.20),
    (23.70, 52.70),
    (23.80, 52.10),
    (23.55, 51.55),
    (23.80, 50.85),
    (23.35, 50.35),
    (22.90, 49.55),
    (22.30, 49.15),
    (21.60, 49.00),
    (20.80, 49.02),
    (20.10, 49.10),
    (19.35, 49.25),
    (18.70, 49.45),
    (18.10, 49.50),
    (17.55, 49.55),
    (17.05, 49.55),
    (16.45, 50.05),
    (16.00, 50.35),
    (15.50, 50.75),
    (15.05, 51.05),
    (14.65, 51.35),
    (14.20, 51.60),
    (14.45, 52.10),
    (14.70, 52.50),
    (14.55, 52.95),
    (14.20, 53.30),
    (14.12, 53.92),
)

# Lotniska są używane wyłącznie do opisu orientacyjnej bliskości pozycji.
# Nie stanowią potwierdzenia startu lub lądowania.
POLISH_AIRPORTS: tuple[tuple[str, str, float, float], ...] = (
    ("EPWA", "Warszawa-Chopin", 52.1657, 20.9671),
    ("EPMM", "Mińsk Mazowiecki", 52.1950, 21.6553),
    ("EPRA", "Radom", 51.3892, 21.2133),
    ("EPPW", "Bydgoszcz", 53.0968, 17.9777),
    ("EPRZ", "Rzeszów-Jasionka", 50.1100, 22.0190),
    ("EPKK", "Kraków-Balice", 50.0777, 19.7848),
    ("EPPO", "Poznań-Ławica", 52.4210, 16.8263),
    ("EPWR", "Wrocław", 51.1027, 16.8858),
    ("EPGD", "Gdańsk", 54.3776, 18.4662),
    ("EPKT", "Katowice", 50.4743, 19.0800),
    ("EPDE", "Dęblin", 51.5519, 21.8933),
    ("EPMB", "Malbork", 54.0275, 19.1342),
    ("EPSN", "Świdwin", 53.7900, 15.8267),
    ("EPIR", "Inowrocław", 52.7944, 18.2639),
    ("EPCE", "Zegrze Pomorskie", 54.4167, 16.2667),
    ("EPKS", "Krzesiny", 52.3319, 16.9661),
    ("EPBL", "Biała Podlaska", 52.0008, 23.1422),
)

AIRPORT_PROXIMITY_KM = 15.0

MILITARY_CALLSIGN_PATTERN = re.compile(
    r"^(PLF|RCH|REACH|NATO|SNAKE|NACHO|HERK(?:Y)?|DUKE|SPAR|EVAC|"
    r"SAM|MMF|ASCOT|RRR|CNV|IAM|LAGR|BAF|FAF|GAF|NOH|SVF|CFC|CEF|"
    r"POL|PLAF|PSYOP|TOPCAT|TIGER|MACE|JEDI|GHOST|RAZOR|VIPER|"
    r"HAWK|RAVEN|COBRA)[A-Z0-9-]*$"
)

MILITARY_TYPE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"^C130J", "C-130J Hercules"),
    (r"^C30J", "C-130J Hercules"),
    (r"^C130", "C-130 Hercules"),
    (r"^C17", "C-17 Globemaster III"),
    (r"^C5", "C-5 Galaxy"),
    (r"^C27", "C-27J Spartan"),
    (r"^C295", "C-295"),
    (r"^C160", "C-160 Transall"),
    (r"^KC10", "KC-10 Extender"),
    (r"^KC135|^K35R|^K35E", "KC-135 Stratotanker"),
    (r"^A332", "A330 MRTT"),
    (r"^A400", "A400M Atlas"),
    (r"^E3", "E-3 Sentry AWACS"),
    (r"^E7", "E-7 Wedgetail"),
    (r"^E2", "E-2 Hawkeye"),
    (r"^P8", "P-8 Poseidon"),
    (r"^F16", "F-16 Fighting Falcon"),
    (r"^F35", "F-35 Lightning II"),
    (r"^F15", "F-15 Eagle"),
    (r"^FA18|^F18", "F/A-18 Hornet"),
    (r"^A10", "A-10 Thunderbolt II"),
    (r"^H60|^S70", "UH-60 / MH-60 Black Hawk"),
    (r"^CH47", "CH-47 Chinook"),
    (r"^V22", "V-22 Osprey"),
)

log = logging.getLogger("live-military-collector")


# =============================================================================
# Modele i funkcje pomocnicze
# =============================================================================

@dataclass(frozen=True)
class Classification:
    type_label: str
    reasons: list[str]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def normalized(value: Any) -> str:
    return str(value or "").strip().upper()


def markdown_safe(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def point_in_polygon(
    latitude: float,
    longitude: float,
    polygon: tuple[tuple[float, float], ...],
) -> bool:
    """
    Algorytm ray-casting.

    polygon przechowuje punkty jako (longitude, latitude).
    """
    inside = False
    previous_lon, previous_lat = polygon[-1]

    for current_lon, current_lat in polygon:
        crosses_latitude = (
            (current_lat > latitude) != (previous_lat > latitude)
        )

        if crosses_latitude:
            intersection_lon = (
                (previous_lon - current_lon)
                * (latitude - current_lat)
                / (previous_lat - current_lat)
                + current_lon
            )

            if longitude < intersection_lon:
                inside = not inside

        previous_lon, previous_lat = current_lon, current_lat

    return inside


def is_over_poland(aircraft: dict[str, Any]) -> bool:
    """
    Przyjmuje tylko obiekty z pozycją wewnątrz Polski.

    Sam bounding box był niewystarczający, bo obejmuje terytorium
    kilku państw sąsiadujących. Najpierw stosujemy bbox dla wydajności,
    następnie rzeczywisty, uproszczony wielokąt granic.
    """
    latitude = safe_float(aircraft.get("lat"))
    longitude = safe_float(aircraft.get("lon"))

    if latitude is None or longitude is None:
        return False

    if not (
        POLAND_BOUNDS["lat_min"] <= latitude <= POLAND_BOUNDS["lat_max"]
        and POLAND_BOUNDS["lon_min"] <= longitude <= POLAND_BOUNDS["lon_max"]
    ):
        return False

    return point_in_polygon(
        latitude=latitude,
        longitude=longitude,
        polygon=POLAND_POLYGON,
    )


def is_recent_position(aircraft: dict[str, Any]) -> bool:
    """
    Odrzuca wpisy, których ostatnia pozycja jest starsza niż ustawiony limit.

    ADSB.lol zwykle używa pola seen_pos w sekundach. Jeśli nie ma pola,
    rekord przechodzi dalej — nie należy odrzucać poprawnego wpisu tylko
    przez brak metadanej.
    """
    seen_pos = safe_float(aircraft.get("seen_pos"))

    if seen_pos is None:
        return True

    return seen_pos <= MAX_SEEN_SECONDS


def valid_altitude(aircraft: dict[str, Any]) -> bool:
    """
    Odrzuca wyłącznie rekordy z jawną, numeryczną wysokością niższą
    niż limit. Brak wysokości pozostawiamy, bo MLAT może jej nie podawać.
    """
    altitude = safe_float(
        aircraft.get("alt_baro") or aircraft.get("alt_geom")
    )

    return altitude is None or altitude >= MIN_ALTITUDE_FT


def distance_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    radius = 6371.0

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    value = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad)
        * math.cos(lat2_rad)
        * math.sin(delta_lon / 2) ** 2
    )

    return 2 * radius * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def nearest_airport_code(latitude: Any, longitude: Any) -> str:
    """
    Zwraca ICAO tylko, gdy obserwacja leżała do 15 km od lotniska.
    Jest to opis „w pobliżu”, nie dowód startu lub lądowania.
    """
    lat = safe_float(latitude)
    lon = safe_float(longitude)

    if lat is None or lon is None:
        return "—"

    nearest_code = "—"
    nearest_distance = float("inf")

    for code, _name, airport_lat, airport_lon in POLISH_AIRPORTS:
        current_distance = distance_km(lat, lon, airport_lat, airport_lon)

        if current_distance < nearest_distance:
            nearest_distance = current_distance
            nearest_code = code

    if nearest_distance <= AIRPORT_PROXIMITY_KM:
        return nearest_code

    return "—"


# =============================================================================
# ADSB.lol
# =============================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "MGYTT-MilitaryFlightReport/6.0",
        }
    )
    return session


def fetch_snapshot(session: requests.Session) -> list[dict[str, Any]]:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                ADSB_API_URL,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                wait_seconds = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "ADSB.lol HTTP 429. Ponowienie za %s sekund.",
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("Odpowiedź API nie jest obiektem JSON.")

            aircraft = payload.get("ac") or payload.get("aircraft") or []

            if not isinstance(aircraft, list):
                raise ValueError("Pole ac/aircraft nie jest listą.")

            return [item for item in aircraft if isinstance(item, dict)]

        except (requests.RequestException, ValueError) as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                wait_seconds = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "Błąd pobrania ADSB.lol: %s. Ponowienie za %s s.",
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)

    raise RuntimeError(f"Nie udało się pobrać ADSB.lol: {last_error}")


# =============================================================================
# Klasyfikacja i SQLite
# =============================================================================

def classify_aircraft(aircraft: dict[str, Any]) -> Classification:
    """
    /v2/mil jest głównym źródłem klasyfikacji. Poniższe elementy
    opisują, dlaczego lot można łatwiej rozpoznać w raporcie.
    """
    callsign = normalized(aircraft.get("flight") or aircraft.get("callsign"))
    aircraft_type = normalized(aircraft.get("t") or aircraft.get("type"))

    reasons = ["źródło API: /v2/mil"]
    type_label = aircraft_type or "Nieznany typ"

    if callsign and MILITARY_CALLSIGN_PATTERN.match(callsign):
        reasons.append(f"callsign: {callsign}")

    for pattern, label in MILITARY_TYPE_PATTERNS:
        if aircraft_type and re.search(pattern, aircraft_type):
            type_label = label
            reasons.append(f"typ ICAO: {aircraft_type}")
            break

    db_flags = safe_int(
        aircraft.get("dbFlags", aircraft.get("dbflags", 0))
    )

    if db_flags is not None and db_flags & 1:
        reasons.append("dbFlags: military")

    return Classification(type_label=type_label, reasons=reasons)


def init_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode = DELETE;
        PRAGMA synchronous = FULL;

        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at_utc TEXT NOT NULL,
            observed_at_lt TEXT NOT NULL,
            hex TEXT NOT NULL,
            registration TEXT,
            callsign TEXT,
            aircraft_type TEXT,
            type_label TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            altitude_ft REAL,
            groundspeed_kt REAL,
            track_deg REAL,
            seen_pos_seconds REAL,
            classification_reasons TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            UNIQUE(observed_at_utc, hex)
        );

        CREATE INDEX IF NOT EXISTS idx_observations_time
        ON observations(observed_at_utc);

        CREATE INDEX IF NOT EXISTS idx_observations_hex_time
        ON observations(hex, observed_at_utc);

        CREATE TABLE IF NOT EXISTS generated_reports (
            hour_start_utc TEXT PRIMARY KEY,
            report_path TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            flights_count INTEGER NOT NULL,
            discord_sent INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    connection.commit()


def save_snapshot(
    connection: sqlite3.Connection,
    aircraft_list: list[dict[str, Any]],
    observed_at: datetime,
) -> tuple[int, int, int]:
    """
    Zwraca:
    - liczba obiektów nad Polską po filtrze polygon,
    - liczba nowych rekordów,
    - liczba odrzuconych jako nieaktualne / niespełniające kryteriów.
    """
    in_poland = 0
    inserted = 0
    rejected = 0

    for aircraft in aircraft_list:
        if not is_over_poland(aircraft):
            continue

        in_poland += 1

        if not is_recent_position(aircraft) or not valid_altitude(aircraft):
            rejected += 1
            continue

        hex_code = normalized(aircraft.get("hex"))
        latitude = safe_float(aircraft.get("lat"))
        longitude = safe_float(aircraft.get("lon"))

        if not hex_code or latitude is None or longitude is None:
            rejected += 1
            continue

        classification = classify_aircraft(aircraft)

        registration = normalized(aircraft.get("r") or aircraft.get("reg")) or None
        callsign = normalized(
            aircraft.get("flight") or aircraft.get("callsign")
        ) or None
        aircraft_type = normalized(
            aircraft.get("t") or aircraft.get("type")
        ) or None

        try:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO observations (
                    observed_at_utc,
                    observed_at_lt,
                    hex,
                    registration,
                    callsign,
                    aircraft_type,
                    type_label,
                    lat,
                    lon,
                    altitude_ft,
                    groundspeed_kt,
                    track_deg,
                    seen_pos_seconds,
                    classification_reasons,
                    raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observed_at.astimezone(UTC).isoformat(),
                    observed_at.astimezone(POLAND_TZ).isoformat(),
                    hex_code,
                    registration,
                    callsign,
                    aircraft_type,
                    classification.type_label,
                    latitude,
                    longitude,
                    safe_float(
                        aircraft.get("alt_baro") or aircraft.get("alt_geom")
                    ),
                    safe_float(aircraft.get("gs")),
                    safe_float(aircraft.get("track")),
                    safe_float(aircraft.get("seen_pos")),
                    ", ".join(classification.reasons),
                    json.dumps(
                        aircraft,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                ),
            )

            if cursor.rowcount > 0:
                inserted += 1

        except (sqlite3.Error, TypeError, ValueError) as exc:
            rejected += 1
            log.warning("Nie zapisano ICAO %s: %s", hex_code, exc)

    connection.commit()
    return in_poland, inserted, rejected


# =============================================================================
# Raport
# =============================================================================

def get_last_closed_hour(now_lt: datetime) -> tuple[datetime, datetime]:
    end_lt = now_lt.replace(minute=0, second=0, microsecond=0)
    return end_lt - timedelta(hours=1), end_lt


def report_exists(
    connection: sqlite3.Connection,
    report_start_lt: datetime,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM generated_reports
        WHERE hour_start_utc = ?
        """,
        (report_start_lt.astimezone(UTC).isoformat(),),
    ).fetchone()

    return row is not None


def get_hour_observations(
    connection: sqlite3.Connection,
    start_lt: datetime,
    end_lt: datetime,
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            observed_at_lt,
            hex,
            registration,
            callsign,
            aircraft_type,
            type_label,
            lat,
            lon,
            altitude_ft,
            groundspeed_kt,
            track_deg,
            seen_pos_seconds,
            classification_reasons
        FROM observations
        WHERE observed_at_utc >= ?
          AND observed_at_utc < ?
        ORDER BY observed_at_utc ASC
        """,
        connection,
        params=(
            start_lt.astimezone(UTC).isoformat(),
            end_lt.astimezone(UTC).isoformat(),
        ),
    )


def aggregate_flights(observations: pd.DataFrame) -> pd.DataFrame:
    """
    Łączy próbki po ICAO Hex + callsignie.

    Wymaga co najmniej MIN_SAMPLES_PER_FLIGHT próbek. Przy schedulerze
    GitHub co 5 minut rozsądna wartość domyślna pozostaje równa 1.
    """
    if observations.empty:
        return pd.DataFrame()

    data = observations.copy()
    data["observed_at_lt"] = pd.to_datetime(data["observed_at_lt"])
    data["hex"] = data["hex"].fillna("NIEZNANY")
    data["callsign"] = data["callsign"].fillna("BRAK")
    data["registration"] = data["registration"].fillna("NIEZNANA")
    data["type_label"] = data["type_label"].fillna("NIEZNANY TYP")

    flights: list[dict[str, Any]] = []

    for (hex_code, callsign), group in data.groupby(
        ["hex", "callsign"],
        dropna=False,
    ):
        group = group.sort_values("observed_at_lt")

        if len(group) < MIN_SAMPLES_PER_FLIGHT:
            continue

        first = group.iloc[0]
        last = group.iloc[-1]

        flights.append(
            {
                "hex": hex_code,
                "callsign": callsign,
                "registration": first["registration"],
                "type_label": first["type_label"],
                "first_seen_lt": first["observed_at_lt"],
                "last_seen_lt": last["observed_at_lt"],
                "samples": len(group),
                "first_lat": first["lat"],
                "first_lon": first["lon"],
                "last_lat": last["lat"],
                "last_lon": last["lon"],
                "near_first_airport": nearest_airport_code(
                    first["lat"],
                    first["lon"],
                ),
                "near_last_airport": nearest_airport_code(
                    last["lat"],
                    last["lon"],
                ),
            }
        )

    if not flights:
        return pd.DataFrame()

    return pd.DataFrame(flights).sort_values("first_seen_lt")


def route_text(flight: pd.Series) -> str:
    """
    Opis jest celowo ostrożny — nie określa potwierdzonej trasy.
    """
    first_airport = str(flight["near_first_airport"])
    last_airport = str(flight["near_last_airport"])

    if first_airport != "—" and last_airport != "—":
        return f"w pobliżu {first_airport} → {last_airport}"

    if first_airport != "—":
        return f"w pobliżu {first_airport}"

    if last_airport != "—":
        return f"w pobliżu {last_airport}"

    return "trasa nieustalona"


def social_line(flight: pd.Series) -> str:
    return (
        f'{str(flight["type_label"]).upper()} '
        f'"{str(flight["registration"]).upper()}" '
        f'{str(flight["callsign"]).upper()} '
        f'{flight["first_seen_lt"].strftime("%H:%M")}LT | '
        f"{route_text(flight)}"
    )


def social_summary(flights: pd.DataFrame, max_lines: int) -> str:
    if flights.empty:
        return "Brak wykrytych lotów."

    lines = [
        social_line(flight)
        for _, flight in flights.head(max_lines).iterrows()
    ]

    remaining = len(flights) - max_lines
    if remaining > 0:
        lines.append(f"… oraz {remaining} kolejnych wpisów w załączniku.")

    return "\n".join(lines)


def build_report(
    observations: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> tuple[str, int, pd.DataFrame]:
    flights = aggregate_flights(observations)

    lines = [
        f"# Loty wojskowe nad Polską — {start_lt.strftime('%d.%m.%Y')}",
        "",
        (
            f"**Okno obserwacji:** "
            f"{start_lt.strftime('%H:%M')}–{end_lt.strftime('%H:%M')} LT"
        ),
        "",
        "## Podsumowanie do publikacji",
        "",
    ]

    if flights.empty:
        lines.extend(
            [
                "```text",
                "Brak zakwalifikowanych publicznie widocznych lotów wojskowych nad Polską.",
                "```",
                "",
                "> Brak publicznych danych ADS-B nie oznacza braku aktywności wojskowej.",
                "",
            ]
        )
        return "\n".join(lines), 0, flights

    lines.extend(
        [
            "```text",
            social_summary(flights, max_lines=999),
            "```",
            "",
            "## Dane szczegółowe",
            "",
            f"**Wykryte loty/ślady:** {len(flights)}",
            "",
            "| Typ | Rejestracja | Callsign | ICAO Hex | Pierwsza → ostatnia obserwacja LT | Obserwacja przy lotnisku | Próbki |",
            "|---|---|---|---|---|---|---:|",
        ]
    )

    for _, flight in flights.iterrows():
        observed_time = (
            f"{flight['first_seen_lt'].strftime('%H:%M')} → "
            f"{flight['last_seen_lt'].strftime('%H:%M')}"
        )

        lines.append(
            f"| {markdown_safe(flight['type_label'])} | "
            f"{markdown_safe(flight['registration'])} | "
            f"`{markdown_safe(flight['callsign'])}` | "
            f"`{markdown_safe(flight['hex'])}` | "
            f"{observed_time} | "
            f"{markdown_safe(route_text(flight))} | "
            f"{int(flight['samples'])} |"
        )

    lines.extend(
        [
            "",
            "## Metoda i ograniczenia",
            "",
            "- Rekord trafia do raportu tylko wtedy, gdy jego pozycja ADS-B znajdowała się wewnątrz uproszczonego wielokąta granic Polski.",
            "- „W pobliżu lotniska” oznacza, że pierwsza lub ostatnia zapisana pozycja była do 15 km od tego lotniska; nie potwierdza startu, lądowania ani pełnej trasy.",
            "- Czas oznacza pierwszą i ostatnią obserwację w zebranych próbkach, nie moment przekroczenia granicy.",
            "- Raport dotyczy wyłącznie publicznie widocznych danych ADS-B/MLAT.",
            "",
        ]
    )

    return "\n".join(lines), len(flights), flights


def save_report(content: str, start_lt: datetime) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    path = REPORTS_DIR / (
        f"raport-{start_lt.strftime('%Y-%m-%d_%H-00')}.md"
    )
    path.write_text(content, encoding="utf-8")
    return path


# =============================================================================
# Discord
# =============================================================================

def send_discord_report(
    session: requests.Session,
    report_path: Path,
    flights: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> bool:
    if flights.empty:
        log.info("Discord: raport pusty — nie wysyłam powiadomienia.")
        return False

    if not DISCORD_WEBHOOK_URL:
        log.warning(
            "Discord: raport zawiera %s lotów, ale DISCORD_WEBHOOK_URL nie jest ustawiony.",
            len(flights),
        )
        return False

    preview = social_summary(flights, max_lines=6)

    if len(preview) > 3500:
        preview = preview[:3490] + "\n… pełna lista w załączniku."

    window = (
        f"{start_lt.strftime('%d.%m.%Y %H:%M')}–"
        f"{end_lt.strftime('%H:%M')} LT"
    )

    payload = {
        "username": "Military Flight Report",
        "content": "📎 Pełny raport w formacie Markdown jest dostępny w załączniku.",
        "embeds": [
            {
                "title": f"✈️ Loty wojskowe nad Polską — {len(flights)}",
                "description": f"```text\n{preview}\n```",
                "color": 15158332,
                "fields": [
                    {
                        "name": "Okno obserwacji",
                        "value": window,
                        "inline": False,
                    },
                    {
                        "name": "Załącznik",
                        "value": f"`{report_path.name}`",
                        "inline": False,
                    },
                ],
                "footer": {
                    "text": (
                        "Publiczne ADS-B/MLAT • pozycje filtrowane do granic Polski"
                    )
                },
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ],
    }

    try:
        with report_path.open("rb") as report_file:
            response = session.post(
                DISCORD_WEBHOOK_URL,
                data={"payload_json": json.dumps(payload)},
                files={
                    "files[0]": (
                        report_path.name,
                        report_file,
                        "text/markdown; charset=utf-8",
                    )
                },
                timeout=30,
            )

        response.raise_for_status()
        log.info("Discord: wysłano raport %s.", report_path.name)
        return True

    except requests.RequestException as exc:
        log.error("Discord: błąd webhooka: %s", exc)
        return False


# =============================================================================
# Retencja i main
# =============================================================================

def delete_old_data(connection: sqlite3.Connection) -> None:
    cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)

    deleted = connection.execute(
        "DELETE FROM observations WHERE observed_at_utc < ?",
        (cutoff.isoformat(),),
    ).rowcount

    reports_cutoff = (
        datetime.now(UTC) - timedelta(days=RETENTION_DAYS * 2)
    ).isoformat()

    connection.execute(
        "DELETE FROM generated_reports WHERE created_at_utc < ?",
        (reports_cutoff,),
    )

    connection.commit()

    if deleted:
        log.info("Usunięto %s obserwacji starszych niż %s dni.", deleted, RETENTION_DAYS)


def main() -> int:
    configure_logging()

    now_lt = datetime.now(POLAND_TZ)
    now_utc = now_lt.astimezone(UTC)

    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    log.info(
        "Start kolektora. Czas lokalny: %s.",
        now_lt.strftime("%Y-%m-%d %H:%M:%S %Z"),
    )

    with sqlite3.connect(DATABASE_PATH) as connection:
        init_database(connection)
        session = create_session()

        try:
            aircraft = fetch_snapshot(session)

            in_poland, inserted, rejected = save_snapshot(
                connection=connection,
                aircraft_list=aircraft,
                observed_at=now_utc,
            )

            log.info(
                "ADSB.lol /v2/mil: %s globalnie; %s wewnątrz granic Polski; %s nowych zapisów; %s odrzuconych.",
                len(aircraft),
                in_poland,
                inserted,
                rejected,
            )

        except Exception as exc:
            # Raport nadal może zostać wygenerowany z próbek zapisanych wcześniej.
            log.exception("Błąd pobierania lub zapisu snapshotu: %s", exc)

        start_lt, end_lt = get_last_closed_hour(now_lt)

        if report_exists(connection, start_lt):
            log.info(
                "Raport za %s już istnieje — pomijam.",
                start_lt.strftime("%Y-%m-%d %H:00"),
            )
            delete_old_data(connection)
            return 0

        observations = get_hour_observations(
            connection=connection,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        content, flights_count, flights = build_report(
            observations=observations,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        report_path = save_report(content, start_lt)

        discord_sent = send_discord_report(
            session=session,
            report_path=report_path,
            flights=flights,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        connection.execute(
            """
            INSERT OR REPLACE INTO generated_reports (
                hour_start_utc,
                report_path,
                created_at_utc,
                flights_count,
                discord_sent
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                start_lt.astimezone(UTC).isoformat(),
                report_path.as_posix(),
                datetime.now(UTC).isoformat(),
                flights_count,
                1 if discord_sent else 0,
            ),
        )
        connection.commit()

        delete_old_data(connection)

        log.info(
            "Utworzono raport: %s | loty: %s | Discord: %s.",
            report_path,
            flights_count,
            "wysłano" if discord_sent else "nie wysłano",
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
