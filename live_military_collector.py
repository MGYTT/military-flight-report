#!/usr/bin/env python3
"""
live_military_collector.py
==========================

Military Flight Report Poland — production-grade collector.

Cel:
- zbiera WYŁĄCZNIE publicznie widoczne, aktualne pozycje ADS-B/MLAT
  samolotów oznaczonych przez źródło jako wojskowe;
- raportuje WYŁĄCZNIE pozycje wewnątrz granic Polski;
- nie przedstawia hipotez jako faktów;
- rozróżnia:
    1) trasę zewnętrznego lookupu po callsignie (prawdopodobna),
    2) obserwację blisko lotniska (obserwacyjna),
    3) brak wystarczających danych (N/A → N/A).

Wymagania:
    pip install requests pandas

Zmienne środowiskowe:
    ADSB_API_URL                 default: https://api.adsb.lol/v2/mil
    DATABASE_PATH                default: data/military_flights.sqlite3
    REPORTS_DIR                  default: reports/hourly
    DISCORD_WEBHOOK_URL          opcjonalny sekret GitHub
    RETENTION_DAYS               default: 14
    MAX_SEEN_SECONDS             default: 90
    MIN_ALTITUDE_FT              default: 0
    MIN_SAMPLES_PER_FLIGHT       default: 2
    BACKFILL_HOURS               default: 1
    DISCORD_SEND_EMPTY           default: 0
    ENABLE_ROUTE_LOOKUP          default: 0
    ROUTE_CACHE_HOURS            default: 12
    ADSBDB_ROUTE_URL_TEMPLATE    default: https://api.adsbdb.com/v0/callsign/{callsign}
    AIRPORT_PROXIMITY_KM         default: 8
    MIN_MOVEMENT_KM              default: 5
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
from typing import Any, Iterable, Optional
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
MAX_SEEN_SECONDS = int(os.getenv("MAX_SEEN_SECONDS", "90"))
MIN_ALTITUDE_FT = int(os.getenv("MIN_ALTITUDE_FT", "0"))
MIN_SAMPLES_PER_FLIGHT = int(os.getenv("MIN_SAMPLES_PER_FLIGHT", "2"))
BACKFILL_HOURS = int(os.getenv("BACKFILL_HOURS", "1"))

AIRPORT_PROXIMITY_KM = float(
    os.getenv("AIRPORT_PROXIMITY_KM", "8")
)
MIN_MOVEMENT_KM = float(os.getenv("MIN_MOVEMENT_KM", "5"))

DISCORD_SEND_EMPTY = os.getenv(
    "DISCORD_SEND_EMPTY",
    "0",
).strip().lower() in {"1", "true", "yes", "on"}

ENABLE_ROUTE_LOOKUP = os.getenv(
    "ENABLE_ROUTE_LOOKUP",
    "0",
).strip().lower() in {"1", "true", "yes", "on"}

ROUTE_CACHE_HOURS = int(os.getenv("ROUTE_CACHE_HOURS", "12"))
ADSBDB_ROUTE_URL_TEMPLATE = os.getenv(
    "ADSBDB_ROUTE_URL_TEMPLATE",
    "https://api.adsbdb.com/v0/callsign/{callsign}",
).strip()

# Wstępny, szybki bounding box — po nim zawsze działa polygon.
POLAND_BOUNDS = {
    "lat_min": 48.70,
    "lat_max": 55.20,
    "lon_min": 13.70,
    "lon_max": 24.40,
}

# Uproszczona granica Polski, punkty: (longitude, latitude).
# Polygon eliminuje fałszywe wpisy z państw sąsiednich, które przechodzą
# przez sam prostokątny bbox.
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

# Lotniska używane tylko do opisu bliskości punktu obserwacji.
# Nie są automatycznym dowodem startu ani przylotu.
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
    ("EPKS", "Poznań-Krzesiny", 52.3319, 16.9661),
    ("EPBL", "Biała Podlaska", 52.0008, 23.1422),
    ("EPD", "Powidz", 52.3794, 17.8547),
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

MILITARY_CALLSIGN_PATTERN = re.compile(
    r"^(PLF|RCH|REACH|NATO|SNAKE|NACHO|HERK(?:Y)?|DUKE|SPAR|EVAC|"
    r"SAM|MMF|ASCOT|RRR|CNV|IAM|LAGR|BAF|FAF|GAF|NOH|SVF|CFC|CEF|"
    r"POL|PLAF|PSYOP|TOPCAT|TIGER|MACE|JEDI|GHOST|RAZOR|VIPER|"
    r"HAWK|RAVEN|COBRA)[A-Z0-9-]*$"
)

log = logging.getLogger("military-flight-report")


# =============================================================================
# Modele
# =============================================================================

@dataclass(frozen=True)
class Classification:
    type_label: str
    confidence: str
    reasons: list[str]


@dataclass(frozen=True)
class ValidationResult:
    accepted: bool
    reason: str


@dataclass(frozen=True)
class RouteInfo:
    route: str
    confidence: str
    source: str


# =============================================================================
# Funkcje ogólne
# =============================================================================

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def normalized(value: Any) -> str:
    return str(value or "").strip().upper()


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


def markdown_safe(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def point_in_polygon(
    latitude: float,
    longitude: float,
    polygon: tuple[tuple[float, float], ...],
) -> bool:
    """Ray-casting; polygon używa formatu (lon, lat)."""
    inside = False
    previous_lon, previous_lat = polygon[-1]

    for current_lon, current_lat in polygon:
        intersects = (current_lat > latitude) != (previous_lat > latitude)

        if intersects:
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


def validate_aircraft(aircraft: dict[str, Any]) -> ValidationResult:
    """Weryfikuje jakość pojedynczej obserwacji."""
    if not is_over_poland(aircraft):
        return ValidationResult(False, "poza granicami Polski")

    seen_pos = safe_float(aircraft.get("seen_pos"))
    if seen_pos is not None and seen_pos > MAX_SEEN_SECONDS:
        return ValidationResult(False, "pozycja zbyt stara")

    altitude_ft = safe_float(
        aircraft.get("alt_baro") or aircraft.get("alt_geom")
    )
    if altitude_ft is not None and altitude_ft < MIN_ALTITUDE_FT:
        return ValidationResult(False, "wysokość poniżej progu")

    hex_code = normalized(aircraft.get("hex")).replace("~", "")
    if not re.fullmatch(r"[0-9A-F]{6}", hex_code):
        return ValidationResult(False, "nieprawidłowy ICAO Hex")

    return ValidationResult(True, "zaakceptowano")


def classify_aircraft(aircraft: dict[str, Any]) -> Classification:
    """Nadaje czytelną nazwę typu i poziom pewności opisu."""
    callsign = normalized(aircraft.get("flight") or aircraft.get("callsign"))
    aircraft_type = normalized(aircraft.get("t") or aircraft.get("type"))

    type_label = aircraft_type or "Nieznany typ"
    confidence = "średnia"
    reasons = ["źródło API: /v2/mil"]

    if callsign and MILITARY_CALLSIGN_PATTERN.match(callsign):
        confidence = "wysoka"
        reasons.append(f"callsign wojskowy: {callsign}")

    for pattern, readable_type in MILITARY_TYPE_PATTERNS:
        if aircraft_type and re.search(pattern, aircraft_type):
            type_label = readable_type
            confidence = "wysoka"
            reasons.append(f"typ ICAO: {aircraft_type}")
            break

    db_flags = safe_int(
        aircraft.get("dbFlags", aircraft.get("dbflags", 0))
    )

    if db_flags is not None and db_flags & 1:
        confidence = "wysoka"
        reasons.append("dbFlags: military")

    return Classification(type_label, confidence, reasons)


def haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    radius_km = 6371.0
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(delta_lon / 2) ** 2
    )

    return 2 * radius_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def bearing_degrees(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    delta_lon = math.radians(lon2 - lon1)

    y = math.sin(delta_lon) * math.cos(math.radians(lat2))
    x = (
        math.cos(math.radians(lat1)) * math.sin(math.radians(lat2))
        - math.sin(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.cos(delta_lon)
    )

    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def cardinal_direction(bearing: float) -> str:
    directions = ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
    return directions[int((bearing + 22.5) // 45) % 8]


def movement_direction(
    first_lat: Any,
    first_lon: Any,
    last_lat: Any,
    last_lon: Any,
) -> tuple[str, Optional[float]]:
    """
    Kierunek tylko z faktycznej zmiany pozycji.
    Przy ruchu poniżej MIN_MOVEMENT_KM zwraca „manewrowanie / mała zmiana”.
    """
    lat1 = safe_float(first_lat)
    lon1 = safe_float(first_lon)
    lat2 = safe_float(last_lat)
    lon2 = safe_float(last_lon)

    if None in (lat1, lon1, lat2, lon2):
        return "N/A", None

    moved_km = haversine_km(lat1, lon1, lat2, lon2)

    if moved_km < MIN_MOVEMENT_KM:
        return "manewrowanie / mała zmiana", moved_km

    return cardinal_direction(
        bearing_degrees(lat1, lon1, lat2, lon2)
    ), moved_km


def nearest_polish_airport(
    latitude: Any,
    longitude: Any,
) -> tuple[Optional[str], Optional[float]]:
    """
    Zwraca najbliższy polski kod ICAO i odległość.
    Nie określa automatycznie miejsca startu/przylotu.
    """
    lat = safe_float(latitude)
    lon = safe_float(longitude)

    if lat is None or lon is None:
        return None, None

    nearest_code: Optional[str] = None
    nearest_distance: Optional[float] = None

    for code, _name, airport_lat, airport_lon in POLISH_AIRPORTS:
        distance = haversine_km(lat, lon, airport_lat, airport_lon)

        if nearest_distance is None or distance < nearest_distance:
            nearest_code = code
            nearest_distance = distance

    return nearest_code, nearest_distance


def airport_near_position(latitude: Any, longitude: Any) -> Optional[str]:
    code, distance = nearest_polish_airport(latitude, longitude)

    if code is None or distance is None:
        return None

    return code if distance <= AIRPORT_PROXIMITY_KM else None


def observed_route(
    first_lat: Any,
    first_lon: Any,
    last_lat: Any,
    last_lon: Any,
) -> RouteInfo:
    """
    Trasa wyłącznie obserwacyjna:
    EPRZ → N/A znaczy: pierwsza próbka była blisko EPRZ,
    ale nie potwierdza wylotu z EPRZ.
    """
    first_airport = airport_near_position(first_lat, first_lon)
    last_airport = airport_near_position(last_lat, last_lon)

    origin = first_airport or "N/A"
    destination = last_airport or "N/A"

    if first_airport or last_airport:
        return RouteInfo(
            route=f"{origin} → {destination}",
            confidence="obserwacyjna",
            source="pozycje ADS-B blisko lotnisk",
        )

    return RouteInfo(
        route="N/A → N/A",
        confidence="brak danych",
        source="brak obserwacji blisko lotnisk",
    )


# =============================================================================
# ADSB.lol
# =============================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "MGYTT-MilitaryFlightReport/9.0",
        }
    )
    return session


def fetch_adsb_snapshot(session: requests.Session) -> list[dict[str, Any]]:
    """Pobiera aktualny snapshot military z retry."""
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                ADSB_API_URL,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429 or response.status_code >= 500:
                wait = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "ADSB.lol HTTP %s; retry za %s s.",
                    response.status_code,
                    wait,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("Nieprawidłowa odpowiedź JSON ADSB.lol.")

            aircraft = payload.get("ac") or payload.get("aircraft") or []

            if not isinstance(aircraft, list):
                raise ValueError("Pole ac/aircraft nie jest listą.")

            return [
                item for item in aircraft
                if isinstance(item, dict)
            ]

        except (requests.RequestException, ValueError) as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                wait = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "Błąd ADSB.lol: %s; retry za %s s.",
                    exc,
                    wait,
                )
                time.sleep(wait)

    raise RuntimeError(f"Nie udało się pobrać ADSB.lol: {last_error}")


# =============================================================================
# SQLite
# =============================================================================

def init_database(connection: sqlite3.Connection) -> None:
    """
    DELETE journal mode jest wygodniejszy w repozytorium GitHub niż WAL,
    bo nie tworzy dodatkowych plików -wal i -shm.
    """
    connection.executescript(
        """
        PRAGMA foreign_keys = ON;
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
            confidence TEXT NOT NULL,
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
            observations_count INTEGER NOT NULL,
            discord_sent INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS route_cache (
            callsign TEXT PRIMARY KEY,
            route TEXT NOT NULL,
            confidence TEXT NOT NULL,
            source TEXT NOT NULL,
            fetched_at_utc TEXT NOT NULL
        );
        """
    )
    connection.commit()


def save_snapshot(
    connection: sqlite3.Connection,
    aircraft_list: list[dict[str, Any]],
    observed_at: datetime,
) -> dict[str, int]:
    stats = {
        "global": len(aircraft_list),
        "inside_poland": 0,
        "inserted": 0,
        "outside": 0,
        "stale": 0,
        "invalid": 0,
    }

    for aircraft in aircraft_list:
        decision = validate_aircraft(aircraft)

        if not decision.accepted:
            if decision.reason == "poza granicami Polski":
                stats["outside"] += 1
            elif decision.reason == "pozycja zbyt stara":
                stats["stale"] += 1
            else:
                stats["invalid"] += 1
            continue

        stats["inside_poland"] += 1
        classification = classify_aircraft(aircraft)

        hex_code = normalized(aircraft.get("hex")).replace("~", "")
        registration = normalized(
            aircraft.get("r") or aircraft.get("reg")
        ) or None
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
                    observed_at_utc, observed_at_lt, hex, registration,
                    callsign, aircraft_type, type_label, confidence,
                    lat, lon, altitude_ft, groundspeed_kt, track_deg,
                    seen_pos_seconds, classification_reasons, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observed_at.astimezone(UTC).isoformat(),
                    observed_at.astimezone(POLAND_TZ).isoformat(),
                    hex_code,
                    registration,
                    callsign,
                    aircraft_type,
                    classification.type_label,
                    classification.confidence,
                    safe_float(aircraft.get("lat")),
                    safe_float(aircraft.get("lon")),
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
                stats["inserted"] += 1

        except (sqlite3.Error, TypeError, ValueError) as exc:
            stats["invalid"] += 1
            log.warning("SQLite: nie zapisano %s: %s", hex_code, exc)

    connection.commit()
    return stats


def report_exists(
    connection: sqlite3.Connection,
    start_lt: datetime,
) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM generated_reports
        WHERE hour_start_utc = ?
        """,
        (start_lt.astimezone(UTC).isoformat(),),
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
            observed_at_lt, hex, registration, callsign, aircraft_type,
            type_label, confidence, lat, lon, altitude_ft, groundspeed_kt,
            track_deg, seen_pos_seconds, classification_reasons
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


def record_report(
    connection: sqlite3.Connection,
    start_lt: datetime,
    report_path: Path,
    flights_count: int,
    observations_count: int,
    discord_sent: bool,
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO generated_reports (
            hour_start_utc, report_path, created_at_utc,
            flights_count, observations_count, discord_sent
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            start_lt.astimezone(UTC).isoformat(),
            report_path.as_posix(),
            datetime.now(UTC).isoformat(),
            flights_count,
            observations_count,
            1 if discord_sent else 0,
        ),
    )
    connection.commit()


def get_cached_route(
    connection: sqlite3.Connection,
    callsign: str,
) -> Optional[RouteInfo]:
    row = connection.execute(
        """
        SELECT route, confidence, source, fetched_at_utc
        FROM route_cache
        WHERE callsign = ?
        """,
        (callsign,),
    ).fetchone()

    if not row:
        return None

    route, confidence, source, fetched_at_utc = row
    fetched_at = datetime.fromisoformat(fetched_at_utc)

    if datetime.now(UTC) - fetched_at > timedelta(hours=ROUTE_CACHE_HOURS):
        return None

    return RouteInfo(route, confidence, source)


def cache_route(
    connection: sqlite3.Connection,
    callsign: str,
    route_info: RouteInfo,
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO route_cache (
            callsign, route, confidence, source, fetched_at_utc
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            callsign,
            route_info.route,
            route_info.confidence,
            route_info.source,
            datetime.now(UTC).isoformat(),
        ),
    )
    connection.commit()


def delete_old_data(connection: sqlite3.Connection) -> None:
    observations_cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)
    reports_cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS * 2)
    route_cutoff = datetime.now(UTC) - timedelta(
        hours=ROUTE_CACHE_HOURS * 3
    )

    deleted_observations = connection.execute(
        "DELETE FROM observations WHERE observed_at_utc < ?",
        (observations_cutoff.isoformat(),),
    ).rowcount

    connection.execute(
        "DELETE FROM generated_reports WHERE created_at_utc < ?",
        (reports_cutoff.isoformat(),),
    )

    connection.execute(
        "DELETE FROM route_cache WHERE fetched_at_utc < ?",
        (route_cutoff.isoformat(),),
    )

    connection.commit()

    if deleted_observations:
        log.info(
            "Retencja: usunięto %s obserwacji.",
            deleted_observations,
        )


# =============================================================================
# Zewnętrzny lookup trasy
# =============================================================================

def extract_airport_code(value: Any) -> Optional[str]:
    """Obsługuje różne możliwe formaty origin/destination w JSON."""
    if isinstance(value, str):
        code = value.strip().upper()
        return code if re.fullmatch(r"[A-Z0-9]{3,4}", code) else None

    if not isinstance(value, dict):
        return None

    for key in ("icao_code", "icao", "iata_code", "iata", "code"):
        code = extract_airport_code(value.get(key))
        if code:
            return code

    return None


def lookup_route(
    connection: sqlite3.Connection,
    session: requests.Session,
    callsign: str,
) -> RouteInfo:
    """
    Lookup trasy po callsignie.

    Zwrócona trasa ma status 'prawdopodobna', nigdy 'potwierdzona',
    ponieważ publiczny lookup może bazować na danych planowych/historycznych.
    """
    if callsign in {"", "BRAK"}:
        return RouteInfo("N/A → N/A", "brak danych", "brak callsignu")

    cached = get_cached_route(connection, callsign)
    if cached:
        return cached

    if not ENABLE_ROUTE_LOOKUP:
        return RouteInfo(
            "N/A → N/A",
            "brak danych",
            "route lookup wyłączony",
        )

    url = ADSBDB_ROUTE_URL_TEMPLATE.format(callsign=callsign)

    try:
        response = session.get(url, timeout=15)

        if response.status_code == 404:
            result = RouteInfo("N/A → N/A", "brak danych", "ADSBDB 404")
            cache_route(connection, callsign, result)
            return result

        response.raise_for_status()
        payload = response.json()

        route_data = (
            payload.get("response", {}).get("flightroute")
            or payload.get("flightroute")
            or payload.get("route")
            or {}
        )

        origin = extract_airport_code(
            route_data.get("origin")
            or route_data.get("departure")
        )
        destination = extract_airport_code(
            route_data.get("destination")
            or route_data.get("arrival")
        )

        if origin or destination:
            result = RouteInfo(
                f"{origin or 'N/A'} → {destination or 'N/A'}",
                "prawdopodobna",
                "ADSBDB callsign lookup",
            )
        else:
            result = RouteInfo(
                "N/A → N/A",
                "brak danych",
                "ADSBDB: brak origin/destination",
            )

    except (requests.RequestException, ValueError, AttributeError) as exc:
        log.warning("Route lookup %s nieudany: %s", callsign, exc)
        result = RouteInfo(
            "N/A → N/A",
            "brak danych",
            "błąd route lookup",
        )

    cache_route(connection, callsign, result)
    return result


# =============================================================================
# Agregacja i raport
# =============================================================================

def pending_report_hours(
    connection: sqlite3.Connection,
    now_lt: datetime,
) -> Iterable[tuple[datetime, datetime]]:
    current_hour = now_lt.replace(minute=0, second=0, microsecond=0)

    for offset in range(BACKFILL_HOURS, 0, -1):
        start_lt = current_hour - timedelta(hours=offset)
        end_lt = start_lt + timedelta(hours=1)

        if not report_exists(connection, start_lt):
            yield start_lt, end_lt


def aggregate_flights(
    observations: pd.DataFrame,
    connection: sqlite3.Connection,
    session: requests.Session,
) -> pd.DataFrame:
    """
    Łączy obserwacje po ICAO Hex i callsignie.

    Wymaga co najmniej MIN_SAMPLES_PER_FLIGHT obserwacji, aby ograniczyć
    fałszywe raporty z pojedynczego, przypadkowego odczytu.
    """
    if observations.empty:
        return pd.DataFrame()

    data = observations.copy()
    data["observed_at_lt"] = pd.to_datetime(data["observed_at_lt"])
    data["hex"] = data["hex"].fillna("NIEZNANY")
    data["callsign"] = data["callsign"].fillna("BRAK")
    data["registration"] = data["registration"].fillna("NIEZNANA")
    data["type_label"] = data["type_label"].fillna("NIEZNANY TYP")
    data["confidence"] = data["confidence"].fillna("średnia")

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

        direction, distance_km = movement_direction(
            first["lat"],
            first["lon"],
            last["lat"],
            last["lon"],
        )

        duration_minutes = int(
            (
                last["observed_at_lt"] - first["observed_at_lt"]
            ).total_seconds()
            // 60
        )

        route_from_lookup = lookup_route(
            connection=connection,
            session=session,
            callsign=str(callsign),
        )

        route_from_positions = observed_route(
            first_lat=first["lat"],
            first_lon=first["lon"],
            last_lat=last["lat"],
            last_lon=last["lon"],
        )

        # Priorytet: zewnętrzny lookup, ale tylko jeśli podał przynajmniej
        # jedno lotnisko. Jeśli nie — pokazujemy dane obserwacyjne.
        if route_from_lookup.route != "N/A → N/A":
            final_route = route_from_lookup
        else:
            final_route = route_from_positions

        flights.append(
            {
                "hex": str(hex_code),
                "callsign": str(callsign),
                "registration": first["registration"],
                "type_label": first["type_label"],
                "confidence": first["confidence"],
                "first_seen_lt": first["observed_at_lt"],
                "last_seen_lt": last["observed_at_lt"],
                "duration_minutes": max(0, duration_minutes),
                "samples": len(group),
                "direction": direction,
                "distance_km": distance_km,
                "max_altitude_ft": pd.to_numeric(
                    group["altitude_ft"],
                    errors="coerce",
                ).max(),
                "max_speed_kt": pd.to_numeric(
                    group["groundspeed_kt"],
                    errors="coerce",
                ).max(),
                "route": final_route.route,
                "route_confidence": final_route.confidence,
                "route_source": final_route.source,
            }
        )

    if not flights:
        return pd.DataFrame()

    return pd.DataFrame(flights).sort_values(
        ["first_seen_lt", "callsign"],
        ascending=True,
    )


def format_metric(value: Any, unit: str) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{int(round(float(value)))} {unit}"


def social_line(flight: pd.Series) -> str:
    """
    Format do bezpośredniego skopiowania na Discord/Facebook/X.
    """
    aircraft_type = str(flight["type_label"]).upper()
    registration = str(flight["registration"]).upper()
    callsign = str(flight["callsign"]).upper()

    first_time = flight["first_seen_lt"].strftime("%H:%M")
    last_time = flight["last_seen_lt"].strftime("%H:%M")

    visible_time = (
        f"{first_time} LT"
        if first_time == last_time
        else f"{first_time}–{last_time} LT"
    )

    return (
        f'{aircraft_type} "{registration}" {callsign} | '
        f"{visible_time} | "
        f"trasa: {flight['route']} [{flight['route_confidence']}] | "
        f"kierunek ADS-B: {flight['direction']}"
    )


def social_summary(flights: pd.DataFrame, max_lines: int) -> str:
    if flights.empty:
        return "Brak zakwalifikowanych publicznych obserwacji nad Polską."

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
    connection: sqlite3.Connection,
    session: requests.Session,
) -> tuple[str, int, pd.DataFrame]:
    flights = aggregate_flights(
        observations=observations,
        connection=connection,
        session=session,
    )

    lines = [
        f"# Loty wojskowe nad Polską — {start_lt.strftime('%d.%m.%Y')}",
        "",
        f"**Okno raportu:** {start_lt.strftime('%H:%M')}–{end_lt.strftime('%H:%M')} LT",
        f"**Zweryfikowane próbki ADS-B wewnątrz Polski:** {len(observations)}",
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
                "> Brak publicznych danych ADS-B/MLAT nie jest dowodem braku aktywności lotniczej.",
                "",
                "## Metoda",
                "",
                "- Zapisano tylko aktualne pozycje mieszczące się wewnątrz wielokąta granic Polski.",
                "- Minimalna liczba próbek dla wpisu: "
                f"{MIN_SAMPLES_PER_FLIGHT}.",
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
            "## Szczegółowe dane",
            "",
            "| Typ | Rejestracja | Callsign | ICAO Hex | Widoczny nad Polską | Trasa | Status trasy | Kierunek | Próbki | Max ALT | Max GS |",
            "|---|---|---|---|---|---|---|---|---:|---:|---:|",
        ]
    )

    for _, flight in flights.iterrows():
        time_range = (
            f"{flight['first_seen_lt'].strftime('%H:%M')} → "
            f"{flight['last_seen_lt'].strftime('%H:%M')} LT"
        )

        lines.append(
            f"| {markdown_safe(flight['type_label'])} | "
            f"{markdown_safe(flight['registration'])} | "
            f"`{markdown_safe(flight['callsign'])}` | "
            f"`{markdown_safe(flight['hex'])}` | "
            f"{time_range} | "
            f"{markdown_safe(flight['route'])} | "
            f"{markdown_safe(flight['route_confidence'])} | "
            f"{markdown_safe(flight['direction'])} | "
            f"{int(flight['samples'])} | "
            f"{format_metric(flight['max_altitude_ft'], 'ft')} | "
            f"{format_metric(flight['max_speed_kt'], 'kt')} |"
        )

    lines.extend(
        [
            "",
            "## Interpretacja trasy",
            "",
            "- **Prawdopodobna:** połączenie origin/destination z zewnętrznego lookupu callsignu; nie jest to operacyjnie potwierdzony plan lotu.",
            "- **Obserwacyjna:** `EPRZ → N/A`, `N/A → EPRZ` lub `EPWA → EPRZ` oznacza pozycje w pobliżu lotnisk na początku/końcu zapisanego okna; nie potwierdza wylotu ani lądowania.",
            "- **Brak danych:** brak wiarygodnego połączenia z lotniskiem lub brak callsignu.",
            "",
            "## Ograniczenia",
            "",
            "- Raport obejmuje wyłącznie publicznie widoczne dane ADS-B/MLAT z danego interwału.",
            "- Czas widoczności jest czasem pierwszej i ostatniej zapisanej próbki nad Polską, a nie potwierdzonym czasem przekroczenia granicy.",
            "- Kierunek jest liczony z pierwszej i ostatniej pozycji w raporcie; przy małym przesunięciu wyświetlany jest jako manewrowanie.",
            "",
        ]
    )

    return "\n".join(lines), len(flights), flights


def save_report(content: str, start_lt: datetime) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    report_path = REPORTS_DIR / (
        f"raport-{start_lt.strftime('%Y-%m-%d_%H-00')}.md"
    )
    report_path.write_text(content, encoding="utf-8")

    return report_path


# =============================================================================
# Discord
# =============================================================================

def truncate_text(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[: limit - 1] + "…"


def send_discord_report(
    session: requests.Session,
    report_path: Path,
    flights: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> bool:
    if flights.empty and not DISCORD_SEND_EMPTY:
        log.info("Discord: pusty raport — nie wysyłam alertu.")
        return False

    if not DISCORD_WEBHOOK_URL:
        log.warning("Discord: brak DISCORD_WEBHOOK_URL.")
        return False

    preview = truncate_text(social_summary(flights, max_lines=6), 3500)

    title = (
        f"✈️ Loty wojskowe nad Polską — {len(flights)}"
        if not flights.empty
        else "✈️ Raport: brak wykrytych lotów"
    )

    time_window = (
        f"{start_lt.strftime('%d.%m.%Y %H:%M')}–"
        f"{end_lt.strftime('%H:%M')} LT"
    )

    payload = {
        "username": "Military Flight Report",
        "content": "📎 Pełny raport Markdown znajduje się w załączniku.",
        "embeds": [
            {
                "title": title,
                "description": f"```text\n{preview}\n```",
                "color": 15158332 if not flights.empty else 9807270,
                "fields": [
                    {
                        "name": "Okno obserwacji",
                        "value": time_window,
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

    for attempt in range(1, MAX_RETRIES + 1):
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
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )

            if response.status_code == 429 or response.status_code >= 500:
                wait = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "Discord HTTP %s; retry za %s s.",
                    response.status_code,
                    wait,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            log.info("Discord: wysłano %s.", report_path.name)
            return True

        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                log.error("Discord: wysyłka nieudana: %s", exc)
                return False

            wait = RETRY_WAIT_SECONDS * attempt
            log.warning(
                "Discord: błąd %s; retry za %s s.",
                exc,
                wait,
            )
            time.sleep(wait)

    return False


# =============================================================================
# Główna procedura
# =============================================================================

def generate_pending_reports(
    connection: sqlite3.Connection,
    session: requests.Session,
    now_lt: datetime,
) -> int:
    generated = 0
    current_hour = now_lt.replace(minute=0, second=0, microsecond=0)

    for offset in range(BACKFILL_HOURS, 0, -1):
        start_lt = current_hour - timedelta(hours=offset)
        end_lt = start_lt + timedelta(hours=1)

        if report_exists(connection, start_lt):
            continue

        observations = get_hour_observations(
            connection=connection,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        content, flights_count, flights = build_report(
            observations=observations,
            start_lt=start_lt,
            end_lt=end_lt,
            connection=connection,
            session=session,
        )

        report_path = save_report(content, start_lt)

        discord_sent = send_discord_report(
            session=session,
            report_path=report_path,
            flights=flights,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        record_report(
            connection=connection,
            start_lt=start_lt,
            report_path=report_path,
            flights_count=flights_count,
            observations_count=len(observations),
            discord_sent=discord_sent,
        )

        generated += 1

        log.info(
            "Raport %s | loty=%s | próbki=%s | Discord=%s.",
            report_path.name,
            flights_count,
            len(observations),
            "wysłano" if discord_sent else "nie wysłano",
        )

    return generated


def main() -> int:
    configure_logging()

    now_lt = datetime.now(POLAND_TZ)
    now_utc = now_lt.astimezone(UTC)

    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    log.info(
        "Start: %s | ADSB: %s | route_lookup: %s",
        now_lt.strftime("%Y-%m-%d %H:%M:%S %Z"),
        ADSB_API_URL,
        ENABLE_ROUTE_LOOKUP,
    )

    with sqlite3.connect(DATABASE_PATH) as connection:
        init_database(connection)
        session = create_session()

        try:
            aircraft_list = fetch_adsb_snapshot(session)

            stats = save_snapshot(
                connection=connection,
                aircraft_list=aircraft_list,
                observed_at=now_utc,
            )

            log.info(
                "Snapshot: globalnie=%s | Polska=%s | zapisano=%s | "
                "poza_PL=%s | stare=%s | błędne=%s.",
                stats["global"],
                stats["inside_poland"],
                stats["inserted"],
                stats["outside"],
                stats["stale"],
                stats["invalid"],
            )

        except Exception as exc:
            log.exception("Snapshot: błąd pobierania/zapisu: %s", exc)

        reports_created = generate_pending_reports(
            connection=connection,
            session=session,
            now_lt=now_lt,
        )

        delete_old_data(connection)

        log.info("Koniec: wygenerowano raportów=%s.", reports_created)

    return 0


if __name__ == "__main__":
    sys.exit(main())
