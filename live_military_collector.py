#!/usr/bin/env python3
"""
live_military_collector.py
==========================

Military Flight Report Poland — wersja produkcyjna v1.

Monitoruje publicznie widoczne dane ADS-B/MLAT lotów wojskowych nad Polską.

Najważniejsze właściwości:
- źródło: ADSB.lol /v2/mil;
- filtr pozycji: bbox + wielokąt Polski, nie sam prostokąt;
- filtr jakości: aktualna pozycja, ICAO Hex, wysokość;
- SQLite: obserwacje, raporty oraz cache tras;
- raport za każdą zamkniętą godzinę, także po opóźnieniu GitHub Actions;
- format do publikowania w social media;
- kierunek lotu, czas widoczności, max wysokość i prędkość;
- opcjonalny route lookup przez ADSBDB z cache;
- Discord embed i kompletny raport Markdown jako załącznik;
- bez wymyślania startu/przylotu — trasa zawsze ma status pewności.

Wymagania:
    pip install requests pandas

Zmienne środowiskowe:
    ADSB_API_URL                 default: https://api.adsb.lol/v2/mil
    DATABASE_PATH                default: data/military_flights.sqlite3
    REPORTS_DIR                  default: reports/hourly
    DISCORD_WEBHOOK_URL          opcjonalny GitHub Secret
    RETENTION_DAYS               default: 14
    MAX_SEEN_SECONDS             default: 120
    MIN_ALTITUDE_FT              default: 0
    MIN_SAMPLES_PER_FLIGHT       default: 1
    BACKFILL_HOURS               default: 2
    DISCORD_SEND_EMPTY           default: 0
    ENABLE_ROUTE_LOOKUP          default: 0
    ROUTE_CACHE_HOURS            default: 12
    ADSBDB_ROUTE_URL_TEMPLATE    default: https://api.adsbdb.com/v0/callsign/{callsign}
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
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "14"))
MAX_SEEN_SECONDS = int(os.getenv("MAX_SEEN_SECONDS", "120"))
MIN_ALTITUDE_FT = int(os.getenv("MIN_ALTITUDE_FT", "0"))
MIN_SAMPLES_PER_FLIGHT = int(os.getenv("MIN_SAMPLES_PER_FLIGHT", "1"))
BACKFILL_HOURS = int(os.getenv("BACKFILL_HOURS", "2"))

DISCORD_SEND_EMPTY = os.getenv(
    "DISCORD_SEND_EMPTY",
    "0",
).strip().lower() in {"1", "true", "yes"}

ENABLE_ROUTE_LOOKUP = os.getenv(
    "ENABLE_ROUTE_LOOKUP",
    "0",
).strip().lower() in {"1", "true", "yes"}

ROUTE_CACHE_HOURS = int(os.getenv("ROUTE_CACHE_HOURS", "12"))
ADSBDB_ROUTE_URL_TEMPLATE = os.getenv(
    "ADSBDB_ROUTE_URL_TEMPLATE",
    "https://api.adsbdb.com/v0/callsign/{callsign}",
).strip()

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 5

# Szybki filtr obszaru otaczającego Polskę.
POLAND_BOUNDS = {
    "lat_min": 48.70,
    "lat_max": 55.20,
    "lon_min": 13.70,
    "lon_max": 24.40,
}

# Uproszczony wielokąt granic Polski — format (longitude, latitude).
# Jest stosowany po bbox, żeby nie raportować samolotów z państw sąsiednich.
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
# Narzędzia
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


def bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def point_in_polygon(
    latitude: float,
    longitude: float,
    polygon: tuple[tuple[float, float], ...],
) -> bool:
    """Ray-casting; polygon używa kolejności (lon, lat)."""
    inside = False
    previous_lon, previous_lat = polygon[-1]

    for current_lon, current_lat in polygon:
        crosses = (current_lat > latitude) != (previous_lat > latitude)

        if crosses:
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
    """Pojedyncze źródło reguł jakości danych."""
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
    """
    /v2/mil jest podstawą kwalifikacji.
    Callsign, typ i dbFlags podnoszą pewność opisową.
    """
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

    return Classification(
        type_label=type_label,
        confidence=confidence,
        reasons=reasons,
    )


def haversine_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    radius_km = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    return 2 * radius_km * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def bearing_degrees(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """Początkowy bearing od punktu 1 do punktu 2."""
    lon_delta = math.radians(lon2 - lon1)

    y = math.sin(lon_delta) * math.cos(math.radians(lat2))
    x = (
        math.cos(math.radians(lat1)) * math.sin(math.radians(lat2))
        - math.sin(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.cos(lon_delta)
    )

    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def cardinal_direction(bearing: float) -> str:
    directions = (
        "N",
        "NE",
        "E",
        "SE",
        "S",
        "SW",
        "W",
        "NW",
    )
    return directions[int((bearing + 22.5) // 45) % 8]


def movement_direction(
    first_lat: Any,
    first_lon: Any,
    last_lat: Any,
    last_lon: Any,
) -> tuple[str, Optional[float]]:
    """
    Kierunek pokazujemy wyłącznie po realnym przesunięciu co najmniej 10 km.
    Krążenie lub błąd GPS nie powinny tworzyć pozornej trasy.
    """
    lat1 = safe_float(first_lat)
    lon1 = safe_float(first_lon)
    lat2 = safe_float(last_lat)
    lon2 = safe_float(last_lon)

    if None in (lat1, lon1, lat2, lon2):
        return "nieustalony", None

    distance = haversine_km(lat1, lon1, lat2, lon2)
    if distance < 10.0:
        return "lokalny / krążenie", distance

    bearing = bearing_degrees(lat1, lon1, lat2, lon2)
    return cardinal_direction(bearing), distance


# =============================================================================
# API
# =============================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "MGYTT-MilitaryFlightReport/8.0",
        }
    )
    return session


def fetch_adsb_snapshot(session: requests.Session) -> list[dict[str, Any]]:
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
                    "ADSB.lol HTTP %s; ponowienie za %s s.",
                    response.status_code,
                    wait,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            payload = response.json()

            if not isinstance(payload, dict):
                raise ValueError("ADSB.lol zwrócił nieprawidłowy JSON.")

            aircraft = payload.get("ac") or payload.get("aircraft") or []
            if not isinstance(aircraft, list):
                raise ValueError("Pole ac/aircraft nie jest listą.")

            return [item for item in aircraft if isinstance(item, dict)]

        except (requests.RequestException, ValueError) as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                wait = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "Błąd ADSB.lol: %s; ponowienie za %s s.",
                    exc,
                    wait,
                )
                time.sleep(wait)

    raise RuntimeError(f"Nie udało się pobrać ADSB.lol: {last_error}")


# =============================================================================
# SQLite
# =============================================================================

def init_database(connection: sqlite3.Connection) -> None:
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

    return RouteInfo(
        route=route,
        confidence=confidence,
        source=source,
    )


def cache_route(
    connection: sqlite3.Connection,
    callsign: str,
    route: RouteInfo,
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO route_cache (
            callsign, route, confidence, source, fetched_at_utc
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            callsign,
            route.route,
            route.confidence,
            route.source,
            datetime.now(UTC).isoformat(),
        ),
    )
    connection.commit()


def delete_old_data(connection: sqlite3.Connection) -> None:
    observations_cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)
    reports_cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS * 2)
    routes_cutoff = datetime.now(UTC) - timedelta(days=ROUTE_CACHE_HOURS * 3)

    deleted = connection.execute(
        "DELETE FROM observations WHERE observed_at_utc < ?",
        (observations_cutoff.isoformat(),),
    ).rowcount

    connection.execute(
        "DELETE FROM generated_reports WHERE created_at_utc < ?",
        (reports_cutoff.isoformat(),),
    )

    connection.execute(
        "DELETE FROM route_cache WHERE fetched_at_utc < ?",
        (routes_cutoff.isoformat(),),
    )

    connection.commit()

    if deleted:
        log.info("Retencja: usunięto %s starych obserwacji.", deleted)


# =============================================================================
# Route lookup
# =============================================================================

def extract_airport_code(data: Any) -> Optional[str]:
    """
    Próbuje wydobyć ICAO/IATA z różnych możliwych struktur JSON.
    Nie zakłada jednego sztywnego formatu zewnętrznego API.
    """
    if isinstance(data, str):
        value = data.strip().upper()
        return value if re.fullmatch(r"[A-Z0-9]{3,4}", value) else None

    if not isinstance(data, dict):
        return None

    for key in ("icao_code", "icao", "iata_code", "iata", "code"):
        value = extract_airport_code(data.get(key))
        if value:
            return value

    return None


def lookup_route(
    connection: sqlite3.Connection,
    session: requests.Session,
    callsign: str,
) -> RouteInfo:
    """
    Opcjonalna, ostrożna trasa po callsignie.

    W razie braku danych zwraca 'nieustalona'. Nie stosuje heurystyki
    „blisko lotniska = lot z lotniska”.
    """
    if not callsign or callsign == "BRAK":
        return RouteInfo("nieustalona", "brak danych", "brak callsignu")

    cached = get_cached_route(connection, callsign)
    if cached:
        return cached

    if not ENABLE_ROUTE_LOOKUP:
        return RouteInfo(
            "nieustalona",
            "brak danych",
            "route lookup wyłączony",
        )

    url = ADSBDB_ROUTE_URL_TEMPLATE.format(callsign=callsign)

    try:
        response = session.get(url, timeout=15)

        if response.status_code == 404:
            result = RouteInfo(
                "nieustalona",
                "brak danych",
                "ADSBDB",
            )
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

        if origin and destination:
            result = RouteInfo(
                f"{origin} → {destination}",
                "prawdopodobna",
                "ADSBDB callsign lookup",
            )
        else:
            result = RouteInfo(
                "nieustalona",
                "brak danych",
                "ADSBDB",
            )

    except (requests.RequestException, ValueError, AttributeError) as exc:
        log.warning("Route lookup dla %s nieudany: %s", callsign, exc)
        result = RouteInfo(
            "nieustalona",
            "brak danych",
            "błąd route lookup",
        )

    cache_route(connection, callsign, result)
    return result


# =============================================================================
# Agregacja
# =============================================================================

def pending_report_hours(
    connection: sqlite3.Connection,
    now_lt: datetime,
) -> Iterable[tuple[datetime, datetime]]:
    """
    Wykrywa brakujące, zakończone godziny. Raporty są ograniczone
    do BACKFILL_HOURS, aby pojedynczy run nie generował setek pustych plików.
    """
    hour_start = now_lt.replace(minute=0, second=0, microsecond=0)

    for offset in range(BACKFILL_HOURS, 0, -1):
        start_lt = hour_start - timedelta(hours=offset)
        end_lt = start_lt + timedelta(hours=1)

        if not report_exists(connection, start_lt):
            yield start_lt, end_lt


def aggregate_flights(
    observations: pd.DataFrame,
    connection: sqlite3.Connection,
    session: requests.Session,
) -> pd.DataFrame:
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

        direction, distance = movement_direction(
            first["lat"],
            first["lon"],
            last["lat"],
            last["lon"],
        )

        duration_minutes = int(
            max(
                0,
                (
                    last["observed_at_lt"] - first["observed_at_lt"]
                ).total_seconds()
                / 60,
            )
        )

        route_info = lookup_route(
            connection=connection,
            session=session,
            callsign=str(callsign),
        )

        flights.append(
            {
                "hex": str(hex_code),
                "callsign": str(callsign),
                "registration": first["registration"],
                "type_label": first["type_label"],
                "confidence": first["confidence"],
                "first_seen_lt": first["observed_at_lt"],
                "last_seen_lt": last["observed_at_lt"],
                "duration_minutes": duration_minutes,
                "samples": len(group),
                "direction": direction,
                "distance_km": distance,
                "max_altitude_ft": pd.to_numeric(
                    group["altitude_ft"],
                    errors="coerce",
                ).max(),
                "max_speed_kt": pd.to_numeric(
                    group["groundspeed_kt"],
                    errors="coerce",
                ).max(),
                "route": route_info.route,
                "route_confidence": route_info.confidence,
                "route_source": route_info.source,
            }
        )

    if not flights:
        return pd.DataFrame()

    return pd.DataFrame(flights).sort_values(
        ["first_seen_lt", "callsign"],
        ascending=True,
    )


# =============================================================================
# Raport
# =============================================================================

def format_metric(value: Any, suffix: str) -> str:
    if pd.isna(value):
        return "—"
    return f"{int(round(float(value)))} {suffix}"


def social_line(flight: pd.Series) -> str:
    """
    Gotowy wpis social media, tylko fakty z raportu.
    """
    aircraft_type = str(flight["type_label"]).upper()
    registration = str(flight["registration"]).upper()
    callsign = str(flight["callsign"]).upper()

    start = flight["first_seen_lt"].strftime("%H:%M")
    end = flight["last_seen_lt"].strftime("%H:%M")

    visibility = (
        f"{start} LT"
        if start == end
        else f"{start}–{end} LT"
    )

    direction = str(flight["direction"])
    route = str(flight["route"])

    return (
        f'{aircraft_type} "{registration}" {callsign} | '
        f"{visibility} | {direction} | trasa: {route}"
    )


def social_summary(flights: pd.DataFrame, max_lines: int) -> str:
    if flights.empty:
        return "Brak zakwalifikowanych obserwacji nad Polską."

    lines = [
        social_line(flight)
        for _, flight in flights.head(max_lines).iterrows()
    ]

    hidden = len(flights) - max_lines
    if hidden > 0:
        lines.append(f"… oraz {hidden} kolejnych pozycji w załączniku.")

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
        f"**Okno obserwacji:** {start_lt.strftime('%H:%M')}–{end_lt.strftime('%H:%M')} LT",
        f"**Próbki po filtracji granic Polski:** {len(observations)}",
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
                "> Brak danych ADS-B/MLAT nie potwierdza braku aktywności wojskowej.",
                "",
                "## Metoda",
                "",
                "- Ujęto wyłącznie pozycje wewnątrz wielokąta granic Polski.",
                "- Dane pochodzą z publicznych obserwacji ADS-B/MLAT.",
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
            f"**Loty/ślady:** {len(flights)}",
            "",
            "| Typ | Rejestracja | Callsign | ICAO Hex | Widoczny nad Polską | Kierunek | Trasa | Próbki | Max ALT | Max GS |",
            "|---|---|---|---|---|---|---|---:|---:|---:|",
        ]
    )

    for _, flight in flights.iterrows():
        time_range = (
            f"{flight['first_seen_lt'].strftime('%H:%M')} → "
            f"{flight['last_seen_lt'].strftime('%H:%M')}"
        )

        route_text = (
            f"{flight['route']} ({flight['route_confidence']})"
            if flight["route"] != "nieustalona"
            else "nieustalona"
        )

        lines.append(
            f"| {markdown_safe(flight['type_label'])} | "
            f"{markdown_safe(flight['registration'])} | "
            f"`{markdown_safe(flight['callsign'])}` | "
            f"`{markdown_safe(flight['hex'])}` | "
            f"{time_range} | "
            f"{markdown_safe(flight['direction'])} | "
            f"{markdown_safe(route_text)} | "
            f"{int(flight['samples'])} | "
            f"{format_metric(flight['max_altitude_ft'], 'ft')} | "
            f"{format_metric(flight['max_speed_kt'], 'kt')} |"
        )

    interesting_types = flights[
        flights["type_label"].str.contains(
            "KC-|A330 MRTT|E-3|E-7|P-8|C-17|C-5",
            case=False,
            na=False,
            regex=True,
        )
    ]

    lines.extend(
        [
            "",
            "## Najciekawsze obserwacje",
            "",
        ]
    )

    if interesting_types.empty:
        lines.append(
            "Brak tankowców, samolotów wczesnego ostrzegania lub dużych transportowców w tym oknie."
        )
    else:
        for _, flight in interesting_types.head(5).iterrows():
            lines.append(
                f"- **{markdown_safe(flight['type_label'])}** "
                f"`{markdown_safe(flight['callsign'])}` "
                f"({markdown_safe(flight['registration'])}), "
                f"widoczny {flight['first_seen_lt'].strftime('%H:%M')}–"
                f"{flight['last_seen_lt'].strftime('%H:%M')} LT, "
                f"kierunek: {markdown_safe(flight['direction'])}."
            )

    lines.extend(
        [
            "",
            "## Metoda i ograniczenia",
            "",
            "- Pozycja musi znajdować się wewnątrz wielokąta granic Polski; sam bbox nie jest wystarczający.",
            "- „Widoczny nad Polską” oznacza pierwszą i ostatnią zapisaną próbkę, nie potwierdzony czas przekroczenia granicy.",
            "- Trasa jest podawana tylko, jeśli zewnętrzny lookup po callsignie zwróci dane; ma status „prawdopodobna”, nie potwierdzona.",
            "- Brak wpisu nie jest dowodem braku lotu: dane ADS-B/MLAT są publiczne, niepełne i zależą od odbioru sygnału.",
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

def truncate(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def send_discord_report(
    session: requests.Session,
    report_path: Path,
    flights: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> bool:
    if flights.empty and not DISCORD_SEND_EMPTY:
        log.info("Discord: pusty raport, alert wyłączony.")
        return False

    if not DISCORD_WEBHOOK_URL:
        log.warning("Discord: brak DISCORD_WEBHOOK_URL.")
        return False

    preview = truncate(social_summary(flights, max_lines=6), 3500)

    title = (
        f"✈️ Loty wojskowe nad Polską — {len(flights)}"
        if not flights.empty
        else "✈️ Raport: brak wykrytych lotów"
    )

    window = (
        f"{start_lt.strftime('%d.%m.%Y %H:%M')}–"
        f"{end_lt.strftime('%H:%M')} LT"
    )

    payload = {
        "username": "Military Flight Report",
        "content": "📎 Pełny raport Markdown jest dostępny w załączniku.",
        "embeds": [
            {
                "title": title,
                "description": f"```text\n{preview}\n```",
                "color": 15158332 if not flights.empty else 9807270,
                "fields": [
                    {
                        "name": "Okno obserwacji",
                        "value": window,
                        "inline": False,
                    },
                    {
                        "name": "Raport",
                        "value": f"`{report_path.name}`",
                        "inline": False,
                    },
                ],
                "footer": {
                    "text": (
                        "Publiczne ADS-B/MLAT • filtr granic Polski • trasy oznaczone poziomem pewności"
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
                    timeout=30,
                )

            if response.status_code == 429 or response.status_code >= 500:
                wait = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "Discord HTTP %s, ponowienie za %s s.",
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
                "Discord: błąd %s, ponowienie za %s s.",
                exc,
                wait,
            )
            time.sleep(wait)

    return False


# =============================================================================
# Główne wykonanie
# =============================================================================

def generate_pending_reports(
    connection: sqlite3.Connection,
    session: requests.Session,
    now_lt: datetime,
) -> int:
    generated = 0

    for start_lt, end_lt in pending_report_hours(connection, now_lt):
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
        "Start %s | ADSB=%s | route_lookup=%s",
        now_lt.strftime("%Y-%m-%d %H:%M:%S %Z"),
        ADSB_API_URL,
        ENABLE_ROUTE_LOOKUP,
    )

    with sqlite3.connect(DATABASE_PATH) as connection:
        init_database(connection)
        session = create_session()

        try:
            aircraft = fetch_adsb_snapshot(session)
            stats = save_snapshot(
                connection=connection,
                aircraft_list=aircraft,
                observed_at=now_utc,
            )

            log.info(
                "Snapshot globalnie=%s | Polska=%s | zapisano=%s | poza_PL=%s | stare=%s | błędne=%s.",
                stats["global"],
                stats["inside_poland"],
                stats["inserted"],
                stats["outside"],
                stats["stale"],
                stats["invalid"],
            )

        except Exception as exc:
            log.exception("Snapshot: błąd pobierania/zapisu: %s", exc)

        report_count = generate_pending_reports(
            connection=connection,
            session=session,
            now_lt=now_lt,
        )

        delete_old_data(connection)

        log.info("Koniec: wygenerowano raportów=%s.", report_count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
