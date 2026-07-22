#!/usr/bin/env python3
"""
live_military_collector.py

Publiczny monitoring widocznych danych ADS-B/MLAT lotów wojskowych nad Polską.

Funkcje:
- pobiera snapshot z ADSB.lol /v2/mil;
- filtruje obiekty z pozycją w obszarze Polski;
- zapisuje dane do SQLite;
- tworzy raport Markdown dla każdej zakończonej godziny;
- nie tworzy duplikatów raportów;
- wysyła do Discorda estetyczny embed + raport Markdown jako plik;
- format raportu jest wygodny do kopiowania na social media.

Wymagania:
    pip install requests pandas

Zmienne środowiskowe:
    ADSB_API_URL          domyślnie https://api.adsb.lol/v2/mil
    DATABASE_PATH         domyślnie data/military_flights.sqlite3
    REPORTS_DIR           domyślnie reports/hourly
    DISCORD_WEBHOOK_URL   opcjonalny GitHub Secret
    RETENTION_DAYS        domyślnie 14
    FORCE_REPORT          1 = uruchom raport dla poprzedniej pełnej godziny
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

POLAND_TZ = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc

POLAND_BOUNDS = {
    "lat_min": 48.70,
    "lat_max": 55.20,
    "lon_min": 13.70,
    "lon_max": 24.40,
}

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

# Obiekty lotnisk: ICAO, nazwa, latitude, longitude.
# Przypisanie jest tylko orientacyjne na podstawie pierwszej/ostatniej pozycji.
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

# Maksymalna odległość od lotniska, przy której pokazujemy kod ICAO.
# 20 km pozwala wskazać okolice lotniska, ale nie udaje potwierdzonego startu/lądowania.
AIRPORT_PROXIMITY_KM = 20.0

MILITARY_CALLSIGN_PATTERN = re.compile(
    r"^(PLF|RCH|REACH|NATO|SNAKE|NACHO|HERK(?:Y)?|DUKE|SPAR|EVAC|"
    r"SAM|MMF|ASCOT|RRR|CNV|IAM|LAGR|BAF|FAF|GAF|NOH|SVF|CFC|CEF|"
    r"POL|PLAF|PSYOP|TOPCAT|TIGER|MACE|JEDI|GHOST|RAZOR|VIPER|"
    r"HAWK|RAVEN|COBRA)[A-Z0-9-]*$"
)

MILITARY_TYPE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"^F16", "F-16 Fighting Falcon"),
    (r"^F35", "F-35 Lightning II"),
    (r"^F15", "F-15 Eagle"),
    (r"^FA18|^F18", "F/A-18 Hornet"),
    (r"^A10", "A-10 Thunderbolt II"),
    (r"^C130J", "C-130J Hercules"),
    (r"^C130", "C-130 Hercules"),
    (r"^C30J", "C-130J Hercules"),
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
    (r"^H60|^S70", "UH-60 / MH-60 Black Hawk"),
    (r"^CH47", "CH-47 Chinook"),
    (r"^V22", "V-22 Osprey"),
)

MILITARY_ICAO_RANGES: tuple[tuple[str, str, str], ...] = (
    ("AE0000", "AEFFFF", "US military ICAO range"),
    ("3B0000", "3B7FFF", "Germany military/government ICAO range"),
    ("43C000", "43CFFF", "United Kingdom military ICAO range"),
)

log = logging.getLogger("live-military-collector")


# =============================================================================
# Dane i funkcje pomocnicze
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


def parse_hex(value: Any) -> Optional[int]:
    hex_code = normalized(value).replace("~", "")
    if not re.fullmatch(r"[0-9A-F]{6}", hex_code):
        return None
    return int(hex_code, 16)


def is_in_hex_range(hex_code: Any, start: str, end: str) -> bool:
    value = parse_hex(hex_code)
    start_value = parse_hex(start)
    end_value = parse_hex(end)

    return (
        value is not None
        and start_value is not None
        and end_value is not None
        and start_value <= value <= end_value
    )


def is_over_poland(aircraft: dict[str, Any]) -> bool:
    lat = safe_float(aircraft.get("lat"))
    lon = safe_float(aircraft.get("lon"))

    if lat is None or lon is None:
        return False

    return (
        POLAND_BOUNDS["lat_min"] <= lat <= POLAND_BOUNDS["lat_max"]
        and POLAND_BOUNDS["lon_min"] <= lon <= POLAND_BOUNDS["lon_max"]
    )


def distance_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
) -> float:
    """Odległość Haversine w kilometrach."""
    radius_km = 6371.0

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad)
        * math.cos(lat2_rad)
        * math.sin(delta_lon / 2) ** 2
    )
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest_polish_airport(lat: Any, lon: Any) -> str:
    """
    Zwraca kod ICAO lotniska tylko, gdy pozycja była maksymalnie 20 km od niego.
    Nie jest to potwierdzony start/przylot.
    """
    latitude = safe_float(lat)
    longitude = safe_float(lon)

    if latitude is None or longitude is None:
        return "—"

    nearest_code = "—"
    nearest_distance = float("inf")

    for code, _name, airport_lat, airport_lon in POLISH_AIRPORTS:
        current_distance = distance_km(
            latitude,
            longitude,
            airport_lat,
            airport_lon,
        )

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
            "User-Agent": "MGYTT-MilitaryFlightReport/5.0",
        }
    )
    return session


def fetch_military_snapshot(session: requests.Session) -> list[dict[str, Any]]:
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
                    "ADSB.lol zwrócił HTTP 429. Ponowienie za %s s.",
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

            return [
                item for item in aircraft
                if isinstance(item, dict)
            ]

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
# Klasyfikacja
# =============================================================================

def classify_aircraft(aircraft: dict[str, Any]) -> Classification:
    hex_code = normalized(aircraft.get("hex"))
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

    for start, end, range_name in MILITARY_ICAO_RANGES:
        if is_in_hex_range(hex_code, start, end):
            reasons.append(f"ICAO Hex: {range_name}")
            break

    db_flags = aircraft.get("dbFlags", aircraft.get("dbflags", 0))
    try:
        if int(db_flags or 0) & 1:
            reasons.append("dbFlags: military")
    except (TypeError, ValueError):
        pass

    return Classification(type_label=type_label, reasons=reasons)


# =============================================================================
# SQLite
# =============================================================================

def init_database(connection: sqlite3.Connection) -> None:
    """
    journal_mode=DELETE ogranicza pliki -wal/-shm i ułatwia commit SQLite do Git.
    """
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
            lat REAL,
            lon REAL,
            altitude_ft REAL,
            groundspeed_kt REAL,
            track_deg REAL,
            dbflags INTEGER,
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
) -> tuple[int, int]:
    in_poland = 0
    inserted = 0

    for aircraft in aircraft_list:
        if not is_over_poland(aircraft):
            continue

        in_poland += 1
        hex_code = normalized(aircraft.get("hex"))

        if not hex_code:
            log.warning("Pomijam obiekt nad Polską bez ICAO Hex.")
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
                    dbflags,
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
                    safe_float(aircraft.get("lat")),
                    safe_float(aircraft.get("lon")),
                    safe_float(
                        aircraft.get("alt_baro") or aircraft.get("alt_geom")
                    ),
                    safe_float(aircraft.get("gs")),
                    safe_float(aircraft.get("track")),
                    aircraft.get("dbFlags", aircraft.get("dbflags", 0)) or 0,
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
            log.warning(
                "Nie udało się zapisać ICAO %s: %s",
                hex_code,
                exc,
            )

    connection.commit()
    return in_poland, inserted


def report_exists(connection: sqlite3.Connection, start_lt: datetime) -> bool:
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
        log.info("Usunięto %s starych obserwacji.", deleted)


# =============================================================================
# Agregacja i raport
# =============================================================================

def get_last_closed_hour(now_lt: datetime) -> tuple[datetime, datetime]:
    """
    Każde uruchomienie po 17:00 próbuje tworzyć raport za 16:00–17:00.
    Jeśli raport istnieje, SQLite blokuje duplikat.
    """
    end_lt = now_lt.replace(minute=0, second=0, microsecond=0)
    start_lt = end_lt - timedelta(hours=1)
    return start_lt, end_lt


def aggregate_flights(observations: pd.DataFrame) -> pd.DataFrame:
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
                "departure_airport": nearest_polish_airport(
                    first["lat"],
                    first["lon"],
                ),
                "arrival_airport": nearest_polish_airport(
                    last["lat"],
                    last["lon"],
                ),
            }
        )

    return pd.DataFrame(flights).sort_values("first_seen_lt")


def route_text(flight: pd.Series) -> str:
    """
    Tworzy ostrożny opis trasy.
    To nie jest potwierdzenie wylotu/przylotu — tylko bliskość lotnisk
    wobec pierwszej i ostatniej zapisanej pozycji.
    """
    departure = str(flight["departure_airport"])
    arrival = str(flight["arrival_airport"])

    if departure != "—" and arrival != "—":
        return f"{departure} → {arrival}"

    if departure != "—":
        return f"w pobliżu {departure}"

    if arrival != "—":
        return f"w pobliżu {arrival}"

    return "trasa nieustalona"


def social_line(flight: pd.Series) -> str:
    """Jedna linia do łatwego kopiowania na social media."""
    aircraft_type = str(flight["type_label"]).upper()
    registration = str(flight["registration"]).upper()
    callsign = str(flight["callsign"]).upper()
    first_time = flight["first_seen_lt"].strftime("%H:%M")

    return (
        f'{aircraft_type} "{registration}" {callsign} '
        f"{first_time}LT | {route_text(flight)}"
    )


def social_summary(flights: pd.DataFrame, max_lines: int = 8) -> str:
    """
    Krótki blok do Discorda. Discord ma limity długości wiadomości,
    dlatego dłuższe raporty trafiają przede wszystkim do załącznika .md.
    """
    if flights.empty:
        return "Brak wykrytych publicznie widocznych lotów wojskowych."

    lines = [
        social_line(flight)
        for _, flight in flights.head(max_lines).iterrows()
    ]

    if len(flights) > max_lines:
        lines.append(f"… oraz {len(flights) - max_lines} kolejnych lotów w załączniku.")

    return "\n".join(lines)


def build_report(
    observations: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> tuple[str, int, pd.DataFrame]:
    """
    Tworzy Markdown z blokiem do social media i tabelą techniczną.
    """
    flights = aggregate_flights(observations)

    lines = [
        f"# Loty wojskowe nad Polską — {start_lt.strftime('%d.%m.%Y')}",
        "",
        (
            f"**Okno obserwacji:** {start_lt.strftime('%H:%M')}–"
            f"{end_lt.strftime('%H:%M')} LT ({POLAND_TZ.key})"
        ),
        "",
        "## Podsumowanie do publikacji",
        "",
    ]

    if flights.empty:
        lines.extend(
            [
                "```text",
                "Brak zakwalifikowanych publicznie widocznych lotów wojskowych w zapisanych próbkach ADS-B.",
                "```",
                "",
                "> Brak danych ADS-B nie oznacza braku aktywności wojskowej.",
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
            "| Typ | Rejestracja | Callsign | ICAO Hex | Pierwsza → ostatnia obserwacja LT | Lotniska orientacyjnie | Próbki |",
            "|---|---|---|---|---|---|---:|",
        ]
    )

    for _, flight in flights.iterrows():
        observation_range = (
            f"{flight['first_seen_lt'].strftime('%H:%M')} → "
            f"{flight['last_seen_lt'].strftime('%H:%M')}"
        )

        lines.append(
            f"| {markdown_safe(flight['type_label'])} | "
            f"{markdown_safe(flight['registration'])} | "
            f"`{markdown_safe(flight['callsign'])}` | "
            f"`{markdown_safe(flight['hex'])}` | "
            f"{observation_range} | "
            f"{markdown_safe(route_text(flight))} | "
            f"{int(flight['samples'])} |"
        )

    suspicious = flights[
        (flights["callsign"] == "BRAK")
        | (flights["registration"] == "NIEZNANA")
    ]

    lines.extend(
        [
            "",
            "## Uwaga operacyjna",
            "",
        ]
    )

    if suspicious.empty:
        lines.append("Brak obiektów bez callsignu lub rejestracji.")
    else:
        lines.append(
            f"Wykryto {len(suspicious)} obiekt(y) z niepełną identyfikacją "
            "(brak callsignu i/lub rejestracji)."
        )

    lines.extend(
        [
            "",
            "## Metoda i ograniczenia",
            "",
            "- Dane: publiczny snapshot ADSB.lol `/v2/mil`, filtrowany do przybliżonego obszaru Polski.",
            "- Godzina w podsumowaniu oznacza pierwszą zapisaną obserwację w danym oknie godzinowym.",
            "- Kody lotnisk są orientacyjne: wynikają z bliskości pierwszej/ostatniej zapisanej pozycji do lotniska i nie potwierdzają startu ani lądowania.",
            "- Raport nie obejmuje samolotów niewidocznych w publicznych danych ADS-B/MLAT.",
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

def send_discord_report(
    session: requests.Session,
    report_path: Path,
    flights: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> bool:
    """
    Wysyła embed Discord + plik Markdown do pobrania.

    Brak lotów = brak wiadomości na Discordzie, ale raport nadal zapisuje się
    do repozytorium.
    """
    if flights.empty:
        log.info("Discord: brak alertu — raport ma 0 lotów/śladów.")
        return False

    if not DISCORD_WEBHOOK_URL:
        log.warning(
            "Discord: wykryto %s lotów, ale brak sekretu DISCORD_WEBHOOK_URL.",
            len(flights),
        )
        return False

    period = (
        f"{start_lt.strftime('%d.%m.%Y %H:%M')}–"
        f"{end_lt.strftime('%H:%M')} LT"
    )

    preview = social_summary(flights, max_lines=6)

    # Discord embed description maksymalnie 4096 znaków.
    if len(preview) > 3500:
        preview = preview[:3490] + "\n… pełny raport w załączniku."

    payload = {
        "username": "Military Flight Report",
        "avatar_url": "https://cdn.discordapp.com/embed/avatars/0.png",
        "content": "📎 Pełny raport Markdown jest dostępny w załączniku.",
        "embeds": [
            {
                "title": f"✈️ Wykryte loty wojskowe nad Polską — {len(flights)}",
                "description": f"```text\n{preview}\n```",
                "color": 15158332,
                "fields": [
                    {
                        "name": "Okno obserwacji",
                        "value": period,
                        "inline": False,
                    },
                    {
                        "name": "Raport",
                        "value": (
                            f"`{report_path.name}`\n"
                            "Pobierz załącznik, aby otrzymać pełny raport."
                        ),
                        "inline": False,
                    },
                ],
                "footer": {
                    "text": (
                        "Publiczne dane ADS-B/MLAT • lotniska są orientacyjne"
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
        log.info("Discord: wysłano embed oraz raport %s.", report_path.name)
        return True

    except requests.RequestException as exc:
        log.error("Discord: błąd wysyłki webhooka: %s", exc)
        return False


# =============================================================================
# Główne wykonanie
# =============================================================================

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
    log.info("API: %s", ADSB_API_URL)

    with sqlite3.connect(DATABASE_PATH) as connection:
        init_database(connection)
        session = create_session()

        try:
            global_aircraft = fetch_military_snapshot(session)

            in_poland, inserted = save_snapshot(
                connection=connection,
                aircraft_list=global_aircraft,
                observed_at=now_utc,
            )

            log.info(
                "ADSB.lol /v2/mil: %s globalnie; %s nad Polską; %s nowych obserwacji.",
                len(global_aircraft),
                in_poland,
                inserted,
            )

        except Exception as exc:
            # Pozwalamy wygenerować raport na podstawie danych już obecnych w SQLite.
            log.exception("Błąd pobierania/zapisu snapshotu: %s", exc)

        # Zawsze próbujemy utworzyć raport poprzedniej pełnej godziny.
        # generated_reports blokuje duplikaty przy kolejnych runach tej samej godziny.
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

        report_content, flights_count, flights = build_report(
            observations=observations,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        report_path = save_report(report_content, start_lt)

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
            "Utworzono raport: %s | loty/ślady: %s | Discord: %s.",
            report_path,
            flights_count,
            "wysłano" if discord_sent else "nie wysłano",
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
