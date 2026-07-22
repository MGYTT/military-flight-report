#!/usr/bin/env python3
"""
Pobiera snapshot ADSB.lol nad Polską, zapisuje obserwacje do SQLite
i po zakończeniu pełnej godziny generuje raport Markdown.

Zależności:
    pip install requests pandas
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests


POLAND_TZ = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc

# ADSB.lol API jest kompatybilne z endpointami readsb/re-api.
# BBox: szerokość południowa, północna, długość zachodnia, wschodnia.
# Jeśli API zwróci błąd, zobacz log i dostosuj adres zgodnie z aktualnym /docs.
ADSB_API_URL = os.getenv(
    "ADSB_API_URL",
    "https://api.adsb.lol/v2/bbox/48.70/55.20/13.70/24.40",
)

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", "data/military_flights.sqlite3"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports/hourly"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETENTION_DAYS = 14

MILITARY_CALLSIGN_REGEX = re.compile(
    r"^(PLF|RCH|REACH|NATO|SNAKE|NACHO|HERK(?:Y)?|DUKE|SPAR|EVAC|"
    r"SAM|MMF|ASCOT|RRR|CNV|IAM|LAGR|BAF|FAF|GAF|NOH|SVF|CFC|CEF|"
    r"POL|PLAF|PSYOP|TOPCAT|TIGER|MACE|JEDI|GHOST|RAZOR|VIPER|"
    r"HAWK|RAVEN|COBRA)[A-Z0-9-]*$"
)

MILITARY_TYPE_PATTERNS = (
    (r"^F16", "F-16 Fighting Falcon"),
    (r"^F35", "F-35 Lightning II"),
    (r"^F15", "F-15 Eagle"),
    (r"^FA18|^F18", "F/A-18 Hornet"),
    (r"^A10", "A-10 Thunderbolt II"),
    (r"^C130|^C30J|^C30", "C-130 Hercules"),
    (r"^C17", "C-17 Globemaster III"),
    (r"^C5", "C-5 Galaxy"),
    (r"^C27", "C-27J Spartan"),
    (r"^C295", "C-295"),
    (r"^KC10", "KC-10 Extender"),
    (r"^KC135|^K35R|^K35E", "KC-135 Stratotanker"),
    (r"^A332", "A330 MRTT"),
    (r"^A400", "A400M Atlas"),
    (r"^E3", "E-3 Sentry AWACS"),
    (r"^E7", "E-7 Wedgetail"),
    (r"^E2", "E-2 Hawkeye"),
    (r"^P8", "P-8 Poseidon"),
    (r"^C160", "C-160 Transall"),
    (r"^H60|^S70", "UH-60 / MH-60 Black Hawk"),
    (r"^CH47", "CH-47 Chinook"),
    (r"^V22", "V-22 Osprey"),
)

MILITARY_ICAO_RANGES = (
    ("AE0000", "AEFFFF", "US military"),
    ("3B0000", "3B7FFF", "Germany military / government range"),
    ("43C000", "43CFFF", "United Kingdom military range"),
)

log = logging.getLogger("live-military-collector")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def text(value: Any) -> str:
    return str(value or "").strip().upper()


def markdown_text(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def hex_to_int(hex_code: str) -> Optional[int]:
    hex_code = text(hex_code).replace("~", "")
    if re.fullmatch(r"[0-9A-F]{6}", hex_code):
        return int(hex_code, 16)
    return None


def in_hex_range(hex_code: str, start: str, end: str) -> bool:
    value = hex_to_int(hex_code)
    start_value = hex_to_int(start)
    end_value = hex_to_int(end)
    return (
        value is not None
        and start_value is not None
        and end_value is not None
        and start_value <= value <= end_value
    )


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "MGYTT-military-flight-report/2.0",
    })
    return session


def fetch_snapshot(session: requests.Session) -> list[dict[str, Any]]:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(ADSB_API_URL, timeout=REQUEST_TIMEOUT)

            if response.status_code == 429:
                wait = 5 * attempt
                log.warning("Limit API (429), czekam %s s.", wait)
                time.sleep(wait)
                continue

            response.raise_for_status()
            payload = response.json()

            aircraft = payload.get("ac") or payload.get("aircraft") or []
            if not isinstance(aircraft, list):
                raise ValueError("Pole aircraft/ac nie jest listą.")

            return aircraft

        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)

    raise RuntimeError(f"Nie udało się pobrać ADSB.lol: {last_error}")


def classify_aircraft(aircraft: dict[str, Any]) -> tuple[bool, list[str], str]:
    hex_code = text(aircraft.get("hex"))
    callsign = text(aircraft.get("flight") or aircraft.get("callsign"))
    aircraft_type = text(aircraft.get("t") or aircraft.get("type"))
    db_flags = aircraft.get("dbFlags", aircraft.get("dbflags", 0))

    reasons: list[str] = []
    type_label = aircraft_type or "Nieznany typ"

    if callsign and MILITARY_CALLSIGN_REGEX.match(callsign):
        reasons.append(f"callsign: {callsign}")

    for pattern, label in MILITARY_TYPE_PATTERNS:
        if aircraft_type and re.search(pattern, aircraft_type):
            reasons.append(f"typ: {aircraft_type}")
            type_label = label
            break

    for start, end, range_name in MILITARY_ICAO_RANGES:
        if in_hex_range(hex_code, start, end):
            reasons.append(f"ICAO: {range_name}")
            break

    # W wielu implementacjach API ADS-B Exchange bit 1 (wartość 1) oznacza military.
    # Zapisujemy flagę jako sygnał pomocniczy, ale nie zakładamy jej dostępności.
    try:
        if int(db_flags or 0) & 1:
            reasons.append("dbFlags: military")
    except (TypeError, ValueError):
        pass

    return bool(reasons), reasons, type_label


def init_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            observed_at_utc TEXT NOT NULL,
            observed_at_lt TEXT NOT NULL,
            hex TEXT NOT NULL,
            registration TEXT,
            callsign TEXT,
            aircraft_type TEXT,
            type_label TEXT,
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

        CREATE INDEX IF NOT EXISTS idx_observations_hex
        ON observations(hex);

        CREATE TABLE IF NOT EXISTS reports (
            hour_start_utc TEXT PRIMARY KEY,
            report_path TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            notified_discord INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    connection.commit()


def save_observations(
    connection: sqlite3.Connection,
    aircraft_list: list[dict[str, Any]],
    observed_at: datetime,
) -> int:
    inserted = 0

    for aircraft in aircraft_list:
        if not isinstance(aircraft, dict):
            continue

        is_military, reasons, type_label = classify_aircraft(aircraft)
        if not is_military:
            continue

        hex_code = text(aircraft.get("hex"))
        if not hex_code:
            continue

        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO observations (
                    observed_at_utc, observed_at_lt, hex, registration, callsign,
                    aircraft_type, type_label, lat, lon, altitude_ft,
                    groundspeed_kt, track_deg, dbflags, classification_reasons, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observed_at.astimezone(UTC).isoformat(),
                    observed_at.astimezone(POLAND_TZ).isoformat(),
                    hex_code,
                    text(aircraft.get("r") or aircraft.get("reg")) or None,
                    text(aircraft.get("flight") or aircraft.get("callsign")) or None,
                    text(aircraft.get("t") or aircraft.get("type")) or None,
                    type_label,
                    aircraft.get("lat"),
                    aircraft.get("lon"),
                    aircraft.get("alt_baro") or aircraft.get("alt_geom"),
                    aircraft.get("gs"),
                    aircraft.get("track"),
                    aircraft.get("dbFlags", aircraft.get("dbflags", 0)) or 0,
                    ", ".join(reasons),
                    json.dumps(aircraft, ensure_ascii=False),
                ),
            )

            if connection.execute("SELECT changes()").fetchone()[0]:
                inserted += 1

        except (sqlite3.Error, TypeError, ValueError) as exc:
            log.warning("Nie zapisano obserwacji %s: %s", hex_code, exc)

    connection.commit()
    return inserted


def hour_window_to_generate(now_lt: datetime) -> tuple[datetime, datetime]:
    """
    Zwraca ostatnią domkniętą godzinę lokalną:
    np. uruchomienie 14:05 -> raport 13:00–13:59.
    """
    current_hour_start = now_lt.replace(minute=0, second=0, microsecond=0)
    end_lt = current_hour_start
    start_lt = end_lt - timedelta(hours=1)
    return start_lt, end_lt


def report_exists(connection: sqlite3.Connection, start_lt: datetime) -> bool:
    row = connection.execute(
        "SELECT 1 FROM reports WHERE hour_start_utc = ?",
        (start_lt.astimezone(UTC).isoformat(),),
    ).fetchone()
    return row is not None


def read_hour_observations(
    connection: sqlite3.Connection,
    start_lt: datetime,
    end_lt: datetime,
) -> pd.DataFrame:
    query = """
        SELECT *
        FROM observations
        WHERE observed_at_utc >= ?
          AND observed_at_utc < ?
        ORDER BY observed_at_utc ASC
    """

    return pd.read_sql_query(
        query,
        connection,
        params=(
            start_lt.astimezone(UTC).isoformat(),
            end_lt.astimezone(UTC).isoformat(),
        ),
    )


def make_hourly_report(
    observations: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> tuple[str, int]:
    title_date = start_lt.strftime("%d.%m.%Y")
    window = f"{start_lt.strftime('%H:%M')}–{end_lt.strftime('%H:%M')} LT"

    lines = [
        f"# Loty wojskowe nad Polską — {title_date}, {window}",
        "",
        f"**Okno obserwacji:** {start_lt.strftime('%Y-%m-%d %H:%M')}–{end_lt.strftime('%H:%M')} czasu polskiego.",
        "",
    ]

    if observations.empty:
        lines.extend([
            "**Wykryte loty/ślady wojskowe: 0**",
            "",
            "## Wykryte loty",
            "",
            "Brak lotów spełniających aktualne kryteria w zapisanych próbkach ADS-B.",
            "",
            "## Alert MLAT / podejrzane cisze",
            "",
            "Brak zakwalifikowanych obserwacji ADS-B w tym oknie. Nie jest to dowód braku ruchu wojskowego.",
            "",
        ])
        return "\n".join(lines), 0

    observations["observed_at_lt"] = pd.to_datetime(observations["observed_at_lt"])
    observations["callsign"] = observations["callsign"].fillna("BRAK")
    observations["registration"] = observations["registration"].fillna("NIEZNANA")
    observations["type_label"] = observations["type_label"].fillna("Nieznany typ")
    observations["hex"] = observations["hex"].fillna("NIEZNANY")

    grouped = []
    for (hex_code, callsign), group in observations.groupby(["hex", "callsign"], dropna=False):
        group = group.sort_values("observed_at_lt")

        first = group.iloc[0]
        last = group.iloc[-1]
        reasons = sorted(set(
            reason.strip()
            for cell in group["classification_reasons"].dropna()
            for reason in str(cell).split(",")
            if reason.strip()
        ))

        grouped.append({
            "hex": hex_code,
            "callsign": callsign,
            "registration": first["registration"],
            "type_label": first["type_label"],
            "first_seen": first["observed_at_lt"],
            "last_seen": last["observed_at_lt"],
            "samples": len(group),
            "reasons": ", ".join(reasons),
            "first_lat": first["lat"],
            "first_lon": first["lon"],
            "last_lat": last["lat"],
            "last_lon": last["lon"],
        })

    flights = pd.DataFrame(grouped).sort_values("first_seen")
    lines.extend([
        f"**Wykryte loty/ślady wojskowe: {len(flights)}**",
        "",
        "## Wykryte loty",
        "",
        "| Typ | Rejestracja | Callsign | Wejście → wyjście (LT) | Próbki |",
        "|---|---|---|---|---:|",
    ])

    for _, flight in flights.iterrows():
        time_range = (
            f"{flight['first_seen'].strftime('%H:%M')} → "
            f"{flight['last_seen'].strftime('%H:%M')}"
        )
        lines.append(
            f"| {markdown_text(flight['type_label'])} | "
            f"{markdown_text(flight['registration'])} | "
            f"`{markdown_text(flight['callsign'])}` | "
            f"{time_range} | {int(flight['samples'])} |"
        )

    lines.extend([
        "",
        "## Statystyki",
        "",
        "### Top 5 typów",
        "",
        "| Typ | Liczba lotów/śladów |",
        "|---|---:|",
    ])

    for aircraft_type, count in flights["type_label"].value_counts().head(5).items():
        lines.append(f"| {markdown_text(aircraft_type)} | {count} |")

    # Obiekty mające sam hex / flagę military, ale brak callsignu.
    unknown_identity = flights[
        (flights["callsign"] == "BRAK")
        | flights["registration"].isin(["NIEZNANA", ""])
    ]

    lines.extend([
        "",
        "## Alert MLAT / podejrzane cisze",
        "",
    ])

    if unknown_identity.empty:
        lines.append("Brak wykrytych obiektów wojskowych bez pełnej identyfikacji.")
    else:
        lines.append(
            "Wykryto obiekty wymagające ręcznej weryfikacji — identyfikacja może "
            "opierać się wyłącznie na ICAO Hex, dbFlags albo typie."
        )
        lines.append("")
        for _, flight in unknown_identity.iterrows():
            lines.append(
                f"- `{markdown_text(flight['hex'])}` | "
                f"`{markdown_text(flight['callsign'])}` | "
                f"{markdown_text(flight['type_label'])} | "
                f"{flight['first_seen'].strftime('%H:%M')}–"
                f"{flight['last_seen'].strftime('%H:%M')} LT | "
                f"{markdown_text(flight['reasons'])}"
            )

    lines.extend([
        "",
        "## Metoda i ograniczenia",
        "",
        "- Źródło: cykliczne snapshoty ADSB.lol nad przybliżonym obszarem Polski.",
        "- Jeden lot może pojawić się wielokrotnie w próbkach; raport grupuje go po ICAO Hex i callsignie.",
        "- ADS-B/MLAT nie jest pełnym źródłem ruchu lotniczego — brak wpisu nie oznacza braku lotu.",
        "- Czas wlotu i wylotu oznacza pierwszą oraz ostatnią obserwację w zapisanych próbkach, a nie potwierdzony przekroczenie granicy FIR.",
        "",
    ])

    return "\n".join(lines), len(flights)


def write_report(
    content: str,
    start_lt: datetime,
) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"raport-{start_lt.strftime('%Y-%m-%d_%H-00')}.md"
    report_path.write_text(content, encoding="utf-8")
    return report_path


def send_discord_notification(
    session: requests.Session,
    flights_count: int,
    report_path: Path,
    start_lt: datetime,
    end_lt: datetime,
) -> bool:
    if not DISCORD_WEBHOOK_URL or flights_count <= 0:
        return False

    message = {
        "content": (
            f"✈️ **Wykryto {flights_count} lotów/śladów wojskowych nad Polską**\n"
            f"Okno: {start_lt.strftime('%d.%m.%Y %H:%M')}–{end_lt.strftime('%H:%M')} LT\n"
            f"Raport: `{report_path.as_posix()}`"
        )
    }

    try:
        response = session.post(DISCORD_WEBHOOK_URL, json=message, timeout=20)
        response.raise_for_status()
        log.info("Wysłano powiadomienie Discord.")
        return True
    except requests.RequestException as exc:
        log.warning("Nie udało się wysłać Discord webhook: %s", exc)
        return False


def cleanup_old_data(connection: sqlite3.Connection) -> None:
    cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)

    connection.execute(
        "DELETE FROM observations WHERE observed_at_utc < ?",
        (cutoff.isoformat(),),
    )
    connection.commit()


def main() -> int:
    configure_logging()

    now_lt = datetime.now(POLAND_TZ)
    now_utc = now_lt.astimezone(UTC)

    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DATABASE_PATH) as connection:
        init_database(connection)

        session = get_session()

        try:
            snapshot = fetch_snapshot(session)
            log.info("ADSB.lol zwrócił %s obiektów w obszarze Polski.", len(snapshot))

            inserted = save_observations(connection, snapshot, now_utc)
            log.info("Zapisano %s nowych obserwacji wojskowych.", inserted)

        except Exception as exc:
            log.exception("Nie udało się pobrać/zapisać snapshotu: %s", exc)
            # Kontynuujemy: raport może powstać z dotychczas zebranych danych.

        # Raport tworzymy tylko w minutach 05–09 po pełnej godzinie.
        # Workflow można uruchamiać co 5 minut.
        if now_lt.minute < 5 or now_lt.minute > 9:
            log.info("To nie jest okno agregacji godzinowej. Kończę po zapisie próbki.")
            cleanup_old_data(connection)
            return 0

        start_lt, end_lt = hour_window_to_generate(now_lt)

        if report_exists(connection, start_lt):
            log.info(
                "Raport za %s już istnieje; nie generuję ponownie.",
                start_lt.strftime("%Y-%m-%d %H:00"),
            )
            cleanup_old_data(connection)
            return 0

        observations = read_hour_observations(connection, start_lt, end_lt)
        report_content, flights_count = make_hourly_report(
            observations,
            start_lt,
            end_lt,
        )
        report_path = write_report(report_content, start_lt)

        sent = send_discord_notification(
            session,
            flights_count,
            report_path,
            start_lt,
            end_lt,
        )

        connection.execute(
            """
            INSERT OR REPLACE INTO reports (
                hour_start_utc, report_path, created_at_utc, notified_discord
            ) VALUES (?, ?, ?, ?)
            """,
            (
                start_lt.astimezone(UTC).isoformat(),
                report_path.as_posix(),
                datetime.now(UTC).isoformat(),
                1 if sent else 0,
            ),
        )
        connection.commit()

        cleanup_old_data(connection)

        log.info(
            "Utworzono raport %s; wykryte loty/ślady: %s.",
            report_path,
            flights_count,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
