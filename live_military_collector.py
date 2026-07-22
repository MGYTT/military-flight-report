#!/usr/bin/env python3
"""
live_military_collector.py

Monitor publicznie widocznych lotów wojskowych nad Polską:
- pobiera aktualny snapshot z ADSB.lol /v2/mil;
- filtruje obiekty z aktualną pozycją nad Polską;
- zapisuje obserwacje do SQLite;
- generuje jeden raport Markdown dla każdej zakończonej godziny;
- wysyła Discord webhook tylko, gdy raport zawiera loty/ślady;
- działa poprawnie również przy opóźnionych uruchomieniach GitHub Actions.

Wymagania:
    pip install requests pandas

Zmienne środowiskowe:
    ADSB_API_URL          domyślnie https://api.adsb.lol/v2/mil
    DATABASE_PATH         domyślnie data/military_flights.sqlite3
    REPORTS_DIR           domyślnie reports/hourly
    DISCORD_WEBHOOK_URL   opcjonalny sekret GitHub Actions
    RETENTION_DAYS        domyślnie 14
    FORCE_REPORT          1 = pozwala utworzyć raport dla ostatniej godziny
"""

from __future__ import annotations

import json
import logging
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

# Przybliżony bbox Polski z niewielkim marginesem.
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

# Callsigny charakterystyczne dla części lotnictwa wojskowego / NATO.
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
    (r"^C130|^C30J|^C30", "C-130 Hercules"),
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
    """Sprawdza, czy bieżąca pozycja samolotu jest w bbox Polski."""
    lat = safe_float(aircraft.get("lat"))
    lon = safe_float(aircraft.get("lon"))

    if lat is None or lon is None:
        return False

    return (
        POLAND_BOUNDS["lat_min"] <= lat <= POLAND_BOUNDS["lat_max"]
        and POLAND_BOUNDS["lon_min"] <= lon <= POLAND_BOUNDS["lon_max"]
    )


# =============================================================================
# ADSB.lol
# =============================================================================

def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "MGYTT-MilitaryFlightReport/4.0",
        }
    )
    return session


def fetch_military_snapshot(session: requests.Session) -> list[dict[str, Any]]:
    """
    Pobiera listę obiektów wojskowych globalnie z ADSB.lol.
    Filtr Polski wykonywany jest lokalnie.
    """
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                ADSB_API_URL,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                wait = RETRY_WAIT_SECONDS * attempt
                log.warning("Limit API HTTP 429. Ponowienie za %s s.", wait)
                time.sleep(wait)
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
                wait = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "Błąd pobrania ADSB.lol: %s. Ponowienie za %s s.",
                    exc,
                    wait,
                )
                time.sleep(wait)

    raise RuntimeError(f"Nie udało się pobrać ADSB.lol: {last_error}")


# =============================================================================
# Klasyfikacja
# =============================================================================

def classify_aircraft(aircraft: dict[str, Any]) -> Classification:
    """
    Buduje opis wykrycia. Endpoint /v2/mil jest źródłem military,
    a reguły callsign/type/ICAO są dodatkowymi uzasadnieniami.
    """
    hex_code = normalized(aircraft.get("hex"))
    callsign = normalized(aircraft.get("flight") or aircraft.get("callsign"))
    aircraft_type = normalized(aircraft.get("t") or aircraft.get("type"))

    reasons: list[str] = ["źródło API: /v2/mil"]
    type_label = aircraft_type or "Nieznany typ"

    if callsign and MILITARY_CALLSIGN_PATTERN.match(callsign):
        reasons.append(f"callsign: {callsign}")

    for pattern, label in MILITARY_TYPE_PATTERNS:
        if aircraft_type and re.search(pattern, aircraft_type):
            type_label = label
            reasons.append(f"typ ICAO: {aircraft_type}")
            break

    for start, end, label in MILITARY_ICAO_RANGES:
        if is_in_hex_range(hex_code, start, end):
            reasons.append(f"ICAO Hex: {label}")
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
    """
    Zapisuje wyłącznie obiekty aktualnie znajdujące się nad Polską.

    Zwraca:
    - liczba obiektów w bbox Polski,
    - liczba nowych rekordów SQLite.
    """
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
                    json.dumps(aircraft, ensure_ascii=False, separators=(",", ":")),
                ),
            )

            if cursor.rowcount > 0:
                inserted += 1

        except (sqlite3.Error, TypeError, ValueError) as exc:
            log.warning(
                "Nie zapisano obserwacji ICAO %s: %s",
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
            dbflags,
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

    deleted_observations = connection.execute(
        "DELETE FROM observations WHERE observed_at_utc < ?",
        (cutoff.isoformat(),),
    ).rowcount

    old_report_cutoff = (datetime.now(UTC) - timedelta(days=RETENTION_DAYS * 2)).isoformat()
    connection.execute(
        "DELETE FROM generated_reports WHERE created_at_utc < ?",
        (old_report_cutoff,),
    )

    connection.commit()

    if deleted_observations:
        log.info("Usunięto %s obserwacji starszych niż %s dni.", deleted_observations, RETENTION_DAYS)


# =============================================================================
# Agregacja i Markdown
# =============================================================================

def get_last_closed_hour(now_lt: datetime) -> tuple[datetime, datetime]:
    """
    Przykład:
    - uruchomienie 17:07, 17:26 lub 17:58
    - raport za 16:00–17:00.
    """
    end_lt = now_lt.replace(minute=0, second=0, microsecond=0)
    start_lt = end_lt - timedelta(hours=1)
    return start_lt, end_lt


def aggregate_flights(observations: pd.DataFrame) -> pd.DataFrame:
    """
    Grupuje próbki po ICAO Hex + callsign.

    Pierwsza/ostatnia obserwacja to czasy zebrane przez kolektor,
    nie potwierdzone momenty przekroczenia granicy Polski.
    """
    if observations.empty:
        return pd.DataFrame()

    data = observations.copy()
    data["observed_at_lt"] = pd.to_datetime(data["observed_at_lt"])
    data["hex"] = data["hex"].fillna("NIEZNANY")
    data["callsign"] = data["callsign"].fillna("BRAK")
    data["registration"] = data["registration"].fillna("NIEZNANA")
    data["type_label"] = data["type_label"].fillna("Nieznany typ")

    flights: list[dict[str, Any]] = []

    for (hex_code, callsign), group in data.groupby(
        ["hex", "callsign"],
        dropna=False,
    ):
        group = group.sort_values("observed_at_lt")
        first = group.iloc[0]
        last = group.iloc[-1]

        reasons = sorted(
            {
                reason.strip()
                for value in group["classification_reasons"].dropna()
                for reason in str(value).split(",")
                if reason.strip()
            }
        )

        flights.append(
            {
                "hex": hex_code,
                "callsign": callsign,
                "registration": first["registration"],
                "type_label": first["type_label"],
                "first_seen_lt": first["observed_at_lt"],
                "last_seen_lt": last["observed_at_lt"],
                "samples": len(group),
                "reasons": ", ".join(reasons),
            }
        )

    return pd.DataFrame(flights).sort_values("first_seen_lt")


def build_report(
    observations: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> tuple[str, int]:
    flights = aggregate_flights(observations)

    lines = [
        f"# Raport lotów wojskowych nad Polską — {start_lt.strftime('%d.%m.%Y')}",
        "",
        (
            f"**Okno obserwacji:** {start_lt.strftime('%H:%M')}–"
            f"{end_lt.strftime('%H:%M')} LT ({POLAND_TZ.key})."
        ),
        "",
    ]

    if flights.empty:
        lines.extend(
            [
                "**Wykryte loty/ślady wojskowe: 0**",
                "",
                "## Wykryte loty",
                "",
                "Brak zakwalifikowanych obiektów w zapisanych próbkach ADS-B.",
                "",
                "## Alert MLAT / podejrzane cisze",
                "",
                (
                    "Brak obserwacji nie potwierdza braku lotów. Samolot może nie "
                    "nadawać ADS-B, dane mogły być nieodebrane albo workflow mógł "
                    "zostać opóźniony."
                ),
                "",
                "## Metoda i ograniczenia",
                "",
                "- Źródło: snapshot ADSB.lol `/v2/mil`, filtrowany do obszaru Polski.",
                "- Raport jest tworzony z publicznie widocznych danych ADS-B/MLAT.",
                "- Brak wpisu nie jest potwierdzeniem braku aktywności wojskowej.",
                "",
            ]
        )
        return "\n".join(lines), 0

    lines.extend(
        [
            f"**Wykryte loty/ślady wojskowe: {len(flights)}**",
            "",
            "## Wykryte loty",
            "",
            "| Typ | Rejestracja | Callsign | ICAO Hex | Pierwsza → ostatnia obserwacja LT | Próbki |",
            "|---|---|---|---|---|---:|",
        ]
    )

    for _, flight in flights.iterrows():
        time_window = (
            f"{flight['first_seen_lt'].strftime('%H:%M')} → "
            f"{flight['last_seen_lt'].strftime('%H:%M')}"
        )

        lines.append(
            f"| {markdown_safe(flight['type_label'])} | "
            f"{markdown_safe(flight['registration'])} | "
            f"`{markdown_safe(flight['callsign'])}` | "
            f"`{markdown_safe(flight['hex'])}` | "
            f"{time_window} | "
            f"{int(flight['samples'])} |"
        )

    lines.extend(
        [
            "",
            "## Statystyki",
            "",
            "### Top 5 typów",
            "",
            "| Typ | Liczba lotów/śladów |",
            "|---|---:|",
        ]
    )

    for aircraft_type, count in flights["type_label"].value_counts().head(5).items():
        lines.append(f"| {markdown_safe(aircraft_type)} | {int(count)} |")

    suspicious = flights[
        (flights["callsign"] == "BRAK")
        | (flights["registration"] == "NIEZNANA")
    ]

    lines.extend(
        [
            "",
            "## Alert MLAT / podejrzane cisze",
            "",
        ]
    )

    if suspicious.empty:
        lines.append("Brak obiektów bez callsignu lub rejestracji.")
    else:
        lines.append(
            "Poniższe obiekty wymagają ręcznej weryfikacji, ponieważ mają "
            "niepełną identyfikację."
        )
        lines.append("")

        for _, flight in suspicious.iterrows():
            lines.append(
                f"- `{markdown_safe(flight['hex'])}` | "
                f"`{markdown_safe(flight['callsign'])}` | "
                f"{markdown_safe(flight['type_label'])} | "
                f"{flight['first_seen_lt'].strftime('%H:%M')}–"
                f"{flight['last_seen_lt'].strftime('%H:%M')} LT | "
                f"{markdown_safe(flight['reasons'])}"
            )

    lines.extend(
        [
            "",
            "## Metoda i ograniczenia",
            "",
            "- Źródło: publiczny endpoint ADSB.lol `/v2/mil`.",
            "- Rekord trafia do raportu, gdy bieżąca pozycja samolotu mieści się w przybliżonym obszarze Polski.",
            "- Czas w tabeli to pierwsza i ostatnia obserwacja w zapisanych próbkach, a nie potwierdzony czas przekroczenia granicy.",
            "- Dane ADS-B/MLAT są niepełne: część lotów wojskowych może nie nadawać, być nieodebrana lub nie mieć pełnej identyfikacji.",
            "",
        ]
    )

    return "\n".join(lines), len(flights)


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

def send_discord_alert(
    session: requests.Session,
    flights_count: int,
    report_path: Path,
    start_lt: datetime,
    end_lt: datetime,
) -> bool:
    """
    Wysyła Discord tylko dla raportu z co najmniej jednym lotem.
    Webhook URL musi być ustawiony jako GitHub Secret.
    """
    if flights_count <= 0:
        log.info("Discord: brak alertu, raport ma 0 lotów/śladów.")
        return False

    if not DISCORD_WEBHOOK_URL:
        log.warning(
            "Discord: raport ma %s lotów, ale brak sekretu DISCORD_WEBHOOK_URL.",
            flights_count,
        )
        return False

    payload = {
        "content": (
            f"✈️ **Wykryto loty wojskowe nad Polską: {flights_count}**\n"
            f"Okno: {start_lt.strftime('%d.%m.%Y %H:%M')}–"
            f"{end_lt.strftime('%H:%M')} LT\n"
            f"Raport: `{report_path.as_posix()}`"
        )
    }

    try:
        response = session.post(
            DISCORD_WEBHOOK_URL,
            json=payload,
            timeout=20,
        )
        response.raise_for_status()
        log.info("Discord: powiadomienie wysłane.")
        return True

    except requests.RequestException as exc:
        log.error("Discord: błąd wysyłki webhooka: %s", exc)
        return False


# =============================================================================
# Główna procedura
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
                "ADSB.lol /v2/mil: %s obiektów globalnie; %s nad Polską; %s nowych zapisów.",
                len(global_aircraft),
                in_poland,
                inserted,
            )

        except Exception as exc:
            # Nie kończymy procesu: można nadal wygenerować raport
            # z wcześniej zapisanych próbek.
            log.exception("Błąd pobrania lub zapisu snapshotu: %s", exc)

        # ZAWSZE próbujemy utworzyć raport dla poprzedniej pełnej godziny.
        # Jeśli istnieje, tabela generated_reports zapobiega duplikatowi.
        start_lt, end_lt = get_last_closed_hour(now_lt)

        if report_exists(connection, start_lt):
            log.info(
                "Raport za %s już istnieje — nie tworzę duplikatu.",
                start_lt.strftime("%Y-%m-%d %H:00"),
            )
            delete_old_data(connection)
            return 0

        observations = get_hour_observations(
            connection=connection,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        content, flights_count = build_report(
            observations=observations,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        report_path = save_report(content, start_lt)

        discord_sent = send_discord_alert(
            session=session,
            flights_count=flights_count,
            report_path=report_path,
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
