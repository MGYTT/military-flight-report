#!/usr/bin/env python3
"""
live_military_collector.py

Cykliczny kolektor obserwacji lotów wojskowych nad Polską.

Założenia:
- workflow GitHub Actions uruchamia ten skrypt co 5 minut;
- skrypt pobiera aktualny snapshot /v2/mil z ADSB.lol;
- filtruje wyłącznie samoloty z aktualną pozycją w obszarze Polski;
- zapisuje obserwacje do SQLite;
- w oknie HH:05–HH:14 generuje raport za poprzednią pełną godzinę;
- opcjonalnie wysyła alert Discord, gdy wykryto co najmniej jeden lot.

Wymagania:
    pip install requests pandas

Zmienne środowiskowe:
    ADSB_API_URL          domyślnie https://api.adsb.lol/v2/mil
    DATABASE_PATH         domyślnie data/military_flights.sqlite3
    REPORTS_DIR           domyślnie reports/hourly
    DISCORD_WEBHOOK_URL   opcjonalny webhook Discord
    RETENTION_DAYS        domyślnie 14
    FORCE_REPORT          1 = wymuś generowanie raportu przy ręcznym teście
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

# Przybliżony bounding box Polski. Zawiera niewielki margines na granicach.
POLAND_BOUNDS = {
    "lat_min": 48.70,
    "lat_max": 55.20,
    "lon_min": 13.70,
    "lon_max": 24.40,
}

ADSB_API_URL = os.getenv("ADSB_API_URL", "https://api.adsb.lol/v2/mil").strip()
DATABASE_PATH = Path(
    os.getenv("DATABASE_PATH", "data/military_flights.sqlite3")
)
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "reports/hourly"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = 5
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "14"))

# Dopuszczalne okno po pełnej godzinie, gdy tworzony jest raport
# za poprzednią zakończoną godzinę.
REPORT_MINUTE_START = 5
REPORT_MINUTE_END = 14

# Callsigny typowo używane przez lotnictwo wojskowe / NATO.
MILITARY_CALLSIGN_PATTERN = re.compile(
    r"^(PLF|RCH|REACH|NATO|SNAKE|NACHO|HERK(?:Y)?|DUKE|SPAR|EVAC|"
    r"SAM|MMF|ASCOT|RRR|CNV|IAM|LAGR|BAF|FAF|GAF|NOH|SVF|CFC|CEF|"
    r"POL|PLAF|PSYOP|TOPCAT|TIGER|MACE|JEDI|GHOST|RAZOR|VIPER|"
    r"HAWK|RAVEN|COBRA)[A-Z0-9-]*$"
)

# ICAO type designators. To lista heurystyczna, nie pełna lista lotnictwa wojskowego.
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

# Zakresy są wskaźnikiem pomocniczym, nie absolutnym potwierdzeniem.
MILITARY_ICAO_RANGES: tuple[tuple[str, str, str], ...] = (
    ("AE0000", "AEFFFF", "US military ICAO range"),
    ("3B0000", "3B7FFF", "Germany military/government ICAO range"),
    ("43C000", "43CFFF", "United Kingdom military ICAO range"),
)

log = logging.getLogger("live-military-collector")


# =============================================================================
# Dane i pomocnicze funkcje tekstowe
# =============================================================================

@dataclass(frozen=True)
class Classification:
    """Wynik heurystycznej klasyfikacji samolotu."""

    is_military: bool
    reasons: list[str]
    type_label: str


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def normalized(value: Any) -> str:
    """Normalizuje dane tekstowe z API."""
    return str(value or "").strip().upper()


def markdown_safe(value: Any) -> str:
    """Zabezpiecza tekst w tabelach Markdown."""
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def parse_hex(hex_code: Any) -> Optional[int]:
    """Konwertuje 6-znakowy ICAO Hex na liczbę."""
    value = normalized(hex_code).replace("~", "")

    if not re.fullmatch(r"[0-9A-F]{6}", value):
        return None

    return int(value, 16)


def is_in_hex_range(hex_code: Any, start: str, end: str) -> bool:
    """Sprawdza przynależność ICAO Hex do zakresu."""
    value = parse_hex(hex_code)
    start_value = parse_hex(start)
    end_value = parse_hex(end)

    return (
        value is not None
        and start_value is not None
        and end_value is not None
        and start_value <= value <= end_value
    )


def safe_float(value: Any) -> Optional[float]:
    """Zamienia wartość na float lub zwraca None."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def is_over_poland(aircraft: dict[str, Any]) -> bool:
    """
    Sprawdza aktualną pozycję samolotu w przybliżonym obszarze Polski.

    Brak pozycji nie kwalifikuje rekordu: nie można potwierdzić,
    że samolot był nad Polską.
    """
    lat = safe_float(aircraft.get("lat"))
    lon = safe_float(aircraft.get("lon"))

    if lat is None or lon is None:
        return False

    return (
        POLAND_BOUNDS["lat_min"] <= lat <= POLAND_BOUNDS["lat_max"]
        and POLAND_BOUNDS["lon_min"] <= lon <= POLAND_BOUNDS["lon_max"]
    )


# =============================================================================
# API ADSB.lol
# =============================================================================

def get_http_session() -> requests.Session:
    """Konfiguruje sesję HTTP."""
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "MGYTT-MilitaryFlightReport/3.0",
        }
    )
    return session


def fetch_military_snapshot(session: requests.Session) -> list[dict[str, Any]]:
    """
    Pobiera bieżący snapshot z ADSB.lol /v2/mil.

    Endpoint zwraca obiekty wojskowe globalnie. Filtrowanie geograficzne
    jest wykonywane później w `is_over_poland`.
    """
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
                    "ADSB.lol zwrócił HTTP 429 (limit). Ponowienie za %s s.",
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
                raise ValueError("Pole ac/aircraft w odpowiedzi nie jest listą.")

            return [
                item for item in aircraft
                if isinstance(item, dict)
            ]

        except (requests.RequestException, ValueError) as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                wait_seconds = RETRY_WAIT_SECONDS * attempt
                log.warning(
                    "Błąd pobierania ADSB.lol: %s. Ponowienie za %s s.",
                    exc,
                    wait_seconds,
                )
                time.sleep(wait_seconds)

    raise RuntimeError(f"Nie udało się pobrać ADSB.lol: {last_error}")


# =============================================================================
# Klasyfikacja wojskowa
# =============================================================================

def classify_aircraft(aircraft: dict[str, Any]) -> Classification:
    """
    Klasyfikacja na podstawie:
    - callsignu,
    - ICAO type designatora,
    - zakresu ICAO Hex,
    - dbFlags (jeżeli endpoint je udostępnia).

    Endpoint /v2/mil powinien już zawierać military, ale zachowujemy
    klasyfikator dla przejrzystego uzasadnienia pozycji w raporcie.
    """
    hex_code = normalized(aircraft.get("hex"))
    callsign = normalized(aircraft.get("flight") or aircraft.get("callsign"))
    aircraft_type = normalized(aircraft.get("t") or aircraft.get("type"))

    reasons: list[str] = []
    type_label = aircraft_type or "Nieznany typ"

    if callsign and MILITARY_CALLSIGN_PATTERN.match(callsign):
        reasons.append(f"callsign: {callsign}")

    for pattern, label in MILITARY_TYPE_PATTERNS:
        if aircraft_type and re.search(pattern, aircraft_type):
            reasons.append(f"typ ICAO: {aircraft_type}")
            type_label = label
            break

    for start, end, range_name in MILITARY_ICAO_RANGES:
        if is_in_hex_range(hex_code, start, end):
            reasons.append(f"ICAO Hex: {range_name}")
            break

    db_flags = aircraft.get("dbFlags", aircraft.get("dbflags", 0))

    try:
        # Bit 0 bywa używany jako military flag w API kompatybilnych z ADSB Exchange.
        if int(db_flags or 0) & 1:
            reasons.append("dbFlags: military")
    except (TypeError, ValueError):
        pass

    # /v2/mil to źródło militarnych wpisów, więc brak własnego powodu
    # nie wyklucza obiektu. Ujmujemy to jawnie w raporcie.
    if not reasons:
        reasons.append("źródło API: /v2/mil")

    return Classification(
        is_military=True,
        reasons=reasons,
        type_label=type_label,
    )


# =============================================================================
# SQLite
# =============================================================================

def init_database(connection: sqlite3.Connection) -> None:
    """Tworzy tabele i indeksy, jeżeli jeszcze nie istnieją."""
    connection.executescript(
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

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
    Zapisuje wojskowe pozycje nad Polską.

    Zwraca:
    - liczbę obiektów nad Polską,
    - liczbę nowo zapisanych obserwacji.
    """
    in_poland_count = 0
    inserted_count = 0

    for aircraft in aircraft_list:
        if not is_over_poland(aircraft):
            continue

        in_poland_count += 1

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
                inserted_count += 1

        except (sqlite3.Error, TypeError, ValueError) as exc:
            log.warning(
                "Nie udało się zapisać obserwacji ICAO %s: %s",
                hex_code,
                exc,
            )

    connection.commit()
    return in_poland_count, inserted_count


def report_already_generated(
    connection: sqlite3.Connection,
    start_lt: datetime,
) -> bool:
    """Chroni przed wygenerowaniem tej samej godziny wiele razy."""
    row = connection.execute(
        """
        SELECT 1
        FROM generated_reports
        WHERE hour_start_utc = ?
        """,
        (start_lt.astimezone(UTC).isoformat(),),
    ).fetchone()

    return row is not None


def get_observations_for_hour(
    connection: sqlite3.Connection,
    start_lt: datetime,
    end_lt: datetime,
) -> pd.DataFrame:
    """Pobiera obserwacje z wybranego okna godzinowego."""
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


def delete_old_observations(connection: sqlite3.Connection) -> None:
    """Usuwa obserwacje starsze niż ustawiony okres retencji."""
    cutoff = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)

    deleted = connection.execute(
        "DELETE FROM observations WHERE observed_at_utc < ?",
        (cutoff.isoformat(),),
    ).rowcount

    connection.commit()

    if deleted:
        log.info("Usunięto %s starych obserwacji z SQLite.", deleted)


# =============================================================================
# Raporty
# =============================================================================

def last_closed_hour(now_lt: datetime) -> tuple[datetime, datetime]:
    """
    Dla uruchomienia o 14:05 zwraca:
    start = 13:00, end = 14:00 czasu Europe/Warsaw.
    """
    current_hour_start = now_lt.replace(minute=0, second=0, microsecond=0)
    return current_hour_start - timedelta(hours=1), current_hour_start


def should_generate_report(now_lt: datetime) -> bool:
    """
    Raport tworzony jest w określonym oknie minutowym.
    FORCE_REPORT=1 pozwala go utworzyć przy ręcznym teście.
    """
    force_report = os.getenv("FORCE_REPORT", "").strip().lower()
    if force_report in {"1", "true", "yes"}:
        return True

    return REPORT_MINUTE_START <= now_lt.minute <= REPORT_MINUTE_END


def aggregate_flights(observations: pd.DataFrame) -> pd.DataFrame:
    """
    Łączy wiele próbek jednego lotu w rekord godzinowy po ICAO Hex + callsign.

    Wejście/wyjście oznacza pierwszą/ostatnią obserwację w zapisanych
    snapshotach, a nie dokładne przekroczenie granicy Polski.
    """
    if observations.empty:
        return pd.DataFrame()

    data = observations.copy()
    data["observed_at_lt"] = pd.to_datetime(data["observed_at_lt"])
    data["callsign"] = data["callsign"].fillna("BRAK")
    data["registration"] = data["registration"].fillna("NIEZNANA")
    data["type_label"] = data["type_label"].fillna("Nieznany typ")
    data["hex"] = data["hex"].fillna("NIEZNANY")

    result: list[dict[str, Any]] = []

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
                for reason_cell in group["classification_reasons"].dropna()
                for reason in str(reason_cell).split(",")
                if reason.strip()
            }
        )

        result.append(
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
                "reasons": ", ".join(reasons),
            }
        )

    return pd.DataFrame(result).sort_values(
        "first_seen_lt",
        ascending=True,
    )


def build_hourly_report(
    observations: pd.DataFrame,
    start_lt: datetime,
    end_lt: datetime,
) -> tuple[str, int]:
    """Generuje kompletny raport Markdown i zwraca liczbę lotów/śladów."""
    flights = aggregate_flights(observations)

    lines = [
        (
            "# Raport lotów wojskowych nad Polską — "
            f"{start_lt.strftime('%d.%m.%Y')}"
        ),
        "",
        (
            f"**Okno obserwacji:** "
            f"{start_lt.strftime('%H:%M')}–{end_lt.strftime('%H:%M')} LT "
            f"({POLAND_TZ.key})."
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
                "Brak zakwalifikowanych obiektów wojskowych nad Polską w zapisanych próbkach ADS-B.",
                "",
                "## Alert MLAT / podejrzane cisze",
                "",
                (
                    "Brak obserwacji nie potwierdza braku lotów: samolot może nie "
                    "nadawać ADS-B, dane mogą być niepełne albo obiekt mógł być "
                    "pomiędzy kolejnymi próbkami."
                ),
                "",
                "## Metoda",
                "",
                "- Źródło: bieżące snapshoty endpointu ADSB.lol `/v2/mil`.",
                "- Obiekty są filtrowane według aktualnej pozycji w przybliżonym obszarze Polski.",
                "- Raport grupuje próbki po ICAO Hex i callsignie.",
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
            "| Typ | Rejestracja | Callsign | ICAO Hex | Wejście → wyjście LT | Próbki |",
            "|---|---|---|---|---|---:|",
        ]
    )

    for _, flight in flights.iterrows():
        time_range = (
            f"{flight['first_seen_lt'].strftime('%H:%M')} → "
            f"{flight['last_seen_lt'].strftime('%H:%M')}"
        )

        lines.append(
            f"| {markdown_safe(flight['type_label'])} | "
            f"{markdown_safe(flight['registration'])} | "
            f"`{markdown_safe(flight['callsign'])}` | "
            f"`{markdown_safe(flight['hex'])}` | "
            f"{time_range} | "
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

    unknown_identity = flights[
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

    if unknown_identity.empty:
        lines.append(
            "Brak wykrytych obiektów wojskowych bez callsignu lub rejestracji."
        )
    else:
        lines.append(
            "Poniższe obiekty wymagają ręcznej weryfikacji — identyfikacja może "
            "opierać się tylko na ICAO Hex, typie lub klasyfikacji źródła."
        )
        lines.append("")

        for _, flight in unknown_identity.iterrows():
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
            "- Źródło: cykliczne snapshoty ADSB.lol `/v2/mil`.",
            "- Obiekt jest zapisywany tylko wtedy, gdy jego aktualna pozycja znajduje się w przybliżonym obszarze Polski.",
            "- Czas wejścia i wyjścia to pierwsza i ostatnia zapisana obserwacja, a nie potwierdzony moment przekroczenia granicy.",
            "- ADS-B i MLAT nie obejmują wszystkich lotów; brak wpisu nie oznacza braku aktywności wojskowej.",
            "",
        ]
    )

    return "\n".join(lines), len(flights)


def write_report(content: str, start_lt: datetime) -> Path:
    """Zapisuje raport jako raport-YYYY-MM-DD_HH-00.md."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    path = REPORTS_DIR / (
        f"raport-{start_lt.strftime('%Y-%m-%d_%H-00')}.md"
    )
    path.write_text(content, encoding="utf-8")

    return path


# =============================================================================
# Discord
# =============================================================================

def send_discord_webhook(
    session: requests.Session,
    flights: int,
    report_path: Path,
    start_lt: datetime,
    end_lt: datetime,
) -> bool:
    """
    Wysyła skrócony alert. Nie wysyła nic, jeśli:
    - webhook nie jest skonfigurowany,
    - nie było wykrytych lotów.
    """
    if not DISCORD_WEBHOOK_URL or flights <= 0:
        return False

    payload = {
        "content": (
            f"✈️ **Loty wojskowe nad Polską: {flights}**\n"
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

        log.info("Wysłano powiadomienie Discord.")
        return True

    except requests.RequestException as exc:
        log.warning("Błąd Discord webhook: %s", exc)
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

        session = get_http_session()

        try:
            global_military = fetch_military_snapshot(session)

            in_poland, inserted = save_snapshot(
                connection=connection,
                aircraft_list=global_military,
                observed_at=now_utc,
            )

            log.info(
                "ADSB.lol /v2/mil: %s obiektów globalnie; %s z pozycją nad Polską; %s nowych zapisów.",
                len(global_military),
                in_poland,
                inserted,
            )

        except Exception as exc:
            # Nie przerywamy: raport może zostać zbudowany z danych
            # już istniejących w SQLite.
            log.exception("Błąd pobierania lub zapisu snapshotu: %s", exc)

        if not should_generate_report(now_lt):
            log.info(
                "Poza oknem raportowania (%02d–%02d minuta godziny). "
                "Zakończono po zapisie snapshotu.",
                REPORT_MINUTE_START,
                REPORT_MINUTE_END,
            )
            delete_old_observations(connection)
            return 0

        start_lt, end_lt = last_closed_hour(now_lt)

        if report_already_generated(connection, start_lt):
            log.info(
                "Raport za %s już istnieje. Pomijam.",
                start_lt.strftime("%Y-%m-%d %H:00"),
            )
            delete_old_observations(connection)
            return 0

        observations = get_observations_for_hour(
            connection=connection,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        report_content, flights_count = build_hourly_report(
            observations=observations,
            start_lt=start_lt,
            end_lt=end_lt,
        )

        report_path = write_report(
            content=report_content,
            start_lt=start_lt,
        )

        discord_sent = send_discord_webhook(
            session=session,
            flights=flights_count,
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

        delete_old_observations(connection)

        log.info(
            "Utworzono raport: %s | loty/ślady: %s | Discord: %s.",
            report_path,
            flights_count,
            "wysłano" if discord_sent else "nie wysłano",
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
