#!/usr/bin/env python3
"""
Codzienny raport lotów wojskowych nad Polską na podstawie historycznych danych ADSB.lol.

Instalacja lokalna:
    pip install requests pandas

Uruchomienie:
    python daily_military_report.py

Zmienne środowiskowe (opcjonalne):
    ADSB_HISTORY_REPO   np. adsblol/globe_history_2026
    ADSB_HISTORY_TOKEN  GitHub token (przydatny dla większych limitów API)
    REPORT_OUTPUT_DIR   katalog raportów, domyślnie reports
    FORCE_RUN=1         pomija kontrolę godziny 07:00 czasu polskiego
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests


# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------

POLAND_TZ = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc

# Przybliżony obszar Polski z marginesem, aby nie zgubić lotów granicznych.
POLAND_BOUNDS = {
    "lat_min": 48.70,
    "lat_max": 55.20,
    "lon_min": 13.70,
    "lon_max": 24.40,
}

GITHUB_API = "https://api.github.com"
DEFAULT_HISTORY_REPO = "adsblol/globe_history_2026"
OUTPUT_DIR = Path(os.getenv("REPORT_OUTPUT_DIR", "reports"))

REQUEST_TIMEOUT_SECONDS = 45
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 2
MAX_ASSETS_PER_RELEASE = 2500
MAX_FLIGHTS_IN_REPORT = 250

# Nie jest to pełna, oficjalna lista. To celowe heurystyki do preselekcji.
MILITARY_CALLSIGN_PREFIXES = (
    "PLF", "RCH", "REACH", "NATO", "SNAKE", "NACHO", "HERKY", "HERC",
    "DUKE", "SPAR", "EVAC", "SAM", "MMF", "ASCOT", "RRR", "CNV",
    "IAM", "LAGR", "BAF", "FAF", "GAF", "NOH", "SVF", "CFC",
    "CEF", "POL", "PLAF", "PSYOP", "TOPCAT", "TIGER", "MACE",
    "JEDI", "GHOST", "RAZOR", "VIPER", "HAWK", "RAVEN", "COBRA",
)

MILITARY_CALLSIGN_REGEX = (
    r"^(PLF|RCH|REACH|NATO|SNAKE|NACHO|HERK(?:Y)?|DUKE|SPAR|EVAC|"
    r"SAM|MMF|ASCOT|RRR|CNV|IAM|LAGR|BAF|FAF|GAF|NOH|SVF|CFC|CEF|"
    r"POL|PLAF|PSYOP|TOPCAT|TIGER|MACE|JEDI|GHOST|RAZOR|VIPER|"
    r"HAWK|RAVEN|COBRA)[A-Z0-9-]*$"
)

# Wzorce ICAO type designator. Pierwszy element: wzorzec, drugi: nazwa raportowa.
MILITARY_TYPE_PATTERNS = (
    (r"^F16", "F-16 Fighting Falcon"),
    (r"^F35", "F-35 Lightning II"),
    (r"^F15", "F-15 Eagle"),
    (r"^FA18|^F18", "F/A-18 Hornet"),
    (r"^A10", "A-10 Thunderbolt II"),
    (r"^C130", "C-130 Hercules"),
    (r"^C17", "C-17 Globemaster III"),
    (r"^C5", "C-5 Galaxy"),
    (r"^C27", "C-27J Spartan"),
    (r"^C295", "C-295"),
    (r"^C30J", "C-130J Hercules"),
    (r"^KC10", "KC-10 Extender"),
    (r"^KC135", "KC-135 Stratotanker"),
    (r"^K35R", "KC-135 Stratotanker"),
    (r"^K35E", "KC-135 Stratotanker"),
    (r"^A332", "A330 MRTT"),
    (r"^A400", "A400M Atlas"),
    (r"^E3", "E-3 Sentry AWACS"),
    (r"^E7", "E-7 Wedgetail"),
    (r"^E2", "E-2 Hawkeye"),
    (r"^P8", "P-8 Poseidon"),
    (r"^C160", "C-160 Transall"),
    (r"^C30", "C-130 Hercules"),
    (r"^LJ35", "Learjet 35 (możliwy wojskowy)"),
    (r"^B350", "King Air 350 (możliwy wojskowy)"),
    (r"^H60", "UH-60 / MH-60 Black Hawk"),
    (r"^S70", "S-70 Black Hawk"),
    (r"^CH47", "CH-47 Chinook"),
    (r"^V22", "V-22 Osprey"),
)

# Przykładowe bloki ICAO używane przez siły USA/NATO.
# Są to wyłącznie wskaźniki pomocnicze: nie mogą samodzielnie przesądzać o statusie.
MILITARY_ICAO_RANGES = (
    ("AE0000", "AEFFFF", "US military"),
    ("3B0000", "3B7FFF", "Germany military / government range"),
    ("43C000", "43CFFF", "United Kingdom military range"),
)

AIRPORTS = [
    ("EPWA", "Warszawa Chopin", 52.1657, 20.9671),
    ("EPRA", "Radom", 51.3892, 21.2133),
    ("EPMM", "Mińsk Mazowiecki", 52.1950, 21.6550),
    ("EPPO", "Poznań-Ławica", 52.4210, 16.8263),
    ("EPKK", "Kraków-Balice", 50.0777, 19.7848),
    ("EPWR", "Wrocław", 51.1027, 16.8858),
    ("EPGD", "Gdańsk", 54.3776, 18.4662),
    ("EPKT", "Katowice", 50.4743, 19.0800),
    ("EPLL", "Łódź", 51.7219, 19.3981),
    ("EPRZ", "Rzeszów-Jasionka", 50.1100, 22.0190),
    ("EPSY", "Olsztyn-Mazury", 53.4819, 20.9377),
    ("EPBY", "Bydgoszcz", 53.0968, 17.9777),
    ("EPCE", "Zegrze Pomorskie", 54.4167, 16.2667),
    ("EPBL", "Biała Podlaska", 52.0000, 23.1500),
    ("EPDE", "Dęblin", 51.5519, 21.8933),
    ("EPIR", "Inowrocław", 52.7944, 18.2639),
    ("EPKS", "Książęce", 51.5556, 18.6861),
    ("EPSN", "Świdwin", 53.7900, 15.8267),
    ("EPST", "Stargard", 53.3522, 15.0347),
    ("EPJG", "Głogów", 51.5667, 16.0667),
    ("EPMB", "Malbork", 54.0275, 19.1342),
]

log = logging.getLogger("military-report")


@dataclass
class Flight:
    hex_code: str
    registration: str
    callsign: str
    aircraft_type: str
    type_label: str
    first_seen: datetime
    last_seen: datetime
    points_in_poland: int
    route_from: str
    route_to: str
    detection_reasons: list[str]
    raw_asset_name: str


# ---------------------------------------------------------------------------
# Narzędzia pomocnicze
# ---------------------------------------------------------------------------

def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def normalize_text(value: Any) -> str:
    return str(value or "").strip().upper()


def safe_markdown(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").strip()


def parse_hex(value: str) -> Optional[int]:
    value = normalize_text(value).replace("~", "")
    if not re.fullmatch(r"[0-9A-F]{6}", value):
        return None
    return int(value, 16)


def is_in_hex_range(hex_code: str, start: str, end: str) -> bool:
    value = parse_hex(hex_code)
    start_int = parse_hex(start)
    end_int = parse_hex(end)
    return value is not None and start_int is not None and end_int is not None and start_int <= value <= end_int


def repo_for_day(report_day: date) -> str:
    explicit_repo = os.getenv("ADSB_HISTORY_REPO", "").strip()
    if explicit_repo:
        return explicit_repo
    return f"adsblol/globe_history_{report_day.year}"


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "User-Agent": "daily-polish-military-flight-report/1.0",
    })

    token = os.getenv("ADSB_HISTORY_TOKEN", "").strip()
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    return session


def request_with_retry(
    session: requests.Session,
    url: str,
    *,
    stream: bool = False,
    expected_json: bool = False,
) -> requests.Response:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS, stream=stream)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RETRY_BACKOFF_SECONDS * attempt))
                log.warning("Limit API (HTTP 429), ponawiam za %s s.", retry_after)
                time.sleep(retry_after)
                continue

            if response.status_code >= 500:
                log.warning(
                    "Błąd serwera HTTP %s dla %s (próba %s/%s).",
                    response.status_code, url, attempt, MAX_RETRIES,
                )
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue

            response.raise_for_status()
            if expected_json:
                response.json()
            return response

        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SECONDS * attempt
                log.warning("Błąd pobierania: %s. Ponawiam za %s s.", exc, wait)
                time.sleep(wait)

    raise RuntimeError(f"Nie udało się pobrać danych: {url}. Ostatni błąd: {last_error}")


def get_previous_polish_day() -> date:
    return (datetime.now(POLAND_TZ).date() - timedelta(days=1))


def should_run_now() -> bool:
    if os.getenv("FORCE_RUN", "").strip().lower() in {"1", "true", "yes"}:
        return True

    now = datetime.now(POLAND_TZ)
    return now.hour == 7


def is_point_in_poland(lat: float, lon: float) -> bool:
    return (
        POLAND_BOUNDS["lat_min"] <= lat <= POLAND_BOUNDS["lat_max"]
        and POLAND_BOUNDS["lon_min"] <= lon <= POLAND_BOUNDS["lon_max"]
    )


def normalize_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    try:
        numeric = float(value)

        # Epoch w ms zamiast s.
        if numeric > 10_000_000_000:
            numeric /= 1000

        if numeric > 946684800:  # po 2000-01-01
            return datetime.fromtimestamp(numeric, tz=UTC)
    except (TypeError, ValueError, OSError):
        pass

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None

    return None


def extract_points(payload: Any) -> list[dict[str, Any]]:
    """
    Obsługuje popularne warianty struktur historycznych readsb / globe_history.
    Wynikiem jest lista punktów ze współrzędnymi i czasem.
    """
    candidates: list[Any] = []

    if isinstance(payload, list):
        candidates = payload
    elif isinstance(payload, dict):
        for key in ("trace", "history", "points", "data", "states"):
            if isinstance(payload.get(key), list):
                candidates = payload[key]
                break

    points: list[dict[str, Any]] = []

    for item in candidates:
        lat = lon = None
        timestamp = None

        if isinstance(item, dict):
            lat = item.get("lat", item.get("latitude"))
            lon = item.get("lon", item.get("lng", item.get("longitude")))
            timestamp = (
                item.get("timestamp")
                or item.get("ts")
                or item.get("seen")
                or item.get("time")
            )

        elif isinstance(item, (list, tuple)):
            # Standardowy trace readsb często ma postać:
            # [timestamp, lat, lon, altitude, ...]
            if len(item) >= 3:
                timestamp, lat, lon = item[0], item[1], item[2]

        try:
            lat_float = float(lat)
            lon_float = float(lon)
        except (TypeError, ValueError):
            continue

        point_time = normalize_timestamp(timestamp)
        if point_time is None:
            continue

        if -90 <= lat_float <= 90 and -180 <= lon_float <= 180:
            points.append({
                "time": point_time,
                "lat": lat_float,
                "lon": lon_float,
            })

    return sorted(points, key=lambda point: point["time"])


def get_metadata(payload: Any, asset_name: str) -> dict[str, str]:
    """
    Odczytuje metadane z kilku możliwych formatów historycznych ADSB.lol.
    """
    metadata: dict[str, Any] = {}

    if isinstance(payload, dict):
        metadata = payload.get("meta") or payload.get("aircraft") or payload

    hex_from_name = re.search(r"([0-9A-Fa-f]{6})", asset_name)

    return {
        "hex": normalize_text(
            metadata.get("hex")
            or metadata.get("icao")
            or metadata.get("icao24")
            or (hex_from_name.group(1) if hex_from_name else "")
        ),
        "registration": normalize_text(
            metadata.get("r")
            or metadata.get("reg")
            or metadata.get("registration")
        ),
        "callsign": normalize_text(
            metadata.get("flight")
            or metadata.get("callsign")
            or metadata.get("call")
        ),
        "type": normalize_text(
            metadata.get("t")
            or metadata.get("type")
            or metadata.get("icao_type")
            or metadata.get("typecode")
        ),
    }


def classify_military(metadata: dict[str, str]) -> tuple[bool, list[str], str]:
    callsign = metadata["callsign"]
    aircraft_type = metadata["type"]
    hex_code = metadata["hex"]

    reasons: list[str] = []
    type_label = aircraft_type or "Nieznany typ"

    if callsign and re.match(MILITARY_CALLSIGN_REGEX, callsign):
        reasons.append(f"callsign: {callsign}")

    for pattern, label in MILITARY_TYPE_PATTERNS:
        if aircraft_type and re.search(pattern, aircraft_type):
            reasons.append(f"typ: {aircraft_type}")
            type_label = label
            break

    for start, end, owner in MILITARY_ICAO_RANGES:
        if is_in_hex_range(hex_code, start, end):
            reasons.append(f"ICAO: {owner}")
            break

    # Wymagamy przynajmniej jednego sygnału. Raport powinien być konserwatywny:
    # lepiej oznaczyć rekord jako możliwy niż bezpodstawnie uznać cywilny lot.
    return bool(reasons), reasons, type_label


def nearest_airport(lat: float, lon: float, maximum_degrees: float = 0.45) -> str:
    """
    Przybliżone przypisanie lotniska na podstawie odległości w stopniach.
    Służy wyłącznie do orientacyjnej trasy, nie zastępuje danych FR24/operacyjnych.
    """
    nearest = None
    nearest_distance = float("inf")

    for icao, name, airport_lat, airport_lon in AIRPORTS:
        distance = ((lat - airport_lat) ** 2 + (lon - airport_lon) ** 2) ** 0.5
        if distance < nearest_distance:
            nearest_distance = distance
            nearest = f"{icao} ({name})"

    if nearest is not None and nearest_distance <= maximum_degrees:
        return nearest

    return "Nieustalone"


def infer_route(points: list[dict[str, Any]]) -> tuple[str, str]:
    if not points:
        return "Nieustalone", "Nieustalone"

    first = points[0]
    last = points[-1]

    return (
        nearest_airport(first["lat"], first["lon"]),
        nearest_airport(last["lat"], last["lon"]),
    )


# ---------------------------------------------------------------------------
# GitHub Releases / ADSB.lol archive
# ---------------------------------------------------------------------------

def find_release_for_day(
    session: requests.Session,
    repo: str,
    report_day: date,
) -> dict[str, Any]:
    """
    Szuka release zawierającego datę w tagu lub nazwie.
    Pobiera pierwsze 100 release'ów; dla archiwum dziennego jest to wystarczające
    dla świeżych raportów.
    """
    url = f"{GITHUB_API}/repos/{repo}/releases?per_page=100"
    response = request_with_retry(session, url, expected_json=True)
    releases = response.json()

    date_tokens = (
        report_day.isoformat(),                      # 2026-07-20
        report_day.strftime("%Y%m%d"),              # 20260720
        report_day.strftime("%d-%m-%Y"),            # 20-07-2026
    )

    for release in releases:
        haystack = " ".join([
            str(release.get("tag_name", "")),
            str(release.get("name", "")),
            str(release.get("published_at", "")),
        ]).lower()

        if any(token.lower() in haystack for token in date_tokens):
            return release

    raise RuntimeError(
        f"Nie znaleziono GitHub Release dla {report_day.isoformat()} "
        f"w repozytorium {repo}. Archiwum mogło nie zostać jeszcze opublikowane."
    )


def get_release_assets(
    session: requests.Session,
    repo: str,
    release: dict[str, Any],
) -> list[dict[str, Any]]:
    assets_url = release.get("assets_url")
    if not assets_url:
        raise RuntimeError("Release nie zawiera adresu API assets_url.")

    assets: list[dict[str, Any]] = []
    page = 1

    while len(assets) < MAX_ASSETS_PER_RELEASE:
        url = f"{assets_url}?per_page=100&page={page}"
        response = request_with_retry(session, url, expected_json=True)
        current = response.json()

        if not current:
            break

        assets.extend(current)

        if len(current) < 100:
            break

        page += 1

    log.info(
        "Release „%s”: znaleziono %s plików archiwalnych.",
        release.get("name") or release.get("tag_name"),
        len(assets),
    )
    return assets


def likely_asset_metadata(asset_name: str) -> dict[str, str]:
    """
    Próbuje wykonać wstępne filtrowanie po nazwie pliku, jeśli zawiera hex/typ.
    Nie odrzuca plików tylko dlatego, że nazwa nie zawiera metadanych.
    """
    hex_match = re.search(r"(?<![0-9A-F])([0-9A-F]{6})(?![0-9A-F])", asset_name.upper())
    return {
        "hex": hex_match.group(1) if hex_match else "",
        "registration": "",
        "callsign": "",
        "type": "",
    }


def download_archive_json(
    session: requests.Session,
    download_url: str,
    asset_name: str,
) -> Any:
    response = request_with_retry(session, download_url, stream=True)
    content = response.content

    try:
        if asset_name.lower().endswith(".gz") or content[:2] == b"\x1f\x8b":
            content = gzip.decompress(content)

        return json.loads(content.decode("utf-8"))

    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Nieprawidłowy JSON/GZIP w pliku {asset_name}: {exc}") from exc


# ---------------------------------------------------------------------------
# Analiza
# ---------------------------------------------------------------------------

def build_flight_from_payload(
    payload: Any,
    asset_name: str,
    report_day: date,
) -> Optional[Flight]:
    metadata = get_metadata(payload, asset_name)
    is_military, reasons, type_label = classify_military(metadata)

    if not is_military:
        return None

    points = extract_points(payload)
    day_start = datetime.combine(report_day, dt_time.min, tzinfo=UTC)
    day_end = day_start + timedelta(days=1)

    # Archiwum może mieć ślady obejmujące pełny dzień UTC. Filtrujemy po czasie
    # lokalnym raportu: 00:00–23:59 w Polsce.
    local_start = datetime.combine(report_day, dt_time.min, tzinfo=POLAND_TZ).astimezone(UTC)
    local_end = local_start + timedelta(days=1)

    poland_points = [
        point for point in points
        if local_start <= point["time"] < local_end
        and is_point_in_poland(point["lat"], point["lon"])
    ]

    if not poland_points:
        return None

    route_from, route_to = infer_route(poland_points)

    return Flight(
        hex_code=metadata["hex"] or "Nieznany",
        registration=metadata["registration"] or "Nieznana",
        callsign=metadata["callsign"] or "Brak",
        aircraft_type=metadata["type"] or "Nieznany",
        type_label=type_label,
        first_seen=poland_points[0]["time"],
        last_seen=poland_points[-1]["time"],
        points_in_poland=len(poland_points),
        route_from=route_from,
        route_to=route_to,
        detection_reasons=reasons,
        raw_asset_name=asset_name,
    )


def detect_suspicious_windows(flights: list[Flight], report_day: date) -> list[tuple[str, str]]:
    """
    Wskazuje luki czasowe w pokryciu wykrytymi lotami wojskowymi.
    To NIE dowód braku lotów: jest to lista okien do ręcznej weryfikacji w FR24.
    """
    active_hours = sorted({
        flight.first_seen.astimezone(POLAND_TZ).hour
        for flight in flights
    } | {
        flight.last_seen.astimezone(POLAND_TZ).hour
        for flight in flights
    })

    suspicious: list[tuple[str, str]] = []

    if not active_hours:
        return [("00:00–23:59", "Brak wykrytych wojskowych ADS-B nad Polską; sprawdź pełną dobę.")]

    # Długie przerwy >= 3h między obserwacjami są warte kontroli.
    hours_with_activity = set(active_hours)
    start_gap: Optional[int] = None

    for hour in range(24):
        if hour not in hours_with_activity and start_gap is None:
            start_gap = hour

        if (hour in hours_with_activity or hour == 23) and start_gap is not None:
            end_gap = hour - 1 if hour in hours_with_activity else hour
            if end_gap - start_gap + 1 >= 3:
                suspicious.append((
                    f"{start_gap:02d}:00–{end_gap:02d}:59",
                    "Brak pozycji zakwalifikowanych jako wojskowe ADS-B; możliwa niepełna widoczność.",
                ))
            start_gap = None

    return suspicious[:8]


def flights_to_dataframe(flights: list[Flight]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "type": flight.type_label,
            "callsign": flight.callsign,
            "registration": flight.registration,
            "first_seen_lt": flight.first_seen.astimezone(POLAND_TZ),
            "last_seen_lt": flight.last_seen.astimezone(POLAND_TZ),
            "hour_lt": flight.first_seen.astimezone(POLAND_TZ).hour,
        }
        for flight in flights
    ])


# ---------------------------------------------------------------------------
# Raport Markdown
# ---------------------------------------------------------------------------

def render_report(
    report_day: date,
    repo: str,
    release: Optional[dict[str, Any]],
    flights: list[Flight],
    assets_total: int,
    assets_downloaded: int,
    assets_failed: int,
) -> str:
    lines: list[str] = []
    report_title_date = report_day.strftime("%d.%m.%Y")
    df = flights_to_dataframe(flights)
    suspicious_windows = detect_suspicious_windows(flights, report_day)

    lines.append(f"# Raport lotów wojskowych nad Polską — {report_title_date}")
    lines.append("")
    lines.append(
        f"**Łączna liczba wykrytych lotów/śladów wojskowych: {len(flights)}**"
    )
    lines.append("")
    lines.append(
        f"> Okres analizy: {report_day.isoformat()} 00:00–23:59 czasu polskiego "
        f"({POLAND_TZ.key})."
    )
    lines.append(
        "> Klasyfikacja ma charakter heurystyczny: callsign, typ ICAO i wybrane "
        "zakresy ICAO. Nie każdy lot wojskowy nadaje ADS-B, a część danych może "
        "być niepełna lub opóźniona."
    )
    lines.append("")

    lines.append("## Wykryte loty")
    lines.append("")

    if not flights:
        lines.append(
            "Nie wykryto lotów spełniających aktualne kryteria klasyfikacji w danych archiwalnych."
        )
    else:
        for flight in flights[:MAX_FLIGHTS_IN_REPORT]:
            first_lt = flight.first_seen.astimezone(POLAND_TZ)
            last_lt = flight.last_seen.astimezone(POLAND_TZ)

            lines.append(
                f'**{safe_markdown(flight.type_label)}** '
                f'"{safe_markdown(flight.registration)}" '
                f'`{safe_markdown(flight.callsign)}` '
                f'{first_lt.strftime("%H:%M")}–{last_lt.strftime("%H:%M")} LT'
            )
            lines.append(
                f"Trasa: {safe_markdown(flight.route_from)} → {safe_markdown(flight.route_to)}"
            )
            lines.append(
                f"Identyfikacja: {safe_markdown(', '.join(flight.detection_reasons))}; "
                f"hex: `{safe_markdown(flight.hex_code)}`; "
                f"punkty ADS-B nad Polską: {flight.points_in_poland}."
            )
            lines.append("")

        if len(flights) > MAX_FLIGHTS_IN_REPORT:
            lines.append(
                f"_Lista skrócona do {MAX_FLIGHTS_IN_REPORT} pozycji z {len(flights)} wykrytych rekordów._"
            )
            lines.append("")

    lines.append("## Statystyki")
    lines.append("")

    if df.empty:
        lines.append("Brak danych do statystyk.")
        lines.append("")
    else:
        top_types = df["type"].value_counts().head(5)
        lines.append("### Top 5 typów")
        lines.append("")
        lines.append("| Typ | Liczba lotów/śladów |")
        lines.append("|---|---:|")
        for aircraft_type, count in top_types.items():
            lines.append(f"| {safe_markdown(aircraft_type)} | {count} |")
        lines.append("")

        hourly = df["hour_lt"].value_counts().sort_values(ascending=False).head(5)
        lines.append("### Godziny największej aktywności")
        lines.append("")
        lines.append("| Godzina lokalna | Liczba rozpoczętych obserwacji |")
        lines.append("|---|---:|")
        for hour, count in hourly.items():
            lines.append(f"| {int(hour):02d}:00–{int(hour):02d}:59 | {count} |")
        lines.append("")

    lines.append("## Do uzupełnienia ręcznie z FR24")
    lines.append("")
    lines.append(
        "Poniższe okna są kandydatami do ręcznej weryfikacji. Nie oznaczają "
        "automatycznie, że doszło do lotu wojskowego — wskazują jedynie możliwe "
        "luki w widoczności ADS-B lub klasyfikacji."
    )
    lines.append("")

    for window, explanation in suspicious_windows:
        lines.append(f"- **{window} LT** — {explanation}")

    lines.append("")
    lines.append("## Źródło i ograniczenia")
    lines.append("")
    lines.append(
        f"- Repozytorium archiwum: `{repo}`."
    )

    if release:
        release_name = release.get("name") or release.get("tag_name") or "nieznany release"
        lines.append(f"- Release: `{safe_markdown(str(release_name))}`.")

    lines.extend([
        f"- Pliki archiwalne w release: {assets_total}.",
        f"- Pobrane i przeanalizowane pliki: {assets_downloaded}.",
        f"- Pliki pominięte z powodu błędów: {assets_failed}.",
        "- ADS-B nie jest pełnym obrazem ruchu: samolot może nie nadawać, nadawać niepełne dane albo nie być odebrany przez lokalną sieć odbiorników.",
        "- Trasy są przybliżone na podstawie pierwszego i ostatniego punktu nad Polską; wymagają potwierdzenia w FR24 lub innym źródle.",
        "",
    ])

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Główna procedura
# ---------------------------------------------------------------------------

def main() -> int:
    configure_logging()

    if not should_run_now():
        current = datetime.now(POLAND_TZ).strftime("%Y-%m-%d %H:%M %Z")
        log.info(
            "Pomijam wykonanie: obecna godzina w Polsce to %s. "
            "Workflow może uruchamiać się o 05:00 UTC codziennie; skrypt wykona "
            "analizę tylko o 07:00 lokalnie. Ustaw FORCE_RUN=1 dla testu.",
            current,
        )
        return 0

    report_day = get_previous_polish_day()
    repo = repo_for_day(report_day)
    session = get_session()

    log.info("Start raportu dla dnia %s.", report_day.isoformat())
    log.info("Źródło historyczne ADSB.lol/GitHub: %s.", repo)

    release: Optional[dict[str, Any]] = None
    assets_total = 0
    assets_downloaded = 0
    assets_failed = 0
    flights: list[Flight] = []

    try:
        release = find_release_for_day(session, repo, report_day)
        assets = get_release_assets(session, repo, release)
        assets_total = len(assets)

        if not assets:
            raise RuntimeError("Release nie zawiera żadnych assetów do analizy.")

        for index, asset in enumerate(assets, start=1):
            asset_name = str(asset.get("name", "unknown"))
            download_url = asset.get("browser_download_url")

            if not download_url:
                log.warning("Pomijam %s: brak browser_download_url.", asset_name)
                continue

            # Pomiń wyraźnie niebędące danymi pliki pomocnicze.
            if not re.search(r"\.(json|gz|json\.gz)$", asset_name, re.IGNORECASE):
                continue

            try:
                payload = download_archive_json(session, download_url, asset_name)
                assets_downloaded += 1

                flight = build_flight_from_payload(payload, asset_name, report_day)
                if flight:
                    flights.append(flight)
                    log.info(
                        "ZŁAPANO | %s | %s | %s | %s | %s",
                        flight.type_label,
                        flight.registration,
                        flight.callsign,
                        flight.first_seen.astimezone(POLAND_TZ).strftime("%H:%M"),
                        "; ".join(flight.detection_reasons),
                    )

            except Exception as exc:
                assets_failed += 1
                log.warning("Błąd pliku %s: %s", asset_name, exc)

            if index % 100 == 0:
                log.info(
                    "Postęp: %s/%s assetów, wykryte rekordy: %s, błędy: %s.",
                    index, assets_total, len(flights), assets_failed,
                )

        # Dedup: ten sam hex/callsign o tej samej godzinie startu traktujemy jako jeden ślad.
        unique: dict[tuple[str, str, datetime], Flight] = {}
        for flight in flights:
            key = (
                flight.hex_code,
                flight.callsign,
                flight.first_seen.replace(second=0, microsecond=0),
            )
            unique[key] = flight

        flights = sorted(
            unique.values(),
            key=lambda item: item.first_seen,
        )

        log.info(
            "Zakończono analizę: %s lotów/śladów wojskowych. "
            "Przeanalizowane assety: %s; błędy: %s.",
            len(flights), assets_downloaded, assets_failed,
        )

    except Exception as exc:
        log.exception("Krytyczny błąd pobierania/analizy danych: %s", exc)

    report = render_report(
        report_day=report_day,
        repo=repo,
        release=release,
        flights=flights,
        assets_total=assets_total,
        assets_downloaded=assets_downloaded,
        assets_failed=assets_failed,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / f"raport-{report_day.isoformat()}.md"
    report_path.write_text(report, encoding="utf-8")

    log.info("Raport zapisano: %s", report_path)
    log.info(
        "Uwaga: brak ADS-B nie oznacza braku lotu. Zweryfikuj wskazane okna ręcznie w FR24."
    )

    # Workflow może zrobić commit nawet przy częściowym błędzie źródła:
    # raport wtedy dokumentuje, że dane były niepełne.
    return 0


if __name__ == "__main__":
    sys.exit(main())
