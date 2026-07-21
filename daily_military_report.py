#!/usr/bin/env python3
"""
Codzienny raport lotów wojskowych nad Polską z historycznych danych ADSB.lol.

Zależności:
    pip install requests pandas
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import re
import sys
import tarfile
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests


POLAND_TZ = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc

POLAND_BOUNDS = {
    "lat_min": 48.70,
    "lat_max": 55.20,
    "lon_min": 13.70,
    "lon_max": 24.40,
}

GITHUB_API = "https://api.github.com"
OUTPUT_DIR = Path(os.getenv("REPORT_OUTPUT_DIR", "reports"))

REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 4
RETRY_BACKOFF_SECONDS = 3
MAX_ASSETS_PER_RELEASE = 100
MAX_FLIGHTS_IN_REPORT = 250

MILITARY_CALLSIGN_REGEX = (
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

AIRPORTS = [
    ("EPWA", "Warszawa Chopin", 52.1657, 20.9671),
    ("EPRA", "Radom", 51.3892, 21.2133),
    ("EPMM", "Mińsk Mazowiecki", 52.1950, 21.6553),
    ("EPPO", "Poznań-Ławica", 52.4210, 16.8263),
    ("EPKK", "Kraków-Balice", 50.0777, 19.7848),
    ("EPWR", "Wrocław", 51.1027, 16.8858),
    ("EPGD", "Gdańsk", 54.3776, 18.4662),
    ("EPKT", "Katowice", 50.4743, 19.0800),
    ("EPRZ", "Rzeszów-Jasionka", 50.1100, 22.0190),
    ("EPDE", "Dęblin", 51.5519, 21.8933),
    ("EPMB", "Malbork", 54.0275, 19.1342),
    ("EPSN", "Świdwin", 53.7900, 15.8267),
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
    return (
        value is not None
        and start_int is not None
        and end_int is not None
        and start_int <= value <= end_int
    )


def repo_for_day(report_day: date) -> str:
    return os.getenv(
        "ADSB_HISTORY_REPO",
        f"adsblol/globe_history_{report_day.year}",
    ).strip()


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "User-Agent": "daily-polish-military-flight-report/1.1",
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
) -> requests.Response:
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS,
                stream=stream,
            )

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 30))
                log.warning("HTTP 429: ponawiam za %s s.", wait)
                time.sleep(wait)
                continue

            if response.status_code >= 500:
                wait = RETRY_BACKOFF_SECONDS * attempt
                log.warning(
                    "Błąd serwera HTTP %s; ponawiam za %s s.",
                    response.status_code,
                    wait,
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SECONDS * attempt
                log.warning("Błąd pobierania: %s; ponawiam za %s s.", exc, wait)
                time.sleep(wait)

    raise RuntimeError(f"Nie udało się pobrać {url}: {last_error}")


def get_previous_polish_day() -> date:
    return datetime.now(POLAND_TZ).date() - timedelta(days=1)


def should_run_now() -> bool:
    if os.getenv("FORCE_RUN", "").strip().lower() in {"1", "true", "yes"}:
        return True

    now = datetime.now(POLAND_TZ)

    # Dopuszczamy 07:00–08:59 jako bufor na opóźnienia harmonogramu GitHub.
    return now.hour in {7, 8}


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
        if numeric > 10_000_000_000:
            numeric /= 1000
        if numeric > 946684800:
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
        lat = lon = timestamp = None

        if isinstance(item, dict):
            lat = item.get("lat", item.get("latitude"))
            lon = item.get("lon", item.get("lng", item.get("longitude")))
            timestamp = (
                item.get("timestamp")
                or item.get("ts")
                or item.get("seen")
                or item.get("time")
            )
        elif isinstance(item, (list, tuple)) and len(item) >= 3:
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


def get_metadata(payload: Any, source_name: str) -> dict[str, str]:
    metadata: dict[str, Any] = {}

    if isinstance(payload, dict):
        metadata = payload.get("meta") or payload.get("aircraft") or payload

    hex_from_name = re.search(r"([0-9A-Fa-f]{6})", source_name)

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

    return bool(reasons), reasons, type_label


def nearest_airport(lat: float, lon: float, maximum_degrees: float = 0.45) -> str:
    closest = None
    closest_distance = float("inf")

    for icao, name, airport_lat, airport_lon in AIRPORTS:
        distance = ((lat - airport_lat) ** 2 + (lon - airport_lon) ** 2) ** 0.5
        if distance < closest_distance:
            closest_distance = distance
            closest = f"{icao} ({name})"

    return closest if closest and closest_distance <= maximum_degrees else "Nieustalone"


def infer_route(points: list[dict[str, Any]]) -> tuple[str, str]:
    if not points:
        return "Nieustalone", "Nieustalone"

    return (
        nearest_airport(points[0]["lat"], points[0]["lon"]),
        nearest_airport(points[-1]["lat"], points[-1]["lon"]),
    )


def find_release_for_day(
    session: requests.Session,
    repo: str,
    report_day: date,
) -> dict[str, Any]:
    response = request_with_retry(
        session,
        f"{GITHUB_API}/repos/{repo}/releases?per_page=100",
    )
    releases = response.json()

    # Release z datą D zwykle zawiera dane z dnia D.
    # Zapasowo uwzględniamy D-1, jeżeli publikacja została opisana inną datą.
    candidates = [
        report_day.isoformat(),
        report_day.strftime("%Y%m%d"),
        (report_day - timedelta(days=1)).isoformat(),
        (report_day - timedelta(days=1)).strftime("%Y%m%d"),
    ]

    for release in releases:
        haystack = " ".join([
            str(release.get("tag_name", "")),
            str(release.get("name", "")),
            str(release.get("published_at", "")),
        ]).lower()

        if any(token.lower() in haystack for token in candidates):
            return release

    raise RuntimeError(
        f"Nie znaleziono release dla daty {report_day.isoformat()} w {repo}."
    )


def get_release_assets(
    session: requests.Session,
    release: dict[str, Any],
) -> list[dict[str, Any]]:
    assets_url = release.get("assets_url")
    if not assets_url:
        raise RuntimeError("Release nie zawiera assets_url.")

    assets: list[dict[str, Any]] = []

    for page in range(1, 20):
        response = request_with_retry(
            session,
            f"{assets_url}?per_page=100&page={page}",
        )
        page_assets = response.json()

        if not page_assets:
            break

        assets.extend(page_assets)

        if len(page_assets) < 100:
            break

    return assets


def decode_json_bytes(raw: bytes, source_name: str) -> Any:
    if raw[:2] == b"\x1f\x8b" or source_name.lower().endswith(".gz"):
        raw = gzip.decompress(raw)

    return json.loads(raw.decode("utf-8"))


def iter_payloads_from_asset(
    session: requests.Session,
    asset: dict[str, Any],
) -> Iterator[tuple[str, Any]]:
    """
    Zwraca kolejne pliki JSON z assetu release:
    - pojedynczy JSON / JSON.GZ,
    - TAR,
    - TAR.GZ / TGZ.

    Archiwum jest czytane w trybie strumieniowym, bez rozpakowywania na dysk.
    """
    asset_name = str(asset.get("name", "unknown"))
    url = asset.get("browser_download_url")

    if not url:
        raise RuntimeError(f"Brak URL pobrania dla {asset_name}.")

    response = request_with_retry(session, url, stream=True)
    response.raw.decode_content = True
    lower_name = asset_name.lower()

    if lower_name.endswith((".tar.gz", ".tgz", ".tar")):
        mode = "r|gz" if lower_name.endswith((".tar.gz", ".tgz")) else "r|"

        with tarfile.open(fileobj=response.raw, mode=mode) as archive:
            for member in archive:
                if not member.isfile():
                    continue

                member_name = member.name
                if not member_name.lower().endswith((".json", ".json.gz", ".gz")):
                    continue

                extracted = archive.extractfile(member)
                if extracted is None:
                    continue

                try:
                    yield member_name, decode_json_bytes(extracted.read(), member_name)
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    log.warning("Nie można odczytać %s w %s: %s", member_name, asset_name, exc)

        return

    raw = response.content
    yield asset_name, decode_json_bytes(raw, asset_name)


def build_flight_from_payload(
    payload: Any,
    source_name: str,
    report_day: date,
) -> Optional[Flight]:
    metadata = get_metadata(payload, source_name)
    is_military, reasons, type_label = classify_military(metadata)

    if not is_military:
        return None

    local_start = datetime.combine(
        report_day,
        dt_time.min,
        tzinfo=POLAND_TZ,
    ).astimezone(UTC)
    local_end = local_start + timedelta(days=1)

    poland_points = [
        point
        for point in extract_points(payload)
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
        raw_asset_name=source_name,
    )


def detect_suspicious_windows(
    flights: list[Flight],
) -> list[tuple[str, str]]:
    active_hours = {
        flight.first_seen.astimezone(POLAND_TZ).hour
        for flight in flights
    } | {
        flight.last_seen.astimezone(POLAND_TZ).hour
        for flight in flights
    }

    if not active_hours:
        return [
            (
                "00:00–23:59",
                "Brak zakwalifikowanych pozycji ADS-B; sprawdź pełną dobę w FR24.",
            )
        ]

    windows: list[tuple[str, str]] = []
    gap_start: Optional[int] = None

    for hour in range(24):
        if hour not in active_hours and gap_start is None:
            gap_start = hour

        if (hour in active_hours or hour == 23) and gap_start is not None:
            gap_end = hour - 1 if hour in active_hours else hour

            if gap_end - gap_start + 1 >= 3:
                windows.append((
                    f"{gap_start:02d}:00–{gap_end:02d}:59",
                    "Brak wykrytych pozycji wojskowych ADS-B; możliwa luka pokrycia.",
                ))

            gap_start = None

    return windows[:8]


def flights_to_dataframe(flights: list[Flight]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "type": flight.type_label,
            "hour_lt": flight.first_seen.astimezone(POLAND_TZ).hour,
        }
        for flight in flights
    ])


def render_report(
    report_day: date,
    repo: str,
    release: Optional[dict[str, Any]],
    flights: list[Flight],
    assets_total: int,
    assets_downloaded: int,
    assets_failed: int,
    payloads_seen: int,
) -> str:
    df = flights_to_dataframe(flights)
    suspicious_windows = detect_suspicious_windows(flights)

    lines = [
        f"# Raport lotów wojskowych nad Polską — {report_day.strftime('%d.%m.%Y')}",
        "",
        f"**Łączna liczba wykrytych lotów/śladów wojskowych: {len(flights)}**",
        "",
        (
            f"> Okres analizy: {report_day.isoformat()} 00:00–23:59 czasu "
            f"polskiego ({POLAND_TZ.key})."
        ),
        (
            "> Klasyfikacja ma charakter heurystyczny: callsign, typ ICAO "
            "i wybrane zakresy ICAO. Nie każdy lot wojskowy nadaje ADS-B, "
            "a część danych może być niepełna lub opóźniona."
        ),
        "",
        "## Wykryte loty",
        "",
    ]

    if not flights:
        lines.append("Nie wykryto lotów spełniających kryteria klasyfikacji.")
        lines.append("")
    else:
        for flight in flights[:MAX_FLIGHTS_IN_REPORT]:
            first_lt = flight.first_seen.astimezone(POLAND_TZ)
            last_lt = flight.last_seen.astimezone(POLAND_TZ)

            lines.extend([
                (
                    f'**{safe_markdown(flight.type_label)}** '
                    f'"{safe_markdown(flight.registration)}" '
                    f'`{safe_markdown(flight.callsign)}` '
                    f'{first_lt.strftime("%H:%M")}–{last_lt.strftime("%H:%M")} LT'
                ),
                (
                    f"Trasa: {safe_markdown(flight.route_from)} → "
                    f"{safe_markdown(flight.route_to)}"
                ),
                (
                    f"Identyfikacja: {safe_markdown(', '.join(flight.detection_reasons))}; "
                    f"hex: `{safe_markdown(flight.hex_code)}`; "
                    f"punkty nad Polską: {flight.points_in_poland}."
                ),
                "",
            ])

    lines.extend([
        "## Statystyki",
        "",
    ])

    if df.empty:
        lines.extend(["Brak danych do statystyk.", ""])
    else:
        lines.extend([
            "### Top 5 typów",
            "",
            "| Typ | Liczba lotów/śladów |",
            "|---|---:|",
        ])

        for aircraft_type, count in df["type"].value_counts().head(5).items():
            lines.append(f"| {safe_markdown(aircraft_type)} | {count} |")

        lines.extend([
            "",
            "### Godziny największej aktywności",
            "",
            "| Godzina lokalna | Liczba rozpoczętych obserwacji |",
            "|---|---:|",
        ])

        for hour, count in (
            df["hour_lt"].value_counts().sort_values(ascending=False).head(5).items()
        ):
            lines.append(f"| {int(hour):02d}:00–{int(hour):02d}:59 | {count} |")

        lines.append("")

    lines.extend([
        "## Do uzupełnienia ręcznie z FR24",
        "",
        (
            "Poniższe okna wskazują możliwe luki w widoczności ADS-B lub "
            "klasyfikacji; nie są potwierdzeniem lotu wojskowego."
        ),
        "",
    ])

    for window, description in suspicious_windows:
        lines.append(f"- **{window} LT** — {description}")

    release_name = "Nie znaleziono"
    if release:
        release_name = str(release.get("name") or release.get("tag_name") or "Nieznany")

    lines.extend([
        "",
        "## Źródło i ograniczenia",
        "",
        f"- Repozytorium archiwum: `{repo}`.",
        f"- Release: `{safe_markdown(release_name)}`.",
        f"- Pliki archiwalne w release: {assets_total}.",
        f"- Pobrane assety: {assets_downloaded}.",
        f"- Odczytane pliki/ślady JSON z archiwów: {payloads_seen}.",
        f"- Assety pominięte z powodu błędów: {assets_failed}.",
        (
            "- ADS-B nie daje pełnego obrazu ruchu: samolot może nie nadawać, "
            "przekazywać niepełne dane albo nie zostać odebrany."
        ),
        (
            "- Trasy są przybliżone na podstawie pierwszego i ostatniego punktu "
            "nad Polską; wymagają potwierdzenia w FR24 lub innym źródle."
        ),
        "",
    ])

    return "\n".join(lines)


def main() -> int:
    configure_logging()

    if not should_run_now():
        log.info(
            "Poza oknem 07:00–08:59 czasu polskiego. "
            "Ustaw FORCE_RUN=1 dla uruchomienia ręcznego."
        )
        return 0

    report_day = get_previous_polish_day()
    repo = repo_for_day(report_day)
    session = get_session()

    log.info("Start analizy: %s.", report_day.isoformat())
    log.info("Repozytorium archiwum: %s.", repo)

    release: Optional[dict[str, Any]] = None
    assets_total = 0
    assets_downloaded = 0
    assets_failed = 0
    payloads_seen = 0
    flights: list[Flight] = []

    try:
        release = find_release_for_day(session, repo, report_day)
        log.info(
            "Używam release: %s.",
            release.get("name") or release.get("tag_name"),
        )

        assets = get_release_assets(session, release)
        assets_total = len(assets)

        for asset in assets:
            asset_name = str(asset.get("name", "unknown"))

            # Release może zawierać metadane typu checksums.txt — pomijamy je.
            if not asset_name.lower().endswith((
                ".json", ".json.gz", ".gz", ".tar", ".tar.gz", ".tgz"
            )):
                log.info("Pomijam plik pomocniczy: %s.", asset_name)
                continue

            try:
                asset_had_payload = False

                for source_name, payload in iter_payloads_from_asset(session, asset):
                    asset_had_payload = True
                    payloads_seen += 1

                    flight = build_flight_from_payload(
                        payload,
                        source_name,
                        report_day,
                    )

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

                if asset_had_payload:
                    assets_downloaded += 1
                    log.info(
                        "Przeanalizowano asset: %s; łącznie rekordów JSON: %s.",
                        asset_name,
                        payloads_seen,
                    )

            except Exception as exc:
                assets_failed += 1
                log.warning("Błąd assetu %s: %s", asset_name, exc)

        unique: dict[tuple[str, str, datetime], Flight] = {}

        for flight in flights:
            key = (
                flight.hex_code,
                flight.callsign,
                flight.first_seen.replace(second=0, microsecond=0),
            )
            unique[key] = flight

        flights = sorted(unique.values(), key=lambda item: item.first_seen)

        log.info(
            "Analiza zakończona. Assety: %s, JSON-y: %s, loty/ślady: %s, błędy: %s.",
            assets_downloaded,
            payloads_seen,
            len(flights),
            assets_failed,
        )

    except Exception as exc:
        log.exception("Krytyczny błąd analizy: %s", exc)

    report = render_report(
        report_day=report_day,
        repo=repo,
        release=release,
        flights=flights,
        assets_total=assets_total,
        assets_downloaded=assets_downloaded,
        assets_failed=assets_failed,
        payloads_seen=payloads_seen,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUTPUT_DIR / f"raport-{report_day.isoformat()}.md"
    report_path.write_text(report, encoding="utf-8")

    log.info("Raport zapisano: %s.", report_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
