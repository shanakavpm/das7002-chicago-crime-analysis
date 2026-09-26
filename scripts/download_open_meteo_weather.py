"""Download the fixed ERA5 weather extract used by DAS7002 Task 2."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARIABLES = (
    "temperature_2m",
    "precipitation",
    "relative_humidity_2m",
    "wind_speed_10m",
    "weather_code",
)
WEATHER_LOCATION = "Chicago city centre ERA5 grid cell"


def parse_arguments() -> argparse.Namespace:
    """Parse the reproducible extract location, period, and output path."""
    parser = argparse.ArgumentParser(
        description="Download Chicago ERA5 hourly weather data."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw/chicago_weather.csv"),
    )
    parser.add_argument("--latitude", type=float, default=41.8781)
    parser.add_argument("--longitude", type=float, default=-87.6298)
    parser.add_argument("--start-date", default="2008-01-01")
    parser.add_argument("--end-date", default="2012-12-31")
    return parser.parse_args()


def build_request_url(args: argparse.Namespace) -> str:
    """Return an explicit ERA5 request without provider-dependent defaults."""
    parameters = {
        "latitude": args.latitude,
        "longitude": args.longitude,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "models": "era5",
        "timezone": "America/Chicago",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
    }
    return f"{API_URL}?{urlencode(parameters)}"


def download_weather(url: str) -> dict:
    """Download and decode one Open-Meteo response."""
    with urlopen(url, timeout=120) as response:
        payload = json.load(response)
    if "hourly" not in payload:
        reason = payload.get("reason", "response did not contain hourly data")
        raise ValueError(f"Open-Meteo request failed: {reason}")
    return payload


def write_hourly_csv(payload: dict, output_path: Path) -> None:
    """Write aligned hourly arrays using the raw-data contract expected by Task 1."""
    hourly = payload["hourly"]
    timestamps = hourly.get("time")
    if not isinstance(timestamps, list) or not timestamps:
        raise ValueError("Open-Meteo returned no hourly timestamps.")

    expected_length = len(timestamps)
    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != expected_length:
            raise ValueError(f"Open-Meteo returned an incomplete {variable} series.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ("weather_location", "weather_timestamp", *HOURLY_VARIABLES)
    with output_path.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for index, timestamp in enumerate(timestamps):
            row = {
                "weather_location": WEATHER_LOCATION,
                "weather_timestamp": timestamp,
            }
            row.update({variable: hourly[variable][index] for variable in HOURLY_VARIABLES})
            writer.writerow(row)


def main() -> None:
    """Download and save the configured historical weather extract."""
    args = parse_arguments()
    request_url = build_request_url(args)
    write_hourly_csv(download_weather(request_url), args.output)
    print(f"Weather data saved to {args.output}")
    print(f"Source request: {request_url}")


if __name__ == "__main__":
    main()
