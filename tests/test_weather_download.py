"""Tests for the reproducible Open-Meteo ERA5 download request."""

from argparse import Namespace
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from scripts.download_open_meteo_weather import build_request_url


def test_weather_request_fixes_model_location_period_and_units():
    args = Namespace(
        output=Path("data/raw/chicago_weather.csv"),
        latitude=41.8781,
        longitude=-87.6298,
        start_date="2008-01-01",
        end_date="2012-12-31",
    )

    query = parse_qs(urlparse(build_request_url(args)).query)

    assert query["models"] == ["era5"]
    assert query["latitude"] == ["41.8781"]
    assert query["longitude"] == ["-87.6298"]
    assert query["start_date"] == ["2008-01-01"]
    assert query["end_date"] == ["2012-12-31"]
    assert query["timezone"] == ["America/Chicago"]
    assert query["temperature_unit"] == ["fahrenheit"]
    assert query["wind_speed_unit"] == ["mph"]
    assert query["precipitation_unit"] == ["inch"]
