"""Shared application logging configuration."""

import logging


def configure_logging() -> None:
    """Configure concise timestamped logs once at each command boundary."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
