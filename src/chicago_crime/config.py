"""Configuration models for the pipeline."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable runtime settings for the crime ETL pipeline."""

    input_path: Path
    output_path: Path
    quality_output_path: Path | None = None
    quarantine_output_path: Path | None = None
    dictionary_output_path: Path | None = None
    app_name: str = "DAS7002-Chicago-Crime-ETL"
    overwrite_output: bool = True
