"""Command-line entry point for the Task 1 ETL pipeline."""

import argparse
from pathlib import Path

from .config import PipelineConfig
from .pipeline import run_pipeline


def parse_arguments() -> PipelineConfig:
    parser = argparse.ArgumentParser(description="Build cleaned, partitioned Chicago Crime Parquet data.")
    parser.add_argument("--input", required=True, type=Path, help="Path to the raw Chicago Crime CSV.")
    parser.add_argument("--output", required=True, type=Path, help="Directory for partitioned Parquet output.")
    parser.add_argument(
        "--fail-if-exists",
        action="store_true",
        help="Fail when the output directory already exists instead of overwriting it.",
    )
    args = parser.parse_args()
    return PipelineConfig(args.input, args.output, overwrite_output=not args.fail_if_exists)


if __name__ == "__main__":
    run_pipeline(parse_arguments())
