"""Command-line entry point for the analytics pipeline."""

import argparse
from pathlib import Path

from retail_analytics.config import PipelineConfig
from retail_analytics.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze retail transactions with PySpark")
    parser.add_argument("--input", default="data/raw/online_retail.csv", help="Input CSV path")
    parser.add_argument("--output", default="data/processed", help="Output directory")
    parser.add_argument("--master", default="local[*]", help="Spark master URL")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.is_file():
        parser.error(f"Input file not found: {input_path}. Run scripts/download_data.py first.")
    config = PipelineConfig(
        input_path=input_path,
        output_dir=Path(args.output).expanduser().resolve(),
        master=args.master,
    )
    run_pipeline(config)
