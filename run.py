import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def parse_args():
    parser = argparse.ArgumentParser(description="MLOps Signal Pipeline")
    parser.add_argument("--input", default="data.csv", help="Path to input CSV")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    parser.add_argument("--output", default="metrics.json", help="Path to output metrics JSON")
    parser.add_argument("--log-file", default="run.log", help="Path to log file")
    return parser.parse_args()


def setup_logging(log_file: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    required_keys = ["seed", "window", "version"]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required config key: {key}")
    return config


def load_data(input_path: str) -> pd.DataFrame:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {input_path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("Data file is empty")
    if "close" not in df.columns:
        raise ValueError(f"Missing required column: 'close'. Found columns: {list(df.columns)}")
    return df


def compute_signal(df: pd.DataFrame, window: int) -> pd.DataFrame:
    df = df.copy()
    df["rolling_mean"] = df["close"].rolling(window=window).mean()
    df["signal"] = (df["close"] > df["rolling_mean"]).astype(int)
    # rows where rolling_mean is NaN get signal=0
    df.loc[df["rolling_mean"].isna(), "signal"] = 0
    return df


def compute_metrics(df: pd.DataFrame, version: str) -> dict:
    total_rows = len(df)
    valid_rows = df["rolling_mean"].notna().sum()
    valid_signals = df.loc[df["rolling_mean"].notna(), "signal"]
    signal_rate = float(valid_signals.mean())
    return {
        "version": version,
        "status": "success",
        "total_rows": int(total_rows),
        "valid_rows": int(valid_rows),
        "signal_rate": round(signal_rate, 4)
    }


def write_metrics(metrics: dict, output_path: str):
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)


def main():
    args = parse_args()
    setup_logging(args.log_file)
    log = logging.getLogger(__name__)

    metrics = {}

    try:
        log.info("Starting MLOps signal pipeline")

        # load config
        log.info(f"Loading config from: {args.config}")
        config = load_config(args.config)
        seed = config["seed"]
        window = config["window"]
        version = config["version"]
        log.info(f"Config loaded — seed={seed}, window={window}, version={version}")

        # set random seed
        np.random.seed(seed)
        log.info(f"Random seed set to {seed}")

        # load data
        log.info(f"Loading data from: {args.input}")
        df = load_data(args.input)
        log.info(f"Data loaded — {len(df)} rows, columns: {list(df.columns)}")

        # compute signal
        log.info(f"Computing rolling mean with window={window}")
        df = compute_signal(df, window)
        log.info("Signal computation complete")

        # compute metrics
        metrics = compute_metrics(df, version)
        log.info(f"Metrics computed: {metrics}")

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        metrics = {
            "version": metrics.get("version", "unknown"),
            "status": "error",
            "error": str(e)
        }

    finally:
        write_metrics(metrics, args.output)
        log.info(f"Metrics written to: {args.output}")
        print("\n--- FINAL METRICS ---")
        print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()