# mlops-task

This project was built as a screening task for Anything.ai. The goal was to create a reproducible, dockerized ML pipeline that computes a simple trading signal from OHLCV price data.

## What it does

The pipeline reads BTC price data, computes a rolling mean over the close prices, and generates a binary signal — 1 if the current close is above the rolling mean, 0 if not. The key metric is `signal_rate`, which is the proportion of 1s across all valid rows.

All parameters (seed, window, version) are driven from `config.yaml` so nothing is hardcoded in the script.

## Project structure
```
mlops-task/
├── data.csv          # BTC OHLCV data (10,000 rows, 1-min candles)
├── config.yaml       # pipeline parameters
├── run.py            # main pipeline script
├── requirements.txt  # dependencies
├── Dockerfile        # containerized setup
├── metrics.json      # sample output (generated on run)
└── run.log           # sample logs (generated on run)
```
## Running locally

Make sure you have Python 3.9+ and install dependencies:

```bash
pip install -r requirements.txt
```

Then run:

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Running with Docker

Build the image:

```bash
docker build -t mlops-task .
```

Run it:

```bash
docker run --rm mlops-task
```

That's it. No external mounts, no extra setup.

## Output

A successful run prints this to stdout and writes it to `metrics.json`:

```json
{
  "version": "v1",
  "status": "success",
  "total_rows": 10000,
  "valid_rows": 9996,
  "signal_rate": 0.4991
}
```

If something goes wrong (missing file, bad CSV), the pipeline still writes `metrics.json` with `"status": "error"` and an error message — it never exits silently.

## Notes

- Results are fully deterministic. Running the pipeline multiple times always produces the same `signal_rate`.
- The first 4 rows (window - 1) produce no rolling mean and are excluded from the `signal_rate` calculation.
- All parameters live in `config.yaml`. Changing the window or seed there affects the entire pipeline — no need to touch the code.

— Husain Bohra
