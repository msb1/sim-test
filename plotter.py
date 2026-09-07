"""Plot IoT sensor datasets and anomaly results with Polars and Flexviz."""

from __future__ import annotations

import os
import threading
from typing import Any

import numpy as np
import polars as pl
import s3fs
from flexviz import Dashboard

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://192.168.1.50:9000")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "iotsim")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "access")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "secret")
S3_PROCESSED_KEY = "processed/water_hammer-2026-08-30T00:00:00Z.parquet"
S3_RESULTS_KEY = "results/"
HAS_RESULTS = False
ANOMALY_THRESHOLD = 3.5
PLOT_COLORS = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7", "#56B4E9", "#F0E442"]
DOWNSAMPLING_INTERVAL: str | None = None  # e.g. "1m"
MIN_PEAK_HEIGHT = 300
FLEXVIZ_POINTS = 2_000
PLOT_HOST = os.getenv("PLOT_HOST", "127.0.0.1")
PLOT_PORT = int(os.getenv("PLOT_PORT", "8050"))


def rustfs() -> s3fs.S3FileSystem:
    """Create a path-style RustFS client using iot-sim defaults."""
    return s3fs.S3FileSystem(key=S3_ACCESS_KEY, secret=S3_SECRET_KEY,
                              client_kwargs={"endpoint_url": S3_ENDPOINT_URL})


def read_parquet(filesystem: s3fs.S3FileSystem, key: str) -> pl.DataFrame:
    """Read an S3/RustFS Parquet object directly into Polars."""
    with filesystem.open(f"{S3_BUCKET_NAME}/{key}", "rb") as source:
        return pl.read_parquet(source)


def _timestamp_expr(frame: pl.DataFrame) -> pl.Expr:
    timestamp = pl.col("timestamp")
    if frame.schema["timestamp"] == pl.String:
        return timestamp.str.to_datetime(strict=False)
    return timestamp.cast(pl.Datetime, strict=False)


def prepare_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize timestamps and numeric series before handing data to Flexviz."""
    if "timestamp" not in frame.columns:
        return frame
    expressions = [_timestamp_expr(frame).alias("timestamp")]
    expressions.extend(
        pl.col(column).cast(pl.Float64, strict=False).alias(column)
        for column in frame.columns if column != "timestamp"
    )
    return frame.select(expressions).drop_nulls(["timestamp"]).sort("timestamp")


def anomaly_mask(frame: pl.DataFrame) -> pl.Series:
    if "is_anomalous" not in frame.columns:
        return pl.Series("is_anomalous", [False] * frame.height, dtype=pl.Boolean)
    return frame.get_column("is_anomalous").cast(pl.Boolean, strict=False).fill_null(False)


def _numeric_columns(frame: pl.DataFrame) -> list[str]:
    return [
        name for name, dtype in frame.schema.items()
        if name != "timestamp"
        and dtype.is_numeric()
        and frame.get_column(name).drop_nulls().len() > 0
    ]


def _numeric_series(frame: pl.DataFrame, column: str) -> pl.DataFrame:
    """Return a sorted, null-free x/y pair for diagnostics and tests."""
    return frame.select(pl.col("timestamp"),
                        pl.col(column).cast(pl.Float64, strict=False).alias("value")) \
        .drop_nulls().sort("timestamp")


def _combined_frame(dfs: pl.DataFrame, dfr: pl.DataFrame | None) -> pl.DataFrame:
    """Combine both logical panels into one source shared by Dashboard figures."""
    sensor = prepare_frame(dfs)
    if dfr is None:
        return sensor
    anomaly = prepare_frame(dfr)
    anomaly = anomaly.rename({c: f"__anomaly__{c}" for c in anomaly.columns if c != "timestamp"})
    return pl.concat([
        sensor,
        anomaly.with_columns(pl.lit(ANOMALY_THRESHOLD).cast(pl.Float64)
                              .alias("__anomaly__threshold")),
    ], how="diagonal_relaxed")


def _add_lines(figure: Any, frame: pl.DataFrame, prefix: str = "") -> None:
    color_index = 0
    for column in _numeric_columns(frame):
        if prefix and not column.startswith(prefix):
            continue
        if not prefix and column.startswith("__anomaly__"):
            continue
        figure.add_line(x="timestamp", y=column,
                        name=column.removeprefix(prefix) or column,
                        color=PLOT_COLORS[color_index % len(PLOT_COLORS)],
                        n_points=FLEXVIZ_POINTS, downsample="minmax",
                        assume_sorted_x=True)
        color_index += 1


def plot_metrics_and_anomalies(
    dfs: pl.DataFrame, dfr: pl.DataFrame | None = None, *, show: bool = True,
    host: str = "127.0.0.1", port: int | str = 8050,
) -> Dashboard:
    """Build a linked Plotly/Flexviz dashboard from Polars frames."""
    source = _combined_frame(dfs, dfr)
    dashboard = Dashboard(source, cache=True)
    metrics = dashboard.add_figure(title="IoT Sensor Metrics", height=400)
    _add_lines(metrics, source)
    metrics.xlabel("Date").ylabel("Sensors").legend(True)
    if dfr is not None:
        anomalies = dashboard.add_figure(
            title=f"Anomaly Tracking (Threshold > {ANOMALY_THRESHOLD})", height=400)
        _add_lines(anomalies, source, prefix="__anomaly__")
        anomalies.xlabel("Date").ylabel("Anomaly Score").legend(True)
    if show:
        dashboard.show(renderer="plotly", host=host, port=port, cols=1,
                       live_brush="auto")
    return dashboard


def downsample(frame: pl.DataFrame) -> pl.DataFrame:
    """Aggregate a frame by time while retaining large within-bucket spikes."""
    if "timestamp" not in frame.columns:
        print("Dataframe is ill-formed; no timestamp column...")
        return frame
    if DOWNSAMPLING_INTERVAL is None:
        return prepare_frame(frame)
    frame = prepare_frame(frame)
    numeric = _numeric_columns(frame)
    if not numeric:
        return frame.select("timestamp")
    aggregations: list[pl.Expr] = []
    for column in numeric:
        values = frame.get_column(column).drop_nulls().to_numpy()
        peaks = ((values[1:-1] > MIN_PEAK_HEIGHT) &
                 (values[1:-1] > values[:-2]) & (values[1:-1] > values[2:])) \
            if len(values) > 2 else np.array([], dtype=bool)
        print(f"Column {column} has {int(peaks.sum())} greater than {MIN_PEAK_HEIGHT}")
        mean = pl.col(column).mean()
        minimum, maximum = pl.col(column).min(), pl.col(column).max()
        threshold = pl.col(column).std().fill_null(0) * 3
        spike = ((maximum - mean) > threshold) | ((mean - minimum) > threshold)
        chosen = pl.when(maximum - mean > mean - minimum).then(maximum).otherwise(minimum)
        aggregations.append(pl.when(spike).then(chosen).otherwise(mean).alias(column))
    return frame.group_by_dynamic("timestamp", every=DOWNSAMPLING_INTERVAL,
                                 closed="left").agg(aggregations).sort("timestamp")


def main() -> None:
    filesystem = rustfs()
    dfs = read_parquet(filesystem, S3_PROCESSED_KEY)
    print(f"Number of rows in IOT Sensor Dataframe: {dfs.height}")
    print(dfs.describe())
    print("--------------")
    dfr: pl.DataFrame | None = None
    if HAS_RESULTS:
        dfr = read_parquet(filesystem, S3_RESULTS_KEY)
        print(f"Number of rows in IOT Anomaly Dataframe: {dfr.height}")
        print(dfr.describe())
        print("--------------")
    if DOWNSAMPLING_INTERVAL is not None:
        dfs = downsample(dfs)
        if dfr is not None:
            dfr = downsample(dfr)
    plot_metrics_and_anomalies(dfs, dfr, host=PLOT_HOST, port=PLOT_PORT)
    print(f"Flexviz dashboard is running at http://{PLOT_HOST}:{PLOT_PORT}")
    print("Press Ctrl-C to stop the server.")
    # Flexviz owns the Uvicorn server in a daemon thread. Keep this CLI
    # process alive or the server will disappear as soon as Safari opens.
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("Stopping Flexviz dashboard.")


if __name__ == "__main__":
    main()
