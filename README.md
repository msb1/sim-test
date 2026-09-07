# sim-test

Live dashboard and Parquet logger for `iot-sim` Kafka telemetry.

```sh
uv run uvicorn app:server --host 0.0.0.0 --port 8050
```

It consumes `SENSOR_KAFKA_TOPIC` (defaulting to the legacy `KAFKA_TOPIC` value)
for simulator telemetry and `ANOMALY_KAFKA_TOPIC` for anomaly-detect messages.
Configure these with `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_GROUP_ID` as needed.
The sensor plot defaults to every line received from its Kafka topic; use the
multi-select sensor control to narrow it to specific sensor IDs. The anomaly
results plot currently always shows every received line and has a right-side
legend. The telemetry legend lists each sensor name, parameter, and normalization scale, for example
`sensor-1[active_energy_a_kwh] (x 10^2)`.

S3 logging is disabled by default. Enable it with `ENABLE_S3_LOGGING=true` and
provide `S3_ENDPOINT_URL`, `S3_BUCKET_NAME`, `S3_ACCESS_KEY`, and
`S3_SECRET_KEY`. Batches default to 1024 messages (`S3_WRITE_BATCH_SIZE`) and
write columnar Parquet files to `s3://<bucket>/sensors/` and
`s3://<bucket>/anomalies/`, respectively.

## Plot stored datasets

`plotter.py` reads a manually named `iot-sim` Parquet object from
`iotsim/processed/` and, optionally, its anomaly-detect results object from
`iotsim/results/`. It reads the data directly into Polars and serves a
Flexviz-backed Plotly dashboard. Flexviz performs viewport-aware min/max
downsampling, so large Parquet datasets do not get serialized in full to the
browser. The RustFS endpoint and credentials can be overridden with
`S3_ENDPOINT_URL`, `S3_BUCKET_NAME`, `S3_ACCESS_KEY`, and `S3_SECRET_KEY`.

```sh
uv run python plotter.py
```

The dashboard opens on port `8050` and contains linked sensor and anomaly
figures when `HAS_RESULTS` is enabled. Set `DOWNSAMPLING_INTERVAL` (for
example, `"1m"`) to aggregate before serving when an additional fixed time
bucket is useful.
