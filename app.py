"""Live Kafka telemetry dashboard with optional batched S3/Parquet logging.

Run with ``uv run uvicorn app:server --host 0.0.0.0 --port 8050``.  All
configuration is environment based so credentials are never committed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import uuid
from collections import defaultdict, deque
from collections.abc import AsyncGenerator, AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import dash
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import uvicorn
from confluent_kafka import KafkaError
from confluent_kafka.aio import AIOConsumer
from dash import Input, Output, dcc, html
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

MAX_WINDOW_POINTS = int(os.getenv("MAX_WINDOW_POINTS", "1024"))
# KAFKA_TOPIC remains a fallback so existing deployments continue to work while
# moving to the two explicit source topics.
SENSOR_KAFKA_TOPIC = os.getenv("SENSOR_KAFKA_TOPIC", os.getenv("KAFKA_TOPIC", "streaming_telemetry"))
ANOMALY_KAFKA_TOPIC = os.getenv("ANOMALY_KAFKA_TOPIC", "streaming_anomalies")
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "telemetry-dash-async-group"),
    "auto.offset.reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest"),
    "enable.auto.commit": os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true").lower() == "true",
}
ENABLE_KAFKA = os.getenv("ENABLE_KAFKA", "true").lower() == "true"

ENABLE_S3_LOGGING = os.getenv("ENABLE_S3_LOGGING", "false").lower() == "true"
S3_WRITE_BATCH_SIZE = int(os.getenv("S3_WRITE_BATCH_SIZE", "1024"))
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "telemetry-database")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

PARQUET_SCHEMA = pa.schema(
    [
        ("entity_type", pa.string()),
        ("entity_id", pa.string()),
        ("sensor_id", pa.string()),
        ("sensor_type", pa.string()),
        ("timestamp_ms", pa.int64()),
        ("metric", pa.string()),
        ("value", pa.float64()),
    ]
)

ANOMALY_PARQUET_SCHEMA = pa.schema(
    [
        ("schema_version", pa.string()),
        ("timestamp_ms", pa.int64()),
        ("model_id", pa.string()),
        ("model_kind", pa.string()),
        ("algorithm", pa.string()),
        ("score_algorithm", pa.string()),
        ("inputs", pa.list_(pa.string())),
        ("anomalous_points", pa.int64()),
        ("window_sample_count", pa.int64()),
        ("is_anomalous", pa.bool_()),
        ("anomaly_score", pa.float64()),
        ("details", pa.string()),
    ]
)


def get_next_power_of_10(max_val: float) -> int:
    """Return the strictly greater power-of-ten normalization ceiling.

    ``90 -> 100``, ``100 -> 1000``, and values below one (or non-positive)
    use the stable baseline of one.
    """
    if max_val <= 0:
        return 1
    if max_val < 1:
        return 1
    exponent = math.log10(max_val)
    ceil_exponent = int(exponent) + 1 if exponent.is_integer() else math.ceil(exponent)
    return int(10**ceil_exponent)


@dataclass(frozen=True)
class TelemetryRecord:
    entity_type: str
    entity_id: str
    sensor_id: str
    sensor_type: str
    timestamp_ms: int
    metric: str
    value: float

    @property
    def trace_name(self) -> str:
        return f"[{self.entity_type}_{self.entity_id}] {self.metric} ({self.sensor_id})"

    def parquet_row(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type,
            "timestamp_ms": self.timestamp_ms,
            "metric": self.metric,
            "value": self.value,
        }


@dataclass(frozen=True)
class AnomalyRecord:
    schema_version: str
    timestamp_ms: int
    model_id: str
    model_kind: str
    algorithm: str
    score_algorithm: str
    inputs: list[str]
    anomalous_points: int
    window_sample_count: int
    is_anomalous: bool
    anomaly_score: float | None
    details: str

    @property
    def trace_name(self) -> str:
        return f"[{self.model_id}] {self.algorithm} ({self.score_algorithm})"

    @property
    def plot_value(self) -> float:
        # Rust anomaly messages may omit a numerical score.  Retain those
        # results in the live plot as a 0/1 anomaly flag rather than dropping
        # them from the stream.
        return self.anomaly_score if self.anomaly_score is not None else float(self.is_anomalous)

    def parquet_row(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "timestamp_ms": self.timestamp_ms,
            "model_id": self.model_id,
            "model_kind": self.model_kind,
            "algorithm": self.algorithm,
            "score_algorithm": self.score_algorithm,
            "inputs": self.inputs,
            "anomalous_points": self.anomalous_points,
            "window_sample_count": self.window_sample_count,
            "is_anomalous": self.is_anomalous,
            "anomaly_score": self.anomaly_score,
            "details": self.details,
        }


def _timestamp_ms(value: Any) -> int:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)  # noqa: FURB162
    raise ValueError("message has no usable timestamp_ms or timestamp")


def parse_records(payload: dict[str, Any]) -> list[TelemetryRecord]:
    """Parse the simulator's single metric messages and common metrics batches."""
    base = payload.get("data", payload)
    if not isinstance(base, dict):
        raise ValueError("message payload must be a JSON object")  # noqa: TRY004
    timestamp = _timestamp_ms(base.get("timestamp_ms", base.get("timestamp", base.get("ts"))))
    common = {
        "entity_type": str(base.get("entity_type", "unknown")),
        "entity_id": str(base.get("entity_id", "unknown")),
        "sensor_id": str(base.get("sensor_id", "unknown")),
        "sensor_type": str(base.get("sensor_type", "unknown")),
        "timestamp_ms": timestamp,
    }
    metrics = base.get("metrics")
    if metrics is None:
        metrics = [{"metric": base.get("metric"), "value": base.get("value")}]
    elif isinstance(metrics, dict):
        metrics = [{"metric": name, "value": value} for name, value in metrics.items()]
    if not isinstance(metrics, list):
        raise ValueError("metrics must be an object or list")  # noqa: TRY004

    records: list[TelemetryRecord] = []
    for item in metrics:
        if not isinstance(item, dict) or item.get("metric") is None:
            raise ValueError("every metric needs a metric name")
        value = float(item.get("value"))
        if not math.isfinite(value):
            raise ValueError("metric value must be finite")
        records.append(TelemetryRecord(metric=str(item["metric"]), value=value, **common))
    return records


def parse_anomaly_record(payload: dict[str, Any]) -> AnomalyRecord:
    """Parse the JSON representation of anomaly-detect's AnomalyMessage."""
    base = payload.get("data", payload)
    if not isinstance(base, dict):
        raise ValueError("anomaly payload must be a JSON object")  # noqa: TRY004
    inputs = base.get("inputs", [])
    if not isinstance(inputs, list):
        raise ValueError("anomaly inputs must be a list")  # noqa: TRY004
    score = base.get("anomaly_score")
    if score is not None:
        score = float(score)
        if not math.isfinite(score):
            raise ValueError("anomaly_score must be finite")  # noqa: TRY004
    details = base.get("details", {})
    return AnomalyRecord(
        schema_version=str(base.get("schema_version", "unknown")),
        timestamp_ms=_timestamp_ms(base.get("timestamp_ms", base.get("timestamp"))),
        model_id=str(base.get("model_id", "unknown")),
        model_kind=json.dumps(base.get("model_kind", "unknown"), sort_keys=True),
        algorithm=str(base.get("algorithm", "unknown")),
        score_algorithm=str(base.get("score_algorithm", "unknown")),
        inputs=[str(value) for value in inputs],
        anomalous_points=int(base.get("anomalous_points", 0)),
        window_sample_count=int(base.get("window_sample_count", 0)),
        is_anomalous=bool(base.get("is_anomalous", False)),
        anomaly_score=score,
        details=json.dumps(details, sort_keys=True, default=str),
    )


@dataclass
class TelemetryStore:
    traces: dict[str, deque[tuple[datetime, float]]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=MAX_WINDOW_POINTS))
    )
    entity_types: set[str] = field(default_factory=set)
    sensor_ids: set[str] = field(default_factory=set)
    trace_dimensions: dict[str, tuple[str, str, str]] = field(default_factory=dict)

    def add(self, records: Iterable[TelemetryRecord]) -> None:
        for record in records:
            self.traces[record.trace_name].append(
                (datetime.fromtimestamp(record.timestamp_ms / 1000, tz=UTC), record.value)
            )
            self.entity_types.add(record.entity_type)
            self.sensor_ids.add(record.sensor_id)
            self.trace_dimensions[record.trace_name] = (
                record.entity_type,
                record.sensor_id,
                record.metric,
            )

    def clear(self) -> None:
        """Return the live telemetry dashboard state to its initial state."""
        self.traces.clear()
        self.entity_types.clear()
        self.sensor_ids.clear()
        self.trace_dimensions.clear()


@dataclass
class AnomalyStore:
    traces: dict[str, deque[tuple[datetime, float]]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=MAX_WINDOW_POINTS))
    )
    model_ids: set[str] = field(default_factory=set)
    algorithms: set[str] = field(default_factory=set)
    trace_dimensions: dict[str, tuple[str, str]] = field(default_factory=dict)

    def add(self, record: AnomalyRecord) -> None:
        self.traces[record.trace_name].append(
            (datetime.fromtimestamp(record.timestamp_ms / 1000, tz=UTC), record.plot_value)
        )
        self.model_ids.add(record.model_id)
        self.algorithms.add(record.algorithm)
        self.trace_dimensions[record.trace_name] = (record.model_id, record.algorithm)

    def clear(self) -> None:
        """Return the live anomaly dashboard state to its initial state."""
        self.traces.clear()
        self.model_ids.clear()
        self.algorithms.clear()
        self.trace_dimensions.clear()


class S3ParquetWriter:
    """Detach full batches quickly; serial writes preserve chronological files."""

    def __init__(self, schema: pa.Schema, folder: str, filename_prefix: str) -> None:
        self.pending: list[dict[str, Any]] = []
        self.schema = schema
        self.folder = folder
        self.filename_prefix = filename_prefix
        self.lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()
        self.tasks: set[asyncio.Task[None]] = set()

    async def stage(self, records: Iterable[TelemetryRecord | AnomalyRecord]) -> None:
        if not ENABLE_S3_LOGGING:
            return
        batches: list[list[dict[str, Any]]] = []
        async with self.lock:
            self.pending.extend(record.parquet_row() for record in records)
            while len(self.pending) >= S3_WRITE_BATCH_SIZE:
                batches.append(self.pending[:S3_WRITE_BATCH_SIZE])
                del self.pending[:S3_WRITE_BATCH_SIZE]
        for batch in batches:
            self._schedule(batch)

    def _schedule(self, batch: list[dict[str, Any]]) -> None:
        task = asyncio.create_task(self._write(batch), name="write-parquet-batch")
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def flush_remaining(self) -> None:
        async with self.lock:
            batch, self.pending = self.pending, []
        if batch:
            self._schedule(batch)
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

    async def _write(self, batch: list[dict[str, Any]]) -> None:
        async with self.write_lock:
            try:
                await asyncio.to_thread(self._write_sync, batch)
            except Exception:
                logger.exception("Parquet batch write failed; %d records were not persisted", len(batch))

    def _write_sync(self, batch: list[dict[str, Any]]) -> None:
        if not S3_ENDPOINT_URL or not S3_ACCESS_KEY or not S3_SECRET_KEY:
            raise RuntimeError("S3_ENDPOINT_URL, S3_ACCESS_KEY, and S3_SECRET_KEY are required")
        filesystem = s3fs.S3FileSystem(
            key=S3_ACCESS_KEY,
            secret=S3_SECRET_KEY,
            client_kwargs={"endpoint_url": S3_ENDPOINT_URL},
        )
        table = pa.Table.from_pylist(batch, schema=self.schema)
        first_timestamp = batch[0]["timestamp_ms"]
        filename = f"{self.filename_prefix}_{first_timestamp}_{uuid.uuid4().hex}.parquet"
        key = f"{self.folder}/{filename}"
        with filesystem.open(f"{S3_BUCKET_NAME}/{key}", "wb") as stream:
            pq.write_table(table, stream, compression="zstd")
        logger.info("Wrote %d records to s3://%s/%s", len(batch), S3_BUCKET_NAME, key)


class TelemetrySocketHub:
    def __init__(self) -> None:
        self.listeners: set[asyncio.Queue[str]] = set()
        self.version = 0

    def publish(self) -> None:
        self.version += 1
        message = json.dumps({"version": self.version})
        for queue in list(self.listeners):
            if queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            queue.put_nowait(message)

    async def listen(self) -> AsyncIterator[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
        self.listeners.add(queue)
        try:
            yield json.dumps({"version": self.version})
            while True:
                yield await queue.get()
        finally:
            self.listeners.discard(queue)


store = TelemetryStore()
anomaly_store = AnomalyStore()
sensor_writer = S3ParquetWriter(PARQUET_SCHEMA, "sensors", "telemetry")
anomaly_writer = S3ParquetWriter(ANOMALY_PARQUET_SCHEMA, "anomalies", "anomaly")
socket_hub = TelemetrySocketHub()

if MAX_WINDOW_POINTS < 1:
    raise ValueError("MAX_WINDOW_POINTS must be at least 1")
if S3_WRITE_BATCH_SIZE < 1:
    raise ValueError("S3_WRITE_BATCH_SIZE must be at least 1")


async def consume_kafka_stream() -> None:
    """Consume batches asynchronously without blocking the FastAPI event loop."""
    consumer = AIOConsumer(KAFKA_CONFIG)
    topics = [SENSOR_KAFKA_TOPIC, ANOMALY_KAFKA_TOPIC]
    await consumer.subscribe(topics)
    logger.info("Kafka consumer subscribed to %s", ", ".join(topics))
    try:
        while True:
            messages = await consumer.consume(num_messages=100, timeout=1.0)
            for message in messages or []:
                if message is None:
                    continue
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        logger.warning("Kafka consumer error: %s", message.error())
                    continue
                try:
                    logger.debug(f"Received Message: {message.value()}")
                    payload = json.loads(message.value())
                    if message.topic() == SENSOR_KAFKA_TOPIC:
                        records = parse_records(payload)
                        store.add(records)
                        await sensor_writer.stage(records)
                    elif message.topic() == ANOMALY_KAFKA_TOPIC:
                        record = parse_anomaly_record(payload)
                        anomaly_store.add(record)
                        await anomaly_writer.stage([record])
                    else:
                        continue
                    socket_hub.publish()
                except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
                    logger.exception("Skipping invalid Kafka message")
    except asyncio.CancelledError:  # noqa: TRY203
        raise
    finally:
        await consumer.close()
        logger.info("Kafka consumer closed")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
    consumer_task = asyncio.create_task(consume_kafka_stream()) if ENABLE_KAFKA else None
    try:
        yield
    finally:
        if consumer_task:
            consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await consumer_task
        await sensor_writer.flush_remaining()
        await anomaly_writer.flush_remaining()


server = FastAPI(title="IoT Simulator Telemetry", lifespan=lifespan)


@server.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {
        "status": "ok",
        "kafka_enabled": ENABLE_KAFKA,
        "sensor_traces": len(store.traces),
        "anomaly_traces": len(anomaly_store.traces),
    }


@server.websocket("/telemetry/ws")
async def telemetry_websocket(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        async for message in socket_hub.listen():
            await websocket.send_text(message)
    except WebSocketDisconnect:
        pass


app = dash.Dash(
    __name__,
    server=server,
    backend="fastapi",
    use_async=True,
    websocket_callbacks=True,
    external_stylesheets=[dbc.themes.SLATE],
    title="IoT Telemetry",
)
app.layout = dbc.Container(
    [
        html.H2("IoT Simulator Telemetry Dashboard", className="my-4"),
        dbc.Row(
            [
                dbc.Col(
                    dbc.Row(
                        [
                            dbc.Col(html.Label("Sensors", htmlFor="sensor-id-filter"), width="auto"),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="sensor-id-filter",
                                    options=[],
                                    multi=True,
                                    placeholder="All discovered sensors",
                                )
                            ),
                        ],
                        className="align-items-center",
                    ),
                    md=8,
                ),
                dbc.Col(
                    dbc.Button("Clear", id="clear-dashboard", color="secondary", outline=True),
                    width="auto",
                    className="ms-auto",
                ),
            ],
            className="align-items-center mb-3",
        ),
        dcc.Graph(id="unified-streaming-plot", style={"height": "65vh", "marginBottom": "32px"}),
        html.Hr(),
        html.H3("Streaming Anomaly Detection", className="mt-4"),
        dcc.Graph(id="anomaly-streaming-plot", style={"height": "65vh"}),
        dcc.Store(id="stream-version", data={"version": 0}),
        # Existing browser tabs can retain callback metadata across a rolling
        # server restart. These hidden compatibility targets let those tabs
        # finish their in-flight requests; they are not filter controls and do
        # not affect which Kafka data is plotted.
        html.Div(
            [
                dcc.Store(id="sensor-filter-state"),
                dcc.Dropdown(id="entity-type-filter", multi=True),
                dcc.Dropdown(id="model-id-filter", multi=True),
                dcc.Dropdown(id="algorithm-filter", multi=True),
                dcc.Checklist(id="anomaly-trace-filter"),
            ],
            style={"display": "none"},
        ),
    ],
    fluid=True,
    className="pb-4",
)


@app.callback(
    Output("sensor-id-filter", "value"),
    Input("clear-dashboard", "n_clicks"),
    prevent_initial_call=True,
)
async def clear_dashboard(_: int | None) -> None:
    """Clear the live view and ask every connected browser to refresh it."""
    store.clear()
    anomaly_store.clear()
    socket_hub.publish()
    return None


@app.callback(
    Output("unified-streaming-plot", "figure"),
    Output("entity-type-filter", "options"),
    Output("sensor-id-filter", "options"),
    Input("stream-version", "data"),
    Input("sensor-id-filter", "value"),
)
async def refresh_dashboard_plot(
    _: dict[str, Any] | None,
    selected_sensor_ids: list[str] | None,
) -> tuple[Any, list[dict[str, str]], list[dict[str, str]]]:
    """Render telemetry traces, optionally limited to selected sensor IDs."""
    figure = go.Figure()
    selected_sensors = set(selected_sensor_ids or [])
    for name, points in list(store.traces.items()):
        if not points:
            continue
        _, sensor_id, metric = store.trace_dimensions[name]
        if selected_sensors and sensor_id not in selected_sensors:
            continue
        timestamps, values = zip(*points)
        factor = get_next_power_of_10(max(values))
        exponent = int(math.log10(factor))
        figure.add_trace(
            go.Scatter(
                x=timestamps,
                y=[value / factor for value in values],
                mode="lines",
                name=f"{sensor_id}[{metric}] (x 10^{exponent})",
            )
        )

    if figure.data:
        title = "Live Graph Stream"
    elif store.traces:
        title = "No telemetry for the selected sensors"
    else:
        title = "Awaiting real-time telemetry inputs..."
    figure.update_layout(
        title=f"{title} ({MAX_WINDOW_POINTS}-point window) | S3 logging: {'Active' if ENABLE_S3_LOGGING else 'Off'}",
        xaxis_title="Telemetry event time",
        yaxis_title="Normalized scale",
        template="plotly_white",
        showlegend=True,
        legend={"x": 1.02, "xanchor": "left", "y": 1, "yanchor": "top"},
        margin={"l": 50, "r": 260, "t": 80, "b": 50},
    )
    options = lambda items: [{"label": item, "value": item} for item in sorted(items)]
    return figure, options(store.entity_types), options(store.sensor_ids)


@app.callback(
    Output("anomaly-streaming-plot", "figure"),
    # Preserve the former multi-output callback ID for open browser tabs.
    Output("model-id-filter", "options"),
    Output("algorithm-filter", "options"),
    Output("anomaly-trace-filter", "options"),
    Output("model-id-filter", "value"),
    Output("algorithm-filter", "value"),
    Output("anomaly-trace-filter", "value"),
    Input("stream-version", "data"),
    Input("model-id-filter", "value"),
    Input("algorithm-filter", "value"),
    Input("anomaly-trace-filter", "value"),
)
async def refresh_anomaly_plot(
    _: dict[str, int] | None,
    model_ids: list[str] | None,
    algorithms: list[str] | None,
    selected_traces: list[str] | None,
) -> tuple[Any, ...]:
    """Render every anomaly trace received from the anomaly Kafka topic."""
    figure = go.Figure()
    for name, points in list(anomaly_store.traces.items()):
        if not points:
            continue
        timestamps, values = zip(*points)
        figure.add_trace(go.Scatter(x=timestamps, y=values, mode="lines+markers", name=name))

    title = "Awaiting anomaly-detection inputs..." if not figure.data else "Live anomaly-detection stream"
    figure.update_layout(
        title=f"{title} ({MAX_WINDOW_POINTS}-point window) | S3 logging: {'Active' if ENABLE_S3_LOGGING else 'Off'}",
        xaxis_title="Anomaly event time",
        yaxis_title="Anomaly score (or 0/1 anomaly flag when no score is supplied)",
        template="plotly_white",
        showlegend=True,
        legend={"x": 1.02, "xanchor": "left", "y": 1, "yanchor": "top"},
        margin={"l": 50, "r": 260, "t": 80, "b": 50},
    )
    options = lambda items: [{"label": item, "value": item} for item in sorted(items)]
    return (
        figure,
        options(anomaly_store.model_ids),
        options(anomaly_store.algorithms),
        options(anomaly_store.traces),
        model_ids,
        algorithms,
        selected_traces,
    )


if __name__ == "__main__":
    uvicorn.run("app:server", host="0.0.0.0", port=8050, reload=True)
