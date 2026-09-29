# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""A real df_engine running the shipped series_parquet configurations, fed
over OTLP/gRPC and read back with DuckDB, on local files and on MinIO."""
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid

import boto3
import duckdb
import grpc
import yaml
from botocore.config import Config as BotoConfig
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2 as logs_pb
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2_grpc as logs_rpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2 as metrics_pb
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2_grpc as metrics_rpc,
)
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2 as trace_pb
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc as trace_rpc

WORKSPACE = Path(__file__).resolve().parents[4]
MINIO_IMAGE = os.environ.get(
    "SERIES_MINIO_IMAGE", "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
)
DOCKER_TIMEOUT_S = 120
ALLOY_IMAGE = os.environ.get("SERIES_ALLOY_IMAGE", "grafana/alloy:v1.19.2")
# The directory holding the reference Alloy configurations; a test of this
# check points it at a copy with a broken file.
ALLOY_CONFIGS = Path(
    os.environ.get("SERIES_ALLOY_CONFIGS", str(WORKSPACE / "configs"))
)

# The deployment example's check of its alert rules and dashboard.
DEPLOY_CHECK = WORKSPACE / "deploy/series-parquet/check.py"
# Metrics those select that no engine file carries: Alloy's own counters,
# which appear only after their first increment.
ALLOY_UNEXPOSED = [
    "loki_process_truncated_fields_total",
    "otelcol_exporter_enqueue_failed_log_records_total",
    "otelcol_exporter_send_failed_log_records_total",
]

# One metrics request carries a gauge point and two histogram points, all
# stored in the `signal=metrics/dataset=values` dataset.
POINTS_PER_METRIC_REQUEST = 3


def free_port():
    """A loopback port that was free when probed."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def log_request(body):
    """A single-record OTLP logs request whose body is `body`."""
    req = logs_pb.ExportLogsServiceRequest()
    resource = req.resource_logs.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    service = resource.resource.attributes.add(key="service.name")
    service.value.string_value = "series-e2e-service"
    scope = resource.scope_logs.add()
    scope.scope.name = "series-e2e"
    record = scope.log_records.add(time_unix_nano=1789960500000000000)
    record.body.string_value = body
    record.attributes.add(key="logger.name").value.string_value = "series.logger"
    return req


def metric_request(request_id):
    """An OTLP metrics request with one gauge and two histogram points."""
    req = metrics_pb.ExportMetricsServiceRequest()
    resource = req.resource_metrics.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    scope = resource.scope_metrics.add()
    scope.scope.name = "series-e2e"
    gauge = scope.metrics.add(name="integer", unit="1")
    point = gauge.gauge.data_points.add(time_unix_nano=1789960500000000000, as_int=7)
    point.attributes.add(key="request.id").value.string_value = request_id
    for name, buckets in (("histogram", [1, 2]), ("histogram_no_buckets", [])):
        hist = scope.metrics.add(name=name, unit="s")
        hist.histogram.aggregation_temporality = 2
        point = hist.histogram.data_points.add(
            time_unix_nano=1789960500000000000, count=3, sum=4.0
        )
        point.bucket_counts.extend(buckets)
        if buckets:
            point.explicit_bounds.append(1.0)
        point.attributes.add(key="request.id").value.string_value = request_id
    return req


def shipped_config(
    name, *, grpc_port, storage, buffer_path=None, window="1s", grpc_settings=None
):
    """A shipped configuration with its port, storage and paths replaced.

    The window is shortened, to one second by default, so a test waits for
    seconds, not for the production interval; `grpc_settings` overrides receiver gRPC
    settings a test provokes; every other setting is the shipped one.
    """
    config = yaml.safe_load((WORKSPACE / "configs" / name).read_text())
    nodes = config["groups"]["default"]["pipelines"]["main"]["nodes"]
    grpc_config = nodes["receiver"]["config"]["protocols"]["grpc"]
    grpc_config["listening_addr"] = f"127.0.0.1:{grpc_port}"
    grpc_config.update(grpc_settings or {})
    exporter = nodes["exporter"]["config"]
    exporter["storage"] = storage
    exporter["window"]["interval"] = window
    if "buffer" in nodes:
        nodes["buffer"]["config"]["path"] = str(buffer_path)
    return config


class Engine:
    """A df_engine process running one shipped configuration."""

    def __init__(
        self, directory, name, storage, buffer_path=None, window="1s", grpc_settings=None
    ):
        self.root = Path(directory)
        self.grpc_port = free_port()
        self.admin_port = free_port()
        config = shipped_config(
            name,
            grpc_port=self.grpc_port,
            storage=storage,
            buffer_path=buffer_path,
            window=window,
            grpc_settings=grpc_settings,
        )
        self.path = self.root / f"pipeline-{uuid.uuid4().hex}.yaml"
        self.path.write_text(yaml.safe_dump(config))
        binary = Path(
            os.environ.get("DF_ENGINE", WORKSPACE / "target/debug/df_engine")
        )
        if not binary.is_file():
            raise AssertionError(f"build the feature-enabled engine first: {binary}")
        self.log = (self.root / f"engine-{self.admin_port}.log").open("w+")
        # Without MALLOC_CONF, so the banner reports the compiled-in
        # allocator configuration.
        env = {k: v for k, v in os.environ.items() if k != "MALLOC_CONF"}
        self.process = subprocess.Popen(
            [
                str(binary),
                "--config",
                str(self.path),
                "--http-admin-bind",
                f"127.0.0.1:{self.admin_port}",
            ],
            stdout=self.log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.grpc_port}")
        try:
            self.wait_ready(30)
            grpc.channel_ready_future(self.channel).result(timeout=30)
        except Exception:
            print(f"engine failed to start, log follows:\n{self.engine_log()}")
            self.close()
            raise
        self.logs = logs_rpc.LogsServiceStub(self.channel)
        self.metrics = metrics_rpc.MetricsServiceStub(self.channel)
        self.traces = trace_rpc.TraceServiceStub(self.channel)

    def wait_ready(self, seconds):
        """Poll the admin API's readiness probe until it answers 200."""
        url = f"http://127.0.0.1:{self.admin_port}/api/v1/readyz"
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError("the engine exited before it became ready")
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if response.status == 200:
                        return
                    last = f"status {response.status}"
            except Exception as error:
                last = error
            time.sleep(0.05)
        raise AssertionError(f"the engine never became ready: {last}")

    def gauge(self, name, node_id):
        """The current value of gauge `name` of node `node_id`, or None."""
        url = (
            f"http://127.0.0.1:{self.admin_port}/api/v1/telemetry/metrics"
            "?format=prometheus&reset=false"
        )
        with urllib.request.urlopen(url, timeout=5) as response:
            text = response.read().decode()
        for line in text.splitlines():
            if line.startswith(name + "{") and f'otel_scope_node_id="{node_id}"' in line:
                return float(line.rsplit(" ", 2)[1])
        return None

    def counter(self, name, node_id, **labels):
        """The sum of counter `name` of node `node_id` over the series whose
        labels include `labels`."""
        url = (
            f"http://127.0.0.1:{self.admin_port}/api/v1/telemetry/metrics"
            "?format=prometheus&reset=false"
        )
        with urllib.request.urlopen(url, timeout=5) as response:
            text = response.read().decode()
        wanted = [f'otel_scope_node_id="{node_id}"']
        wanted += [f'{key}="{value}"' for key, value in labels.items()]
        return sum(
            float(line.rsplit(" ", 2)[1])
            for line in text.splitlines()
            if line.startswith(name + "{") and all(label in line for label in wanted)
        )

    def scrape(self):
        """The Prometheus text of the admin API's metrics endpoint."""
        url = f"http://127.0.0.1:{self.admin_port}/api/v1/metrics"
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.read().decode()

    def schema(self):
        """Every registered metric with every label value, as JSON text."""
        url = (
            f"http://127.0.0.1:{self.admin_port}/api/v1/telemetry/metrics"
            "?format=json&keep_all_zeroes=true"
        )
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.read().decode()

    def engine_log(self):
        """Everything the engine has written to its log."""
        self.log.flush()
        return Path(self.log.name).read_text(errors="replace")

    def shutdown(self, seconds=60):
        """Stop every pipeline gracefully through the admin API and wait."""
        url = (
            f"http://127.0.0.1:{self.admin_port}/api/v1/groups/shutdown"
            f"?wait=true&timeout_secs={seconds}"
        )
        request = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(request, timeout=seconds + 5) as response:
            if response.status != 200:
                raise AssertionError(response.read().decode())

    def kill(self):
        """SIGKILL the engine, as a crash or an OOM kill would, and wait."""
        self.process.kill()
        self.process.wait(timeout=30)

    def close(self):
        """Terminate the engine process and release its resources."""
        self.channel.close()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if exc[0] is not None:
            print(f"engine log follows:\n{self.engine_log()}")
        self.close()


def unavailable(reason):
    """Skip, or fail when `SERIES_REQUIRE_DOCKER=1` demands the container."""
    if os.environ.get("SERIES_REQUIRE_DOCKER") == "1":
        raise AssertionError(reason)
    raise unittest.SkipTest(reason)


class MinioStore:
    """A MinIO container serving S3 on a loopback port."""

    def __init__(self):
        self.name = "series-e2e-" + uuid.uuid4().hex
        self.bucket = "series-test"
        self.key = "series-test-access"
        self.secret = "series-test-secret-12345"

    def __enter__(self):
        if not shutil.which("docker"):
            unavailable("Docker CLI absent")
        inspect = subprocess.run(
            ["docker", "image", "inspect", MINIO_IMAGE],
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )
        if inspect.returncode:
            unavailable(f"the MinIO image is absent: {MINIO_IMAGE}")
        self.port = free_port()
        subprocess.run(
            [
                "docker", "run", "--pull=never", "--detach", "--name", self.name,
                "--publish", f"127.0.0.1:{self.port}:9000",
                "-e", f"MINIO_ROOT_USER={self.key}",
                "-e", f"MINIO_ROOT_PASSWORD={self.secret}",
                MINIO_IMAGE, "server", "/data", "--address", ":9000",
            ],
            check=True,
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )
        try:
            self.endpoint = f"http://127.0.0.1:{self.port}"
            self.client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                region_name="us-east-1",
                aws_access_key_id=self.key,
                aws_secret_access_key=self.secret,
                config=BotoConfig(
                    connect_timeout=5,
                    read_timeout=10,
                    retries={"max_attempts": 0},
                    s3={"addressing_style": "path"},
                ),
            )
            self.wait_ready()
            self.client.create_bucket(Bucket=self.bucket)
        except BaseException:
            self.remove()
            raise
        self.storage = {
            "s3": {
                "base_uri": f"s3://{self.bucket}/otel",
                "region": "us-east-1",
                "endpoint": self.endpoint,
                "allow_http": True,
                "virtual_hosted_style_request": False,
                "auth": {
                    "type": "static_credentials",
                    "access_key_id": self.key,
                    "secret_access_key": self.secret,
                },
            }
        }
        return self

    def wait_ready(self):
        """Block until MinIO answers an S3 request."""
        deadline = time.monotonic() + 60
        last = None
        while time.monotonic() < deadline:
            try:
                self.client.list_buckets()
                return
            except Exception as error:
                last = error
                time.sleep(0.2)
        raise AssertionError(f"MinIO never became S3-ready: {last}")

    def keys(self):
        """The keys of every Parquet object in the bucket."""
        paginator = self.client.get_paginator("list_objects_v2")
        return {
            item["Key"]
            for page in paginator.paginate(Bucket=self.bucket, Prefix="otel/")
            for item in page.get("Contents", [])
            if item["Key"].endswith(".parquet")
        }

    def download(self, directory):
        """Copy every Parquet object into `directory`, keeping its key."""
        directory = Path(directory)
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix="otel/"):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith(".parquet"):
                    target = directory / key.removeprefix("otel/")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    self.client.download_file(self.bucket, key, str(target))

    def pause(self):
        """Freeze MinIO: connections open, but no request is answered."""
        subprocess.run(
            ["docker", "pause", self.name],
            check=True,
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )

    def unpause(self):
        """Let a paused MinIO answer again."""
        subprocess.run(
            ["docker", "unpause", self.name],
            check=True,
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )

    def remove(self):
        """Remove the container, whether or not it started."""
        subprocess.run(
            ["docker", "rm", "--force", "--volumes", self.name],
            check=False,
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )

    def __exit__(self, *exc):
        self.remove()


def trace_request():
    """A traces request with one span, a signal the exporter refuses."""
    req = trace_pb.ExportTraceServiceRequest()
    resource = req.resource_spans.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    span = resource.scope_spans.add().spans.add(name="families")
    span.trace_id = bytes(range(16))
    span.span_id = bytes(range(8))
    return req


def altered_metric_request():
    """A metrics request the exporter stores altered: a summary point it
    drops, a gauge point whose exemplar it drops and whose timestamp does
    not fit i64 nanoseconds."""
    req = metrics_pb.ExportMetricsServiceRequest()
    resource = req.resource_metrics.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    scope = resource.scope_metrics.add()
    summary = scope.metrics.add(name="families_summary", unit="s")
    summary.summary.data_points.add(time_unix_nano=1789960500000000000, count=1, sum=1.0)
    gauge = scope.metrics.add(name="families_gauge", unit="1")
    point = gauge.gauge.data_points.add(time_unix_nano=(1 << 64) - 1, as_int=1)
    point.exemplars.add(time_unix_nano=1789960500000000000, as_int=1)
    return req


def invalid_utf8_log_request():
    """The bytes of a logs request whose body holds invalid UTF-8, which
    protobuf libraries refuse to build."""
    body = log_request("families-invalid-XXXX").SerializeToString()
    assert body.count(b"XXXX") == 1
    return body.replace(b"XXXX", b"\xff\xfe\xfd\xfc")


def export_raw_logs(engine, body):
    """Send already-serialized logs request bytes and wait for the answer."""
    export = engine.channel.unary_unary(
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        request_serializer=lambda raw: raw,
        response_deserializer=logs_pb.ExportLogsServiceResponse.FromString,
    )
    return export(body, timeout=30)


def has_series(scrape, name, scope, positive=False, **labels):
    """Whether `scrape` has a `name` series of set `scope` carrying `labels`,
    with a value above zero when `positive`."""
    wanted = [f'otel_scope_name="{scope}"'] + [f'{k}="{v}"' for k, v in labels.items()]
    for line in scrape.splitlines():
        if line.startswith(name + "{") and all(w in line for w in wanted):
            if not positive or float(line.rsplit(" ", 2)[1]) > 0:
                return True
    return False


def parquet_files(root, signal, dataset):
    """The Parquet files of one dataset under a lake root, as strings."""
    return [
        str(path)
        for path in Path(root).glob(f"v=1/signal={signal}/dataset={dataset}/**/*.parquet")
    ]


def wait_for(condition, seconds, what):
    """Poll `condition` until it holds, failing after `seconds`."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


METRIC_NAMES = ("integer", "histogram", "histogram_no_buckets")


def missing_from_lake(root, bodies, metric_ids):
    """What of `bodies` and of the points of `metric_ids` a lake root does
    not hold at least once."""
    logs_values = parquet_files(root, "logs", "values")
    metrics_values = parquet_files(root, "metrics", "values")
    metrics_series = parquet_files(root, "metrics", "series")
    stored_bodies, stored_points = set(), set()
    with duckdb.connect() as db:
        if logs_values:
            stored_bodies = {
                row[0]
                for row in db.execute(
                    "SELECT DISTINCT body FROM read_parquet(?)", [logs_values]
                ).fetchall()
            }
        if metrics_values and metrics_series:
            stored_points = set(
                db.execute(
                    "SELECT DISTINCT s.attrs['request.id'], v.metric_name "
                    "FROM read_parquet(?) v "
                    "JOIN (SELECT DISTINCT series_id, attrs FROM read_parquet(?)) s "
                    "USING (series_id)",
                    [metrics_values, metrics_series],
                ).fetchall()
            )
    wanted_points = {(m, name) for m in metric_ids for name in METRIC_NAMES}
    return sorted(set(bodies) - stored_bodies) + sorted(wanted_points - stored_points)


def wait_stored_at_least_once(store, directory, bodies, metric_ids, seconds):
    """Poll the bucket until it holds every body and metric point at least
    once, failing with what is missing after `seconds`."""
    deadline = time.monotonic() + seconds
    attempt = 0
    while True:
        attempt += 1
        root = Path(directory) / f"poll-{attempt}"
        store.download(root)
        missing = missing_from_lake(root, bodies, metric_ids)
        shutil.rmtree(root, ignore_errors=True)
        if not missing:
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"not stored after the restart: {missing}")
        time.sleep(1)


def wait_early_in_window(interval_s, remaining_s=10):
    """Sleep until the current window, aligned to the epoch, has at least
    `remaining_s` left, so requests sent now stay in its open block."""
    phase = time.time() % interval_s
    if phase > interval_s - remaining_s:
        time.sleep(interval_s - phase + 0.5)


def export_all(engine, bodies, metric_ids):
    """Send every logs and metrics request concurrently and wait for each OK."""
    calls = [engine.logs.Export.future(log_request(b), timeout=30) for b in bodies]
    calls += [
        engine.metrics.Export.future(metric_request(m), timeout=30) for m in metric_ids
    ]
    for call in calls:
        call.result()


class LakeAssertions:
    """Checks on a lake root read back with DuckDB."""

    def assert_lake(self, root, bodies, metric_ids):
        """Every log body and metric point is stored exactly once and joins
        to exactly one series row."""
        logs_values = parquet_files(root, "logs", "values")
        logs_series = parquet_files(root, "logs", "series")
        metrics_values = parquet_files(root, "metrics", "values")
        metrics_series = parquet_files(root, "metrics", "series")
        for files in (logs_values, logs_series, metrics_values, metrics_series):
            self.assertTrue(files, f"a dataset is missing under {root}")
        with duckdb.connect() as db:
            stored = db.execute(
                "SELECT body FROM read_parquet(?) ORDER BY body", [logs_values]
            ).fetchall()
            self.assertEqual(stored, sorted((body,) for body in bodies))
            joined = db.execute(
                "SELECT v.body, v.producer_id, s.scope_name, s.attrs['logger.name'] "
                "FROM read_parquet(?) v "
                "JOIN (SELECT DISTINCT series_id, scope_name, attrs "
                "FROM read_parquet(?)) s USING (series_id) ORDER BY v.body",
                [logs_values, logs_series],
            ).fetchall()
            self.assertEqual(
                joined,
                sorted(
                    (body, "producer-1", "series-e2e", "series.logger")
                    for body in bodies
                ),
            )
            points = db.execute(
                "SELECT count(*), count(DISTINCT series_id) FROM read_parquet(?)",
                [metrics_values],
            ).fetchone()
            self.assertEqual(
                points,
                (
                    POINTS_PER_METRIC_REQUEST * len(metric_ids),
                    POINTS_PER_METRIC_REQUEST * len(metric_ids),
                ),
            )
            unmatched = db.execute(
                "SELECT count(*) FROM read_parquet(?) v WHERE v.series_id NOT IN "
                "(SELECT series_id FROM read_parquet(?))",
                [metrics_values, metrics_series],
            ).fetchone()[0]
            self.assertEqual(unmatched, 0)


class LocalFiles(unittest.TestCase, LakeAssertions):
    """configs/series-parquet-local.yaml on a local directory."""

    # Scenario: concurrent OTLP logs and metrics requests reach the shipped
    # local configuration, whose receiver waits for the exporter's result.
    # Guarantees: once every request is answered OK, before any shutdown,
    # each log body and metric point is in the lake exactly once and joins
    # to its series row; the engine, started without MALLOC_CONF, reports
    # jemalloc with its background purging thread running.
    def test_acknowledged_requests_are_stored(self):
        with tempfile.TemporaryDirectory() as directory:
            lake = Path(directory) / "lake"
            lake.mkdir()
            storage = {"file": {"base_uri": str(lake)}}
            bodies = [f"local-{n}" for n in range(8)]
            metric_ids = [f"local-metric-{n}" for n in range(4)]
            with Engine(directory, "series-parquet-local.yaml", storage) as engine:
                export_all(engine, bodies, metric_ids)
                self.assert_lake(lake, bodies, metric_ids)
                engine.shutdown()
                self.assertIn(
                    "Memory allocator: jemalloc, background_thread on",
                    engine.engine_log(),
                    "the engine runs without jemalloc's background purging thread",
                )


class AlloyConfigs(unittest.TestCase):
    """The reference Alloy producers, checked by Alloy itself."""

    # Scenario: `alloy validate` of the shipped Alloy image, with the stability
    # level the configurations document, reads each reference configuration.
    # Guarantees: both parse and pass Alloy's component and argument checks;
    # a file Alloy rejects fails the test with Alloy's own message.
    def test_reference_configs_validate(self):
        if not shutil.which("docker"):
            unavailable("Docker CLI absent")
        inspect = subprocess.run(
            ["docker", "image", "inspect", ALLOY_IMAGE],
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )
        if inspect.returncode:
            unavailable(f"the Alloy image is absent: {ALLOY_IMAGE}")
        for name in ["series-parquet.alloy", "series-parquet-strict.alloy"]:
            with self.subTest(config=name):
                result = subprocess.run(
                    [
                        "docker", "run", "--rm", "--pull=never",
                        "--volume", f"{ALLOY_CONFIGS}:/configs:ro",
                        "--env", "OTLP_ENDPOINT=127.0.0.1:4317",
                        ALLOY_IMAGE, "validate",
                        "--stability.level=public-preview", f"/configs/{name}",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=DOCKER_TIMEOUT_S,
                )
                self.assertEqual(
                    result.returncode, 0, f"{name}:\n{result.stdout}{result.stderr}"
                )


class AlloyRetries(unittest.TestCase):
    """The strict reference Alloy producer against a saturated receiver."""

    PRODUCERS = 3
    LINES_PER_PRODUCER = 10_000

    # Scenario: three instances of the shipped strict Alloy producer, each on
    # its own connection, tail a file of 10,000 lines each into the shipped
    # local configuration, whose receiver takes one request at a time
    # (`max_concurrent_requests: 1`), so exports that meet another in flight
    # are refused with RESOURCE_EXHAUSTED and a RetryInfo delay.
    # Guarantees: the receiver refuses exports at its concurrency limit, Alloy
    # retries every refusal instead of dropping it, and every line reaches
    # the lake.
    def test_alloy_retries_refusals_with_retry_info(self):
        if not shutil.which("docker"):
            unavailable("Docker CLI absent")
        inspect = subprocess.run(
            ["docker", "image", "inspect", ALLOY_IMAGE],
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )
        if inspect.returncode:
            unavailable(f"the Alloy image is absent: {ALLOY_IMAGE}")
        with tempfile.TemporaryDirectory() as directory:
            lines = []
            for producer in range(self.PRODUCERS):
                logs = Path(directory) / f"logs-{producer}"
                logs.mkdir()
                mine = [
                    f"alloy-retry-{producer}-{n:05d}"
                    for n in range(self.LINES_PER_PRODUCER)
                ]
                (logs / "app.log").write_text("".join(f"{line}\n" for line in mine))
                lines += mine
            lake = Path(directory) / "lake"
            lake.mkdir()
            storage = {"file": {"base_uri": str(lake)}}
            containers = [
                f"series-alloy-{uuid.uuid4().hex[:12]}" for _ in range(self.PRODUCERS)
            ]
            with Engine(
                directory,
                "series-parquet-local.yaml",
                storage,
                grpc_settings={"max_concurrent_requests": 1},
            ) as engine:
                try:
                    for producer, container in enumerate(containers):
                        alloy = subprocess.run(
                            [
                                "docker", "run", "--detach", "--pull=never",
                                "--name", container, "--network", "host",
                                "--volume", f"{ALLOY_CONFIGS}:/configs:ro",
                                "--volume",
                                f"{Path(directory) / f'logs-{producer}'}:/logs:ro",
                                "--env",
                                f"OTLP_ENDPOINT=127.0.0.1:{engine.grpc_port}",
                                "--env", "SERIES_LOG_PATH=/logs/app.log",
                                "--env", f"SERIES_PRODUCER_ID=producer-{producer}",
                                ALLOY_IMAGE, "run",
                                "--stability.level=public-preview",
                                "--storage.path=/tmp/alloy",
                                f"--server.http.listen-addr=127.0.0.1:{free_port()}",
                                "/configs/series-parquet-strict.alloy",
                            ],
                            capture_output=True,
                            text=True,
                            timeout=DOCKER_TIMEOUT_S,
                        )
                        self.assertEqual(alloy.returncode, 0, alloy.stderr)
                    missing = lines
                    deadline = time.monotonic() + 180
                    while missing and time.monotonic() < deadline:
                        time.sleep(1)
                        missing = self.missing_lines(lake, lines)
                    alloy_log = ""
                    for container in containers:
                        logged = subprocess.run(
                            ["docker", "logs", container],
                            capture_output=True,
                            text=True,
                            timeout=DOCKER_TIMEOUT_S,
                        )
                        alloy_log += logged.stdout + logged.stderr
                    refused = engine.counter(
                        "rejected_total",
                        "receiver",
                        otel_scope_name="receiver.otlp.requests",
                        error_type="concurrency_limit",
                    )
                finally:
                    for container in containers:
                        subprocess.run(
                            ["docker", "rm", "--force", container],
                            capture_output=True,
                            timeout=DOCKER_TIMEOUT_S,
                        )
                engine.shutdown()
            retried = [
                line
                for line in alloy_log.splitlines()
                if "Will retry" in line and "ResourceExhausted" in line
            ]
            print(
                f"alloy retry E2E: {len(lines)} lines from {self.PRODUCERS} producers, "
                f"{refused:.0f} refusals at the concurrency limit, {len(retried)} "
                f"retries logged by Alloy, {len(missing)} lines missing"
            )
            if retried:
                print(f"first retry logged by Alloy: {retried[0]}")
            self.assertGreater(refused, 0, "the receiver never refused an export")
            self.assertTrue(retried, "Alloy logged no retry of a RESOURCE_EXHAUSTED")
            self.assertNotIn("Dropping data", alloy_log, "Alloy dropped an export")
            self.assertEqual(missing, [], "lines missing from the lake")

    @staticmethod
    def missing_lines(lake, lines):
        """The lines of `lines` whose body the lake does not hold yet."""
        files = parquet_files(lake, "logs", "values")
        if not files:
            return lines
        with duckdb.connect() as db:
            stored = {
                row[0]
                for row in db.execute(
                    "SELECT DISTINCT body FROM read_parquet(?)", [files]
                ).fetchall()
            }
        return [line for line in lines if line not in stored]


class Minio(unittest.TestCase, LakeAssertions):
    """The S3 configurations against a MinIO container."""

    # Scenario: concurrent OTLP logs and metrics requests reach the shipped
    # strict S3 configuration writing to MinIO, which is then shut down.
    # Guarantees: once every request is answered OK, before any shutdown, the
    # objects in the bucket hold each log body and metric point exactly once,
    # and the graceful shutdown adds no copy.
    def test_strict_s3(self):
        with MinioStore() as store, tempfile.TemporaryDirectory() as directory:
            bodies = [f"strict-{n}" for n in range(8)]
            metric_ids = [f"strict-metric-{n}" for n in range(4)]
            with Engine(directory, "series-parquet-s3.yaml", store.storage) as engine:
                export_all(engine, bodies, metric_ids)
                acknowledged = Path(directory) / "acknowledged"
                store.download(acknowledged)
                self.assert_lake(acknowledged, bodies, metric_ids)
                engine.shutdown()
            downloaded = Path(directory) / "downloaded"
            store.download(downloaded)
            self.assert_lake(downloaded, bodies, metric_ids)

    # Scenario: the strict S3 configuration with a 30 s window holds
    # concurrent requests in the exporter's open block when the engine is
    # shut down with a 5 s deadline; the producer then retries every request
    # that was not answered OK against a second engine.
    # Guarantees: every request is either answered OK or refused with a
    # status an OTLP client retries, never dropped, and after the retries
    # every log body and metric point is in the lake at least once.
    def test_strict_s3_shutdown_then_producer_retries(self):
        retryable = {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.ABORTED,
        }
        with MinioStore() as store, tempfile.TemporaryDirectory() as directory:
            bodies = [f"strict-retry-{n}" for n in range(8)]
            metric_ids = [f"strict-retry-metric-{n}" for n in range(4)]
            name = "series-parquet-s3.yaml"
            refused_bodies, refused_metrics = [], []
            with Engine(directory, name, store.storage, window="30s") as engine:
                wait_early_in_window(30)
                logs = {b: engine.logs.Export.future(log_request(b), timeout=60) for b in bodies}
                metrics = {
                    m: engine.metrics.Export.future(metric_request(m), timeout=60)
                    for m in metric_ids
                }
                wait_for(
                    lambda: engine.gauge("block_requests_pending", "exporter")
                    == len(bodies) + len(metric_ids),
                    30,
                    "the exporter to hold every request in its open block",
                )
                try:
                    engine.shutdown(seconds=5)
                except urllib.error.HTTPError as error:
                    self.assertEqual(error.code, 504, "only the deadline may end the call")
                for calls, refused in ((logs, refused_bodies), (metrics, refused_metrics)):
                    for key, call in calls.items():
                        try:
                            call.result()
                        except grpc.RpcError as error:
                            self.assertIn(error.code(), retryable, f"{key}: {error}")
                            refused.append(key)
            with Engine(directory, name, store.storage) as engine:
                export_all(engine, refused_bodies, refused_metrics)
                engine.shutdown()
            wait_stored_at_least_once(store, directory, bodies, metric_ids, 30)

    # Scenario: the shipped buffered configuration (durable_buffer in front
    # of the exporter, 30 s window) takes concurrent requests on MinIO and is
    # shut down gracefully once the buffer has handed every request to the
    # exporter's open block; a second engine then starts on the same buffer
    # directory, takes one more logs request and stores it.
    # Guarantees: the graceful shutdown writes the open block and records its
    # acknowledgements, so the lake holds every request exactly once, and the
    # restart replays none of them: the objects it adds hold only the new
    # request.
    def test_buffered_s3_shutdown_and_restart(self):
        with MinioStore() as store, tempfile.TemporaryDirectory() as directory:
            wal = Path(directory) / "wal"
            wal.mkdir()
            bodies = [f"buffered-{n}" for n in range(8)]
            metric_ids = [f"buffered-metric-{n}" for n in range(4)]
            name = "series-parquet-buffered.yaml"
            requests = len(bodies) + len(metric_ids)
            with Engine(
                directory, name, store.storage, buffer_path=wal, window="30s"
            ) as engine:
                wait_early_in_window(30)
                export_all(engine, bodies, metric_ids)
                wait_for(
                    lambda: engine.gauge("in_flight", "buffer") == requests,
                    30,
                    "the buffer to hand every request to the exporter",
                )
                engine.shutdown()
            shut_down = Path(directory) / "shut-down"
            store.download(shut_down)
            self.assert_lake(shut_down, bodies, metric_ids)
            before = store.keys()

            # The buffer delivers in WAL order, so a bundle replayed from the
            # first run would reach the store no later than this request.
            probe = "buffered-after-restart"
            values = "/signal=logs/dataset=values/"
            with Engine(directory, name, store.storage, buffer_path=wal) as engine:
                export_all(engine, [probe], [])
                wait_for(
                    lambda: any(values in key for key in store.keys() - before),
                    60,
                    "the restarted engine to store its request",
                )
                engine.shutdown()
            after = store.keys()
            self.assertLessEqual(before, after, "an object of the first run is gone")
            added = after - before
            self.assertFalse(
                [key for key in added if "/signal=metrics/dataset=values/" in key],
                "the restart stored metric points again",
            )
            downloaded = Path(directory) / "downloaded"
            store.download(downloaded)
            added_values = [
                str(downloaded / key.removeprefix("otel/"))
                for key in added
                if values in key
            ]
            with duckdb.connect() as db:
                stored = db.execute(
                    "SELECT body FROM read_parquet(?)", [added_values]
                ).fetchall()
            self.assertEqual(stored, [(probe,)], "the restart replayed a request")
            self.assert_lake(downloaded, bodies + [probe], metric_ids)

    # Scenario: the shipped buffered configuration on MinIO, with a 30 s
    # window, acknowledges concurrent logs and metrics requests; once the
    # buffer has handed every one to the exporter's open block the engine is
    # SIGKILLed, and a second engine starts on the same buffer directory.
    # Guarantees: every request acknowledged before the kill is in the lake
    # at least once after the restart (at-least-once across a crash).
    def test_buffered_s3_sigkill_replays(self):
        with MinioStore() as store, tempfile.TemporaryDirectory() as directory:
            wal = Path(directory) / "wal"
            wal.mkdir()
            bodies = [f"killed-{n}" for n in range(8)]
            metric_ids = [f"killed-metric-{n}" for n in range(4)]
            name = "series-parquet-buffered.yaml"
            requests = len(bodies) + len(metric_ids)
            with Engine(
                directory, name, store.storage, buffer_path=wal, window="30s"
            ) as engine:
                wait_early_in_window(30)
                export_all(engine, bodies, metric_ids)
                wait_for(
                    lambda: engine.gauge("in_flight", "buffer") == requests,
                    30,
                    "the buffer to hand every request to the exporter",
                )
                engine.kill()
            with Engine(directory, name, store.storage, buffer_path=wal) as engine:
                wait_stored_at_least_once(store, directory, bodies, metric_ids, 90)
                engine.shutdown()

    # Scenario: the shipped buffered configuration on MinIO, with a 1 s
    # window, acknowledges logs and metrics requests while MinIO is paused, so
    # the block holding them is being written when the engine is SIGKILLed;
    # MinIO then returns and a second engine starts on the same buffer
    # directory.
    # Guarantees: every request acknowledged before the kill is in the lake
    # at least once after the restart; a write cut by the kill loses nothing.
    def test_buffered_s3_sigkill_during_paused_write(self):
        with MinioStore() as store, tempfile.TemporaryDirectory() as directory:
            wal = Path(directory) / "wal"
            wal.mkdir()
            bodies = [f"paused-{n}" for n in range(8)]
            metric_ids = [f"paused-metric-{n}" for n in range(4)]
            name = "series-parquet-buffered.yaml"
            requests = len(bodies) + len(metric_ids)
            with Engine(directory, name, store.storage, buffer_path=wal) as engine:
                store.pause()
                try:
                    export_all(engine, bodies, metric_ids)
                    wait_for(
                        lambda: engine.gauge("in_flight", "buffer") == requests,
                        30,
                        "the buffer to hand every request to the exporter",
                    )
                    wait_for(
                        lambda: (engine.gauge("block_flushing_bytes", "exporter") or 0) > 0,
                        30,
                        "the exporter to write a block to the paused store",
                    )
                    engine.kill()
                finally:
                    store.unpause()
            with Engine(directory, name, store.storage, buffer_path=wal) as engine:
                wait_stored_at_least_once(store, directory, bodies, metric_ids, 90)
                engine.shutdown()

    # Scenario: the shipped buffered configuration on MinIO takes a request
    # above max_decoding_message_size, a traces request, a metrics request
    # with a summary point, an exemplar and a timestamp beyond i64
    # nanoseconds, a logs request with invalid UTF-8, and a logs request
    # while MinIO is paused past the flush deadline, which is stored once
    # MinIO returns.
    # Guarantees: the scrape carries each series the alert rules watch for
    # these failures, by name, scope and label value, and the deployment
    # example's check.py, given the scrape and the engine's JSON of every
    # registered metric, finds every metric, label and label value its rules
    # and dashboard select; only Alloy's counters are absent. The
    # PrometheusRule it generates carries the rules file's groups.
    def test_alert_families_exposed(self):
        with MinioStore() as store, tempfile.TemporaryDirectory() as directory:
            wal = Path(directory) / "wal"
            wal.mkdir()
            name = "series-parquet-buffered.yaml"
            with Engine(directory, name, store.storage, buffer_path=wal) as engine:
                export_all(engine, ["families-ok"], [])
                with self.assertRaises(grpc.RpcError) as refused:
                    engine.logs.Export(log_request("x" * (17 << 20)), timeout=30)
                self.assertEqual(refused.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
                engine.traces.Export(trace_request(), timeout=30)
                engine.metrics.Export(altered_metric_request(), timeout=30)
                export_raw_logs(engine, invalid_utf8_log_request())

                before = store.keys()
                store.pause()
                try:
                    engine.logs.Export(log_request("families-outage"), timeout=30)
                    wait_for(
                        lambda: has_series(
                            engine.scrape(),
                            "block_write_failures_total",
                            "exporter.series_parquet",
                            error_type="deadline",
                        ),
                        90,
                        "a block write to fail at the flush deadline",
                    )
                finally:
                    store.unpause()
                wait_for(
                    lambda: any(
                        "/signal=logs/dataset=values/" in key
                        for key in store.keys() - before
                    ),
                    90,
                    "the blocks to be stored after the outage",
                )
                # Counters, each with a value above zero once provoked.
                expected = [
                    ("rejected_total", "receiver.otlp.requests", {"error_type": "payload_too_large"}),
                    ("resolved_total", "processor.durable_buffer.bundles", {"outcome": "permanently_rejected"}),
                    ("nacks_total", "exporter.series_parquet", {"error_type": "unsupported"}),
                    ("block_write_failures_total", "exporter.series_parquet", {"error_type": "deadline"}),
                    ("retries_scheduled_total", "processor.durable_buffer", {}),
                    ("dropped_unsupported_total", "exporter.series_parquet", {"kind": "summary"}),
                    ("dropped_exemplars_total", "exporter.series_parquet", {"signal": "metrics"}),
                    ("repaired_invalid_utf8_total", "exporter.series_parquet", {"signal": "logs"}),
                    ("timestamp_out_of_range_total", "exporter.series_parquet", {}),
                ]

                def missing():
                    scrape = engine.scrape()
                    return [
                        f"{metric}{labels} of {scope}"
                        for metric, scope, labels in expected
                        if not has_series(scrape, metric, scope, positive=True, **labels)
                    ]

                deadline = time.monotonic() + 60
                while missing() and time.monotonic() < deadline:
                    time.sleep(0.5)
                self.assertEqual(missing(), [], "provoked families not in the scrape")
                self.assert_deploy_check(engine, directory)
                engine.shutdown()

    def assert_deploy_check(self, engine, directory):
        """Run the deployment example's check.py on `engine`'s scrape and
        registered metrics, and on the PrometheusRule it generates."""
        scrape = Path(directory) / "engine.prom"
        scrape.write_text(engine.scrape())
        schema = Path(directory) / "engine.json"
        schema.write_text(engine.schema())
        crd = Path(directory) / "prometheusrule.yaml"
        command = [sys.executable, str(DEPLOY_CHECK), "--write-crd", str(crd)]
        command += ["--crd", str(crd), "--schema", str(schema), str(scrape)]
        for name in ALLOY_UNEXPOSED:
            command += ["--absent", name]
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

if __name__ == "__main__":
    unittest.main()
