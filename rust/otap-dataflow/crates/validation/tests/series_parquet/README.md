# Series Parquet end-to-end tests

`test_e2e.py` runs a real `df_engine` on the shipped
`configs/series-parquet-*.yaml`, sends OTLP/gRPC logs and metrics, and reads
the lake back with DuckDB:

- `LocalFiles`: `series-parquet-local.yaml` on a local directory; every
  acknowledged request is stored before shutdown.
- `Minio.test_strict_s3`: `series-parquet-s3.yaml` on a MinIO container;
  every acknowledged request is stored before shutdown.
- `Minio.test_strict_s3_shutdown_then_producer_retries`: the same
  configuration shut down with a 5 second deadline while the exporter holds
  every request in its open block; every request is answered OK or refused
  with a retryable status, and once the producer retries the refused ones
  against a new engine every request is stored at least once.
- `Minio.test_buffered_s3_shutdown_and_restart`: `series-parquet-buffered.yaml`
  (durable_buffer in front of the exporter) on MinIO, shut down gracefully
  while the exporter holds every request in its open block, which the shutdown
  writes; a restart on the same buffer directory stores one new request and
  replays nothing.
- `Minio.test_buffered_s3_sigkill_replays` and
  `Minio.test_buffered_s3_sigkill_during_paused_write`: the same configuration
  SIGKILLed while the exporter holds every acknowledged request in its open
  block, or while it writes that block to a paused MinIO; after a restart on
  the same buffer directory every request is stored at least once.
- `AlloyRetries`: three instances of `configs/series-parquet-strict.alloy`
  in the Alloy image tail 10,000 lines each into `series-parquet-local.yaml`
  with `max_concurrent_requests: 1`; the receiver refuses exports at its
  concurrency limit with RESOURCE_EXHAUSTED and a RetryInfo delay, Alloy
  retries every one, and every line is stored.
- `Minio.test_alert_families_exposed`: the buffered configuration provoked
  into the failures the deployment example's alert rules watch, and its
  `check.py` run on the engine's scrape.
- `AlloyConfigs`: `alloy validate` of the Alloy image checks
  `configs/series-parquet.alloy` and `configs/series-parquet-strict.alloy`
  at the stability level they document.

Each test shortens the window, to one second (30 seconds for the two shutdown
tests, so the requests are still in the open block at shutdown); every other
setting is the shipped one. The suite takes about a minute and a half.

## Running

From `rust/otap-dataflow`:

```bash
cargo build -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer
python3 -m venv /tmp/series-parquet-venv
/tmp/series-parquet-venv/bin/pip install --require-hashes \
  -r crates/validation/tests/series_parquet/requirements.lock.txt
docker pull pgsty/minio:RELEASE.2026-08-04T00-00-00Z
docker pull grafana/alloy:v1.19.2
SERIES_REQUIRE_DOCKER=1 /tmp/series-parquet-venv/bin/python3 -m unittest \
  crates.validation.tests.series_parquet.test_e2e -v
```

`DF_ENGINE` selects another engine binary (default `target/debug/df_engine`),
`SERIES_MINIO_IMAGE` another MinIO image and `SERIES_ALLOY_IMAGE` another
Alloy image. `pgsty/minio` is a community build of the MinIO server with the
same command line; MinIO no longer publishes `minio/minio` on Docker Hub. The
tests never pull an image. Without Docker or an image the MinIO and Alloy
tests skip, unless `SERIES_REQUIRE_DOCKER=1` turns the skip into a failure.
