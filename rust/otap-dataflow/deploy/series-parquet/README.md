# Series Parquet test deployment

Ready files for the reference deployment: Grafana Alloy tails log files and
sends them over OTLP/gRPC to the engine, which writes them to its
`durable_buffer` WAL and from there through `exporter:series_parquet` to an
S3-compatible bucket. Why each setting has its value, the sizing rules and
the failure behaviour are in the exporter README,
[Deploying with Alloy](../../crates/core-nodes/src/exporters/series_parquet_exporter/README.md#deploying-with-alloy);
this directory does not repeat them.

| Path | What |
| --- | --- |
| `k8s/engine/` | Kustomization: engine config, site values, StatefulSet with a WAL volume per pod, Services, PodDisruptionBudget, NetworkPolicy; `secret.example.yaml` and `servicemonitor.yaml` are applied separately |
| `k8s/engine/engine.yaml` | [`series-parquet-buffered.yaml`](../../configs/series-parquet-buffered.yaml) with the Kubernetes site lines; the config crate's tests check that nothing else differs |
| `k8s/alloy/` | Kustomization: a DaemonSet sketch running [`series-parquet.alloy`](../../configs/series-parquet.alloy) unchanged, and its NetworkPolicy; `podmonitor.yaml` is applied separately |
| `bucket/lifecycle.json` | The `AbortIncompleteMultipartUpload` rule |
| `bucket/iam-policy.json` | The writer's S3 permissions |
| `alerts/series-parquet.rules.yaml` | Prometheus rules for the README "Alerts" table |
| `dashboard/series-parquet.json` | Grafana dashboard with a Prometheus data source variable |
| `check.py` | Generates the rules as a prometheus-operator `PrometheusRule`, and checks that every metric, label and label value the rules and the dashboard select exists in the engine |
| `test_check.py` | Unit tests of `check.py` |

## Image

The repository `Dockerfile` builds `df_engine` with the features passed in
`FEATURES`. From `rust/otap-dataflow`:

```bash
docker build --build-context otel-arrow=../../ -f Dockerfile \
  --build-arg FEATURES=series-parquet,aws,azure,durable-buffer \
  --build-arg RUSTFLAGS= \
  -t df_engine:series-parquet .
```

`k8s/engine/statefulset.yaml` names this local tag; push the image to a
registry the cluster pulls from and set its name there (step 4).

The Dockerfile's default `RUSTFLAGS` is `-C target-cpu=native`, which builds
for the build host's CPU and can fail with an illegal instruction elsewhere;
`RUSTFLAGS=` builds for any x86-64 host, and
`--build-arg RUSTFLAGS="-C target-cpu=x86-64-v3"` for hosts with AVX2. `azure`
is only needed for Azure Blob Storage. The image is about 800 MB, almost all
of it the binary's line tables, which the release profile keeps for
profiling. It runs as `nonroot` (uid 65532) from
`gcr.io/distroless/cc-debian13`, with a read-only root file system; the WAL
directory must be writable by that uid.

Ports: 4317 for OTLP/gRPC (the config has no OTLP/HTTP) and the admin server
given by `--http-admin-bind`, 8080 here. On the admin port, `/api/v1/metrics`
is the Prometheus endpoint, `/api/v1/livez` and `/api/v1/readyz` the probes.
The admin server also takes unauthenticated shutdown and reconfiguration
calls, so keep it off the producers' network: `k8s/engine/networkpolicy.yaml`
admits it only from the Prometheus pods.

To run it with Docker from this directory, with the WAL on a volume, the site
values from `k8s/engine/site.env` plus credentials, and a MinIO that listens
on port 9000 of the Docker host (the MinIO values of `site.env`, with the
Docker host's address; plain HTTP needs signed payloads):

```bash
docker volume create series-wal
docker run --rm -v series-wal:/w alpine chown 65532:65532 /w
docker run -d --name series-parquet --read-only --memory 7g \
  -p 4317:4317 -p 127.0.0.1:8080:8080 \
  -v series-wal:/var/lib/otap/series-wal \
  -v "$PWD/k8s/engine/engine.yaml:/etc/series-parquet/engine.yaml:ro" \
  --env-file k8s/engine/site.env \
  --add-host host.docker.internal:host-gateway \
  -e AWS_ENDPOINT_URL=http://host.docker.internal:9000 \
  -e AWS_ALLOW_HTTP=true -e AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false \
  -e AWS_UNSIGNED_PAYLOAD=false \
  -e POD_NAME=series-parquet-0 -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  df_engine:series-parquet \
  --config /etc/series-parquet/engine.yaml --http-admin-bind 0.0.0.0:8080
```

For AWS S3 leave out the four MinIO variables. Stop it with
`docker stop -t 90`: the default 10 s is shorter than the shutdown.

A container memory limit is required: Docker `--memory`, or
`resources.limits.memory` in Kubernetes. The memory limiter in `engine.yaml`
(`source: auto`) takes its thresholds from that limit, and without one the
engine refuses to start.

## Order of steps

Run the commands from this directory.

1. **Bucket.** Create the bucket and its lifecycle rule:

   ```bash
   aws s3api create-bucket --bucket series-test --region us-east-1
   # outside us-east-1 add: --create-bucket-configuration LocationConstraint=<region>
   aws s3api put-bucket-lifecycle-configuration --bucket series-test \
     --lifecycle-configuration file://bucket/lifecycle.json
   ```

   Expiry or a storage-class transition go into the same `Rules` list, with
   the retention your site chooses:

   ```text
   {"ID": "expire", "Status": "Enabled", "Filter": {"Prefix": "otel/"},
    "Expiration": {"Days": <retention days>}}
   ```

   MinIO (RELEASE.2025-04-22) refuses `AbortIncompleteMultipartUpload` in a
   bucket lifecycle (`mc ilm import` answers that the XML does not
   validate); it removes incomplete uploads server-wide instead, after
   `api stale_uploads_expiry` (24h by default): check it with
   `mc admin config get <alias> api`. On MinIO create the bucket with
   `mc mb <alias>/series-test`. Other S3-compatible stores need the
   equivalent. The rule is the only guarantee against incomplete uploads.

   Give the writer `bucket/iam-policy.json` with the bucket and prefix of
   `SERIES_S3_BASE_URI` in the object ARN (`arn:aws:s3:::<bucket>/<prefix>/*`)
   and the bucket alone in the `s3:ListBucket` ARN (`arn:aws:s3:::<bucket>`):
   `ListBucket` applies to the bucket, never to an object ARN, and without it
   a HEAD of a missing key answers 403 instead of 404. An `s3:prefix`
   condition cannot narrow it: the condition matches the `prefix` parameter
   of a list request, and a HEAD carries none, so it would be denied. The
   grant lets these credentials list every key in the bucket; the writer
   never lists or deletes. On a versioned bucket each retry that reached the store
   leaves a noncurrent version, so expire noncurrent versions.

2. **Credentials.** Either create the Secret from `k8s/engine/secret.example.yaml`
   (static keys, read as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`), or
   use IRSA: annotate the pod's ServiceAccount with the role, whose webhook
   sets `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`. The config's
   `auth: {type: default}` reads both.

3. **Config.** Edit `k8s/engine/site.env`: `SERIES_S3_BASE_URI` and
   `AWS_REGION`. It ships the AWS S3 values (HTTPS) with
   `AWS_UNSIGNED_PAYLOAD=true`, which skips the payload hash and relies on
   TLS; for a MinIO in the cluster set it to `false` and uncomment the three
   MinIO lines, which also allow plain HTTP. Keep `engine.yaml` as it is
   unless the sizing below changes.

4. **Engine.** Set the image (`kustomize edit set image
   df_engine=<registry>/df_engine:<tag>` in `k8s/engine`, or edit
   `statefulset.yaml`), the WAL storage class in `volumeClaimTemplates`, and
   apply:

   ```bash
   kubectl create namespace series-parquet
   kubectl -n series-parquet apply -f k8s/engine/secret.example.yaml   # edited, or IRSA
   kubectl -n series-parquet apply -k k8s/engine
   kubectl -n series-parquet apply -f k8s/engine/servicemonitor.yaml   # with the prometheus-operator
   ```

   `networkpolicy.yaml` admits OTLP only from namespaces labelled
   `series-parquet/producer=true`, and the admin port only from pods labelled
   `app.kubernetes.io/name: prometheus` (the prometheus-operator's label) in
   the namespace labelled `kubernetes.io/metadata.name: monitoring`; change
   both to where your producers and Prometheus run, and label the producer
   namespaces, the one Alloy runs in included:

   ```bash
   kubectl label namespace series-parquet series-parquet/producer=true
   ```

   It takes effect only with a CNI that enforces NetworkPolicy (Calico,
   Cilium, and others; not the default kindnet or flannel). The kubelet's
   probes come from the node and are admitted by those CNIs.

5. **Alloy.** The kustomization reads `configs/series-parquet.alloy` from
   outside its directory:

   ```bash
   kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/alloy | kubectl -n series-parquet apply -f -
   kubectl -n series-parquet apply -f k8s/alloy/podmonitor.yaml   # with the prometheus-operator
   ```

   Set `SERIES_LOG_PATH`, `SERIES_SERVICE_NAME` and the `logs` host path in
   `daemonset.yaml`; `OTLP_ENDPOINT` names the engine Service in the
   `series-parquet` namespace. Alloy runs as the image's `alloy` user (473)
   with no capability, and reads the log files through their group: set
   `supplementalGroups` to the group that owns them, and give that group
   read permission on the files and their directory. An init container
   holding only `CAP_CHOWN` gives the storage host directory to that user.
   `networkpolicy.yaml` admits Alloy's HTTP port only from the Prometheus
   pods, as for the engine. Reading files of any owner would need
   `CAP_DAC_READ_SEARCH`, which with a host mount also opens every other
   file of that host file system through `open_by_handle_at`; prefer the
   group.

6. **Alerts and dashboard.** Load `alerts/series-parquet.rules.yaml` into
   Prometheus (or apply the `PrometheusRule` below) and import
   `dashboard/series-parquet.json` into Grafana. Both select
   `job="series-parquet"` for the engine and `job="series-parquet-alloy"` for
   Alloy, the job names the ServiceMonitor and the PodMonitor set; a plain
   Prometheus needs scrape jobs of those names. `SeriesParquetEngineDown`
   and `SeriesParquetAlloyDown` fire when a job has a target Prometheus
   cannot scrape, or no target at all; the other rules need the scrape, and
   the restart rules need kube-state-metrics.

   The prometheus-operator loads only the rules and monitors its
   `ruleSelector`, `serviceMonitorSelector` and `podMonitorSelector` match;
   kube-prometheus-stack matches `release: <its Helm release>` by default.
   Generate the PrometheusRule with that label
   (`python3 check.py --write-crd prometheusrule.yaml --release <release>`,
   then `kubectl apply -f prometheusrule.yaml`) and uncomment the
   `labels` of `servicemonitor.yaml` and `podmonitor.yaml`. Confirm that the
   rules loaded with `kubectl get prometheusrule` and on the Prometheus
   `/rules` page: an ignored rule file fires nothing, not even
   `SeriesParquetEngineDown`.

## What the Kubernetes files decide

- **One pod, one WAL.** Each pod has its own `wal` volume; the WAL does not
  lock its directory, so a volume must never be mounted by two pods.
  `writer_id` is the pod name. Scaling down keeps the removed pod's volume
  (`whenScaled: Retain`), and what its WAL still holds is stored only when a
  pod with that name starts on it again: scale back up or drain it first.
  No metric reports what a retained volume still holds, since no engine
  runs on it.
- **Grace period 90 s.** SIGTERM grants the engine 60 s; the exporter needs up
  to `upload.abort_timeout` (5 s) more, and 90 s leaves a margin. What the
  drain does not store stays in the WAL for the next start.
- **Resources.** `core_allocation` is one worker. Memory is the README
  "Sizing" formula at the receiver's 4MiB request limit, 4.86 GB per worker
  with `max_in_flight` 640, times 1.25 for RSS: 6.1 GB, 81% of the 7Gi
  limit (3.25 GB live, 4.1 GB RSS with Alloy's 2MiB exports). The limit is
  required: the memory limiter in `engine.yaml` reads it from the cgroup, the
  engine does not start without it, and above 95% of it requests are refused
  with RESOURCE_EXHAUSTED and a RetryInfo delay before the kernel kills the
  pod;
  `SeriesParquetMemoryNearLimit` fires above 90% of its soft limit. The
  limiter refuses only new requests: the buffer hands bundles to the
  exporter, also when it replays the WAL after a restart, up to
  `max_in_flight` whatever the memory pressure, so raising
  `max_decoding_message_size` or `max_in_flight` needs a larger limit.
  The CPU request is the worker count; there is
  no CPU limit, since the admin and WAL threads run beside the worker and
  throttling them delays acknowledgements.
- **WAL volume 40Gi** for the 32GiB `retention_size_cap`, which covers about
  34 minutes of store outage at 40k lines/s.
- **Probes.** Readiness on `/api/v1/readyz`, liveness on `/api/v1/livez`;
  see "Probes and WAL replay" below.
- **Disruptions.** `podManagementPolicy: Parallel`; the PodDisruptionBudget
  allows one engine down at a time. Producers retry and queue while theirs
  is down.
- **Load spreading.** A gRPC connection stays on the pod it opened, so each
  Alloy sends to one engine until it reconnects.

To run more workers per pod, raise `core_allocation.count` in `engine.yaml`,
the CPU request to the same number, and the memory request and limit by 7Gi
per worker; the memory rule follows the limit. The WAL cap is divided
between the workers. Keep the `num_consumers` of the producers that reach
one worker at most its 128 receiver slots.

Lowering `core_allocation.count` strands data: each worker has its own WAL
directory, `core_<id>`, and a removed core's directory is not read by any
other worker, so the acknowledged data it holds is stored only when a worker
with that core id starts on it again. Drain the pod first: stop the
producers' traffic to it (or scale the producers down) and wait until
`oldest_pending.age` reads 0 on every core, then change the count.

## Probes and WAL replay

The admin server, which answers the probes, runs on its own thread, and the
buffer opens its WAL on the pipeline thread when the first message reaches
it, so a replay does not hold the probes back. Measured with a debug build
after a SIGKILL: on a 2.1 GB WAL `/api/v1/livez` and `/api/v1/readyz`
answered 200 one second after start and at every probe after, and the buffer
had opened the WAL within 3.2 s. The liveness probe's 30 s delay and six
failures 10 s apart leave room for a full 32GiB WAL, so no startup probe is
needed. The pod is ready before the replay ends; requests that arrive during
it wait for the pipeline thread.

## Shutdown time

Measured with the store frozen (`docker pause` of MinIO) and SIGTERM sent
just after a window boundary started a flush: the buffered configuration
exited after 39 s, bounded by the two blocks' `flush_retry_deadline` of 15 s
each plus `upload.abort_timeout`, and the strict `series-parquet-s3.yaml`,
whose `flush_retry_deadline` is 60 s, after 64 s, the 60 s deadline plus
`upload.abort_timeout`. Both exited 0 inside the 90 s grace period.

## Trust boundary

Every producer that reaches the receiver writes into one lake, and the lake
trusts what it sends: `producer_id` and every identity attribute are the
producer's own claim, and the series id is a public, unkeyed hash of them, so
a producer can write rows under another's identity, or craft one whose id
collides with another series in the same partition (series-lake FORMAT.md).
Keep one trust domain per lake: `k8s/engine/networkpolicy.yaml` admits OTLP
only from the labelled producer namespaces. The receiver has no TLS or
authentication in `engine.yaml`; for producers on a network you do not
control, give it a certificate and the `bearer_token_authorizer` extension
(OTLP receiver README), and each producer its token.

## Metrics

The engine names a metric by its instrument only (`block.write_failures`
becomes `block_write_failures_total`) and puts its metric set in the
`otel_scope_name` label, so several sets share a name: select by both, as the
rules do. A counter with labels appears only after its first increment, so
the rules for "any" also fire on a series that is new within their range.

The PrometheusRule is not kept in this directory: `check.py --write-crd`
generates it from the rules file when it is applied, and
`check.py --crd <file>` checks that a generated or applied copy still
carries the rules file's groups. After a change to the rules or the
dashboard, check them against a running engine (and Alloy):

```bash
curl -s http://127.0.0.1:8080/api/v1/metrics > engine.prom
curl -s 'http://127.0.0.1:8080/api/v1/telemetry/metrics?format=json&keep_all_zeroes=true' > engine.json
curl -s http://127.0.0.1:12345/metrics > alloy.prom
python3 check.py --schema engine.json engine.prom alloy.prom
```

The JSON lists every metric the engine registered with every value of its
labels, counters that never moved included, so `check.py` reports a metric,
a label or a label value that a rule or a panel names and the engine does
not have. A metric no file has, such as an Alloy counter that never moved,
is accepted with `--absent NAME` (or `--absent 'NAME{otel_scope_name="SET"}'`
for an engine metric). The E2E test `test_alert_families_exposed` runs this
check with the engine's scrape and JSON after provoking the failures the
rules watch; only Alloy's counters are accepted as absent.

## Test checklist

1. **Send data.** Write lines to the tailed file; within about 20 s
   (`window.interval` plus the flush) objects appear under
   `v=1/signal=logs/dataset=values/` and `dataset=series/`, named with the
   pod name. Read them back with the recipes in the
   [series-lake README](../../crates/series-lake/README.md#reading-the-data)
   and compare the line count per `producer_id`.
2. **Alerts quiet.** With steady input no rule fires; the dashboard shows
   freshness under the 30 s alert threshold, the WAL near its steady size and
   no refusals.
3. **Graceful restart.** `kubectl delete pod series-parquet-0`: the log ends
   with `series_parquet.shutdown.complete` and `durable_buffer.shutdown.complete`,
   the container exits 0, and after the restart the line count is still
   exact.
4. **Kill.** On the node, `pkill -KILL df_engine`: the pod restarts, the WAL
   replays, no acknowledged line is missing; up to `num_consumers` exports per
   producer may be stored twice.
5. **Store outage.** Block the store for a few minutes: producers keep
   getting OK, `SeriesParquetFlushFailing` and `SeriesParquetFreshness` fire,
   and after the store returns the backlog drains with no loss.

## Known limits

- The lake format is version 1 and may still change before a stable
  release; keep test data under its own base URI.
- The lifecycle rule (or MinIO's stale-upload expiry) is the only guarantee
  against incomplete multipart uploads.
- An OK to the producer means the WAL has the request, not that it is synced:
  a host crash or power loss can lose about the last 100 ms of acknowledged
  requests. A process crash loses nothing.
- At a graceful shutdown, bundles the buffer drains after the shutdown began
  are admitted while the exporter's admission is open and written until its
  cut, 1.2s before the deadline (see "The drain" in the exporter README).
  Bundles refused or cut stay in the WAL and are stored after the next start,
  so a stopped pod's tail waits for its return.
- The failure measurements come from one worker at 40k lines/s; the soak
  evidence is single-worker too.
- One worker takes about 100k lines/s behind the buffer (104k sustained on
  MinIO; its in-flight bound is 128k lines/s). One NVMe WAL device shared with
  the store carried 144k to 152k records/s with four workers; beyond that give
  each engine its own device or run more engines.
