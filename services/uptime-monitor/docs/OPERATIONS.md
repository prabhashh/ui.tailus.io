# Uptime Monitor — Operations Guide

This is a **setup and operations** runbook, not a design document. For how
and why the system works the way it does, read the package docs in the
source (`internal/scheduler`, `internal/latency`, `internal/circuitbreaker`,
etc.) — every non-obvious decision is documented at the point it's made.

Services in this repo (`services/uptime-monitor/`):

| Binary | Role | Scales by |
|---|---|---|
| `scheduler` | decides when each monitor's next check fires, publishes jobs | replica count (consistent-hash sharded) |
| `prober` | executes checks for one region, publishes results | replica count per region |
| `resultprocessor` | writes results to Postgres, runs incident detection, sends alerts | replica count (shared NATS consumer group) |
| `api` | monitor CRUD | replica count behind a load balancer |

---

## 1. Target topology

For the 50,000-monitor / 30s-interval baseline:

| Component | Instances | Per-instance spec |
|---|---|---|
| Postgres | 1 primary + 1 streaming replica | 4 vCPU / 16 GB RAM / NVMe SSD, 200+ GB |
| Redis | 1 (+ replica optional) | 2 vCPU / 4 GB RAM |
| NATS (JetStream) | 3-node cluster | 2 vCPU / 4 GB RAM / 50 GB disk each |
| `scheduler` | 2 replicas | 1 vCPU / 1 GB RAM |
| `prober` | 4-6 replicas total, split across regions | 2 vCPU / 2 GB RAM each |
| `resultprocessor` | 2 replicas | 1 vCPU / 1 GB RAM |
| `api` | 2 replicas behind a load balancer | 1 vCPU / 1 GB RAM |

This fits comfortably on 4-6 mid-size VPS instances (e.g. Hetzner CPX31 /
DigitalOcean 4vCPU droplets). See §11 for how these numbers scale past 50k.

---

## 2. Provision dependencies

Run these on dedicated small VPS instances (or managed equivalents — RDS,
Upstash, a NATS-hosting provider — if you'd rather not operate them
yourself; the app only needs a DSN/address, it doesn't care who runs it).

### 2.1 PostgreSQL 16+

```bash
# Debian/Ubuntu
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt update && sudo apt install -y postgresql-16 postgresql-16-cron

sudo -u postgres psql -c "CREATE ROLE uptime WITH LOGIN PASSWORD 'CHANGE_ME';"
sudo -u postgres psql -c "CREATE DATABASE uptime OWNER uptime;"
```

Tune `postgresql.conf` for the write-heavy `check_results` table:

```
shared_buffers = 4GB
effective_cache_size = 12GB
max_wal_size = 4GB
checkpoint_completion_target = 0.9
synchronous_commit = off   # acceptable: losing the last <1s of check_results on a
                            # crash is not an SLA-relevant loss for this workload
shared_preload_libraries = 'pg_cron'
cron.database_name = 'uptime'
```

Restart Postgres after editing. Then run the migrations:

```bash
cd services/uptime-monitor
export UPTIME_POSTGRES_DSN="postgres://uptime:CHANGE_ME@localhost:5432/uptime?sslmode=disable"
make migrate
```

Wire up partition maintenance (adjust retention as needed — 7 days raw is
the default assumed elsewhere in this doc):

```sql
SELECT cron.schedule('uptime-create-partitions', '0 0 * * *', 'SELECT uptime_create_daily_partitions()');
SELECT cron.schedule('uptime-rollup-hourly',      '5 * * * *', 'SELECT uptime_rollup_hourly()');
SELECT cron.schedule('uptime-drop-old-partitions','15 0 * * *','SELECT uptime_drop_old_partitions(7)');
```

If `pg_cron` isn't available on your provider, add these as a systemd timer
instead (see `deploy/systemd/uptime-partition-maintenance.timer` — create
one running `psql "$DSN" -c "SELECT uptime_create_daily_partitions();"` etc.
daily via `psql`).

For HA, set up one streaming replica (`pg_basebackup` + `primary_conninfo`)
and promote it manually on primary failure — automatic failover (Patroni/
repmgr) is worth adding once you're past the single-VPS stage.

### 2.2 Redis

```bash
sudo apt install -y redis-server
sudo sed -i 's/^# requirepass.*/requirepass CHANGE_ME/' /etc/redis/redis.conf
sudo sed -i 's/^appendonly no/appendonly no/' /etc/redis/redis.conf   # caches only: no persistence needed
sudo systemctl restart redis-server
```

Redis here holds only caches and short-TTL counters (monitor config cache,
failure counters, notification dedup) — losing it on restart just means a
brief wave of cache misses, not data loss. Don't bother with AOF/RDB
persistence or a cluster for the 50k baseline; a single instance with
maxmemory-policy `allkeys-lru` is enough.

```
maxmemory 1gb
maxmemory-policy allkeys-lru
```

### 2.3 NATS with JetStream

```bash
curl -L https://github.com/nats-io/nats-server/releases/latest/download/nats-server-linux-amd64.tar.gz | tar xz
sudo mv nats-server-*/nats-server /usr/local/bin/
```

3-node cluster config (`/etc/nats/nats-server.conf` on each node):

```
server_name: nats-1
listen: 0.0.0.0:4222
http: 0.0.0.0:8222

jetstream {
  store_dir: /var/lib/nats/jetstream
  max_mem: 1GB
  max_file: 40GB
}

cluster {
  name: uptime-cluster
  listen: 0.0.0.0:6222
  routes: [nats-route://nats-2:6222, nats-route://nats-3:6222]
}
```

Run as a systemd service (`ExecStart=/usr/local/bin/nats-server -c
/etc/nats/nats-server.conf`). Streams (`CHECKS`, `RESULTS`) are created
idempotently by the app itself on startup (`queue.EnsureStreams`) — no
manual `nats stream add` needed, though `nats stream ls` is useful for
inspecting them once running.

---

## 3. Build and ship the binaries

```bash
cd services/uptime-monitor
make build          # -> bin/scheduler, bin/prober, bin/resultprocessor, bin/api
```

Copy to each target VPS:

```bash
scp bin/scheduler        deploy-host:/opt/uptime-monitor/bin/
scp bin/prober            deploy-host:/opt/uptime-monitor/bin/
scp bin/resultprocessor   deploy-host:/opt/uptime-monitor/bin/
scp bin/api                deploy-host:/opt/uptime-monitor/bin/
```

Or build container images and push to your registry:

```bash
make docker-build
docker tag uptime-monitor-prober:latest registry.example.com/uptime-monitor-prober:latest
docker push registry.example.com/uptime-monitor-prober:latest
```

---

## 4. Deploy with systemd (bare VPS)

On every VPS that will run a service:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin uptime
sudo mkdir -p /opt/uptime-monitor/bin /etc/uptime-monitor
sudo chown -R uptime:uptime /opt/uptime-monitor
```

Copy `.env.example` to `/etc/uptime-monitor/uptime-monitor.env`, fill in
real DSNs/passwords, and restrict permissions:

```bash
sudo cp .env.example /etc/uptime-monitor/uptime-monitor.env
sudo chown root:uptime /etc/uptime-monitor/uptime-monitor.env
sudo chmod 640 /etc/uptime-monitor/uptime-monitor.env
```

Copy the relevant unit file(s) from `deploy/systemd/` to
`/etc/systemd/system/` and enable:

```bash
# Scheduler node
sudo cp deploy/systemd/uptime-scheduler.service /etc/systemd/system/
sudo systemctl enable --now uptime-scheduler

# Prober node — set UPTIME_REGION in the env file first, then start
# as many instances as you have spare cores (see §11 sizing).
sudo cp deploy/systemd/uptime-prober@.service /etc/systemd/system/
sudo systemctl enable --now uptime-prober@1
sudo systemctl enable --now uptime-prober@2

# Result processor / API nodes
sudo cp deploy/systemd/uptime-resultprocessor.service /etc/systemd/system/
sudo systemctl enable --now uptime-resultprocessor
sudo cp deploy/systemd/uptime-api.service /etc/systemd/system/
sudo systemctl enable --now uptime-api
```

Check logs with `journalctl -u uptime-prober@1 -f`.

### 4.1 Multi-region prober rollout

Each region is just a distinct `UPTIME_REGION` value plus a VPS placed in
that region's datacenter. To add a new region:

1. Provision a VPS in the target region (any provider with presence there).
2. Install the `prober` binary + env file with `UPTIME_REGION=<new-region>`.
3. Start it — `queue.EnsureConsumer` creates the region's durable NATS
   consumer automatically on first connect.
4. Add the region to the relevant monitors' `regions` array via the API
   (`PATCH`/recreate) so the scheduler starts publishing jobs for it.
5. Add a Prometheus scrape target for the new prober (see §6).

No coordination with other regions is required — NATS subjects are
partitioned per region (`checks.<region>`), so a new region's prober fleet
only ever sees its own jobs.

---

## 5. Zero-downtime deploys

All four services are stateless (state lives in Postgres/Redis/NATS), so a
rolling restart is safe:

```bash
# Build + ship new binary, then:
sudo systemctl restart uptime-prober@1
# wait for it to reconnect (check `journalctl -u uptime-prober@1 -f`), then:
sudo systemctl restart uptime-prober@2
```

- **scheduler**: a restarted replica re-reads its owned monitors from the
  consistent-hash ring within one reconcile cycle (15s default); in-flight
  dispatch state is not persisted, so at most a few due checks are delayed
  by a few hundred ms, never lost.
- **prober**: in-flight checks are abandoned on SIGTERM (systemd sends
  SIGTERM, then SIGKILL after 90s by default); their JetStream messages are
  redelivered (`AckWait`) and picked up by a surviving replica. Adaptive
  timeout state is periodically flushed to Postgres (`latencyFlushLoop`,
  every 30s) and reloaded on the next start, so restarts don't reset
  timeout sizing to cold-start defaults.
- **resultprocessor**: unacked messages simply redeliver to another
  replica; no special drain procedure needed.
- **api**: put it behind a load balancer with a health check on
  `/healthz` and drain connections normally.

Rollback is the same procedure with the previous binary/image.

---

## 6. Observability setup

Every service exposes Prometheus metrics on `UPTIME_METRICS_ADDR`
(default `:9090`; `api` serves `/metrics` on its main port instead — see
`deploy/prometheus/prometheus.yml`). Point your existing Prometheus at
`deploy/prometheus/prometheus.yml`'s scrape_configs, adding one static
target per prober region/replica.

Key metrics to build dashboards/alerts around:

| Metric | What it tells you |
|---|---|
| `uptime_checks_total{outcome}` | check volume and failure rate, by region |
| `uptime_check_duration_seconds` | latency distribution, by region |
| `uptime_worker_pool_in_flight{lane}` vs `uptime_worker_pool_capacity{lane}` | how close a prober is to saturating its pool |
| `uptime_worker_pool_dropped_total{lane}` | jobs rejected due to saturation — sustained >0 means you need more prober capacity |
| `uptime_circuit_breaker_open_count` | monitors currently quarantined per prober |
| `uptime_scheduler_owned_monitors` | monitors owned per scheduler replica — should be roughly even across replicas |
| `uptime_result_batch_write_seconds` | Postgres write latency for result batches |
| `uptime_incidents_opened_total` / `_resolved_total` | incident churn |
| `uptime_notifications_sent_total{result}` | alert delivery success rate |

Example alerting rules (Prometheus `alerting_rules.yml`):

```yaml
groups:
  - name: uptime-monitor
    rules:
      - alert: ProberPoolSaturated
        expr: rate(uptime_worker_pool_dropped_total[5m]) > 0
        for: 5m
        annotations: { summary: "Prober worker pool dropping jobs — scale out probers" }

      - alert: ResultProcessorLag
        expr: rate(uptime_result_batch_write_seconds_sum[5m]) / rate(uptime_result_batch_write_seconds_count[5m]) > 2
        for: 10m
        annotations: { summary: "Postgres batch writes are slow — check Postgres load/disk" }

      - alert: HighCircuitBreakerCount
        expr: uptime_circuit_breaker_open_count > 500
        for: 15m
        annotations: { summary: "Unusually many monitors quarantined — check for a regional network issue" }
```

For distributed tracing, run an OTel Collector (or point directly at
Tempo/Jaeger/Honeycomb's OTLP endpoint) and set `UPTIME_OTLP_ENDPOINT` in
the env file on every service. Leave it empty to disable tracing export
entirely (spans are created but dropped — near-zero overhead).

---

## 7. Backups

- **Postgres**: nightly `pg_dump` of the `monitors`, `incidents`,
  `notification_channels`, and `check_results_hourly` tables (skip raw
  `check_results` partitions — they're short-retention and not worth
  backing up) to off-box object storage:

  ```bash
  pg_dump "$UPTIME_POSTGRES_DSN" \
    -t monitors -t incidents -t notification_channels -t check_results_hourly -t latency_stats \
    | gzip > /backup/uptime-$(date +%F).sql.gz
  ```

  Also keep continuous WAL archiving to object storage if you need
  point-in-time recovery beyond the nightly dump.

- **Redis**: not backed up — it's fully derivable from Postgres (monitor
  cache) or safely lossy (failure counters, dedup keys) by design.

- **NATS JetStream**: file-backed streams already survive a NATS restart;
  for disaster recovery, `nats stream backup CHECKS /backup/checks-stream`
  is available but generally unnecessary since streams only hold ~minutes
  of transient job/result data (`MaxAge` in `EnsureStreams`).

---

## 8. Security checklist

- [ ] Postgres: `sslmode=require` in the DSN once you have certs;
      restrict `pg_hba.conf` to the app hosts' IPs only.
- [ ] Redis: `requirepass` set (done in §2.2); bind to a private interface,
      never expose 6379 publicly.
- [ ] NATS: enable TLS between cluster nodes and clients in production
      (`tls { cert_file, key_file, ca_file }` in `nats-server.conf`); use
      `UPTIME_NATS_CREDS_FILE` for client auth instead of no-auth.
- [ ] `api`: put behind a reverse proxy (nginx/Caddy) that terminates TLS
      and performs real authentication before setting `X-Owner-Id` — the
      handler in `cmd/api` trusts that header as-is (see the package doc
      comment in `cmd/api/main.go`).
- [ ] Firewall: only expose `api`'s port and each service's metrics port
      (restricted to your Prometheus host) publicly; everything else
      (Postgres, Redis, NATS, inter-service traffic) should be on a
      private network/VPC.
- [ ] Rotate `UPTIME_REDIS_PASSWORD` / Postgres password / NATS creds
      periodically; they're all read from the env file at process start,
      so a rotation requires a restart of affected services.

---

## 9. Capacity planning quick reference

Baseline: 50,000 monitors × 1 region × 30s interval ≈ 1,667 checks/sec
sustained.

| Scale | Checks/sec | Prober vCPUs needed* | Postgres write rows/day |
|---|---|---|---|
| 50,000 monitors | ~1,667/s | ~8-12 vCPU total | ~144M |
| 250,000 monitors | ~8,333/s | ~40-60 vCPU total | ~720M |
| 1,000,000 monitors | ~33,333/s | ~160-240 vCPU total | ~2.9B |

\* Rule of thumb from this codebase's HTTP prober: ~150-200 concurrent
in-flight checks per vCPU sustainably (I/O-bound, not CPU-bound — the
limiting factor is usually file descriptors and network bandwidth before
CPU). Each `prober` process defaults to a 500-slot main pool
(`UPTIME_WORKER_POOL_SIZE`); size vCPU count accordingly and run multiple
`prober@N` instances per box past ~4 vCPUs to use additional cores (Go's
scheduler parallelizes across a single process fine, but separate
processes give you independent NATS consumer parallelism too).

To scale past 50k, in order of what to add first:

1. **More prober replicas** in existing regions — linear scaling, no
   coordination needed (NATS work-queue distributes load automatically).
2. **More scheduler replicas** — only needed once a single replica's
   `uptime_scheduler_owned_monitors` × dispatch overhead approaches its
   200ms tick budget; one replica comfortably handles millions of owned
   monitors since dispatch is O(log n) heap operations, not O(n) scans.
3. **Postgres**: partition pruning already keeps write/query cost roughly
   constant per day regardless of total monitor count; the real limit is
   disk throughput for the `check_results` COPY writes — move to a bigger
   NVMe volume or shard `check_results` across multiple Postgres instances
   by region before it becomes a bottleneck (watch
   `uptime_result_batch_write_seconds`).
4. **NATS cluster**: add nodes and let JetStream rebalance; 3 nodes handles
   the 1M-monitor tier comfortably on modest hardware since messages are
   small (a few hundred bytes) and short-lived.

---

## 10. Runbooks

**Prober region falling behind (rising `uptime_worker_pool_dropped_total`)**
1. Check `uptime_worker_pool_in_flight` vs capacity — if consistently at
   capacity, add another `prober@N` instance for that region (§4.1) or a
   new VPS.
2. Check `uptime_circuit_breaker_open_count` — a spike usually means a
   real regional network problem (upstream ISP, DNS resolver issue) rather
   than a capacity problem; quarantined monitors free up the main pool
   automatically (that's what the quarantine lane is for), so this
   shouldn't itself cause drops — if it does, increase
   `UPTIME_QUARANTINE_POOL_SIZE`.

**Postgres disk filling up**
1. Check `SELECT pg_size_pretty(pg_total_relation_size('check_results'))`.
2. Confirm `uptime_drop_old_partitions` is actually running
   (`SELECT * FROM cron.job_run_details ORDER BY start_time DESC LIMIT 20;`
   if using pg_cron).
3. As an emergency measure, manually run
   `SELECT uptime_drop_old_partitions(3);` to shrink the retention window
   temporarily, then restore to 7 once disk pressure is resolved.

**JetStream consumer backlog growing (`nats consumer info CHECKS
prober-<region>` shows rising `Pending`)**
1. This means probers aren't pulling/acking fast enough for that region —
   same remediation as the first runbook above.
2. If it's the `RESULTS` stream backing up instead, check
   `resultprocessor` logs for repeated Postgres write failures (batches
   get Nak'd and redelivered on failure, which is the expected safe
   behavior, but sustained failures need the underlying Postgres issue
   fixed).

**A monitor's circuit breaker seems stuck open**
- Breaker state is in-process on whichever prober owns that monitor's
  region subject — it self-heals every `OpenDuration` (2 minutes default)
  by allowing a trial request. If it's been open far longer than that,
  the monitor is probably just genuinely down; check
  `check_results.error_message` for that monitor's recent rows to confirm.

**Notifications not arriving**
1. Check `uptime_notifications_sent_total{result="failure"}` — if
   elevated, check `resultprocessor` logs for the specific channel error
   (webhook non-2xx response, SMTP auth failure, etc.).
2. Confirm the dedup key isn't suppressing a legitimate resend — dedup TTL
   is 10 minutes (`internal/cache/redis/cache.go`), so a transition
   re-sent within that window for the *same* incident ID is expected to be
   suppressed; a new incident always gets a fresh ID and isn't affected.
