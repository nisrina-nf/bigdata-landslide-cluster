import csv
import json
import math
import os
import statistics
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from cassandra.cluster import Cluster
from cassandra.query import ConsistencyLevel


# ----------------------------- CONFIG -----------------------------

BASE_DIR = Path(__file__).resolve().parent
CSV_FILE = BASE_DIR / "glc_ready_cassandra.csv"
OUT_DIR = BASE_DIR / "benchmark_results"

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "glc_disaster_ks")
TABLE = os.getenv("CASSANDRA_TABLE", "landslide_events")

ES_URL = os.getenv("ES_URL", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "global_landslide_catalog")

ITERATIONS = int(os.getenv("BENCH_ITERATIONS", "100"))
WARMUP = int(os.getenv("BENCH_WARMUP", "10"))
TIMEOUT = int(os.getenv("BENCH_TIMEOUT", "60"))
RESOURCE_INTERVAL = float(os.getenv("RESOURCE_INTERVAL", "1.0"))

RUN_ID = uuid.uuid4().hex[:10]
TEMP_CASSANDRA_TABLE = f"bench_landslide_{RUN_ID}"
TEMP_ES_INDEX = f"bench-landslide-{RUN_ID}"

DETAILS = []
SUMMARIES = []
RESOURCE_SAMPLES = []
RESOURCE_STOP = threading.Event()


# ----------------------------- HELPERS -----------------------------

def parse_datetime(value):
    value = str(value or "").strip()
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def iso_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat() + ("Z" if value.tzinfo is None else "")
    return str(value)


def load_dataset():
    if not CSV_FILE.exists():
        raise FileNotFoundError(
            f"Dataset tidak ditemukan: {CSV_FILE}\n"
            "Letakkan benchmark.py di root repository."
        )

    rows = []
    with CSV_FILE.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {
            "country_code", "event_year", "event_date", "event_id",
            "event_title", "event_description", "notes",
            "landslide_category", "longitude", "latitude",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Kolom CSV tidak ditemukan: {sorted(missing)}")

        for raw in reader:
            try:
                event_date = parse_datetime(raw["event_date"])
                if not raw["country_code"].strip() or event_date is None:
                    continue

                row = dict(raw)
                row["country_code"] = raw["country_code"].strip()
                row["event_year"] = int(raw["event_year"])
                row["event_date"] = event_date
                row["event_id"] = int(float(raw["event_id"]))
                row["event_title"] = raw.get("event_title") or ""
                row["event_description"] = raw.get("event_description") or ""
                row["notes"] = raw.get("notes") or ""
                row["landslide_category"] = raw.get("landslide_category") or ""

                for key in ("longitude", "latitude"):
                    try:
                        row[key] = float(raw[key])
                    except (TypeError, ValueError):
                        row[key] = None

                rows.append(row)
            except (TypeError, ValueError, KeyError):
                continue

    if len(rows) < ITERATIONS:
        raise ValueError(
            f"Data valid hanya {len(rows)} baris, sedangkan iterasi insert "
            f"{ITERATIONS}. Kurangi BENCH_ITERATIONS atau periksa CSV."
        )
    return rows


def percentile(values, pct):
    vals = sorted(values)
    if not vals:
        return 0.0
    return vals[max(0, math.ceil(pct * len(vals)) - 1)]


def record_result(stage, operation, database, iteration, latency_ms):
    DETAILS.append({
        "stage": stage,
        "operation": operation,
        "database": database,
        "iteration": iteration,
        "latency_ms": round(latency_ms, 6),
    })


def run_repeated(stage, operation, database, function, count=ITERATIONS):
    print(f"  {database}: {operation} ({count} iterasi)")
    for _ in range(WARMUP):
        function()

    values = []
    for i in range(1, count + 1):
        start = time.perf_counter()
        function()
        elapsed = (time.perf_counter() - start) * 1000
        values.append(elapsed)
        record_result(stage, operation, database, i, elapsed)

    total_seconds = sum(values) / 1000
    SUMMARIES.append({
        "stage": stage,
        "operation": operation,
        "database": database,
        "iterations": count,
        "avg_ms": statistics.mean(values),
        "median_ms": statistics.median(values),
        "min_ms": min(values),
        "max_ms": max(values),
        "p95_ms": percentile(values, 0.95),
        "stddev_ms": statistics.stdev(values) if len(values) > 1 else 0,
        "throughput_ops_sec": count / total_seconds if total_seconds else 0,
    })


def es_request(method, path, **kwargs):
    response = requests.request(
        method, f"{ES_URL}{path}", timeout=TIMEOUT, **kwargs
    )
    response.raise_for_status()
    if response.text:
        return response.json()
    return {}


# ----------------------------- RESOURCE MONITOR -----------------------------

def docker_stats_monitor(container_names):
    """Sampling resource Docker; perlu Docker CLI tersedia di PATH."""
    while not RESOURCE_STOP.is_set():
        stamp = datetime.now().isoformat(timespec="seconds")
        for name in container_names:
            try:
                result = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{json .}}", name],
                    capture_output=True, text=True, timeout=10, check=False,
                )
                if result.returncode == 0 and result.stdout.strip():
                    item = json.loads(result.stdout.strip().splitlines()[0])
                    RESOURCE_SAMPLES.append({
                        "timestamp": stamp,
                        "container": name,
                        "cpu_percent": item.get("CPUPerc", "").replace("%", ""),
                        "memory_usage": item.get("MemUsage", ""),
                        "memory_percent": item.get("MemPerc", "").replace("%", ""),
                    })
            except Exception:
                pass
        RESOURCE_STOP.wait(RESOURCE_INTERVAL)


# ----------------------------- CASSANDRA / ES SETUP -----------------------------

def main():
    if ITERATIONS < 1:
        raise ValueError("BENCH_ITERATIONS harus minimal 1.")

    rows = load_dataset()
    sample = rows[0]
    update_row = sample
    update_original_notes = sample["notes"]

    print("=" * 72)
    print("BENCHMARK CASSANDRA vs ELASTICSEARCH")
    print(f"Dataset valid       : {len(rows)} baris")
    print(f"Iterasi             : {ITERATIONS}; warm-up: {WARMUP}")
    print(f"Cassandra           : {CASSANDRA_HOST}:{CASSANDRA_PORT}/{KEYSPACE}.{TABLE}")
    print(f"Elasticsearch       : {ES_URL}/{ES_INDEX}")
    print("=" * 72)

    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)
    session.default_timeout = TIMEOUT
    es = requests.Session()
    es.headers.update({"Content-Type": "application/json"})

    temp_table_created = False
    temp_index_created = False
    update_restore_needed = False

    try:
        # Verify services and target resources.
        es_request("GET", "/_cluster/health")
        es_request("GET", f"/{ES_INDEX}")
        session.execute(f"SELECT country_code FROM {TABLE} LIMIT 1").one()

        # Retrieve the exact document _id from the existing Elasticsearch index.
        identity_query = {
            "size": 1,
            "_source": ["notes"],
            "query": {"bool": {"filter": [
                {"term": {"country_code": sample["country_code"]}},
                {"term": {"event_year": sample["event_year"]}},
                {"term": {"event_id": sample["event_id"]}},
                {"term": {"event_date": iso_datetime(sample["event_date"])}},
            ]}},
        }
        found = es_request(
            "POST", f"/{ES_INDEX}/_search", json=identity_query
        )
        hits = found.get("hits", {}).get("hits", [])
        if not hits:
            raise RuntimeError(
                "Record sample CSV tidak ditemukan di Elasticsearch. "
                "Pastikan dataset/index sinkron dan mapping tanggal sesuai."
            )
        es_doc_id = hits[0]["_id"]

        # Prepared Cassandra statements use the table's actual primary key:
        # ((country_code, event_year), event_date, event_id)
        cql_partition = session.prepare(
            f"SELECT country_code, event_year, event_date, event_id "
            f"FROM {TABLE} WHERE country_code=? AND event_year=? LIMIT 5"
        )
        cql_lookup = session.prepare(
            f"SELECT country_code, event_year, event_date, event_id, notes "
            f"FROM {TABLE} WHERE country_code=? AND event_year=? "
            f"AND event_date=? AND event_id=?"
        )
        cql_range = session.prepare(
            f"SELECT country_code, event_year, event_date, event_id "
            f"FROM {TABLE} WHERE country_code=? AND event_year=? "
            f"AND event_date>=? AND event_date<=? LIMIT 20"
        )
        cql_get_notes = session.prepare(
            f"SELECT notes FROM {TABLE} WHERE country_code=? AND event_year=? "
            f"AND event_date=? AND event_id=?"
        )
        cql_update = session.prepare(
            f"UPDATE {TABLE} SET notes=? WHERE country_code=? AND event_year=? "
            f"AND event_date=? AND event_id=?"
        )

        pk = (
            sample["country_code"], sample["event_year"],
            sample["event_date"], sample["event_id"],
        )

        # Start resource sampling (Docker container names can be overridden).
        cass_container = os.getenv("CASSANDRA_CONTAINER", "glc-cassandra")
        es_container = os.getenv("ELASTICSEARCH_CONTAINER", "glc-elasticsearch")
        monitor = threading.Thread(
            target=docker_stats_monitor,
            args=([cass_container, es_container],),
            daemon=True,
        )
        monitor.start()

        # ============================ 1. READ ============================
        print("\n[1/5] READ PERFORMANCE")

        def cass_read():
            list(session.execute(cql_partition, (sample["country_code"], sample["event_year"])))

        def es_read():
            es_request("POST", f"/{ES_INDEX}/_search", json={
                "size": 5, "_source": ["country_code", "event_year", "event_date", "event_id"],
                "query": {"bool": {"filter": [
                    {"term": {"country_code": sample["country_code"]}},
                    {"term": {"event_year": sample["event_year"]}},
                ]}},
            })

        run_repeated("1_read", "partition_read", "Cassandra", cass_read)
        run_repeated("1_read", "partition_read", "Elasticsearch", es_read)

        def cass_lookup():
            list(session.execute(cql_lookup, pk))

        def es_lookup():
            es_request("POST", f"/{ES_INDEX}/_search", json={
                "size": 1, "_source": ["country_code", "event_year", "event_date", "event_id", "notes"],
                "query": {"bool": {"filter": [
                    {"term": {"country_code": sample["country_code"]}},
                    {"term": {"event_year": sample["event_year"]}},
                    {"term": {"event_id": sample["event_id"]}},
                    {"term": {"event_date": iso_datetime(sample["event_date"])}},
                ]}},
            })

        run_repeated("1_read", "primary_key_lookup", "Cassandra", cass_lookup)
        run_repeated("1_read", "primary_key_lookup", "Elasticsearch", es_lookup)

        # ============================ 2. WRITE ============================
        print("\n[2/5] WRITE / INSERT PERFORMANCE")
        session.execute(f"""
            CREATE TABLE {TEMP_CASSANDRA_TABLE} (
                country_code text, event_year int, event_date timestamp,
                event_id bigint, event_title text, event_description text,
                notes text, landslide_category text, longitude double, latitude double,
                PRIMARY KEY ((country_code, event_year), event_date, event_id)
            )
        """)
        temp_table_created = True

        es_request("PUT", f"/{TEMP_ES_INDEX}", json={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
            "mappings": {"properties": {
                "country_code": {"type": "keyword"},
                "event_year": {"type": "integer"},
                "event_date": {"type": "date"},
                "event_id": {"type": "long"},
                "event_title": {"type": "text"},
                "event_description": {"type": "text"},
                "notes": {"type": "text"},
                "landslide_category": {"type": "keyword"},
                "longitude": {"type": "double"},
                "latitude": {"type": "double"},
            }},
        })
        temp_index_created = True

        cql_insert = session.prepare(
            f"INSERT INTO {TEMP_CASSANDRA_TABLE} "
            "(country_code,event_year,event_date,event_id,event_title,event_description,"
            "notes,landslide_category,longitude,latitude) VALUES (?,?,?,?,?,?,?,?,?,?)"
        )
        cql_insert.consistency_level = ConsistencyLevel.ONE

        write_rows = rows[:ITERATIONS]

        def cass_insert_at(i):
            r = write_rows[i]
            event_id = r["event_id"] + 1_000_000_000_000 + i
            session.execute(cql_insert, (
                r["country_code"], r["event_year"], r["event_date"], event_id,
                r["event_title"], r["event_description"], r["notes"],
                r["landslide_category"], r["longitude"], r["latitude"],
            ))

        def es_insert_at(i):
            r = write_rows[i]
            event_id = r["event_id"] + 1_000_000_000_000 + i
            doc = {
                "country_code": r["country_code"], "event_year": r["event_year"],
                "event_date": iso_datetime(r["event_date"]), "event_id": event_id,
                "event_title": r["event_title"], "event_description": r["event_description"],
                "notes": r["notes"], "landslide_category": r["landslide_category"],
                "longitude": r["longitude"], "latitude": r["latitude"],
            }
            es_request("PUT", f"/{TEMP_ES_INDEX}/_doc/{event_id}", json=doc, params={"refresh": "false"})

        # Per-index operation timing; each iteration writes one equivalent record.
        for db, fn in (("Cassandra", cass_insert_at), ("Elasticsearch", es_insert_at)):
            print(f"  {db}: insert ({ITERATIONS} iterasi)")
            vals = []
            for i in range(ITERATIONS):
                start = time.perf_counter()
                fn(i)
                elapsed = (time.perf_counter() - start) * 1000
                vals.append(elapsed)
                record_result("2_write", "insert_one_record", db, i + 1, elapsed)
            total = sum(vals) / 1000
            SUMMARIES.append({
                "stage": "2_write", "operation": "insert_one_record", "database": db,
                "iterations": ITERATIONS, "avg_ms": statistics.mean(vals),
                "median_ms": statistics.median(vals), "min_ms": min(vals),
                "max_ms": max(vals), "p95_ms": percentile(vals, .95),
                "stddev_ms": statistics.stdev(vals) if len(vals) > 1 else 0,
                "throughput_ops_sec": ITERATIONS / total if total else 0,
            })

        es_request("POST", f"/{TEMP_ES_INDEX}/_refresh")

        # ============================ 3. UPDATE ============================
        print("\n[3/5] UPDATE PERFORMANCE")
        update_restore_needed = True

        def cass_update(i):
            new_notes = f"benchmark-update-{RUN_ID}-{i}"
            session.execute(cql_update, (
                new_notes, sample["country_code"], sample["event_year"],
                sample["event_date"], sample["event_id"],
            ))

        def es_update(i):
            new_notes = f"benchmark-update-{RUN_ID}-{i}"
            es_request("POST", f"/{ES_INDEX}/_update/{es_doc_id}", json={
                "doc": {"notes": new_notes}
            })

        run_repeated("3_update", "update_notes", "Cassandra", lambda: cass_update(1))
        run_repeated("3_update", "update_notes", "Elasticsearch", lambda: es_update(1))

        # Restore original value before continuing.
        session.execute(cql_update, (
            update_original_notes, sample["country_code"], sample["event_year"],
            sample["event_date"], sample["event_id"],
        ))
        es_request("POST", f"/{ES_INDEX}/_update/{es_doc_id}", json={
            "doc": {"notes": update_original_notes}
        })
        update_restore_needed = False

        # ============================ 4. QUERY ============================
        print("\n[4/5] QUERY PERFORMANCE")

        # Query range uses the same partition and same date interval on both DBs.
        start_date = sample["event_date"].replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date.replace(hour=23, minute=59, second=59, microsecond=999000)

        def cass_range_query():
            list(session.execute(cql_range, (
                sample["country_code"], sample["event_year"], start_date, end_date
            )))

        def es_range_query():
            es_request("POST", f"/{ES_INDEX}/_search", json={
                "size": 20,
                "_source": ["country_code", "event_year", "event_date", "event_id"],
                "query": {"bool": {"filter": [
                    {"term": {"country_code": sample["country_code"]}},
                    {"term": {"event_year": sample["event_year"]}},
                    {"range": {"event_date": {
                        "gte": iso_datetime(start_date), "lte": iso_datetime(end_date)
                    }}},
                ]}},
            })

        run_repeated("4_query", "partition_and_date_range", "Cassandra", cass_range_query)
        run_repeated("4_query", "partition_and_date_range", "Elasticsearch", es_range_query)

        # Elasticsearch-specific capabilities: do not compare these as like-for-like
        # operations with Cassandra.
        print("  Elasticsearch: full-text, geo-distance, aggregation")

        def es_full_text():
            es_request("POST", f"/{ES_INDEX}/_search", json={
                "size": 5, "query": {"multi_match": {
                    "query": "landslide",
                    "fields": ["event_title", "event_description", "location_description", "notes"],
                }},
            })

        def es_geo():
            if sample["latitude"] is None or sample["longitude"] is None:
                raise RuntimeError("Sample CSV tidak memiliki latitude/longitude untuk geo test.")
            es_request("POST", f"/{ES_INDEX}/_search", json={
                "size": 5, "query": {"geo_distance": {
                    "distance": "100km",
                    "geo_point": {"lat": sample["latitude"], "lon": sample["longitude"]},
                }},
            })

        def es_aggregation():
            es_request("POST", f"/{ES_INDEX}/_search", json={
                "size": 0, "aggs": {
                    "jumlah_per_negara": {"terms": {"field": "country_name", "size": 10}},
                    "jumlah_per_kategori": {"terms": {"field": "landslide_category", "size": 10}},
                    "jumlah_per_tahun": {"terms": {"field": "event_year", "size": 15}},
                },
            })

        run_repeated("4_query_special", "full_text_search", "Elasticsearch", es_full_text)
        run_repeated("4_query_special", "geo_distance_search", "Elasticsearch", es_geo)
        run_repeated("4_query_special", "aggregations", "Elasticsearch", es_aggregation)

        # ============================ 5. RESOURCE ============================
        print("\n[5/5] RESOURCE UTILIZATION")
        # Keep monitor alive briefly to collect a final sample.
        time.sleep(max(RESOURCE_INTERVAL, 1.1))
        RESOURCE_STOP.set()
        monitor.join(timeout=5)

        # Cleanup only the temporary benchmark table and index.
        if temp_table_created:
            session.execute(f"DROP TABLE {TEMP_CASSANDRA_TABLE}")
            temp_table_created = False
        if temp_index_created:
            es_request("DELETE", f"/{TEMP_ES_INDEX}")
            temp_index_created = False

        # Save outputs.
        OUT_DIR.mkdir(exist_ok=True)
        details_path = OUT_DIR / "benchmark_results.csv"
        summary_path = OUT_DIR / "benchmark_summary.csv"
        resource_path = OUT_DIR / "benchmark_resource_usage.csv"

        with details_path.open("w", newline="", encoding="utf-8-sig") as f:
            fields = ["stage", "operation", "database", "iteration", "latency_ms"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(DETAILS)

        summary_fields = [
            "stage", "operation", "database", "iterations", "avg_ms", "median_ms",
            "min_ms", "max_ms", "p95_ms", "stddev_ms", "throughput_ops_sec",
        ]
        with summary_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=summary_fields)
            writer.writeheader()
            for row in SUMMARIES:
                writer.writerow({k: round(v, 6) if isinstance(v, float) else v
                                 for k, v in row.items()})

        with resource_path.open("w", newline="", encoding="utf-8-sig") as f:
            fields = ["timestamp", "container", "cpu_percent", "memory_usage", "memory_percent"]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(RESOURCE_SAMPLES)

        print("\n" + "=" * 72)
        print("RINGKASAN")
        print("=" * 72)
        for row in SUMMARIES:
            print(
                f"{row['stage']:16s} | {row['operation']:25s} | "
                f"{row['database']:13s} | avg {row['avg_ms']:.3f} ms | "
                f"P95 {row['p95_ms']:.3f} ms | "
                f"{row['throughput_ops_sec']:.2f} ops/s"
            )

        print("\nFile hasil:")
        print(f"- {details_path}")
        print(f"- {summary_path}")
        print(f"- {resource_path}")
        if not RESOURCE_SAMPLES:
            print(
                "\nPERINGATAN: Docker stats tidak menghasilkan sampel. "
                "Pastikan Docker CLI aktif dan nama container benar. "
                "Atur CASSANDRA_CONTAINER dan ELASTICSEARCH_CONTAINER bila perlu."
            )
        print("\nBenchmark selesai.")

    finally:
        RESOURCE_STOP.set()
        # Restore original notes even if a later test fails.
        if update_restore_needed:
            try:
                session.execute(cql_update, (
                    update_original_notes, sample["country_code"], sample["event_year"],
                    sample["event_date"], sample["event_id"],
                ))
                es_request("POST", f"/{ES_INDEX}/_update/{es_doc_id}", json={
                    "doc": {"notes": update_original_notes}
                })
            except Exception as restore_error:
                print(f"PERINGATAN: gagal memulihkan notes: {restore_error}")

        if temp_table_created:
            try:
                session.execute(f"DROP TABLE {TEMP_CASSANDRA_TABLE}")
            except Exception as cleanup_error:
                print(f"PERINGATAN cleanup Cassandra: {cleanup_error}")
        if temp_index_created:
            try:
                es_request("DELETE", f"/{TEMP_ES_INDEX}")
            except Exception as cleanup_error:
                print(f"PERINGATAN cleanup Elasticsearch: {cleanup_error}")

        es.close()
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    main()