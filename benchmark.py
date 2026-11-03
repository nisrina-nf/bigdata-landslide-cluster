import csv
import math
import os
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory

# ------------------------- KONFIGURASI -------------------------

BASE_DIR = Path(__file__).resolve().parent
CSV_FILE = BASE_DIR / "glc_ready_cassandra.csv"
OUTPUT_DIR = BASE_DIR / "benchmark_results"

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "glc_disaster_ks")
TABLE = os.getenv("CASSANDRA_TABLE", "landslide_events")

ES_URL = os.getenv("ES_URL", "http://localhost:9200").rstrip("/")
ES_INDEX = os.getenv("ES_INDEX", "global_landslide_catalog")

ITERATIONS = int(os.getenv("BENCH_ITERATIONS", "100"))
WARMUP = int(os.getenv("BENCH_WARMUP", "10"))
REQUEST_TIMEOUT = 30

# ------------------------- DATASET SAMPLE -------------------------

def parse_cassandra_timestamp(value):
    """Ubah timestamp CSV menjadi datetime naive UTC untuk cassandra-driver."""
    value = str(value).strip()
    if not value:
        raise ValueError("event_date kosong pada sample CSV")

    # Mendukung ISO-8601 dengan Z/offset maupun format tanggal biasa.
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed

def load_sample():
    if not CSV_FILE.exists():
        raise FileNotFoundError(
            f"File dataset tidak ditemukan: {CSV_FILE}\n"
            "Simpan benchmark.py di root repository yang sama dengan CSV."
        )

    required = {"country_code", "event_year", "event_date", "event_id"}

    with CSV_FILE.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise RuntimeError("Header CSV tidak terbaca.")

        missing = required - set(reader.fieldnames)
        if missing:
            raise RuntimeError(
                f"Kolom CSV tidak ditemukan: {sorted(missing)}. "
                f"Header yang tersedia: {reader.fieldnames}"
            )

        for row in reader:
            try:
                country = str(row["country_code"]).strip()
                if not country:
                    continue

                return {
                    "country_code": country,
                    "event_year": int(row["event_year"]),
                    "event_date": parse_cassandra_timestamp(row["event_date"]),
                    "event_id": int(row["event_id"]),
                }
            except (TypeError, ValueError):
                continue

    raise RuntimeError("Tidak ada baris CSV yang memiliki primary key valid.")

# ------------------------- STATISTIK -------------------------

def percentile(values, percent):
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    index = max(0, math.ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


def summarize(samples):
    total_seconds = sum(samples) / 1000.0
    return {
        "iterations": len(samples),
        "avg_ms": statistics.mean(samples),
        "median_ms": statistics.median(samples),
        "min_ms": min(samples),
        "max_ms": max(samples),
        "p95_ms": percentile(samples, 95),
        "stddev_ms": statistics.stdev(samples) if len(samples) > 1 else 0.0,
        "throughput_ops_sec": len(samples) / total_seconds if total_seconds else 0.0,
    }


def timed_call(fn):
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0

# ------------------------- BENCHMARK -------------------------

def main():
    if ITERATIONS < 1:
        raise ValueError("BENCH_ITERATIONS minimal 1.")

    sample = load_sample()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print("BENCHMARK APACHE CASSANDRA vs ELASTICSEARCH")
    print("=" * 72)
    print(f"Dataset sample : {sample}")
    print(f"Iterasi        : {ITERATIONS} (warm-up {WARMUP} kali)")
    print(f"Cassandra      : {CASSANDRA_HOST}:{CASSANDRA_PORT}/{KEYSPACE}.{TABLE}")
    print(f"Elasticsearch  : {ES_URL}/{ES_INDEX}")
    print()

    # Koneksi dan validasi service.
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)
    session.row_factory = tuple_factory

    es = requests.Session()
    es.headers.update({"Content-Type": "application/json"})

    try:
        health = es.get(f"{ES_URL}/_cluster/health", timeout=REQUEST_TIMEOUT)
        health.raise_for_status()

        index_check = es.get(f"{ES_URL}/{ES_INDEX}", timeout=REQUEST_TIMEOUT)
        index_check.raise_for_status()

        # Prepare query sesuai primary key pada schema.cql:
        # PRIMARY KEY ((country_code, event_year), event_date, event_id)
        cql_partition = session.prepare(
            f"SELECT country_code, event_year, event_date, event_id "
            f"FROM {TABLE} WHERE country_code = ? AND event_year = ? LIMIT 5"
        )
        cql_event = session.prepare(
            f"SELECT country_code, event_year, event_date, event_id "
            f"FROM {TABLE} WHERE country_code = ? AND event_year = ? "
            f"AND event_date = ? AND event_id = ?"
        )

        # Operasi yang setara pada kedua sistem.
        def cassandra_partition():
            rows = session.execute(
                cql_partition,
                (sample["country_code"], sample["event_year"])
            )
            list(rows)

        def elasticsearch_partition():
            query = {
                "size": 5,
                "track_total_hits": False,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"country_code": sample["country_code"]}},
                            {"term": {"event_year": sample["event_year"]}},
                        ]
                    }
                },
            }
            response = es.post(
                f"{ES_URL}/{ES_INDEX}/_search",
                json=query,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            response.json()

        def cassandra_event_lookup():
            rows = session.execute(
                cql_event,
                (
                    sample["country_code"],
                    sample["event_year"],
                    sample["event_date"],
                    sample["event_id"],
                ),
            )
            list(rows)

        def elasticsearch_event_lookup():
            query = {
                "size": 1,
                "track_total_hits": False,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"country_code": sample["country_code"]}},
                            {"term": {"event_year": sample["event_year"]}},
                            {"term": {"event_date": sample["event_date"].isoformat()}},
                            {"term": {"event_id": sample["event_id"]}},
                        ]
                    }
                },
            }
            response = es.post(
                f"{ES_URL}/{ES_INDEX}/_search",
                json=query,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            response.json()

        tests = [
            ("partition_filter", cassandra_partition, elasticsearch_partition),
            ("primary_key_lookup", cassandra_event_lookup, elasticsearch_event_lookup),
        ]

        raw_results = []
        summary_rows = []

        for operation, cassandra_fn, elasticsearch_fn in tests:
            print(f"\n--- {operation} ---")

            # Warm-up tidak dimasukkan ke hasil statistik.
            for _ in range(WARMUP):
                cassandra_fn()
                elasticsearch_fn()

            cassandra_times = []
            elasticsearch_times = []

            # Urutan dibalik setiap iterasi untuk mengurangi bias urutan eksekusi.
            for iteration in range(ITERATIONS):
                if iteration % 2 == 0:
                    order = [
                        ("Cassandra", cassandra_fn, cassandra_times),
                        ("Elasticsearch", elasticsearch_fn, elasticsearch_times),
                    ]
                else:
                    order = [
                        ("Elasticsearch", elasticsearch_fn, elasticsearch_times),
                        ("Cassandra", cassandra_fn, cassandra_times),
                    ]

                for database, fn, destination in order:
                    duration = timed_call(fn)
                    destination.append(duration)
                    raw_results.append({
                        "operation": operation,
                        "database": database,
                        "iteration": iteration + 1,
                        "latency_ms": round(duration, 6),
                    })

                if (iteration + 1) % 20 == 0 or iteration + 1 == ITERATIONS:
                    print(f"  selesai {iteration + 1}/{ITERATIONS}")

            for database, times in (
                ("Cassandra", cassandra_times),
                ("Elasticsearch", elasticsearch_times),
            ):
                stats = summarize(times)
                summary_rows.append({
                    "operation": operation,
                    "database": database,
                    **{key: round(value, 6) if isinstance(value, float) else value
                       for key, value in stats.items()},
                })

        # Simpan hasil detail.
        raw_path = OUTPUT_DIR / "benchmark_results.csv"
        with raw_path.open("w", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=["operation", "database", "iteration", "latency_ms"]
            )
            writer.writeheader()
            writer.writerows(raw_results)

        # Simpan ringkasan.
        summary_path = OUTPUT_DIR / "benchmark_summary.csv"
        fields = [
            "operation", "database", "iterations", "avg_ms", "median_ms",
            "min_ms", "max_ms", "p95_ms", "stddev_ms", "throughput_ops_sec"
        ]
        with summary_path.open("w", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(summary_rows)

        print("\n" + "=" * 72)
        print("RINGKASAN BENCHMARK (latency lebih kecil = lebih cepat)")
        print("=" * 72)
        for row in summary_rows:
            print(
                f"{row['operation']:20s} | {row['database']:13s} | "
                f"avg={row['avg_ms']:.3f} ms | "
                f"median={row['median_ms']:.3f} ms | "
                f"p95={row['p95_ms']:.3f} ms | "
                f"throughput={row['throughput_ops_sec']:.2f} ops/s"
            )

        print("\nFile hasil:")
        print(f"- {raw_path}")
        print(f"- {summary_path}")
        print("\nBenchmark selesai.")

    finally:
        es.close()
        session.shutdown()
        cluster.shutdown()

if __name__ == "__main__":
    main()