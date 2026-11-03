import time
import json
import pandas as pd
import requests
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel

# ==========================================
# BAGIAN 1: INGESTION KE APACHE CASSANDRA
# ==========================================
print("--- Memulai Ingestion ke Apache Cassandra ---")
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('glc_disaster_ks')

df = pd.read_csv("glc_ready_cassandra.csv").fillna('')

# Mengonversi kolom tanggal kosong menjadi None agar diterima tipe timestamp CQL
date_cols = ['event_date', 'submitted_date', 'created_date', 'last_edited_date']
for col in date_cols:
    df[col] = pd.to_datetime(df[col], errors='coerce')

insert_query = session.prepare("""
    INSERT INTO landslide_events (
        country_code, event_year, event_date, event_id, source_name, source_link,
        event_time, event_title, event_description, location_description,
        location_accuracy, landslide_category, landslide_trigger, landslide_size,
        landslide_setting, fatality_count, injury_count, storm_name, photo_link,
        notes, event_import_source, event_import_id, country_name,
        admin_division_name, admin_division_population, gazeteer_closest_point,
        gazeteer_distance, submitted_date, created_date, last_edited_date,
        longitude, latitude
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
# Set Tunable Consistency sesuai proposal (ONE untuk kecepatan tulis tinggi)
insert_query.consistency_level = ConsistencyLevel.ONE

start_cas = time.time()
success_cas = 0

for _, row in df.iterrows():
    params = (
        str(row['country_code']), int(row['event_year']),
        row['event_date'].to_pydatetime() if pd.notna(row['event_date']) else None,
        int(row['event_id']), str(row['source_name']), str(row['source_link']),
        str(row['event_time']), str(row['event_title']), str(row['event_description']),
        str(row['location_description']), str(row['location_accuracy']),
        str(row['landslide_category']), str(row['landslide_trigger']),
        str(row['landslide_size']), str(row['landslide_setting']),
        int(row['fatality_count']), int(row['injury_count']), str(row['storm_name']),
        str(row['photo_link']), str(row['notes']), str(row['event_import_source']),
        float(row['event_import_id']), str(row['country_name']),
        str(row['admin_division_name']), int(row['admin_division_population']),
        str(row['gazeteer_closest_point']), float(row['gazeteer_distance']),
        row['submitted_date'].to_pydatetime() if pd.notna(row['submitted_date']) else None,
        row['created_date'].to_pydatetime() if pd.notna(row['created_date']) else None,
        row['last_edited_date'].to_pydatetime() if pd.notna(row['last_edited_date']) else None,
        float(row['longitude']), float(row['latitude'])
    )
    session.execute(insert_query, params)
    success_cas += 1

durasi_cas = time.time() - start_cas
print(f"Selesai Cassandra: {success_cas} baris dalam {durasi_cas:.2f} detik ({success_cas/durasi_cas:.0f} baris/detik)\n")
cluster.shutdown()

# ==========================================
# BAGIAN 2: BULK INGESTION KE ELASTICSEARCH
# ==========================================
print("--- Memulai Bulk Ingestion ke Elasticsearch ---")
es_url = "http://localhost:9200/_bulk"
headers = {"Content-Type": "application/x-ndjson"}

start_es = time.time()
batch_lines = []
batch_size = 2000 # 1000 dokumen per request bulk (2 baris per dokumen)
total_docs = 0

with open("glc_ready_elasticsearch.json", "r", encoding="utf-8") as f:
    for line in f:
        batch_lines.append(line)
        if len(batch_lines) >= batch_size:
            payload = "".join(batch_lines)
            res = requests.post(es_url, data=payload.encode('utf-8'), headers=headers)
            total_docs += len(batch_lines) // 2
            batch_lines = []

    # Kirim sisa baris terakhir
    if batch_lines:
        payload = "".join(batch_lines)
        res = requests.post(es_url, data=payload.encode('utf-8'), headers=headers)
        total_docs += len(batch_lines) // 2

durasi_es = time.time() - start_es
print(f"Selesai Elasticsearch: {total_docs} dokumen dalam {durasi_es:.2f} detik ({total_docs/durasi_es:.0f} dok/detik)")