import time
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import subprocess

print("--- 1. Konversi Data ke Apache Parquet & Tulis ke HDFS ---")
df = pd.read_csv("glc_ready_cassandra.csv")

start_write = time.time()

# Konversi DataFrame ke tabel PyArrow dan simpan sebagai Parquet (Kompresi Snappy)
local_parquet = "glc_baseline.parquet"
table = pa.Table.from_pandas(df)
pq.write_table(table, local_parquet, compression='SNAPPY')

# Buat direktori di HDFS dan upload file Parquet ke dalam kontainer HDFS
subprocess.run(["docker", "exec", "glc-hdfs-namenode", "hdfs", "dfs", "-mkdir", "-p", "/glc_data"], check=True)
subprocess.run(["docker", "cp", local_parquet, "glc-hdfs-namenode:/tmp/glc_baseline.parquet"], check=True)
subprocess.run(["docker", "exec", "glc-hdfs-namenode", "hdfs", "dfs", "-put", "-f", "/tmp/glc_baseline.parquet", "/glc_data/"], check=True)

durasi_write = time.time() - start_write
ukuran_parquet = os.path.getsize(local_parquet)

print(f"Waktu Tulis (CSV -> Parquet -> HDFS): {durasi_write:.3f} detik")
print(f"Ukuran File Parquet di Disk: {ukuran_parquet:,} bytes (~{ukuran_parquet / (1024*1024):.2f} MB)\n")

print("--- 2. Cek Statistik Ruang di HDFS ---")
subprocess.run(["docker", "exec", "glc-hdfs-namenode", "hdfs", "dfs", "-du", "-h", "/glc_data/"])