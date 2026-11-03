
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GLC-HDFS-Parquet-Test")
    .master("local[2]")
    .config(
        "spark.hadoop.fs.defaultFS",
        "hdfs://hdfs-namenode:9000"
    )
    .getOrCreate()
)

print("\n=== MEMBACA DATA PARQUET DARI HDFS ===")

df = spark.read.parquet(
    "hdfs://hdfs-namenode:9000/glc_data/glc_baseline.parquet"
)

print("\n=== STRUKTUR DATA ===")
df.printSchema()

print("\n=== 5 DATA PERTAMA ===")
df.show(5, truncate=False)

print("\n=== JUMLAH DATA ===")
print("Total records:", df.count())

spark.stop()
