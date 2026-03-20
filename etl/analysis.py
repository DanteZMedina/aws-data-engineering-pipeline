from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder \
    .appName("NYC Analysis") \
    .getOrCreate()

# Leer datos procesados
df = spark.read.parquet("/home/ubuntu/nyc_project/data/processed/yellow")

# Leer dataset de zonas
zones = spark.read.csv(
    "/home/ubuntu/nyc_project/data/taxi_zone_lookup.csv",
    header=True,
    inferSchema=True
)

# JOIN
df_joined = df.join(
    zones,
    df.PULocationID == zones.LocationID,
    "left"
)

# -----------------------------
# ANALISIS
# -----------------------------

print("Schema final:")
df_joined.printSchema()

print("\nPromedio de tarifa:")
df_joined.select(avg("fare_amount")).show()

print("\nTotal de viajes:")
df_joined.select(count("*")).show()

print("\nTop zonas por tarifa promedio:")
df_joined.groupBy("Zone") \
    .agg(avg("fare_amount").alias("avg_fare")) \
    .orderBy("avg_fare", ascending=False) \
    .show(10)

print("\nTop zonas con más viajes:")
df_joined.groupBy("Zone") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10)

spark.stop()