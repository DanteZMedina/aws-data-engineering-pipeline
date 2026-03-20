from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("NYC ETL") \
    .getOrCreate()
    
# Config acceso a s3 
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-west-1.amazonaws.com")
print("📥 Leyendo archivos desde S3...")

# Leer TODOS los archivos como lista
paths = spark._jvm.org.apache.hadoop.fs.FileSystem \
    .get(spark._jsc.hadoopConfiguration()) \
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path("s3a://xideralaws-curso-dante/raw/"))

dataframes = []

for file in paths:
    path = file.getPath().toString()

    if not path.endswith(".parquet"):
        continue

    print(f"Procesando: {path}")

    df = spark.read.parquet(path)

    cols = df.columns

    # NORMALIZACIÓN DINÁMICA
    if "pickup_datetime" in cols:
        df = df.withColumnRenamed("pickup_datetime", "tpep_pickup_datetime")

    if "dropoff_datetime" in cols:
        df = df.withColumnRenamed("dropoff_datetime", "tpep_dropoff_datetime")

    # Columnas obligatorias (relleno si faltan)
    required_cols = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "fare_amount",
        "PULocationID", "DOLocationID", "payment_type"
    ]

    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))

    dataframes.append(df.select(required_cols))

# UNIÓN FINAL
df_final = dataframes[0]

for df in dataframes[1:]:
    df_final = df_final.unionByName(df, allowMissingColumns=True)

# LIMPIEZA
df_final = df_final.filter(col("fare_amount") > 0)

print("💾 Guardando en processed...")

df_final.write.mode("overwrite").parquet(
    "s3a://xideralaws-curso-dante/processed/yellow"
)

print("✅ ETL COMPLETADO")
print("Total rows:", df_final.count())

spark.stop()
