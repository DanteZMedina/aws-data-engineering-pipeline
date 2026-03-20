import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, hour, col, when, to_date, sum as _sum
import pandas as pd
import numpy as np

st.set_page_config(layout="wide")

st.title("🚕 NYC Taxi Data Dashboard ")

# -----------------------------
# CONFIG
# -----------------------------
st.sidebar.header("⚙️ Configuración")

SAMPLE_FRACTION = st.sidebar.slider(
    "📊 Porcentaje de datos",
    0.01, 1.0, 0.1
)

# -----------------------------
# SPARK
# -----------------------------
spark = SparkSession.builder \
    .appName("Streamlit App") \
    .getOrCreate()

# -----------------------------
# LOAD DATA
# -----------------------------
with st.spinner("Cargando datos..."):
    df = spark.read.parquet("/home/ubuntu/nyc_project/data/processed/yellow")

    # Sampling
    df = df.sample(fraction=SAMPLE_FRACTION, seed=42)

    zones = spark.read.csv(
        "/home/ubuntu/nyc_project/data/taxi_zone_lookup.csv",
        header=True,
        inferSchema=True
    )

# -----------------------------
# LIMPIEZA
# -----------------------------
df = df.filter(col("fare_amount") > 0)

df = df.withColumn(
    "payment_type_name",
    when(col("payment_type") == 1, "Tarjeta")
    .when(col("payment_type") == 2, "Efectivo")
    .otherwise("Otros")
)

df = df.withColumn("date", to_date("tpep_pickup_datetime"))

# -----------------------------
# JOIN
# -----------------------------
df_joined = df.join(
    zones,
    df.PULocationID == zones.LocationID,
    "left"
)

# -----------------------------
# FILTROS
# -----------------------------
st.sidebar.header("🎛️ Filtros")

# Fechas
min_date = df_joined.selectExpr("min(date)").collect()[0][0]
max_date = df_joined.selectExpr("max(date)").collect()[0][0]

date_range = st.sidebar.date_input(
    "📅 Rango de fechas",
    [min_date, max_date]
)

if len(date_range) == 2:
    df_joined = df_joined.filter(
        (col("date") >= str(date_range[0])) &
        (col("date") <= str(date_range[1]))
    )

# Zona
zones_list = [row["Zone"] for row in zones.select("Zone").distinct().collect()]
selected_zone = st.sidebar.selectbox("Zona", ["Todas"] + zones_list)

if selected_zone != "Todas":
    df_joined = df_joined.filter(col("Zone") == selected_zone)

# -----------------------------
# KPIs AVANZADOS
# -----------------------------
avg_fare = df_joined.select(avg("fare_amount")).collect()[0][0]
total_trips = df_joined.count()
total_revenue = df_joined.select(_sum("fare_amount")).collect()[0][0]

col1, col2, col3 = st.columns(3)

col1.metric("💰 Tarifa promedio", f"${avg_fare:.2f}")
col2.metric("🚕 Total viajes", f"{total_trips:,}")
col3.metric("💵 Ingreso total", f"${total_revenue:,.0f}")

# -----------------------------
# MAPA (SIMULADO PRO)
# -----------------------------
st.subheader("🗺️ Mapa de demanda por zona")

map_df = df_joined.groupBy("Zone") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(20) \
    .toPandas()

# Coordenadas fake NYC (aproximadas)
np.random.seed(42)
map_df["lat"] = np.random.uniform(40.70, 40.85, size=len(map_df))
map_df["lon"] = np.random.uniform(-74.02, -73.90, size=len(map_df))

st.map(map_df.rename(columns={"lat": "latitude", "lon": "longitude"}))

# -----------------------------
# TOP ZONAS
# -----------------------------
st.subheader("📊 Top zonas con más viajes")

top_zones = map_df.copy()
st.bar_chart(top_zones.set_index("Zone")["count"])

# -----------------------------
# DEMANDA POR HORA
# -----------------------------
df_hours = df_joined.withColumn("hour", hour("tpep_pickup_datetime"))

hours_df = df_hours.groupBy("hour") \
    .count() \
    .orderBy("hour") \
    .toPandas()

hours_df["count_millions"] = hours_df["count"] / 1_000_000

st.subheader("⏰ Demanda por hora (millones)")
st.line_chart(hours_df.set_index("hour")["count_millions"])

# -----------------------------
# TARIFA POR HORA
# -----------------------------
fare_hour = df_hours.groupBy("hour") \
    .agg(avg("fare_amount").alias("avg_fare")) \
    .orderBy("hour") \
    .toPandas()

st.subheader("💰 Tarifa promedio por hora")
st.line_chart(fare_hour.set_index("hour"))

# -----------------------------
# PAGOS
# -----------------------------
payment_df = df_joined.groupBy("payment_type_name") \
    .count() \
    .orderBy("count", ascending=False) \
    .toPandas()

st.subheader("💳 Tipos de pago")
st.bar_chart(payment_df.set_index("payment_type_name"))

spark.stop()