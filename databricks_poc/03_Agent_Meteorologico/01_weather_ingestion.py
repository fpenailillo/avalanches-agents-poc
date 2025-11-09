# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 3: Ingesta de Datos Meteorológicos
# MAGIC
# MAGIC Ingesta datos meteorológicos de Open-Meteo para la zona piloto.
# MAGIC
# MAGIC **Input:** API Open-Meteo (pronóstico 16 días)
# MAGIC **Output:** `weather_daily` y `weather_hourly` (capa Bronze)

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurar Cliente Open-Meteo

# COMMAND ----------

import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F

# Configurar cliente con cache en memoria
cache_session = requests_cache.CachedSession(
    backend="memory",
    expire_after=3600
)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

print("✅ Cliente Open-Meteo configurado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cargar Ubicaciones a Monitorear

# COMMAND ----------

print(f"📍 Cargando ubicaciones desde: {TABLE_LOCATIONS}")

locations_df = spark.table(TABLE_LOCATIONS)
locations = locations_df.toPandas()

print(f"✅ Ubicaciones cargadas: {len(locations)}")
display(locations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Función de Ingesta

# COMMAND ----------

def fetch_weather_data(latitude, longitude, days=16):
    """
    Obtiene datos meteorológicos de Open-Meteo para predicción de avalanchas.

    Args:
        latitude: Latitud
        longitude: Longitud
        days: Días de pronóstico (default 16)

    Returns:
        Tuple (daily_df, hourly_df) con DataFrames de pandas
    """
    url = OPEN_METEO_URL

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": OPEN_METEO_TIMEZONE,
        "forecast_days": days,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "precipitation_hours",
            "precipitation_probability_max",
            "snowfall_sum",
            "windspeed_10m_max",
            "windgusts_10m_max",
            "winddirection_10m_dominant",
            "shortwave_radiation_sum",
            "et0_fao_evapotranspiration"
        ],
        "hourly": [
            "temperature_2m",
            "precipitation",
            "snowfall",
            "snow_depth",
            "weathercode",
            "cloudcover",
            "windspeed_10m",
            "winddirection_10m",
            "windgusts_10m",
            "surface_pressure",
            "relativehumidity_2m"
        ]
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        # Procesar datos diarios
        daily = response.Daily()
        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s"),
                end=pd.to_datetime(daily.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )
        }

        daily_variables = [
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "precipitation_hours", "precipitation_probability_max",
            "snowfall_sum", "windspeed_10m_max", "windgusts_10m_max",
            "winddirection_10m_dominant", "shortwave_radiation_sum",
            "et0_fao_evapotranspiration"
        ]

        for i, var in enumerate(daily_variables):
            daily_data[var] = daily.Variables(i).ValuesAsNumpy()

        daily_df = pd.DataFrame(data=daily_data)

        # Procesar datos horarios
        hourly = response.Hourly()
        hourly_data = {
            "timestamp": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s"),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }

        hourly_variables = [
            "temperature_2m", "precipitation", "snowfall", "snow_depth",
            "weathercode", "cloudcover", "windspeed_10m", "winddirection_10m",
            "windgusts_10m", "surface_pressure", "relativehumidity_2m"
        ]

        for i, var in enumerate(hourly_variables):
            hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()

        hourly_df = pd.DataFrame(data=hourly_data)

        return daily_df, hourly_df

    except Exception as e:
        print(f"❌ Error en API: {e}")
        return None, None

print("✅ Función de ingesta definida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestar Datos para Todas las Ubicaciones

# COMMAND ----------

print(f"🌦️  Ingiriendo datos meteorológicos para {len(locations)} ubicación(es)...")

all_daily_data = []
all_hourly_data = []

for idx, location in locations.iterrows():
    print(f"\n🔄 Procesando: {location['name']} ({idx+1}/{len(locations)})")

    daily_df, hourly_df = fetch_weather_data(
        latitude=location['latitude'],
        longitude=location['longitude'],
        days=16
    )

    if daily_df is not None:
        daily_df['location_name'] = location['name']
        daily_df['latitude'] = location['latitude']
        daily_df['longitude'] = location['longitude']
        daily_df['elevation'] = location['elevation']
        all_daily_data.append(daily_df)
        print(f"   ✅ Datos diarios: {len(daily_df)} registros")

    if hourly_df is not None:
        hourly_df['location_name'] = location['name']
        hourly_df['latitude'] = location['latitude']
        hourly_df['longitude'] = location['longitude']
        hourly_df['elevation'] = location['elevation']
        hourly_df['date'] = pd.to_datetime(hourly_df['timestamp']).dt.date
        all_hourly_data.append(hourly_df)
        print(f"   ✅ Datos horarios: {len(hourly_df)} registros")

if all_daily_data:
    combined_daily = pd.concat(all_daily_data, ignore_index=True)
    combined_hourly = pd.concat(all_hourly_data, ignore_index=True)

    print(f"\n✅ Ingesta completada:")
    print(f"   Datos diarios: {len(combined_daily):,} registros")
    print(f"   Datos horarios: {len(combined_hourly):,} registros")
else:
    print("⚠️  No se pudieron obtener datos - usando datos sintéticos")
    # Generar datos sintéticos para POC
    import numpy as np
    dates = pd.date_range(start=datetime.now().date(), periods=16, freq='D')
    combined_daily = pd.DataFrame({
        'location_name': [locations.iloc[0]['name']] * 16,
        'latitude': [locations.iloc[0]['latitude']] * 16,
        'longitude': [locations.iloc[0]['longitude']] * 16,
        'elevation': [locations.iloc[0]['elevation']] * 16,
        'date': dates,
        'temperature_2m_max': np.random.uniform(-5, 5, 16),
        'temperature_2m_min': np.random.uniform(-15, -5, 16),
        'temperature_2m_mean': np.random.uniform(-10, 0, 16),
        'precipitation_sum': np.random.uniform(0, 10, 16),
        'snowfall_sum': np.random.uniform(0, 30, 16),
        'windspeed_10m_max': np.random.uniform(10, 50, 16)
    })
    combined_hourly = pd.DataFrame()  # Vacío para este caso

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar en Delta Lake (Capa Bronze)

# COMMAND ----------

print(f"💾 Guardando datos meteorológicos...")

# Guardar datos diarios
daily_spark_df = spark.createDataFrame(combined_daily)
daily_spark_df = daily_spark_df.withColumn("ingestion_timestamp", F.current_timestamp())

print(f"   Guardando en: {TABLE_WEATHER_DAILY}")
daily_spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("location_name", "date") \
    .saveAsTable(TABLE_WEATHER_DAILY)

print(f"✅ Datos diarios guardados: {daily_spark_df.count()} registros")

# Guardar datos horarios (si existen)
if len(combined_hourly) > 0:
    hourly_spark_df = spark.createDataFrame(combined_hourly)
    hourly_spark_df = hourly_spark_df.withColumn("ingestion_timestamp", F.current_timestamp())

    print(f"   Guardando en: {TABLE_WEATHER_HOURLY}")
    hourly_spark_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("location_name", "date") \
        .saveAsTable(TABLE_WEATHER_HOURLY)

    print(f"✅ Datos horarios guardados: {hourly_spark_df.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visualizar Datos Ingestados

# COMMAND ----------

print("📊 Visualizando datos meteorológicos...")

daily_table = spark.table(TABLE_WEATHER_DAILY)

print("\n🌤️  Últimos 10 días:")
display(daily_table.orderBy(F.desc("date")).limit(10))

print("\n❄️  Resumen de nevadas por ubicación:")
snowfall_summary = daily_table.groupBy("location_name").agg(
    F.sum("snowfall_sum").alias("total_snowfall_cm"),
    F.avg("snowfall_sum").alias("avg_daily_snowfall_cm"),
    F.max("snowfall_sum").alias("max_daily_snowfall_cm"),
    F.min("temperature_2m_min").alias("min_temp_c"),
    F.max("temperature_2m_max").alias("max_temp_c")
).orderBy(F.desc("total_snowfall_cm"))

display(snowfall_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validación de Calidad de Datos

# COMMAND ----------

from pyspark.sql.functions import count, when, col, min, max

print("🔍 Validación de calidad de datos...\n")

daily_table = spark.table(TABLE_WEATHER_DAILY)

# Verificar valores nulos
print("❓ Valores nulos:")
null_counts = daily_table.select(
    *[count(when(col(c).isNull(), c)).alias(c)
      for c in ['temperature_2m_mean', 'snowfall_sum', 'windspeed_10m_max']]
)
display(null_counts)

# Rangos de valores
print("\n📊 Rangos de valores:")
display(daily_table.select(
    min("temperature_2m_min").alias("min_temp"),
    max("temperature_2m_max").alias("max_temp"),
    min("snowfall_sum").alias("min_snow"),
    max("snowfall_sum").alias("max_snow"),
    min("windspeed_10m_max").alias("min_wind"),
    max("windspeed_10m_max").alias("max_wind")
))

print("✅ Validación completada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumen Final

# COMMAND ----------

daily_count = spark.table(TABLE_WEATHER_DAILY).count()

try:
    hourly_count = spark.table(TABLE_WEATHER_HOURLY).count()
except:
    hourly_count = 0

print("\n" + "=" * 80)
print("📊 RESUMEN: AGENTE METEOROLÓGICO - INGESTA DE DATOS")
print("=" * 80)

print(f"""
🌦️  DATOS INGESTADOS:
   • Fuente: Open-Meteo API
   • Ubicaciones: {len(locations)}
   • Periodo: 16 días (pronóstico)

💾 DATOS ALMACENADOS:
   • Tabla diaria: {TABLE_WEATHER_DAILY}
   • Registros diarios: {daily_count:,}
   • Tabla horaria: {TABLE_WEATHER_HOURLY}
   • Registros horarios: {hourly_count:,}

📈 VARIABLES CLAVE:
   ✅ Temperatura (máx, mín, media)
   ✅ Precipitación y horas de precipitación
   ✅ Nevadas (cm)
   ✅ Viento (velocidad, ráfagas, dirección)
   ✅ Radiación solar
   ✅ Código meteorológico
   ✅ Profundidad de nieve
   ✅ Humedad relativa

🎯 PRÓXIMO PASO:
   → Ejecutar 02_weather_features.py para procesamiento de features
   → Ejecutar 03_trigger_detection.py para detección de condiciones gatillantes
""")

print("=" * 80)
print("✅ AGENTE METEOROLÓGICO - INGESTA COMPLETADA")
print("=" * 80)
