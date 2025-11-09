# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta de Datos Meteorológicos
# MAGIC
# MAGIC ## Descripción
# MAGIC Este notebook ingesta datos meteorológicos de Open-Meteo para predicción de nieve y avalanchas.
# MAGIC
# MAGIC **Unity Catalog Edition:** Guarda datos en tablas Delta, usa cache en volumes, mejor trazabilidad.

# COMMAND ----------

# MAGIC %run ../00_setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración de Unity Catalog

# COMMAND ----------

TABLE_WEATHER_DAILY="weather_daily"
TABLE_WEATHER_HOURLY="weather_hourly"
TABLE_PREDICTIO="avalanche_predictions"


# COMMAND ----------

print(f"Base de datos: {FULL_DATABASE}")
print(f"Tabla diaria: {TABLE_WEATHER_DAILY}")
print(f"Tabla horaria: {TABLE_WEATHER_HOURLY}")
print(f"Cache: {VOLUME_RAW_FILES_PATH}/.cache")

# COMMAND ----------

import sys
sys.path.append('/Workspace/Repos/avalanches-agents/config')

print(f"Base de datos: {FULL_DATABASE}")
print(f"Tabla diaria: {TABLE_WEATHER_DAILY}")
print(f"Tabla horaria: {TABLE_WEATHER_HOURLY}")
print(f"Cache: {VOLUME_RAW_FILES_PATH}/.cache")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cliente de API Open-Meteo

# COMMAND ----------

import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd

# Use in-memory cache to avoid disk I/O issues on Databricks
cache_session = requests_cache.CachedSession(
    backend="memory",
    expire_after=3600
)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

print("Client configured with in-memory cache.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Función de Ingesta de Datos

# COMMAND ----------

def fetch_weather_data(latitude, longitude, days=16):
    """Obtiene datos meteorológicos para predicción de avalanchas."""
    url = "https://api.open-meteo.com/v1/forecast"
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": "America/Santiago",
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
            "winddirection_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration"
        ]
        
        for i, var in enumerate(daily_variables):
            daily_data[var] = daily.Variables(i).ValuesAsNumpy()
        
        daily_df = pd.DataFrame(data=daily_data)
        
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
        print(f"Error: {e}")
        return None, None

print("Función de ingesta definida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingesta de Datos para Todas las Ubicaciones

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

locations_df = spark.table(TABLE_LOCATIONS)
locations = locations_df.toPandas()

print(f"Ubicaciones desde: {TABLE_LOCATIONS}")
print(f"Total: {len(locations)}")

all_daily_data = []
all_hourly_data = []

for _, location in locations.iterrows():
    print(f"Procesando {location['name']}...")
    
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
    
    if hourly_df is not None:
        hourly_df['location_name'] = location['name']
        hourly_df['latitude'] = location['latitude']
        hourly_df['longitude'] = location['longitude']
        hourly_df['elevation'] = location['elevation']
        hourly_df['date'] = pd.to_datetime(hourly_df['timestamp']).dt.date
        all_hourly_data.append(hourly_df)

combined_daily = pd.concat(all_daily_data, ignore_index=True)
combined_hourly = pd.concat(all_hourly_data, ignore_index=True)

print(f"\nDatos recopilados:")
print(f"  Daily: {len(combined_daily)} registros")
print(f"  Hourly: {len(combined_hourly)} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar Datos en Unity Catalog

# COMMAND ----------

daily_spark_df = spark.createDataFrame(combined_daily)
daily_spark_df = daily_spark_df.withColumn("ingestion_timestamp", current_timestamp())

hourly_spark_df = spark.createDataFrame(combined_hourly)
hourly_spark_df = hourly_spark_df.withColumn("ingestion_timestamp", current_timestamp())

print(f"Guardando en {TABLE_WEATHER_DAILY}...")
daily_spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("location_name", "date") \
    .saveAsTable(TABLE_WEATHER_DAILY)

print(f"Guardando en {TABLE_WEATHER_HOURLY}...")
hourly_spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("location_name", "date") \
    .saveAsTable(TABLE_WEATHER_HOURLY)

print("Datos guardados en Unity Catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visualización de Datos Ingestados

# COMMAND ----------

from pyspark.sql.functions import desc, sum, avg, max

daily_table = spark.table(TABLE_WEATHER_DAILY)

print("Últimas 10 filas:")
display(daily_table.orderBy(desc("ingestion_timestamp")).limit(10))

print("\nResumen de nevadas por ubicación:")
snowfall_summary = daily_table.groupBy("location_name").agg(
    sum("snowfall_sum").alias("total_snowfall_cm"),
    avg("snowfall_sum").alias("avg_daily_snowfall_cm"),
    max("snowfall_sum").alias("max_daily_snowfall_cm")
).orderBy(desc("total_snowfall_cm"))

display(snowfall_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validación de Calidad de Datos

# COMMAND ----------

from pyspark.sql.functions import count, when, col, min, max

daily_table = spark.table(TABLE_WEATHER_DAILY)

print("Valores nulos:")
null_counts = daily_table.select(
    *[count(when(col(c).isNull(), c)).alias(c) for c in daily_table.columns if c not in ['location_name', 'date']]
)
display(null_counts)

print("\nRangos de valores:")
display(daily_table.select(
    min("temperature_2m_min").alias("min_temp"),
    max("temperature_2m_max").alias("max_temp"),
    min("snowfall_sum").alias("min_snow"),
    max("snowfall_sum").alias("max_snow"),
    min("windspeed_10m_max").alias("min_wind"),
    max("windspeed_10m_max").alias("max_wind")
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Metadata de la Ingesta

# COMMAND ----------

print(f"Metadata: {TABLE_WEATHER_DAILY}")
spark.sql(f"DESCRIBE EXTENDED {TABLE_WEATHER_DAILY}").show(50, False)

daily_count = spark.table(TABLE_WEATHER_DAILY).count()
hourly_count = spark.table(TABLE_WEATHER_HOURLY).count()

print(f"\nRegistros totales:")
print(f"  Diarios: {daily_count:,}")
print(f"  Horarios: {hourly_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notas de Uso
# MAGIC
# MAGIC **Ventajas de Unity Catalog:**
# MAGIC - Datos centralizados y gobernados
# MAGIC - Control de acceso granular
# MAGIC - Linaje de datos automático
# MAGIC - Auditoría completa
# MAGIC
# MAGIC **Lectura:**
# MAGIC ```python
# MAGIC df = spark.table("avalanche_prediction.avalanche_data.weather_daily")
# MAGIC ```
# MAGIC
# MAGIC **Escritura:**
# MAGIC ```python
# MAGIC df.write.format("delta").mode("append").saveAsTable(table_name)
# MAGIC ```