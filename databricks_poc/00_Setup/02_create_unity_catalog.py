# Databricks notebook source
# MAGIC %md
# MAGIC # Creación de Unity Catalog - POC Avalanche Prediction
# MAGIC
# MAGIC **Ejecutar UNA VEZ** para crear la estructura completa de Unity Catalog:
# MAGIC - Catálogo
# MAGIC - Schema
# MAGIC - Volumes
# MAGIC - Tablas base (estructura vacía)
# MAGIC
# MAGIC **IMPORTANTE:** Requiere permisos de ADMIN en Databricks

# COMMAND ----------

# MAGIC %run ./00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear Catálogo

# COMMAND ----------

print(f"📦 Creando catálogo: {CATALOG}")

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print(f"✅ Catálogo '{CATALOG}' creado/verificado")
except Exception as e:
    print(f"⚠️  Error creando catálogo: {e}")
    print("   → Puede que ya exista o que necesites permisos de ADMIN")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear Schema

# COMMAND ----------

print(f"📊 Creando schema: {SCHEMA}")

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_DATABASE}")
    spark.sql(f"USE {FULL_DATABASE}")
    print(f"✅ Schema '{FULL_DATABASE}' creado/verificado")
except Exception as e:
    print(f"⚠️  Error creando schema: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear Volume para Archivos Raw

# COMMAND ----------

print(f"💾 Creando volume: {VOLUME}")

try:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {FULL_DATABASE}.{VOLUME}
    """)
    print(f"✅ Volume '{VOLUME}' creado/verificado")
    print(f"   Path: {VOLUME_PATH}")
except Exception as e:
    print(f"⚠️  Error creando volume: {e}")
    print("   → Volumes solo disponibles en Unity Catalog habilitado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear Directorios en Volume

# COMMAND ----------

print("📁 Creando estructura de directorios en volume...")

directories = [
    f"{VOLUME_PATH}/raw",
    f"{VOLUME_PATH}/raw/srtm",
    f"{VOLUME_PATH}/raw/weather",
    f"{VOLUME_PATH}/raw/relatos",
    f"{VOLUME_PATH}/processed",
    f"{VOLUME_PATH}/processed/topo",
    f"{VOLUME_PATH}/processed/nlp",
    f"{VOLUME_PATH}/processed/weather",
    f"{VOLUME_PATH}/models",
    f"{VOLUME_PATH}/models/nlp",
    f"{VOLUME_PATH}/outputs",
    f"{VOLUME_PATH}/outputs/maps",
    f"{VOLUME_PATH}/outputs/boletines"
]

for directory in directories:
    try:
        dbutils.fs.mkdirs(directory)
        print(f"   ✅ {directory}")
    except Exception as e:
        print(f"   ⚠️  {directory}: {e}")

print("✅ Estructura de directorios creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tabla: Ubicaciones de Centros de Esquí

# COMMAND ----------

print(f"📍 Creando tabla: {TABLE_LOCATIONS}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_LOCATIONS} (
    location_id STRING,
    name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    region STRING,
    country STRING,
    timezone STRING,
    created_at TIMESTAMP
)
USING DELTA
COMMENT 'Ubicaciones de centros de esquí y zonas de monitoreo'
""")

print(f"✅ Tabla '{TABLE_LOCATIONS}' creada")

# COMMAND ----------

# Insertar La Parva como ubicación piloto
spark.sql(f"""
INSERT INTO {TABLE_LOCATIONS} VALUES (
    'la_parva_001',
    'La Parva',
    -33.35,
    -70.27,
    3000,
    'Región Metropolitana',
    'Chile',
    'America/Santiago',
    current_timestamp()
)
""")

print("✅ Ubicación piloto 'La Parva' insertada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tablas Bronze Layer (Raw Data)

# COMMAND ----------

print("🥉 Creando tablas Bronze Layer (Raw Data)...\n")

# === Tabla: Weather Daily ===
print(f"   Creando: {TABLE_WEATHER_DAILY}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_WEATHER_DAILY} (
    location_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    date DATE,
    temperature_2m_max DOUBLE,
    temperature_2m_min DOUBLE,
    temperature_2m_mean DOUBLE,
    precipitation_sum DOUBLE,
    precipitation_hours DOUBLE,
    precipitation_probability_max DOUBLE,
    snowfall_sum DOUBLE,
    windspeed_10m_max DOUBLE,
    windgusts_10m_max DOUBLE,
    winddirection_10m_dominant DOUBLE,
    shortwave_radiation_sum DOUBLE,
    et0_fao_evapotranspiration DOUBLE,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (location_name, date)
COMMENT 'Datos meteorológicos diarios de Open-Meteo'
""")

# === Tabla: Weather Hourly ===
print(f"   Creando: {TABLE_WEATHER_HOURLY}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_WEATHER_HOURLY} (
    location_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    timestamp TIMESTAMP,
    date DATE,
    temperature_2m DOUBLE,
    precipitation DOUBLE,
    snowfall DOUBLE,
    snow_depth DOUBLE,
    weathercode INT,
    cloudcover INT,
    windspeed_10m DOUBLE,
    winddirection_10m DOUBLE,
    windgusts_10m DOUBLE,
    surface_pressure DOUBLE,
    relativehumidity_2m INT,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (location_name, date)
COMMENT 'Datos meteorológicos horarios de Open-Meteo'
""")

# === Tabla: SRTM Raw ===
print(f"   Creando: {TABLE_SRTM_RAW}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_SRTM_RAW} (
    pixel_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    zone_name STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Datos raw de elevación SRTM para zona piloto'
""")

print("\n✅ Tablas Bronze Layer creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tablas Silver Layer (Processed Features)

# COMMAND ----------

print("🥈 Creando tablas Silver Layer (Processed Features)...\n")

# === Tabla: Topographic Features ===
print(f"   Creando: {TABLE_TOPO_FEATURES}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_TOPO_FEATURES} (
    feature_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    slope_degrees DOUBLE,
    aspect_degrees DOUBLE,
    elevation_band STRING,
    is_critical_slope BOOLEAN,
    zone_name STRING,
    processing_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Features topográficos procesados (pendiente, orientación, banda altitudinal)'
""")

# === Tabla: Topographic Susceptibility ===
print(f"   Creando: {TABLE_TOPO_SUSCEPTIBILITY}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_TOPO_SUSCEPTIBILITY} (
    zone_id STRING,
    zone_name STRING,
    elevation_band STRING,
    total_area_km2 DOUBLE,
    critical_slope_area_km2 DOUBLE,
    susceptibility_score DOUBLE,
    dominant_aspect STRING,
    avg_slope_degrees DOUBLE,
    processing_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Mapa de susceptibilidad topográfica por zona y banda altitudinal'
""")

# === Tabla: NLP Risk Patterns ===
print(f"   Creando: {TABLE_NLP_PATTERNS}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NLP_PATTERNS} (
    pattern_id STRING,
    zone_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    risk_mentions INT,
    avalanche_events INT,
    difficulty_level STRING,
    sentiment STRING,
    confidence DOUBLE,
    source STRING,
    processing_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Patrones de riesgo extraídos de relatos NLP'
""")

# === Tabla: Weather Features ===
print(f"   Creando: {TABLE_WEATHER_FEATURES}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_WEATHER_FEATURES} (
    feature_id STRING,
    location_name STRING,
    date DATE,
    elevation_band STRING,
    snowfall_24h DOUBLE,
    snowfall_48h DOUBLE,
    snowfall_72h DOUBLE,
    temp_variation_24h DOUBLE,
    max_wind_speed DOUBLE,
    precipitation_intensity STRING,
    isoterma_0c INT,
    processing_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (location_name, date)
COMMENT 'Features meteorológicos agregados por ventanas temporales'
""")

# === Tabla: Weather Triggers ===
print(f"   Creando: {TABLE_WEATHER_TRIGGERS}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_WEATHER_TRIGGERS} (
    trigger_id STRING,
    location_name STRING,
    date DATE,
    elevation_band STRING,
    trigger_type STRING,
    trigger_severity STRING,
    snowfall_cm DOUBLE,
    temp_change_c DOUBLE,
    wind_kmh DOUBLE,
    is_critical BOOLEAN,
    processing_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (location_name, date)
COMMENT 'Detección de condiciones gatillantes de avalanchas'
""")

print("\n✅ Tablas Silver Layer creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tablas Gold Layer (Analytics & Predictions)

# COMMAND ----------

print("🥇 Creando tablas Gold Layer (Analytics & Predictions)...\n")

# === Tabla: Avalanche Predictions ===
print(f"   Creando: {TABLE_RISK_PREDICTIONS}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_RISK_PREDICTIONS} (
    prediction_id STRING,
    zone_name STRING,
    forecast_date DATE,
    elevation_band STRING,
    eaws_level INT,
    eaws_label STRING,
    risk_score DOUBLE,
    confidence DOUBLE,
    topo_contribution DOUBLE,
    weather_contribution DOUBLE,
    nlp_contribution DOUBLE,
    main_factors ARRAY<STRING>,
    prediction_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (zone_name, forecast_date)
COMMENT 'Predicciones de riesgo de avalanchas por zona y banda altitudinal'
""")

# === Tabla: Avalanche Bulletins ===
print(f"   Creando: {TABLE_BOLETINES}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_BOLETINES} (
    boletin_id STRING,
    zone_name STRING,
    issue_date DATE,
    valid_from DATE,
    valid_to DATE,
    eaws_level INT,
    eaws_label STRING,
    summary STRING,
    danger_description STRING,
    avalanche_problems ARRAY<STRING>,
    affected_elevations STRING,
    affected_aspects STRING,
    recommendations STRING,
    confidence STRING,
    bulletin_text STRING,
    generated_by STRING,
    creation_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (zone_name, issue_date)
COMMENT 'Boletines de avalanchas generados automáticamente'
""")

# === Tabla: Susceptibility Map ===
print(f"   Creando: {TABLE_SUSCEPTIBILITY_MAP}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_SUSCEPTIBILITY_MAP} (
    grid_id STRING,
    zone_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation INT,
    elevation_band STRING,
    slope_degrees DOUBLE,
    aspect_degrees DOUBLE,
    susceptibility_score DOUBLE,
    risk_category STRING,
    last_updated TIMESTAMP
)
USING DELTA
COMMENT 'Mapa de susceptibilidad de alta resolución para visualización'
""")

print("\n✅ Tablas Gold Layer creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verificación de Tablas Creadas

# COMMAND ----------

print("🔍 Verificando tablas creadas...\n")

tables = spark.sql(f"SHOW TABLES IN {FULL_DATABASE}").collect()

print(f"📊 Tablas en {FULL_DATABASE}:")
print("=" * 70)

bronze_count = 0
silver_count = 0
gold_count = 0

for table in tables:
    table_name = table.tableName
    print(f"   ✅ {table_name}")

    # Clasificar por layer
    if any(x in table_name for x in ['weather_daily', 'weather_hourly', 'srtm_raw', 'locations']):
        bronze_count += 1
    elif any(x in table_name for x in ['topo_features', 'topo_susceptibility', 'nlp_', 'weather_features', 'weather_triggers']):
        silver_count += 1
    elif any(x in table_name for x in ['predictions', 'bulletins', 'susceptibility_map']):
        gold_count += 1

print("=" * 70)
print(f"\n📈 RESUMEN:")
print(f"   🥉 Bronze Layer: {bronze_count} tablas")
print(f"   🥈 Silver Layer: {silver_count} tablas")
print(f"   🥇 Gold Layer: {gold_count} tablas")
print(f"   📊 TOTAL: {len(tables)} tablas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verificación de Ubicaciones

# COMMAND ----------

print("\n📍 Verificando ubicaciones piloto:")
print("=" * 70)

locations_df = spark.table(TABLE_LOCATIONS)
display(locations_df)

print(f"\n✅ {locations_df.count()} ubicación(es) configurada(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Resumen Final

# COMMAND ----------

print("\n" + "=" * 70)
print("📋 RESUMEN DE UNITY CATALOG - POC AVALANCHE PREDICTION")
print("=" * 70)

print(f"""
✅ CATÁLOGO CREADO: {CATALOG}
✅ SCHEMA CREADO: {SCHEMA}
✅ DATABASE COMPLETO: {FULL_DATABASE}
✅ VOLUME CREADO: {VOLUME}

📊 ESTRUCTURA DE DATOS (Medallion Architecture):

🥉 BRONZE LAYER (Raw Data):
   • {TABLE_LOCATIONS.split('.')[-1]}
   • {TABLE_WEATHER_DAILY.split('.')[-1]}
   • {TABLE_WEATHER_HOURLY.split('.')[-1]}
   • {TABLE_SRTM_RAW.split('.')[-1]}

🥈 SILVER LAYER (Processed Features):
   • {TABLE_TOPO_FEATURES.split('.')[-1]}
   • {TABLE_TOPO_SUSCEPTIBILITY.split('.')[-1]}
   • {TABLE_NLP_PATTERNS.split('.')[-1]}
   • {TABLE_WEATHER_FEATURES.split('.')[-1]}
   • {TABLE_WEATHER_TRIGGERS.split('.')[-1]}

🥇 GOLD LAYER (Analytics & Predictions):
   • {TABLE_RISK_PREDICTIONS.split('.')[-1]}
   • {TABLE_BOLETINES.split('.')[-1]}
   • {TABLE_SUSCEPTIBILITY_MAP.split('.')[-1]}

📁 VOLUMES:
   • {VOLUME_PATH}/raw/
   • {VOLUME_PATH}/processed/
   • {VOLUME_PATH}/models/
   • {VOLUME_PATH}/outputs/

🏔️  ZONA PILOTO:
   • La Parva, Región Metropolitana
   • Coordenadas: (-33.35, -70.27)
   • Elevación: 3000m

📝 PRÓXIMOS PASOS:
   1. Ejecutar Agente 1: Topográfico (SRTM processing)
   2. Ejecutar Agente 2: NLP (relatos analysis)
   3. Ejecutar Agente 3: Meteorológico (weather ingestion)
   4. Ejecutar Agente 4: Integrador (risk fusion)
   5. Visualizar resultados en dashboard

""")

print("=" * 70)
print("🎉 UNITY CATALOG CONFIGURADO EXITOSAMENTE")
print("   → El sistema está listo para ingesta de datos")
print("=" * 70)
