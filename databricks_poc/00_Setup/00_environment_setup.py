# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup - POC Avalanche Prediction
# MAGIC
# MAGIC Este notebook configura el entorno completo para el POC de predicción de avalanchas.
# MAGIC Debe ser ejecutado con `%run` al inicio de cada notebook del sistema.

# COMMAND ----------

import sys
import os
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración de Unity Catalog

# COMMAND ----------

# Configuración de Unity Catalog
CATALOG = "workspace"
SCHEMA = "avalanches_agents"
VOLUME = "raw_files"

# Nombres completos
FULL_DATABASE = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
VOLUME_RAW_FILES_PATH = f"{VOLUME_PATH}/raw"
VOLUME_PROCESSED_FILES_PATH = f"{VOLUME_PATH}/processed"
VOLUME_MODELS_PATH = f"{VOLUME_PATH}/models"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tablas Delta Lake

# COMMAND ----------

# === BRONZE LAYER (Raw Data) ===
TABLE_LOCATIONS = f"{FULL_DATABASE}.ski_resort_locations"
TABLE_WEATHER_DAILY = f"{FULL_DATABASE}.weather_daily"
TABLE_WEATHER_HOURLY = f"{FULL_DATABASE}.weather_hourly"
TABLE_SRTM_RAW = f"{FULL_DATABASE}.srtm_raw"
TABLE_RELATOS_RAW = f"{FULL_DATABASE}.andes_handbook_routes"

# === SILVER LAYER (Processed Features) ===
TABLE_TOPO_FEATURES = f"{FULL_DATABASE}.topo_features"
TABLE_TOPO_SUSCEPTIBILITY = f"{FULL_DATABASE}.topo_susceptibility"
TABLE_NLP_PATTERNS = f"{FULL_DATABASE}.nlp_risk_patterns"
TABLE_NLP_EMBEDDINGS = f"{FULL_DATABASE}.nlp_embeddings"
TABLE_WEATHER_FEATURES = f"{FULL_DATABASE}.weather_features"
TABLE_WEATHER_TRIGGERS = f"{FULL_DATABASE}.weather_triggers"

# === GOLD LAYER (Analytics & Predictions) ===
TABLE_RISK_PREDICTIONS = f"{FULL_DATABASE}.avalanche_predictions"
TABLE_BOLETINES = f"{FULL_DATABASE}.avalanche_bulletins"
TABLE_SUSCEPTIBILITY_MAP = f"{FULL_DATABASE}.susceptibility_map"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Zona Piloto: La Parva

# COMMAND ----------

# Configuración geográfica
PILOT_ZONE = {
    "name": "La Parva",
    "center_lat": -33.35,
    "center_lon": -70.27,
    "bbox": {
        "south": -33.40,
        "north": -33.30,
        "west": -70.32,
        "east": -70.22
    },
    "elevation_min": 2500,
    "elevation_max": 3500
}

# Bandas altitudinales (m.s.n.m.)
ELEVATION_BANDS = [
    {"name": "Baja", "min": 0, "max": 2500, "risk_weight": 0.2},
    {"name": "Media-Baja", "min": 2500, "max": 3200, "risk_weight": 0.5},
    {"name": "Media-Alta", "min": 3200, "max": 3500, "risk_weight": 0.8},
    {"name": "Alta", "min": 3500, "max": 6000, "risk_weight": 1.0}
]

# Pendientes críticas (grados)
CRITICAL_SLOPE_MIN = 30
CRITICAL_SLOPE_MAX = 45

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Escala Europea de Peligro de Aludes (EAWS)

# COMMAND ----------

EAWS_SCALE = {
    1: {"level": "Débil", "color": "#CCFF66", "description": "Nieve generalmente bien estabilizada"},
    2: {"level": "Limitado", "color": "#FFFF00", "description": "Nieve moderadamente estabilizada"},
    3: {"level": "Notable", "color": "#FF9900", "description": "Nieve débilmente estabilizada en muchas pendientes"},
    4: {"level": "Fuerte", "color": "#FF0000", "description": "Nieve débilmente estabilizada en mayoría de pendientes"},
    5: {"level": "Muy Fuerte", "color": "#9900CC", "description": "Nieve inestable generalizada"}
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Umbrales de Decisión

# COMMAND ----------

# Umbrales meteorológicos (24h)
THRESHOLD_SNOWFALL_LOW = 10      # cm
THRESHOLD_SNOWFALL_MODERATE = 30
THRESHOLD_SNOWFALL_HIGH = 50
THRESHOLD_SNOWFALL_EXTREME = 80

# Umbrales de temperatura (°C fluctuación en 24h)
THRESHOLD_TEMP_VAR_LOW = 5
THRESHOLD_TEMP_VAR_MODERATE = 10
THRESHOLD_TEMP_VAR_HIGH = 15

# Umbrales de viento (km/h)
THRESHOLD_WIND_LOW = 30
THRESHOLD_WIND_MODERATE = 50
THRESHOLD_WIND_HIGH = 70

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Configuración de APIs

# COMMAND ----------

# Open-Meteo
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_TIMEZONE = "America/Santiago"

# Google Earth Engine (requiere autenticación separada)
GEE_PROJECT = "avalanche-chile-poc"

# Databricks LLM
try:
    DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    DATABRICKS_LLM_ENDPOINT = "https://dbc-6f162706-efdf.cloud.databricks.com/serving-endpoints"
except:
    DATABRICKS_TOKEN = None
    DATABRICKS_LLM_ENDPOINT = None
    print("⚠️  LLM token no disponible (OK si no estás en Databricks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Configuración de Modelos ML

# COMMAND ----------

# Modelos NLP
NLP_MODEL_SENTIMENT = "Manauu17/enhanced_roberta_sentiments_es"
NLP_MODEL_NER = "dslim/bert-large-NER"
NLP_MODEL_EMBEDDINGS = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# LLM
LLM_MODEL_NAME = "databricks-llama-4-maverick"
LLM_TEMPERATURE = 0.3
LLM_MAX_TOKENS = 2000

# Procesamiento
MAX_TEXT_LENGTH = 2000
BATCH_SIZE = 32
CONFIDENCE_THRESHOLD = 0.7

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Funciones Helper

# COMMAND ----------

def get_elevation_band(elevation_m):
    """Determina banda altitudinal para elevación dada"""
    for band in ELEVATION_BANDS:
        if band["min"] <= elevation_m < band["max"]:
            return band
    return ELEVATION_BANDS[-1]  # Default: banda más alta

def get_eaws_level_from_score(risk_score):
    """
    Convierte score continuo (0-1) a nivel EAWS (1-5)
    """
    if risk_score < 0.2:
        return 1
    elif risk_score < 0.4:
        return 2
    elif risk_score < 0.6:
        return 3
    elif risk_score < 0.8:
        return 4
    else:
        return 5

def is_in_pilot_zone(lat, lon):
    """Verifica si coordenadas están dentro de zona piloto"""
    bbox = PILOT_ZONE["bbox"]
    return (bbox["south"] <= lat <= bbox["north"] and
            bbox["west"] <= lon <= bbox["east"])

def create_timestamp():
    """Crea timestamp actual en formato ISO"""
    return datetime.now().isoformat()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verificación de Entorno

# COMMAND ----------

def verify_environment():
    """Verifica que el entorno esté correctamente configurado"""
    print("=" * 70)
    print("🏔️  POC SISTEMA DE PREDICCIÓN DE AVALANCHAS - ENVIRONMENT SETUP")
    print("=" * 70)

    print(f"\n📊 UNITY CATALOG:")
    print(f"   Catálogo: {CATALOG}")
    print(f"   Schema: {SCHEMA}")
    print(f"   Database completo: {FULL_DATABASE}")
    print(f"   Volume path: {VOLUME_PATH}")

    print(f"\n🗺️  ZONA PILOTO:")
    print(f"   Nombre: {PILOT_ZONE['name']}")
    print(f"   Centro: ({PILOT_ZONE['center_lat']}, {PILOT_ZONE['center_lon']})")
    print(f"   Elevación: {PILOT_ZONE['elevation_min']}-{PILOT_ZONE['elevation_max']} m")

    print(f"\n📈 BANDAS ALTITUDINALES:")
    for band in ELEVATION_BANDS:
        print(f"   {band['name']:12s}: {band['min']:4d}-{band['max']:4d}m (peso riesgo: {band['risk_weight']:.1f})")

    print(f"\n🎯 ESCALA EAWS: {len(EAWS_SCALE)} niveles configurados")

    print(f"\n🔧 CONFIGURACIÓN ML:")
    print(f"   Modelo sentimientos: {NLP_MODEL_SENTIMENT.split('/')[-1]}")
    print(f"   Modelo NER: {NLP_MODEL_NER.split('/')[-1]}")
    print(f"   LLM: {LLM_MODEL_NAME}")

    print(f"\n✅ Entorno configurado correctamente")
    print(f"   Timestamp: {create_timestamp()}")
    print("=" * 70)

# Ejecutar verificación
verify_environment()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Mensaje de Éxito

# COMMAND ----------

print("\n🎉 ENVIRONMENT SETUP COMPLETADO")
print("   → Usa: %run ../00_Setup/00_environment_setup")
print("   → Todas las variables globales están disponibles")
print(f"   → Database: {FULL_DATABASE}")
print(f"   → Zona piloto: {PILOT_ZONE['name']}\n")
