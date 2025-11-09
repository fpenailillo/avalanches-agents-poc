# Databricks notebook source
# MAGIC %md
# MAGIC # Configuración de Zona Piloto: La Parva
# MAGIC
# MAGIC Define parámetros geográficos y operacionales para el POC

# COMMAND ----------

"""
Configuración centralizada para POC de Predicción de Avalanchas
Zona Piloto: La Parva, Región Metropolitana, Chile
"""

# ============================================================================
# ZONA PILOTO: LA PARVA
# ============================================================================

ZONE_CONFIG = {
    "name": "La Parva",
    "region": "Región Metropolitana",
    "country": "Chile",

    # Coordenadas centrales
    "center_lat": -33.35,
    "center_lon": -70.27,

    # Bounding box para análisis SRTM (approx 10-20 km²)
    "bbox": {
        "south": -33.40,
        "north": -33.30,
        "west": -70.32,
        "east": -70.22
    },

    # Bandas altitudinales (metros sobre nivel del mar)
    "elevation_bands": [
        {"name": "Baja", "min": 0, "max": 2500, "risk_weight": 0.2},
        {"name": "Media-Baja", "min": 2500, "max": 3200, "risk_weight": 0.5},
        {"name": "Media-Alta", "min": 3200, "max": 3500, "risk_weight": 0.8},
        {"name": "Alta", "min": 3500, "max": 6000, "risk_weight": 1.0}
    ],

    # Pendientes críticas (grados)
    "critical_slopes": {
        "min": 30,
        "max": 45,
        "very_high_risk_min": 35,
        "very_high_risk_max": 42
    },

    # Estaciones cercanas para validación
    "nearby_stations": [
        {"name": "La Parva", "lat": -33.35, "lon": -70.27, "elevation": 3000},
        {"name": "Valle Nevado", "lat": -33.35, "lon": -70.25, "elevation": 3025},
        {"name": "Farellones", "lat": -33.35, "lon": -70.31, "elevation": 2470}
    ]
}

# ============================================================================
# CONFIGURACIÓN DE UNITY CATALOG
# ============================================================================

CATALOG_CONFIG = {
    "catalog": "workspace",
    "schema": "avalanches_agents",
    "volume": "raw_files"
}

# Tablas Delta Lake
DELTA_TABLES = {
    # Bronze Layer (Raw Data)
    "bronze": {
        "weather_raw": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.weather_daily",
        "weather_hourly": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.weather_hourly",
        "srtm_raw": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.srtm_raw",
        "relatos_raw": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.andes_handbook_routes"
    },

    # Silver Layer (Processed Features)
    "silver": {
        "topo_features": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.topo_features",
        "nlp_patterns": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.nlp_risk_patterns",
        "weather_features": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.weather_features",
        "weather_triggers": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.weather_triggers"
    },

    # Gold Layer (Analytics & Predictions)
    "gold": {
        "risk_predictions": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.avalanche_predictions",
        "boletines": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.avalanche_bulletins",
        "susceptibility_map": f"{CATALOG_CONFIG['catalog']}.{CATALOG_CONFIG['schema']}.susceptibility_map"
    }
}

# ============================================================================
# ESCALA EUROPEA DE PELIGRO DE ALUDES (EAWS)
# ============================================================================

EAWS_SCALE = {
    1: {
        "level": "Débil",
        "level_en": "Low",
        "color": "#CCFF66",
        "description": "La nieve está generalmente bien estabilizada",
        "probability": "improbable",
        "avalanche_size": "pequeño",
        "recommendations": "Condiciones favorables. Riesgo en pendientes muy empinadas y extremas."
    },
    2: {
        "level": "Limitado",
        "level_en": "Moderate",
        "color": "#FFFF00",
        "description": "Nieve moderadamente estabilizada en algunas pendientes",
        "probability": "posible",
        "avalanche_size": "pequeño a mediano",
        "recommendations": "Condiciones mayormente favorables. Evaluar pendientes específicas."
    },
    3: {
        "level": "Notable",
        "level_en": "Considerable",
        "color": "#FF9900",
        "description": "Nieve moderada o débilmente estabilizada en muchas pendientes",
        "probability": "probable",
        "avalanche_size": "mediano a grande",
        "recommendations": "Condiciones críticas. Experiencia y evaluación del terreno esencial."
    },
    4: {
        "level": "Fuerte",
        "level_en": "High",
        "color": "#FF0000",
        "description": "Nieve débilmente estabilizada en la mayoría de pendientes empinadas",
        "probability": "muy probable",
        "avalanche_size": "grande a muy grande",
        "recommendations": "Condiciones muy críticas. Restricción a terreno simple solamente."
    },
    5: {
        "level": "Muy Fuerte",
        "level_en": "Very High",
        "color": "#9900CC",
        "description": "Nieve inestable generalizada",
        "probability": "seguro",
        "avalanche_size": "muy grande a extremadamente grande",
        "recommendations": "Evitar todo terreno de avalanchas. Condiciones extraordinarias."
    }
}

# ============================================================================
# UMBRALES DE DECISIÓN (HEURÍSTICAS)
# ============================================================================

DECISION_THRESHOLDS = {
    # Meteorológicos (24h)
    "snowfall_24h_cm": {
        "low": 10,
        "moderate": 30,
        "high": 50,
        "very_high": 80
    },

    # Temperatura (fluctuaciones °C/24h)
    "temp_variation_24h": {
        "low": 5,
        "moderate": 10,
        "high": 15,
        "very_high": 20
    },

    # Viento (km/h)
    "wind_speed_kmh": {
        "low": 30,
        "moderate": 50,
        "high": 70,
        "very_high": 90
    },

    # Topográficos
    "slope_susceptibility": {
        "low": 0.3,      # <30% área crítica
        "moderate": 0.5, # 30-50%
        "high": 0.7,     # 50-70%
        "very_high": 0.85 # >70%
    },

    # NLP (menciones de riesgo histórico)
    "historical_risk_mentions": {
        "low": 2,
        "moderate": 5,
        "high": 10,
        "very_high": 20
    }
}

# ============================================================================
# APIS Y CREDENCIALES
# ============================================================================

API_ENDPOINTS = {
    "open_meteo": "https://api.open-meteo.com/v1/forecast",
    "google_earth_engine": "https://earthengine.googleapis.com",
    "databricks_llm": "https://dbc-6f162706-efdf.cloud.databricks.com/serving-endpoints"
}

# ============================================================================
# PARÁMETROS DE MODELOS ML
# ============================================================================

ML_CONFIG = {
    "nlp_models": {
        "sentiment": "Manauu17/enhanced_roberta_sentiments_es",
        "ner": "dslim/bert-large-NER",
        "embeddings": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    },

    "llm": {
        "model": "databricks-llama-4-maverick",
        "temperature": 0.3,
        "max_tokens": 2000
    },

    "processing": {
        "max_text_length": 2000,
        "batch_size": 32,
        "confidence_threshold": 0.7
    }
}

# ============================================================================
# FUNCIONES HELPER
# ============================================================================

def get_full_table_name(layer: str, table: str) -> str:
    """Retorna nombre completo de tabla Delta"""
    return DELTA_TABLES.get(layer, {}).get(table, "")

def get_elevation_band(elevation_m: float) -> dict:
    """Determina banda altitudinal para elevación dada"""
    for band in ZONE_CONFIG["elevation_bands"]:
        if band["min"] <= elevation_m < band["max"]:
            return band
    return ZONE_CONFIG["elevation_bands"][-1]  # Default: banda más alta

def get_eaws_level(risk_score: float) -> int:
    """
    Convierte score continuo (0-1) a nivel EAWS (1-5)

    Args:
        risk_score: Score de riesgo normalizado entre 0 y 1

    Returns:
        Nivel EAWS entre 1 y 5
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

def is_in_zone(lat: float, lon: float) -> bool:
    """Verifica si coordenadas están dentro de la zona piloto"""
    bbox = ZONE_CONFIG["bbox"]
    return (bbox["south"] <= lat <= bbox["north"] and
            bbox["west"] <= lon <= bbox["east"])

# ============================================================================
# EXPORTAR CONFIGURACIÓN
# ============================================================================

def print_config_summary():
    """Imprime resumen de configuración"""
    print("=" * 70)
    print("POC SISTEMA DE PREDICCIÓN DE AVALANCHAS - CONFIGURACIÓN")
    print("=" * 70)
    print(f"\n🏔️  ZONA PILOTO: {ZONE_CONFIG['name']}")
    print(f"   Región: {ZONE_CONFIG['region']}")
    print(f"   Coordenadas: ({ZONE_CONFIG['center_lat']}, {ZONE_CONFIG['center_lon']})")
    print(f"   Área: ~{abs(ZONE_CONFIG['bbox']['north'] - ZONE_CONFIG['bbox']['south']) * abs(ZONE_CONFIG['bbox']['east'] - ZONE_CONFIG['bbox']['west']) * 111 * 111:.1f} km²")

    print(f"\n📊 UNITY CATALOG:")
    print(f"   Catálogo: {CATALOG_CONFIG['catalog']}")
    print(f"   Schema: {CATALOG_CONFIG['schema']}")
    print(f"   Tablas configuradas: {sum(len(tables) for tables in DELTA_TABLES.values())}")

    print(f"\n⚙️  BANDAS ALTITUDINALES: {len(ZONE_CONFIG['elevation_bands'])}")
    for band in ZONE_CONFIG["elevation_bands"]:
        print(f"   {band['name']:12s}: {band['min']:4d}-{band['max']:4d}m (peso: {band['risk_weight']:.1f})")

    print(f"\n📈 ESCALA EAWS: 5 niveles configurados")
    print(f"   1-Débil | 2-Limitado | 3-Notable | 4-Fuerte | 5-Muy Fuerte")

    print("\n✅ Configuración cargada exitosamente")
    print("=" * 70)

# COMMAND ----------

# Ejecutar resumen al importar
print_config_summary()
