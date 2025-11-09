# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 2: Extracción de Patrones de Riesgo desde Relatos NLP
# MAGIC
# MAGIC Este agente analiza la base de datos de relatos de montañismo (AndesHandbook)
# MAGIC para extraer conocimiento experto sobre riesgos de avalanchas.
# MAGIC
# MAGIC **Input:** `andes_handbook_routes` (base de relatos existente)
# MAGIC **Output:** `nlp_risk_patterns` (capa Silver)

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importar Librerías NLP

# COMMAND ----------

import re
import pandas as pd
from datetime import datetime
import uuid

# NLP
try:
    from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
    import torch
    print("✅ Transformers importado")
except ImportError:
    print("⚠️  Transformers no disponible - ejecutar notebook de instalación")

# PySpark
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("✅ Librerías importadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cargar Base de Relatos de AndesHandbook

# COMMAND ----------

print(f"📚 Cargando relatos desde: {TABLE_RELATOS_RAW}")

try:
    relatos_df = spark.table(TABLE_RELATOS_RAW)
    total_relatos = relatos_df.count()
    print(f"✅ Relatos cargados: {total_relatos:,}")

    # Mostrar muestra
    print("\n📋 Muestra de relatos:")
    display(relatos_df.select("route_id", "name", "description").limit(5))

except Exception as e:
    print(f"⚠️  Error cargando relatos: {e}")
    print("   → Creando datos de ejemplo para POC")

    # Crear relatos de ejemplo para POC
    example_relatos = [
        {
            "route_id": "001",
            "name": "Volcán Plomo por Cara Norte",
            "description": "Ascenso con riesgo de avalanchas en pendientes de 35-40 grados sobre 3500m. Cuidado con cornisas. Realizado en enero con buenas condiciones de nieve.",
            "latitude": -33.35,
            "longitude": -70.27,
            "elevation": 3500
        },
        {
            "route_id": "002",
            "name": "La Parva - Ruta del Cordón",
            "description": "Zona con historial de aludes en sector este. Pendientes críticas sobre 3000m. Precaución con acumulación de nieve reciente.",
            "latitude": -33.36,
            "longitude": -70.28,
            "elevation": 3200
        },
        {
            "route_id": "003",
            "name": "Valle Nevado - Travesía Alta",
            "description": "Hermosa ruta con exposición moderada. Algunos tramos técnicos en pendientes de 30 grados. Recomendado para montañistas experimentados.",
            "latitude": -33.34,
            "longitude": -70.26,
            "elevation": 3100
        }
    ]

    relatos_df = spark.createDataFrame(example_relatos)
    print(f"✅ {relatos_df.count()} relatos de ejemplo creados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Filtrar Relatos de Zona Piloto

# COMMAND ----------

print(f"🎯 Filtrando relatos para zona piloto: {PILOT_ZONE['name']}")

bbox = PILOT_ZONE['bbox']

# Filtrar por bounding box
relatos_zona = relatos_df.filter(
    (F.col("latitude").between(bbox['south'], bbox['north'])) &
    (F.col("longitude").between(bbox['west'], bbox['east']))
)

zona_count = relatos_zona.count()
print(f"✅ Relatos en zona piloto: {zona_count}")

if zona_count == 0:
    print("⚠️  No hay relatos en zona piloto, usando todos los relatos disponibles")
    relatos_zona = relatos_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Definir Vocabulario de Riesgo de Avalanchas

# COMMAND ----------

# Vocabulario especializado en avalanchas
RISK_VOCABULARY = {
    'avalanche_keywords': [
        'avalancha', 'alud', 'aludes', 'desprendimiento', 'deslizamiento',
        'cornisa', 'cornisas', 'placa', 'placas', 'nieve inestable'
    ],

    'risk_indicators': [
        'peligro', 'peligroso', 'riesgo', 'riesgoso', 'cuidado', 'precaución',
        'atención', 'evitar', 'no recomendado', 'crítico', 'exposición',
        'inestable', 'inseguro'
    ],

    'slope_keywords': [
        'pendiente', 'pendientes', 'grado', 'grados', 'inclinación',
        'empinado', 'empinada', 'pronunciado', 'vertical'
    ],

    'weather_triggers': [
        'nieve reciente', 'nevada', 'nevadas', 'precipitación', 'viento',
        'temperatura', 'calor', 'frío', 'deshielo', 'lluvia'
    ],

    'temporal_indicators': [
        'historial', 'frecuente', 'recurrente', 'anterior', 'previo',
        'ha ocurrido', 'se han reportado', 'conocido por'
    ],

    'severity_high': [
        'muy peligroso', 'extremadamente', 'mortal', 'fatal', 'grave',
        'severo', 'alto riesgo', 'máxima precaución'
    ],

    'severity_moderate': [
        'moderado', 'medio', 'considerable', 'significativo', 'notable'
    ]
}

print("✅ Vocabulario de riesgo definido")
print(f"   Categorías: {len(RISK_VOCABULARY)}")
print(f"   Total keywords: {sum(len(v) for v in RISK_VOCABULARY.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Función de Extracción de Menciones de Riesgo

# COMMAND ----------

def extract_risk_mentions(text: str) -> dict:
    """
    Extrae menciones de riesgo de avalanchas en texto.

    Returns:
        Dict con conteo de menciones por categoría
    """
    if not text or pd.isna(text):
        return {
            'avalanche_mentions': 0,
            'risk_mentions': 0,
            'slope_mentions': 0,
            'weather_triggers': 0,
            'temporal_mentions': 0,
            'severity_high': 0,
            'severity_moderate': 0,
            'total_risk_score': 0.0
        }

    text_lower = str(text).lower()

    # Contar menciones por categoría
    avalanche_count = sum(1 for kw in RISK_VOCABULARY['avalanche_keywords'] if kw in text_lower)
    risk_count = sum(1 for kw in RISK_VOCABULARY['risk_indicators'] if kw in text_lower)
    slope_count = sum(1 for kw in RISK_VOCABULARY['slope_keywords'] if kw in text_lower)
    weather_count = sum(1 for kw in RISK_VOCABULARY['weather_triggers'] if kw in text_lower)
    temporal_count = sum(1 for kw in RISK_VOCABULARY['temporal_indicators'] if kw in text_lower)
    severity_high_count = sum(1 for kw in RISK_VOCABULARY['severity_high'] if kw in text_lower)
    severity_mod_count = sum(1 for kw in RISK_VOCABULARY['severity_moderate'] if kw in text_lower)

    # Calcular score total de riesgo (normalizado 0-1)
    total_score = min(1.0, (
        avalanche_count * 0.3 +
        risk_count * 0.2 +
        slope_count * 0.15 +
        weather_count * 0.15 +
        severity_high_count * 0.15 +
        severity_mod_count * 0.05
    ) / 5.0)

    return {
        'avalanche_mentions': avalanche_count,
        'risk_mentions': risk_count,
        'slope_mentions': slope_count,
        'weather_triggers': weather_count,
        'temporal_mentions': temporal_count,
        'severity_high': severity_high_count,
        'severity_moderate': severity_mod_count,
        'total_risk_score': float(total_score)
    }

# Crear UDF para Spark
extract_risk_schema = StructType([
    StructField("avalanche_mentions", IntegerType()),
    StructField("risk_mentions", IntegerType()),
    StructField("slope_mentions", IntegerType()),
    StructField("weather_triggers", IntegerType()),
    StructField("temporal_mentions", IntegerType()),
    StructField("severity_high", IntegerType()),
    StructField("severity_moderate", IntegerType()),
    StructField("total_risk_score", DoubleType())
])

extract_risk_udf = F.udf(extract_risk_mentions, extract_risk_schema)

print("✅ Función de extracción definida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Extraer Patrones de Riesgo de Relatos

# COMMAND ----------

print("🔍 Extrayendo patrones de riesgo...")

# Aplicar extracción de riesgo
relatos_with_risk = relatos_zona.withColumn(
    "risk_analysis",
    extract_risk_udf(F.col("description"))
)

# Expandir estructura
relatos_with_risk = relatos_with_risk.select(
    "*",
    F.col("risk_analysis.avalanche_mentions").alias("avalanche_mentions"),
    F.col("risk_analysis.risk_mentions").alias("risk_mentions"),
    F.col("risk_analysis.slope_mentions").alias("slope_mentions"),
    F.col("risk_analysis.weather_triggers").alias("weather_trigger_mentions"),
    F.col("risk_analysis.temporal_mentions").alias("temporal_mentions"),
    F.col("risk_analysis.severity_high").alias("severity_high_count"),
    F.col("risk_analysis.severity_moderate").alias("severity_moderate_count"),
    F.col("risk_analysis.total_risk_score").alias("nlp_risk_score")
).drop("risk_analysis")

print("✅ Patrones extraídos")

# Mostrar muestra
print("\n📋 Muestra de análisis:")
display(relatos_with_risk.select(
    "name",
    "avalanche_mentions",
    "risk_mentions",
    "slope_mentions",
    "nlp_risk_score"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Extraer Información Numérica de Pendientes

# COMMAND ----------

def extract_slope_values(text: str) -> dict:
    """
    Extrae valores numéricos de pendientes mencionados en el texto.

    Returns:
        Dict con pendiente mínima, máxima y promedio mencionadas
    """
    if not text or pd.isna(text):
        return {'min_slope': None, 'max_slope': None, 'avg_slope': None}

    # Patrones regex para extraer pendientes
    # Busca: "35 grados", "pendiente de 40°", "35-45 grados", etc.
    patterns = [
        r'(\d{1,2})\s*(?:grados|°|degrees)',
        r'pendiente\s+de\s+(\d{1,2})',
        r'(\d{1,2})\s*-\s*(\d{1,2})\s*(?:grados|°)',
        r'inclinación\s+de\s+(\d{1,2})'
    ]

    slopes = []
    for pattern in patterns:
        matches = re.findall(pattern, str(text), re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                slopes.extend([int(m) for m in match if m.isdigit()])
            elif match.isdigit():
                slopes.append(int(match))

    # Filtrar valores válidos (0-90 grados)
    slopes = [s for s in slopes if 0 <= s <= 90]

    if slopes:
        return {
            'min_slope': min(slopes),
            'max_slope': max(slopes),
            'avg_slope': sum(slopes) / len(slopes)
        }
    else:
        return {'min_slope': None, 'max_slope': None, 'avg_slope': None}

# Crear UDF
extract_slope_schema = StructType([
    StructField("min_slope", IntegerType()),
    StructField("max_slope", IntegerType()),
    StructField("avg_slope", DoubleType())
])

extract_slope_udf = F.udf(extract_slope_values, extract_slope_schema)

# Aplicar extracción de pendientes
relatos_with_risk = relatos_with_risk.withColumn(
    "slope_info",
    extract_slope_udf(F.col("description"))
).select(
    "*",
    F.col("slope_info.min_slope").alias("mentioned_slope_min"),
    F.col("slope_info.max_slope").alias("mentioned_slope_max"),
    F.col("slope_info.avg_slope").alias("mentioned_slope_avg")
).drop("slope_info")

print("✅ Pendientes numéricas extraídas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Clasificar Nivel de Dificultad

# COMMAND ----------

# Clasificar dificultad basada en keywords y pendientes
relatos_with_risk = relatos_with_risk.withColumn(
    "difficulty_level",
    F.when(
        (F.col("nlp_risk_score") >= 0.7) |
        (F.col("mentioned_slope_max") >= 40) |
        (F.col("severity_high_count") > 0),
        "Muy Difícil"
    ).when(
        (F.col("nlp_risk_score") >= 0.5) |
        (F.col("mentioned_slope_max") >= 35),
        "Difícil"
    ).when(
        (F.col("nlp_risk_score") >= 0.3) |
        (F.col("mentioned_slope_max") >= 30),
        "Moderado"
    ).when(
        F.col("nlp_risk_score") >= 0.1,
        "Fácil"
    ).otherwise("Muy Fácil")
)

print("✅ Nivel de dificultad clasificado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Análisis de Sentimiento (opcional si modelo disponible)

# COMMAND ----------

def simple_sentiment_analysis(text: str) -> str:
    """
    Análisis de sentimiento simple basado en keywords.
    (Versión simplificada - usar transformer model si disponible)
    """
    if not text or pd.isna(text):
        return "neutral"

    text_lower = str(text).lower()

    positive_words = [
        'excelente', 'hermoso', 'hermosa', 'espectacular', 'increíble',
        'recomendado', 'recomendable', 'fácil', 'seguro', 'bueno', 'buena'
    ]

    negative_words = [
        'peligroso', 'difícil', 'malo', 'mala', 'evitar', 'no recomendado',
        'riesgoso', 'inseguro', 'problemático', 'complicado'
    ]

    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)

    if positive_count > negative_count:
        return "positivo"
    elif negative_count > positive_count:
        return "negativo"
    else:
        return "neutral"

sentiment_udf = F.udf(simple_sentiment_analysis, StringType())

relatos_with_risk = relatos_with_risk.withColumn(
    "sentiment",
    sentiment_udf(F.col("description"))
)

print("✅ Análisis de sentimiento aplicado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Crear Tabla de Patrones de Riesgo NLP

# COMMAND ----------

print("📊 Creando tabla de patrones de riesgo NLP...")

# Agregar elevación band si falta
relatos_with_risk = relatos_with_risk.withColumn(
    "elevation_band",
    F.when(F.col("elevation") < 2500, "Baja")
     .when(F.col("elevation") < 3200, "Media-Baja")
     .when(F.col("elevation") < 3500, "Media-Alta")
     .otherwise("Alta")
)

# Seleccionar columnas finales
nlp_patterns = relatos_with_risk.select(
    F.concat(F.lit("nlp_"), F.col("route_id")).alias("pattern_id"),
    F.lit(PILOT_ZONE['name']).alias("zone_name"),
    F.col("latitude"),
    F.col("longitude"),
    F.col("elevation"),
    F.col("elevation_band"),
    (F.col("avalanche_mentions") + F.col("risk_mentions")).alias("risk_mentions"),
    F.col("avalanche_mentions").alias("avalanche_events"),
    F.col("difficulty_level"),
    F.col("sentiment"),
    F.col("nlp_risk_score").alias("confidence"),
    F.lit("AndesHandbook").alias("source"),
    F.current_timestamp().alias("processing_timestamp")
)

print("✅ Tabla de patrones creada")

display(nlp_patterns.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Guardar en Delta Lake (Capa Silver)

# COMMAND ----------

print(f"💾 Guardando en: {TABLE_NLP_PATTERNS}")

# Verificar que tenemos datos antes de guardar
nlp_count_before = nlp_patterns.count()
if nlp_count_before == 0:
    raise Exception("❌ ERROR: DataFrame de patrones NLP está VACÍO. No hay datos para guardar.")

print(f"   Registros a guardar: {nlp_count_before}")

nlp_patterns.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_NLP_PATTERNS)

# Verificar que se guardó correctamente
nlp_count_after = spark.table(TABLE_NLP_PATTERNS).count()
if nlp_count_after == 0:
    raise Exception(f"❌ ERROR: Tabla {TABLE_NLP_PATTERNS} está VACÍA después de guardar")

print(f"✅ Datos guardados: {nlp_count_after} patrones de riesgo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Estadísticas de Patrones Extraídos

# COMMAND ----------

print("📊 ESTADÍSTICAS DE PATRONES DE RIESGO")
print("=" * 80)

# Por nivel de dificultad
print("\n🎯 Distribución por Nivel de Dificultad:")
display(
    nlp_patterns.groupBy("difficulty_level")
    .agg(
        F.count("*").alias("count"),
        F.avg("confidence").alias("avg_confidence")
    )
    .orderBy(F.desc("count"))
)

# Por banda altitudinal
print("\n⛰️  Distribución por Banda Altitudinal:")
display(
    nlp_patterns.groupBy("elevation_band")
    .agg(
        F.count("*").alias("count"),
        F.sum("avalanche_events").alias("total_avalanche_mentions"),
        F.avg("confidence").alias("avg_risk_score")
    )
    .orderBy("elevation_band")
)

# Por sentimiento
print("\n😊 Distribución por Sentimiento:")
display(
    nlp_patterns.groupBy("sentiment")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Identificar Zonas de Alto Riesgo Histórico

# COMMAND ----------

print("⚠️  ZONAS CON MAYOR MENCIÓN DE RIESGOS HISTÓRICOS")
print("=" * 80)

high_risk_historical = nlp_patterns.filter(
    (F.col("avalanche_events") > 0) | (F.col("confidence") >= 0.5)
).orderBy(F.desc("confidence"))

display(high_risk_historical.limit(20))

print(f"\n⚠️  {high_risk_historical.count()} zona(s) con menciones significativas de riesgo detectadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Resumen para Agente Integrador

# COMMAND ----------

# Crear resumen agregado para integración
nlp_summary = {
    "agent": "NLP",
    "zone": PILOT_ZONE['name'],
    "total_reports": nlp_patterns.count(),
    "high_risk_reports": high_risk_historical.count(),
    "avg_risk_score": float(nlp_patterns.agg(F.avg("confidence")).collect()[0][0] or 0),
    "total_avalanche_mentions": int(nlp_patterns.agg(F.sum("avalanche_events")).collect()[0][0] or 0),
    "most_common_difficulty": nlp_patterns.groupBy("difficulty_level").count().orderBy(F.desc("count")).first()["difficulty_level"],
    "timestamp": datetime.now().isoformat()
}

print("\n📋 RESUMEN PARA AGENTE INTEGRADOR:")
print("=" * 80)
for key, value in nlp_summary.items():
    print(f"   {key:25s}: {value}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Resumen Final

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 RESUMEN: AGENTE NLP - EXTRACCIÓN DE PATRONES DE RIESGO")
print("=" * 80)

print(f"""
📚 DATOS PROCESADOS:
   • Fuente: {TABLE_RELATOS_RAW}
   • Relatos analizados: {relatos_zona.count():,}
   • Zona: {PILOT_ZONE['name']}

🔍 PATRONES EXTRAÍDOS:
   • Total patrones: {nlp_patterns.count()}
   • Menciones de avalanchas: {nlp_summary['total_avalanche_mentions']}
   • Reportes de alto riesgo: {nlp_summary['high_risk_reports']}
   • Score promedio de riesgo: {nlp_summary['avg_risk_score']:.3f}

💾 OUTPUT:
   • Tabla: {TABLE_NLP_PATTERNS}
   • Formato: Delta Lake
   • Capa: Silver (Processed Features)

🎯 INSIGHTS CLAVE:
   • Dificultad más común: {nlp_summary['most_common_difficulty']}
   • Zonas con historial de riesgo identificadas
   • Conocimiento experto estructurado

✅ AGENTE NLP COMPLETADO
   → Datos listos para Agente Integrador
   → Próximo paso: Ejecutar Agente 3 (Meteorológico)
""")

print("=" * 80)
