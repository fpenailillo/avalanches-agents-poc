# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 4: Fusión y Clasificación de Riesgo
# MAGIC
# MAGIC Este agente integra outputs de los 3 agentes anteriores y genera:
# MAGIC - Clasificación de riesgo según Escala Europea (EAWS)
# MAGIC - Score multi-factorial de riesgo
# MAGIC - Predicciones por zona y banda altitudinal
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `topo_susceptibility` (Agente 1)
# MAGIC - `nlp_risk_patterns` (Agente 2)
# MAGIC - `weather_triggers` (Agente 3)
# MAGIC
# MAGIC **Output:** `avalanche_predictions` (capa Gold)

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar Outputs de Agentes Previos

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import uuid

print("📊 Cargando outputs de agentes previos...\n")

# Agente 1: Topográfico
print(f"   Cargando: {TABLE_TOPO_SUSCEPTIBILITY}")
try:
    topo_data = spark.table(TABLE_TOPO_SUSCEPTIBILITY)
    print(f"   ✅ Susceptibilidad topográfica: {topo_data.count()} registros")
except Exception as e:
    print(f"   ⚠️  Error: {e}")
    topo_data = None

# Agente 2: NLP
print(f"   Cargando: {TABLE_NLP_PATTERNS}")
try:
    nlp_data = spark.table(TABLE_NLP_PATTERNS)
    print(f"   ✅ Patrones NLP: {nlp_data.count()} registros")
except Exception as e:
    print(f"   ⚠️  Error: {e}")
    nlp_data = None

# Agente 3: Meteorológico - Triggers
print(f"   Cargando: {TABLE_WEATHER_TRIGGERS}")
try:
    weather_triggers = spark.table(TABLE_WEATHER_TRIGGERS)
    print(f"   ✅ Triggers meteorológicos: {weather_triggers.count()} registros")
except Exception as e:
    print(f"   ⚠️  Error: {e}")
    weather_triggers = None

# Agente 3: Meteorológico - Features
print(f"   Cargando: {TABLE_WEATHER_FEATURES}")
try:
    weather_features = spark.table(TABLE_WEATHER_FEATURES)
    print(f"   ✅ Features meteorológicos: {weather_features.count()} registros")
except Exception as e:
    print(f"   ⚠️  Error: {e}")
    weather_features = None

print("\n✅ Datos cargados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preparar Datos para Fusión

# COMMAND ----------

print("🔧 Preparando datos para fusión...")

# === TOPOGRAFÍA: Agregar por zona y banda altitudinal ===
if topo_data:
    topo_agg = topo_data.select(
        "zone_name",
        "elevation_band",
        F.col("susceptibility_score").alias("topo_score"),
        F.col("critical_slope_area_km2").alias("critical_area_km2")
    )
    print(f"   ✅ Topografía preparada: {topo_agg.count()} registros")
else:
    # Datos sintéticos para POC
    topo_agg = spark.createDataFrame([
        (PILOT_ZONE['name'], "Media-Alta", 0.7, 2.5),
        (PILOT_ZONE['name'], "Alta", 0.8, 1.8)
    ], ["zone_name", "elevation_band", "topo_score", "critical_area_km2"])
    print("   ⚠️  Usando datos topográficos sintéticos")

# === NLP: Agregar por zona y banda altitudinal ===
if nlp_data:
    nlp_agg = nlp_data.groupBy("zone_name", "elevation_band").agg(
        F.avg("confidence").alias("nlp_score"),
        F.sum("risk_mentions").alias("total_risk_mentions"),
        F.sum("avalanche_events").alias("total_avalanche_mentions"),
        F.count("*").alias("nlp_report_count")
    )
    print(f"   ✅ NLP preparado: {nlp_agg.count()} registros")
else:
    # Datos sintéticos
    nlp_agg = spark.createDataFrame([
        (PILOT_ZONE['name'], "Media-Alta", 0.5, 10, 3, 5),
        (PILOT_ZONE['name'], "Alta", 0.6, 15, 5, 8)
    ], ["zone_name", "elevation_band", "nlp_score", "total_risk_mentions", "total_avalanche_mentions", "nlp_report_count"])
    print("   ⚠️  Usando datos NLP sintéticos")

# === METEOROLOGÍA: Agregar triggers por banda altitudinal ===
if weather_triggers and weather_features:
    # Obtener última fecha disponible
    max_date = weather_features.agg(F.max("date")).collect()[0][0]

    # Filtrar últimos 3 días
    recent_weather = weather_features.filter(
        F.col("date") >= F.date_sub(F.lit(max_date), 2)
    )

    # Agregar por banda altitudinal
    weather_agg = recent_weather.groupBy("location_name", "elevation_band").agg(
        F.max("snowfall_24h").alias("max_snowfall_24h"),
        F.max("snowfall_72h").alias("max_snowfall_72h"),
        F.max("temp_variation_24h").alias("max_temp_variation"),
        F.max("windspeed_10m_max").alias("max_wind_speed"),
        F.avg("isoterma_0c").alias("avg_isoterma")
    ).withColumnRenamed("location_name", "zone_name")

    # Contar triggers críticos
    if weather_triggers.count() > 0:
        critical_triggers_count = weather_triggers.filter(
            (F.col("is_critical") == True) &
            (F.col("date") >= F.date_sub(F.lit(max_date), 2))
        ).groupBy("location_name", "elevation_band").agg(
            F.count("*").alias("critical_trigger_count")
        ).withColumnRenamed("location_name", "zone_name")

        weather_agg = weather_agg.join(critical_triggers_count, ["zone_name", "elevation_band"], "left")
        weather_agg = weather_agg.fillna({"critical_trigger_count": 0})
    else:
        weather_agg = weather_agg.withColumn("critical_trigger_count", F.lit(0))

    print(f"   ✅ Meteorología preparada: {weather_agg.count()} registros")
else:
    # Datos sintéticos
    weather_agg = spark.createDataFrame([
        (PILOT_ZONE['name'], "Media-Alta", 25.0, 60.0, 12.0, 45.0, 3200.0, 2),
        (PILOT_ZONE['name'], "Alta", 35.0, 85.0, 15.0, 55.0, 3500.0, 3)
    ], ["zone_name", "elevation_band", "max_snowfall_24h", "max_snowfall_72h", "max_temp_variation", "max_wind_speed", "avg_isoterma", "critical_trigger_count"])
    print("   ⚠️  Usando datos meteorológicos sintéticos")

    max_date = datetime.now().date()

print("\n✅ Datos preparados para fusión")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fusionar Datos de los 3 Agentes

# COMMAND ----------

print("🔗 Fusionando datos de agentes...")

# Fusión completa
risk_fusion = topo_agg.join(
    nlp_agg,
    ["zone_name", "elevation_band"],
    "outer"
).join(
    weather_agg,
    ["zone_name", "elevation_band"],
    "outer"
)

# Rellenar valores nulos con defaults
risk_fusion = risk_fusion.fillna({
    "topo_score": 0.0,
    "nlp_score": 0.0,
    "total_risk_mentions": 0,
    "total_avalanche_mentions": 0,
    "critical_trigger_count": 0,
    "max_snowfall_24h": 0.0,
    "max_snowfall_72h": 0.0,
    "max_temp_variation": 0.0,
    "max_wind_speed": 0.0
})

print(f"✅ Fusión completada: {risk_fusion.count()} combinaciones zona-banda")

display(risk_fusion)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Calcular Score Multi-Factorial de Riesgo

# COMMAND ----------

print("📐 Calculando score multi-factorial de riesgo...")

# === PESOS DE CONTRIBUCIÓN ===
# Pesos ajustables según importancia de cada factor
WEIGHT_TOPO = 0.35  # 35% topografía (base persistente)
WEIGHT_WEATHER = 0.45  # 45% meteorología (gatillante principal)
WEIGHT_NLP = 0.20  # 20% conocimiento histórico

# === COMPONENTE TOPOGRÁFICO ===
# Ya está normalizado 0-1 (susceptibility_score)
risk_fusion = risk_fusion.withColumn(
    "topo_contribution",
    F.col("topo_score") * WEIGHT_TOPO
)

# === COMPONENTE METEOROLÓGICO ===
# Normalizar y combinar múltiples factores meteorológicos
risk_fusion = risk_fusion.withColumn(
    "weather_score_raw",
    (
        # Nevada 24h (normalizado respecto a 80cm = 1.0)
        F.least(F.col("max_snowfall_24h") / 80.0, F.lit(1.0)) * 0.3 +

        # Nevada 72h (normalizado respecto a 120cm = 1.0)
        F.least(F.col("max_snowfall_72h") / 120.0, F.lit(1.0)) * 0.25 +

        # Variación térmica (normalizado respecto a 20°C = 1.0)
        F.least(F.col("max_temp_variation") / 20.0, F.lit(1.0)) * 0.2 +

        # Viento (normalizado respecto a 90 km/h = 1.0)
        F.least(F.col("max_wind_speed") / 90.0, F.lit(1.0)) * 0.15 +

        # Triggers críticos (cada trigger crítico suma 0.1, max 1.0)
        F.least(F.col("critical_trigger_count") * 0.1, F.lit(1.0)) * 0.1
    )
).withColumn(
    "weather_contribution",
    F.col("weather_score_raw") * WEIGHT_WEATHER
)

# === COMPONENTE NLP ===
# Normalizar basado en menciones de riesgo
risk_fusion = risk_fusion.withColumn(
    "nlp_score_adjusted",
    (
        # Score base de NLP (ya 0-1)
        F.col("nlp_score") * 0.6 +

        # Menciones de avalanchas (normalizado respecto a 10 menciones = 1.0)
        F.least(F.col("total_avalanche_mentions") / 10.0, F.lit(1.0)) * 0.4
    )
).withColumn(
    "nlp_contribution",
    F.col("nlp_score_adjusted") * WEIGHT_NLP
)

# === SCORE FINAL INTEGRADO ===
risk_fusion = risk_fusion.withColumn(
    "risk_score",
    F.col("topo_contribution") + F.col("weather_contribution") + F.col("nlp_contribution")
).withColumn(
    "risk_score",
    # Asegurar rango 0-1
    F.when(F.col("risk_score") > 1.0, 1.0)
     .when(F.col("risk_score") < 0.0, 0.0)
     .otherwise(F.col("risk_score"))
)

print("✅ Score multi-factorial calculado")

# Mostrar contribuciones
display(risk_fusion.select(
    "zone_name",
    "elevation_band",
    "topo_contribution",
    "weather_contribution",
    "nlp_contribution",
    "risk_score"
).orderBy(F.desc("risk_score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clasificar según Escala EAWS (1-5)

# COMMAND ----------

print("🎯 Clasificando riesgo según Escala Europea (EAWS)...")

risk_fusion = risk_fusion.withColumn(
    "eaws_level",
    F.when(F.col("risk_score") < 0.2, 1)
     .when(F.col("risk_score") < 0.4, 2)
     .when(F.col("risk_score") < 0.6, 3)
     .when(F.col("risk_score") < 0.8, 4)
     .otherwise(5)
).withColumn(
    "eaws_label",
    F.when(F.col("eaws_level") == 1, "Débil")
     .when(F.col("eaws_level") == 2, "Limitado")
     .when(F.col("eaws_level") == 3, "Notable")
     .when(F.col("eaws_level") == 4, "Fuerte")
     .otherwise("Muy Fuerte")
)

print("✅ Clasificación EAWS aplicada")

# Mostrar distribución
print("\n📊 Distribución de Niveles de Peligro:")
display(
    risk_fusion.groupBy("eaws_level", "eaws_label")
    .agg(F.count("*").alias("count"))
    .orderBy("eaws_level")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calcular Confianza de Predicción

# COMMAND ----------

print("📊 Calculando confianza de predicción...")

# La confianza depende de la disponibilidad y calidad de datos de cada agente
risk_fusion = risk_fusion.withColumn(
    "confidence",
    (
        # Si topo_score > 0, contribuye 0.4 a confianza
        F.when(F.col("topo_score") > 0, 0.4).otherwise(0.0) +

        # Si weather_score_raw > 0, contribuye 0.4 a confianza
        F.when(F.col("weather_score_raw") > 0, 0.4).otherwise(0.0) +

        # Si nlp_score > 0, contribuye 0.2 a confianza
        F.when(F.col("nlp_score") > 0, 0.2).otherwise(0.0)
    )
).withColumn(
    "confidence",
    # Normalizar a rango 0-1
    F.col("confidence") / 1.0
)

print("✅ Confianza calculada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Identificar Factores Principales

# COMMAND ----------

print("🔍 Identificando factores principales de riesgo...")

def identify_main_factors(row):
    """Identifica los 3 factores principales que contribuyen al riesgo"""
    factors = []

    # Topográficos
    if row['topo_score'] >= 0.6:
        factors.append(f"Pendientes críticas ({row['critical_area_km2']:.1f} km²)")

    # Meteorológicos
    if row['max_snowfall_24h'] >= THRESHOLD_SNOWFALL_MODERATE:
        factors.append(f"Nevada intensa ({row['max_snowfall_24h']:.0f} cm/24h)")

    if row['max_snowfall_72h'] >= 50:
        factors.append(f"Nevada acumulada ({row['max_snowfall_72h']:.0f} cm/72h)")

    if row['max_temp_variation'] >= THRESHOLD_TEMP_VAR_MODERATE:
        factors.append(f"Fluctuación térmica ({row['max_temp_variation']:.0f}°C)")

    if row['max_wind_speed'] >= THRESHOLD_WIND_MODERATE:
        factors.append(f"Viento fuerte ({row['max_wind_speed']:.0f} km/h)")

    # Históricos
    if row['total_avalanche_mentions'] > 0:
        factors.append(f"Historial de {row['total_avalanche_mentions']} evento(s)")

    if row['critical_trigger_count'] > 0:
        factors.append(f"{row['critical_trigger_count']} condición(es) gatillante(s)")

    # Retornar top 5 factores
    return factors[:5] if factors else ["Condiciones normales"]

# Aplicar como UDF
identify_factors_udf = F.udf(identify_main_factors, ArrayType(StringType()))

risk_fusion = risk_fusion.withColumn(
    "main_factors",
    identify_factors_udf(F.struct([F.col(c) for c in risk_fusion.columns]))
)

print("✅ Factores principales identificados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Crear Tabla de Predicciones

# COMMAND ----------

print("📋 Creando tabla de predicciones...")

# Crear predicciones con fecha de forecast
predictions = risk_fusion.select(
    F.concat(
        F.lit("pred_"),
        F.date_format(F.lit(max_date), "yyyyMMdd"),
        F.lit("_"),
        F.regexp_replace("zone_name", " ", "_"),
        F.lit("_"),
        F.regexp_replace("elevation_band", " ", "_")
    ).alias("prediction_id"),
    "zone_name",
    F.lit(max_date).cast("date").alias("forecast_date"),
    "elevation_band",
    "eaws_level",
    "eaws_label",
    "risk_score",
    "confidence",
    "topo_contribution",
    "weather_contribution",
    "nlp_contribution",
    "main_factors",
    F.current_timestamp().alias("prediction_timestamp")
)

print(f"✅ Predicciones creadas: {predictions.count()} registros")

display(predictions.orderBy(F.desc("risk_score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Guardar Predicciones en Delta Lake (Capa Gold)

# COMMAND ----------

print(f"💾 Guardando predicciones en: {TABLE_RISK_PREDICTIONS}")

predictions.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("zone_name", "forecast_date") \
    .saveAsTable(TABLE_RISK_PREDICTIONS)

print(f"✅ Predicciones guardadas: {predictions.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Análisis de Predicciones

# COMMAND ----------

print("📊 ANÁLISIS DE PREDICCIONES")
print("=" * 80)

# Por nivel EAWS
print("\n🎯 Distribución por Nivel de Peligro:")
display(
    predictions.groupBy("eaws_level", "eaws_label")
    .agg(
        F.count("*").alias("count"),
        F.avg("confidence").alias("avg_confidence")
    )
    .orderBy("eaws_level")
)

# Por banda altitudinal
print("\n⛰️  Riesgo por Banda Altitudinal:")
display(
    predictions.groupBy("elevation_band")
    .agg(
        F.avg("risk_score").alias("avg_risk_score"),
        F.max("eaws_level").alias("max_eaws_level"),
        F.avg("confidence").alias("avg_confidence")
    )
    .orderBy(F.desc("avg_risk_score"))
)

# Zonas de mayor riesgo
print("\n⚠️  Zonas de Mayor Riesgo:")
high_risk = predictions.filter(F.col("eaws_level") >= 3).orderBy(F.desc("risk_score"))
display(high_risk)

print(f"\n⚠️  {high_risk.count()} zona(s)-banda(s) con nivel NOTABLE o superior")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Visualización de Predicciones

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

predictions_pd = predictions.toPandas()

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Gráfico 1: Risk score por banda altitudinal
if len(predictions_pd) > 0:
    predictions_pd_sorted = predictions_pd.sort_values('risk_score')
    axes[0].barh(
        predictions_pd_sorted['elevation_band'],
        predictions_pd_sorted['risk_score'],
        color=['green', 'yellow', 'orange', 'red', 'purple'][:len(predictions_pd_sorted)]
    )
    axes[0].set_xlabel('Risk Score', fontsize=12)
    axes[0].set_ylabel('Banda Altitudinal', fontsize=12)
    axes[0].set_title('Score de Riesgo por Banda Altitudinal', fontsize=14, fontweight='bold')
    axes[0].grid(True, alpha=0.3, axis='x')

    # Gráfico 2: Contribuciones por agente
    contributions = predictions_pd[['topo_contribution', 'weather_contribution', 'nlp_contribution']].mean()
    axes[1].pie(
        contributions.values,
        labels=['Topografía', 'Meteorología', 'NLP'],
        autopct='%1.1f%%',
        colors=['brown', 'skyblue', 'lightgreen'],
        startangle=90
    )
    axes[1].set_title('Contribución Promedio por Agente', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.show()

print("✅ Visualizaciones generadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Resumen Final

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 RESUMEN: AGENTE INTEGRADOR - CLASIFICACIÓN DE RIESGO")
print("=" * 80)

total_predictions = predictions.count()
high_risk_count = predictions.filter(F.col("eaws_level") >= 3).count()
avg_confidence = predictions.agg(F.avg("confidence")).collect()[0][0]

print(f"""
🔗 FUSIÓN DE DATOS:
   • Agente Topográfico: {topo_agg.count()} registros
   • Agente NLP: {nlp_agg.count()} registros
   • Agente Meteorológico: {weather_agg.count()} registros

📊 PREDICCIONES GENERADAS:
   • Total predicciones: {total_predictions}
   • Zona: {PILOT_ZONE['name']}
   • Fecha pronóstico: {max_date}
   • Predicciones alto riesgo: {high_risk_count}

🎯 CLASIFICACIÓN EAWS:
   • Niveles identificados: {predictions.select("eaws_level").distinct().count()}
   • Confianza promedio: {avg_confidence:.2%}

⚖️  PESOS DE INTEGRACIÓN:
   • Topografía: {WEIGHT_TOPO:.0%}
   • Meteorología: {WEIGHT_WEATHER:.0%}
   • NLP (histórico): {WEIGHT_NLP:.0%}

💾 OUTPUT:
   • Tabla: {TABLE_RISK_PREDICTIONS}
   • Formato: Delta Lake
   • Capa: Gold (Analytics & Predictions)

✅ AGENTE INTEGRADOR COMPLETADO
   → Predicciones listas para generación de boletín
   → Próximo paso: Ejecutar 02_boletin_generation.py
""")

print("=" * 80)
