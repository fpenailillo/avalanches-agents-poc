# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 3: Detección de Condiciones Gatillantes
# MAGIC
# MAGIC Analiza datos meteorológicos para identificar condiciones que pueden gatillar avalanchas.
# MAGIC
# MAGIC **Input:** `weather_daily` (capa Bronze)
# MAGIC **Output:** `weather_triggers` (capa Silver)

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar Datos Meteorológicos

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

print(f"🌦️  Cargando datos desde: {TABLE_WEATHER_DAILY}")

weather_df = spark.table(TABLE_WEATHER_DAILY)

print(f"✅ Datos cargados: {weather_df.count():,} registros")

display(weather_df.orderBy(F.desc("date")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calcular Features Meteorológicos

# COMMAND ----------

print("📊 Calculando features meteorológicos por ventanas temporales...")

# Ordenar por ubicación y fecha
window_spec = Window.partitionBy("location_name").orderBy("date")

# Calcular nevadas acumuladas en ventanas de tiempo
weather_features = weather_df.withColumn(
    "snowfall_24h",
    F.col("snowfall_sum")  # Diario = últimas 24h
).withColumn(
    "snowfall_48h",
    F.sum("snowfall_sum").over(window_spec.rowsBetween(-1, 0))  # Últimos 2 días
).withColumn(
    "snowfall_72h",
    F.sum("snowfall_sum").over(window_spec.rowsBetween(-2, 0))  # Últimos 3 días
).withColumn(
    "snowfall_7d",
    F.sum("snowfall_sum").over(window_spec.rowsBetween(-6, 0))  # Última semana
)

# Calcular variación de temperatura
weather_features = weather_features.withColumn(
    "temp_variation_24h",
    F.abs(F.col("temperature_2m_max") - F.col("temperature_2m_min"))
).withColumn(
    "temp_variation_48h",
    F.abs(F.col("temperature_2m_max") - F.lag("temperature_2m_min", 1).over(window_spec))
)

# Estimar isoterma 0°C (altura donde temperatura = 0°C)
# Aproximación: descenso de ~0.65°C cada 100m
weather_features = weather_features.withColumn(
    "isoterma_0c",
    F.when(
        F.col("temperature_2m_mean") > 0,
        F.col("elevation") + (F.col("temperature_2m_mean") / 0.0065)
    ).otherwise(
        F.col("elevation")
    ).cast("int")
)

# Clasificar intensidad de precipitación
weather_features = weather_features.withColumn(
    "precipitation_intensity",
    F.when(F.col("precipitation_sum") < 5, "Baja")
     .when(F.col("precipitation_sum") < 15, "Moderada")
     .when(F.col("precipitation_sum") < 30, "Alta")
     .otherwise("Muy Alta")
)

print("✅ Features calculados")

# Mostrar muestra
display(weather_features.select(
    "location_name",
    "date",
    "snowfall_24h",
    "snowfall_72h",
    "temp_variation_24h",
    "isoterma_0c",
    "windspeed_10m_max"
).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Detectar Condiciones Gatillantes

# COMMAND ----------

print("🎯 Detectando condiciones gatillantes de avalanchas...")

# === TRIGGER 1: Nevada intensa ===
weather_features = weather_features.withColumn(
    "trigger_snowfall_24h",
    F.when(F.col("snowfall_24h") >= THRESHOLD_SNOWFALL_EXTREME, "Extremo")
     .when(F.col("snowfall_24h") >= THRESHOLD_SNOWFALL_HIGH, "Alto")
     .when(F.col("snowfall_24h") >= THRESHOLD_SNOWFALL_MODERATE, "Moderado")
     .when(F.col("snowfall_24h") >= THRESHOLD_SNOWFALL_LOW, "Bajo")
     .otherwise(None)
)

# === TRIGGER 2: Nevada acumulada 48-72h ===
weather_features = weather_features.withColumn(
    "trigger_snowfall_accumulation",
    F.when(F.col("snowfall_72h") >= 80, "Crítico")
     .when(F.col("snowfall_72h") >= 50, "Alto")
     .when(F.col("snowfall_72h") >= 30, "Moderado")
     .otherwise(None)
)

# === TRIGGER 3: Fluctuación térmica ===
weather_features = weather_features.withColumn(
    "trigger_temp_variation",
    F.when(F.col("temp_variation_24h") >= THRESHOLD_TEMP_VAR_HIGH, "Alto")
     .when(F.col("temp_variation_24h") >= THRESHOLD_TEMP_VAR_MODERATE, "Moderado")
     .when(F.col("temp_variation_24h") >= THRESHOLD_TEMP_VAR_LOW, "Bajo")
     .otherwise(None)
)

# === TRIGGER 4: Viento fuerte ===
weather_features = weather_features.withColumn(
    "trigger_wind",
    F.when(F.col("windspeed_10m_max") >= THRESHOLD_WIND_HIGH, "Alto")
     .when(F.col("windspeed_10m_max") >= THRESHOLD_WIND_MODERATE, "Moderado")
     .when(F.col("windspeed_10m_max") >= THRESHOLD_WIND_LOW, "Bajo")
     .otherwise(None)
)

# === TRIGGER 5: Isoterma 0°C elevada (deshielo) ===
weather_features = weather_features.withColumn(
    "trigger_isoterma",
    F.when(
        (F.col("isoterma_0c") > 3000) & (F.col("isoterma_0c") < 4000),
        "Zona Crítica"
    ).otherwise(None)
)

# === TRIGGER 6: Combinación lluvia-nieve ===
weather_features = weather_features.withColumn(
    "trigger_rain_on_snow",
    F.when(
        (F.col("precipitation_sum") > 10) &
        (F.col("temperature_2m_mean") > 0) &
        (F.col("temperature_2m_mean") < 5),
        "Detectado"
    ).otherwise(None)
)

print("✅ Condiciones gatillantes detectadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear Tabla de Triggers

# COMMAND ----------

print("📋 Creando tabla de triggers...")

# Filtrar solo registros con al menos un trigger
triggers_data = weather_features.filter(
    (F.col("trigger_snowfall_24h").isNotNull()) |
    (F.col("trigger_snowfall_accumulation").isNotNull()) |
    (F.col("trigger_temp_variation").isNotNull()) |
    (F.col("trigger_wind").isNotNull()) |
    (F.col("trigger_isoterma").isNotNull()) |
    (F.col("trigger_rain_on_snow").isNotNull())
)

# Transformar a formato largo (unpivot)
trigger_types = [
    ("trigger_snowfall_24h", "Nevada Intensa 24h", "snowfall_24h"),
    ("trigger_snowfall_accumulation", "Nevada Acumulada 72h", "snowfall_72h"),
    ("trigger_temp_variation", "Fluctuación Térmica", "temp_variation_24h"),
    ("trigger_wind", "Viento Fuerte", "windspeed_10m_max"),
    ("trigger_isoterma", "Isoterma Crítica", "isoterma_0c"),
    ("trigger_rain_on_snow", "Lluvia sobre Nieve", "precipitation_sum")
]

all_triggers = []

for trigger_col, trigger_type, value_col in trigger_types:
    trigger_subset = triggers_data.filter(F.col(trigger_col).isNotNull()).select(
        F.concat(F.lit("trigger_"), F.lit(uuid.uuid4().hex[:8])).alias("trigger_id"),
        F.col("location_name"),
        F.col("date"),
        F.when(F.col("elevation") < 2500, "Baja")
         .when(F.col("elevation") < 3200, "Media-Baja")
         .when(F.col("elevation") < 3500, "Media-Alta")
         .otherwise("Alta").alias("elevation_band"),
        F.lit(trigger_type).alias("trigger_type"),
        F.col(trigger_col).alias("trigger_severity"),
        F.col(value_col).alias("trigger_value"),
        F.when(
            (F.col(trigger_col) == "Extremo") | (F.col(trigger_col) == "Crítico") | (F.col(trigger_col) == "Alto"),
            True
        ).otherwise(False).alias("is_critical"),
        F.current_timestamp().alias("processing_timestamp")
    )

    all_triggers.append(trigger_subset)

# Unir todos los triggers
if all_triggers:
    triggers_final = all_triggers[0]
    for trigger_df in all_triggers[1:]:
        triggers_final = triggers_final.union(trigger_df)

    print(f"✅ Tabla de triggers creada: {triggers_final.count()} triggers detectados")
else:
    print("⚠️  No se detectaron triggers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar Triggers en Delta Lake (Capa Silver)

# COMMAND ----------

if all_triggers:
    print(f"💾 Guardando triggers en: {TABLE_WEATHER_TRIGGERS}")

    # Verificar que tenemos datos antes de guardar
    triggers_count_before = triggers_final.count()
    if triggers_count_before == 0:
        raise Exception("❌ ERROR: DataFrame de triggers meteorológicos está VACÍO. No hay datos para guardar.")

    print(f"   Registros a guardar: {triggers_count_before}")

    triggers_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("location_name", "date") \
        .saveAsTable(TABLE_WEATHER_TRIGGERS)

    # Verificar que se guardó correctamente
    triggers_count_after = spark.table(TABLE_WEATHER_TRIGGERS).count()
    if triggers_count_after == 0:
        raise Exception(f"❌ ERROR: Tabla {TABLE_WEATHER_TRIGGERS} está VACÍA después de guardar")

    print(f"✅ Triggers guardados: {triggers_count_after} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análisis de Triggers Detectados

# COMMAND ----------

if all_triggers:
    print("📊 ANÁLISIS DE TRIGGERS DETECTADOS")
    print("=" * 80)

    # Por tipo de trigger
    print("\n🎯 Distribución por Tipo de Trigger:")
    display(
        triggers_final.groupBy("trigger_type", "trigger_severity")
        .agg(F.count("*").alias("count"))
        .orderBy("trigger_type", F.desc("count"))
    )

    # Por severidad
    print("\n⚠️  Distribución por Severidad:")
    display(
        triggers_final.groupBy("trigger_severity")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )

    # Triggers críticos
    print("\n🚨 Triggers Críticos:")
    critical_triggers = triggers_final.filter(F.col("is_critical") == True)
    display(critical_triggers.orderBy(F.desc("date")).limit(20))

    print(f"\n⚠️  {critical_triggers.count()} condiciones CRÍTICAS detectadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Crear Features Meteorológicos Agregados (para Agente Integrador)

# COMMAND ----------

print("📊 Creando features meteorológicos agregados...")

weather_features_final = weather_features.select(
    F.concat(F.lit("weather_"), F.date_format("date", "yyyyMMdd"), F.lit("_"), F.col("location_name")).alias("feature_id"),
    "location_name",
    "date",
    F.when(F.col("elevation") < 2500, "Baja")
     .when(F.col("elevation") < 3200, "Media-Baja")
     .when(F.col("elevation") < 3500, "Media-Alta")
     .otherwise("Alta").alias("elevation_band"),
    "snowfall_24h",
    "snowfall_48h",
    "snowfall_72h",
    "temp_variation_24h",
    "windspeed_10m_max",
    "precipitation_intensity",
    "isoterma_0c",
    F.current_timestamp().alias("processing_timestamp")
)

print(f"💾 Guardando en: {TABLE_WEATHER_FEATURES}")

weather_features_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("location_name", "date") \
    .saveAsTable(TABLE_WEATHER_FEATURES)

print(f"✅ Features guardados: {weather_features_final.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Visualización de Triggers en el Tiempo

# COMMAND ----------

if all_triggers:
    import matplotlib.pyplot as plt
    import pandas as pd

    # Convertir a Pandas para visualización
    triggers_pd = triggers_final.toPandas()

    # Gráfico de triggers por día
    triggers_by_date = triggers_pd.groupby(['date', 'trigger_severity']).size().unstack(fill_value=0)

    fig, ax = plt.subplots(figsize=(14, 6))
    triggers_by_date.plot(kind='bar', stacked=True, ax=ax, color=['green', 'yellow', 'orange', 'red'])
    ax.set_xlabel('Fecha', fontsize=12)
    ax.set_ylabel('Número de Triggers', fontsize=12)
    ax.set_title('Evolución de Condiciones Gatillantes por Severidad', fontsize=14, fontweight='bold')
    ax.legend(title='Severidad', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    print("✅ Visualización generada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumen para Agente Integrador

# COMMAND ----------

if all_triggers:
    weather_summary = {
        "agent": "Meteorologico",
        "zone": PILOT_ZONE['name'],
        "total_days_analyzed": weather_features.select("date").distinct().count(),
        "total_triggers": triggers_final.count(),
        "critical_triggers": triggers_final.filter(F.col("is_critical") == True).count(),
        "max_snowfall_24h": float(weather_features.agg(F.max("snowfall_24h")).collect()[0][0] or 0),
        "max_snowfall_72h": float(weather_features.agg(F.max("snowfall_72h")).collect()[0][0] or 0),
        "max_wind_speed": float(weather_features.agg(F.max("windspeed_10m_max")).collect()[0][0] or 0),
        "timestamp": datetime.now().isoformat()
    }

    print("\n📋 RESUMEN PARA AGENTE INTEGRADOR:")
    print("=" * 80)
    for key, value in weather_summary.items():
        print(f"   {key:25s}: {value}")
    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumen Final

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 RESUMEN: AGENTE METEOROLÓGICO - DETECCIÓN DE GATILLANTES")
print("=" * 80)

if all_triggers:
    print(f"""
🌦️  ANÁLISIS COMPLETADO:
   • Días analizados: {weather_summary['total_days_analyzed']}
   • Ubicaciones: {weather_features.select("location_name").distinct().count()}

🎯 TRIGGERS DETECTADOS:
   • Total triggers: {weather_summary['total_triggers']}
   • Triggers críticos: {weather_summary['critical_triggers']}

📈 CONDICIONES MÁXIMAS:
   • Nevada 24h: {weather_summary['max_snowfall_24h']:.1f} cm
   • Nevada 72h: {weather_summary['max_snowfall_72h']:.1f} cm
   • Viento máximo: {weather_summary['max_wind_speed']:.1f} km/h

💾 OUTPUTS GENERADOS:
   1. {TABLE_WEATHER_FEATURES}
      → Features meteorológicos agregados por ventanas temporales

   2. {TABLE_WEATHER_TRIGGERS}
      → Condiciones gatillantes detectadas y clasificadas

🎯 TIPOS DE TRIGGERS MONITOREADOS:
   ✅ Nevada intensa 24h (>30cm)
   ✅ Nevada acumulada 72h (>50cm)
   ✅ Fluctuación térmica (>10°C)
   ✅ Viento fuerte (>50km/h)
   ✅ Isoterma 0°C en zona crítica
   ✅ Lluvia sobre nieve

✅ AGENTE METEOROLÓGICO COMPLETADO
   → Datos listos para Agente Integrador
   → Próximo paso: Ejecutar Agente 4 (Integrador)
""")
else:
    print("\n⚠️  No se detectaron condiciones gatillantes en el periodo analizado")

print("=" * 80)
