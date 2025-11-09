# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 1: Mapa de Susceptibilidad Topográfica
# MAGIC
# MAGIC Este notebook genera el mapa de susceptibilidad basado en features topográficos procesados.
# MAGIC
# MAGIC **Input:** `topo_features` (capa Silver)
# MAGIC **Output:** `topo_susceptibility` (capa Silver) y `susceptibility_map` (capa Gold)

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar Features Topográficos

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid
from datetime import datetime

print(f"📊 Cargando features topográficos desde: {TABLE_TOPO_FEATURES}")

topo_features = spark.table(TABLE_TOPO_FEATURES)

print(f"✅ Features cargados: {topo_features.count():,} registros")

# Mostrar muestra
display(topo_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calcular Susceptibilidad por Zona y Banda Altitudinal

# COMMAND ----------

print("🎯 Calculando susceptibilidad topográfica por zona y banda altitudinal...")

# Agregar por zona y banda altitudinal
susceptibility_stats = topo_features.groupBy("zone_name", "elevation_band").agg(
    F.count("*").alias("total_pixels"),
    F.sum(F.when(F.col("is_critical_slope"), 1).otherwise(0)).alias("critical_pixels"),
    F.avg("slope_degrees").alias("avg_slope_degrees"),
    F.avg("elevation").alias("avg_elevation"),
    F.avg("aspect_degrees").alias("avg_aspect_degrees"),
    F.stddev("slope_degrees").alias("stddev_slope")
).withColumn(
    "critical_area_ratio",
    F.col("critical_pixels") / F.col("total_pixels")
)

# Calcular susceptibility score (0-1)
# Score basado en:
# - 60% ratio de pendientes críticas
# - 30% pendiente promedio normalizada (max 60°)
# - 10% desviación estándar (indica variabilidad del terreno)

susceptibility_stats = susceptibility_stats.withColumn(
    "susceptibility_score",
    (
        0.6 * F.col("critical_area_ratio") +
        0.3 * (F.col("avg_slope_degrees") / 60.0) +
        0.1 * (F.col("stddev_slope") / 30.0)
    )
).withColumn(
    "susceptibility_score",
    F.when(F.col("susceptibility_score") > 1.0, 1.0)
     .otherwise(F.col("susceptibility_score"))
)

# Determinar orientación dominante
susceptibility_stats = susceptibility_stats.withColumn(
    "dominant_aspect",
    F.when(F.col("avg_aspect_degrees") < 45, "Norte")
     .when(F.col("avg_aspect_degrees") < 135, "Este")
     .when(F.col("avg_aspect_degrees") < 225, "Sur")
     .when(F.col("avg_aspect_degrees") < 315, "Oeste")
     .otherwise("Norte")
)

# Calcular área aproximada (30m x 30m = 900 m² por pixel)
susceptibility_stats = susceptibility_stats.withColumn(
    "total_area_km2",
    (F.col("total_pixels") * 900) / 1_000_000  # Convertir a km²
).withColumn(
    "critical_slope_area_km2",
    (F.col("critical_pixels") * 900) / 1_000_000
)

# Agregar metadatos
susceptibility_stats = susceptibility_stats.withColumn(
    "zone_id",
    F.concat(
        F.lit("zone_"),
        F.regexp_replace(F.col("zone_name"), " ", "_"),
        F.lit("_"),
        F.regexp_replace(F.col("elevation_band"), " ", "_")
    )
).withColumn(
    "processing_timestamp",
    F.current_timestamp()
)

# Reordenar columnas
susceptibility_final = susceptibility_stats.select(
    "zone_id",
    "zone_name",
    "elevation_band",
    "total_area_km2",
    "critical_slope_area_km2",
    "susceptibility_score",
    "dominant_aspect",
    "avg_slope_degrees",
    "processing_timestamp"
)

print("✅ Susceptibilidad calculada")

display(susceptibility_final.orderBy(F.desc("susceptibility_score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Guardar en Tabla Silver: Susceptibility

# COMMAND ----------

print(f"💾 Guardando en: {TABLE_TOPO_SUSCEPTIBILITY}")

susceptibility_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_TOPO_SUSCEPTIBILITY)

print(f"✅ Tabla guardada: {susceptibility_final.count()} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear Mapa de Susceptibilidad de Alta Resolución (Gold)

# COMMAND ----------

print(f"🗺️  Creando mapa de susceptibilidad de alta resolución...")

# Unir features con scores de susceptibilidad
susceptibility_map = topo_features.alias("feat").join(
    susceptibility_final.alias("susc"),
    (F.col("feat.zone_name") == F.col("susc.zone_name")) &
    (F.col("feat.elevation_band") == F.col("susc.elevation_band")),
    "left"
)

# Seleccionar columnas para mapa final
susceptibility_map_final = susceptibility_map.select(
    F.concat(
        F.lit("grid_"),
        F.substring(F.col("feat.feature_id"), 6, 12)
    ).alias("grid_id"),
    F.col("feat.zone_name").alias("zone_name"),
    F.col("feat.latitude").alias("latitude"),
    F.col("feat.longitude").alias("longitude"),
    F.col("feat.elevation").alias("elevation"),
    F.col("feat.elevation_band").alias("elevation_band"),
    F.col("feat.slope_degrees").alias("slope_degrees"),
    F.col("feat.aspect_degrees").alias("aspect_degrees"),
    F.col("susc.susceptibility_score").alias("susceptibility_score"),
    F.when(F.col("susc.susceptibility_score") >= 0.8, "Muy Alto")
     .when(F.col("susc.susceptibility_score") >= 0.6, "Alto")
     .when(F.col("susc.susceptibility_score") >= 0.4, "Moderado")
     .when(F.col("susc.susceptibility_score") >= 0.2, "Bajo")
     .otherwise("Muy Bajo")
     .alias("risk_category"),
    F.current_timestamp().alias("last_updated")
)

print("✅ Mapa de susceptibilidad creado")

# Mostrar distribución de categorías de riesgo
print("\n📊 Distribución de Categorías de Riesgo:")
display(
    susceptibility_map_final
    .groupBy("risk_category")
    .agg(F.count("*").alias("pixel_count"))
    .withColumn("percentage", F.round(F.col("pixel_count") / F.sum("pixel_count").over(Window.partitionBy()) * 100, 2))
    .orderBy(F.desc("pixel_count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar Mapa en Tabla Gold

# COMMAND ----------

print(f"💾 Guardando mapa en: {TABLE_SUSCEPTIBILITY_MAP}")

susceptibility_map_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_SUSCEPTIBILITY_MAP)

print(f"✅ Mapa guardado: {susceptibility_map_final.count():,} píxeles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visualización: Heatmap de Susceptibilidad

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Convertir a Pandas para visualización
map_sample = susceptibility_map_final.sample(0.1).toPandas()  # 10% muestra

fig, axes = plt.subplots(1, 2, figsize=(18, 7))

# Mapa 1: Scatter plot de susceptibilidad
scatter = axes[0].scatter(
    map_sample['longitude'],
    map_sample['latitude'],
    c=map_sample['susceptibility_score'],
    cmap='YlOrRd',
    s=1,
    alpha=0.6
)
axes[0].set_xlabel('Longitud', fontsize=12)
axes[0].set_ylabel('Latitud', fontsize=12)
axes[0].set_title(f'Mapa de Susceptibilidad - {PILOT_ZONE["name"]}', fontsize=14, fontweight='bold')
cbar = plt.colorbar(scatter, ax=axes[0])
cbar.set_label('Score de Susceptibilidad', fontsize=11)

# Mapa 2: Scatter plot de pendientes críticas
critical_only = map_sample[map_sample['slope_degrees'] >= CRITICAL_SLOPE_MIN]
axes[1].scatter(
    critical_only['longitude'],
    critical_only['latitude'],
    c='red',
    s=2,
    alpha=0.7,
    label=f'Pendientes {CRITICAL_SLOPE_MIN}-{CRITICAL_SLOPE_MAX}°'
)
axes[1].set_xlabel('Longitud', fontsize=12)
axes[1].set_ylabel('Latitud', fontsize=12)
axes[1].set_title('Pendientes Críticas (30-45°)', fontsize=14, fontweight='bold')
axes[1].legend()

plt.tight_layout()
plt.show()

print("✅ Visualizaciones generadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Análisis Estadístico por Banda Altitudinal

# COMMAND ----------

print("📊 ANÁLISIS DETALLADO POR BANDA ALTITUDINAL")
print("=" * 80)

susceptibility_analysis = spark.table(TABLE_TOPO_SUSCEPTIBILITY).orderBy("avg_slope_degrees")

display(susceptibility_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Identificar Zonas de Mayor Riesgo

# COMMAND ----------

print("🎯 ZONAS DE MAYOR RIESGO TOPOGRÁFICO")
print("=" * 80)

high_risk_zones = spark.table(TABLE_TOPO_SUSCEPTIBILITY).filter(
    F.col("susceptibility_score") >= 0.6
).orderBy(F.desc("susceptibility_score"))

display(high_risk_zones)

print(f"\n⚠️  {high_risk_zones.count()} zona(s) con susceptibilidad ALTA o MUY ALTA detectadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Exportar Resultados para Visualización

# COMMAND ----------

# Crear resumen para el agente integrador
topo_summary = {
    "agent": "Topografico",
    "zone": PILOT_ZONE['name'],
    "total_area_km2": float(susceptibility_final.agg(F.sum("total_area_km2")).collect()[0][0]),
    "critical_area_km2": float(susceptibility_final.agg(F.sum("critical_slope_area_km2")).collect()[0][0]),
    "max_susceptibility": float(susceptibility_final.agg(F.max("susceptibility_score")).collect()[0][0]),
    "avg_susceptibility": float(susceptibility_final.agg(F.avg("susceptibility_score")).collect()[0][0]),
    "high_risk_zones": high_risk_zones.count(),
    "timestamp": datetime.now().isoformat()
}

print("\n📋 RESUMEN PARA AGENTE INTEGRADOR:")
print("=" * 80)
for key, value in topo_summary.items():
    print(f"   {key:25s}: {value}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumen Final

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 RESUMEN: MAPA DE SUSCEPTIBILIDAD TOPOGRÁFICA")
print("=" * 80)

print(f"""
🏔️  ZONA ANALIZADA: {PILOT_ZONE['name']}

📊 DATOS PROCESADOS:
   • Tabla origen: {TABLE_TOPO_FEATURES}
   • Registros procesados: {topo_features.count():,}

💾 OUTPUTS GENERADOS:
   1. {TABLE_TOPO_SUSCEPTIBILITY}
      → Susceptibilidad agregada por zona y banda altitudinal
      → {susceptibility_final.count()} registros

   2. {TABLE_SUSCEPTIBILITY_MAP}
      → Mapa de alta resolución (píxel level)
      → {susceptibility_map_final.count():,} píxeles

📈 MÉTRICAS CLAVE:
   • Área total: {topo_summary['total_area_km2']:.2f} km²
   • Área crítica (30-45°): {topo_summary['critical_area_km2']:.2f} km²
   • Porcentaje crítico: {(topo_summary['critical_area_km2'] / topo_summary['total_area_km2'] * 100):.2f}%
   • Susceptibilidad máxima: {topo_summary['max_susceptibility']:.3f}
   • Susceptibilidad promedio: {topo_summary['avg_susceptibility']:.3f}
   • Zonas de alto riesgo: {topo_summary['high_risk_zones']}

🎯 BANDAS ALTITUDINALES CON MAYOR RIESGO:
""")

high_risk_summary = spark.table(TABLE_TOPO_SUSCEPTIBILITY).orderBy(F.desc("susceptibility_score")).limit(3)
for row in high_risk_summary.collect():
    print(f"   • {row.elevation_band:12s}: Score {row.susceptibility_score:.3f} | Área crítica: {row.critical_slope_area_km2:.2f} km²")

print(f"""
✅ AGENTE TOPOGRÁFICO COMPLETADO
   → Datos listos para Agente Integrador
   → Próximo paso: Ejecutar Agente 2 (NLP) y Agente 3 (Meteorológico)
""")

print("=" * 80)
