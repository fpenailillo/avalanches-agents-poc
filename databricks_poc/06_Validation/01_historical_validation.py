# Databricks notebook source
# MAGIC %md
# MAGIC # Validación Histórica del Sistema de Predicción
# MAGIC
# MAGIC Compara predicciones del sistema vs eventos reales de avalanchas

# COMMAND ----------

%run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definir Eventos Reales de Avalanchas

# COMMAND ----------

from pyspark.sql.types import *
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# Schema para eventos reales
schema = StructType([
    StructField("fecha", DateType(), False),
    StructField("zona", StringType(), False),
    StructField("elevation_band", StringType(), False),
    StructField("severidad_observada", StringType(), False),
    StructField("nivel_eaws_observado", IntegerType(), False),
    StructField("descripcion", StringType(), True),
    StructField("fuente", StringType(), True)
])

# DATOS DE EJEMPLO - REEMPLAZAR CON EVENTOS REALES DOCUMENTADOS
eventos_reales = spark.createDataFrame([
    (
        datetime(2023, 8, 15).date(),
        "La Parva",
        "Alta",
        "Notable",
        3,
        "Avalancha sector El Corralito, cierre preventivo",
        "AndesHandbook + Reporte centro esquí"
    ),
    (
        datetime(2023, 7, 22).date(),
        "Valle Nevado",
        "Media-Alta",
        "Fuerte",
        4,
        "Gran nevada + viento, avalancha en Tres Puntas",
        "Medios de comunicación"
    ),
    # AGREGAR MÁS EVENTOS REALES AQUÍ
], schema)

# Guardar en tabla
eventos_reales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{FULL_DATABASE}.avalanche_events_real")

print(f"✅ {eventos_reales.count()} eventos reales registrados")
display(eventos_reales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ejecutar Predicciones Históricas

# COMMAND ----------

resultados_validacion = []

eventos_list = eventos_reales.collect()

print(f"🔍 Analizando {len(eventos_list)} eventos...")
print("=" * 80)

for evento in eventos_list:

    fecha_evento = evento['fecha']
    zona_evento = evento['zona']

    print(f"\n📅 {fecha_evento} - {zona_evento}")
    print(f"   Severidad observada: Nivel {evento['nivel_eaws_observado']} ({evento['severidad_observada']})")

    try:
        # Configurar modo histórico para esta fecha
        set_operation_mode("historical", fecha_evento)

        # Ejecutar solo agentes que dependen de fecha
        # (Topográfico y NLP ya están listos)

        print("   🌦️  Ejecutando Agente Meteorológico...")
        %run ../03_Agent_Meteorologico/01_weather_ingestion
        %run ../03_Agent_Meteorologico/02_trigger_detection

        print("   🔗 Ejecutando Agente Integrador...")
        %run ../04_Agent_Integrador/01_risk_classification

        # Obtener predicción generada
        prediccion = spark.table(TABLE_RISK_PREDICTIONS).filter(
            (F.col("forecast_date") == fecha_evento) &
            (F.col("zone_name") == zona_evento) &
            (F.col("elevation_band") == evento['elevation_band'])
        ).first()

        if prediccion:
            nivel_predicho = prediccion['eaws_level']

            # Calcular error
            error = abs(nivel_predicho - evento['nivel_eaws_observado'])
            acierto = error <= 1  # Tolerancia ±1 nivel

            print(f"   🎯 Nivel predicho: {nivel_predicho} ({prediccion['eaws_label']})")
            print(f"   📊 Error: {error:+d} niveles")
            print(f"   {'✅ ACIERTO' if acierto else '❌ ERROR'}")

            resultados_validacion.append({
                'fecha': fecha_evento,
                'zona': zona_evento,
                'elevation_band': evento['elevation_band'],
                'nivel_observado': evento['nivel_eaws_observado'],
                'nivel_predicho': nivel_predicho,
                'error': error,
                'acierto': acierto,
                'risk_score': prediccion['risk_score'],
                'confidence': prediccion['confidence']
            })
        else:
            print(f"   ⚠️  No se generó predicción")

    except Exception as e:
        print(f"   ❌ Error: {e}")

print("\n" + "=" * 80)
print(f"✅ Validación completada: {len(resultados_validacion)} predicciones generadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Calcular Métricas de Validación

# COMMAND ----------

if len(resultados_validacion) == 0:
    print("⚠️  No hay resultados para analizar")
else:
    df_val = pd.DataFrame(resultados_validacion)

    print("=" * 80)
    print("📊 MÉTRICAS DE VALIDACIÓN DEL MODELO")
    print("=" * 80)

    # Accuracy general
    total = len(df_val)
    aciertos = df_val['acierto'].sum()
    accuracy = (aciertos / total * 100) if total > 0 else 0

    print(f"\n🎯 ACCURACY GENERAL (±1 nivel):")
    print(f"   Total eventos: {total}")
    print(f"   Aciertos: {aciertos}")
    print(f"   Accuracy: {accuracy:.1f}%")

    # Error absoluto medio
    mae = df_val['error'].abs().mean()
    print(f"\n📐 ERROR ABSOLUTO MEDIO:")
    print(f"   MAE: {mae:.2f} niveles")

    # Distribución de errores
    print(f"\n📊 DISTRIBUCIÓN DE ERRORES:")
    error_dist = df_val['error'].value_counts().sort_index()
    for error, count in error_dist.items():
        emoji = "✅" if error == 0 else "⚠️" if abs(error) == 1 else "❌"
        print(f"   {emoji} Error {error:+d}: {count} casos")

    # Por nivel de severidad
    print(f"\n⚠️  ACCURACY POR NIVEL OBSERVADO:")
    for nivel in sorted(df_val['nivel_observado'].unique()):
        subset = df_val[df_val['nivel_observado'] == nivel]
        acc = subset['acierto'].mean() * 100
        n = len(subset)
        print(f"   Nivel {nivel}: {acc:.0f}% ({n} eventos)")

    # Guardar resultados
    df_val_spark = spark.createDataFrame(df_val)
    df_val_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{FULL_DATABASE}.validation_results")

    print(f"\n💾 Resultados guardados en: {FULL_DATABASE}.validation_results")
    print("=" * 80)

    # Visualización
    display(df_val_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resumen Final

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 VALIDACIÓN HISTÓRICA COMPLETADA")
print("=" * 80)

if len(resultados_validacion) > 0:
    print(f"""
✅ SISTEMA VALIDADO:
   • Eventos analizados: {len(resultados_validacion)}
   • Accuracy promedio: {accuracy:.1f}%
   • Error absoluto medio: {mae:.2f} niveles

🎯 PRÓXIMOS PASOS:
   1. Agregar más eventos reales documentados
   2. Ajustar pesos de fusión multi-factorial
   3. Optimizar umbrales de decisión
   4. Generar reportes de backtesting

💾 DATOS:
   • Eventos reales: {FULL_DATABASE}.avalanche_events_real
   • Resultados: {FULL_DATABASE}.validation_results
""")
else:
    print("""
⚠️  NO SE PUDO COMPLETAR LA VALIDACIÓN

📋 CHECKLIST:
   1. ✓ Eventos reales registrados
   2. ✗ Predicciones generadas (revisar errores arriba)
   3. ✗ Métricas calculadas
""")

print("=" * 80)
