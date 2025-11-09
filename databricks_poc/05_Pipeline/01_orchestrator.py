# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Orquestador - Sistema Multi-Agente
# MAGIC
# MAGIC Ejecuta todos los agentes en secuencia para generar predicción end-to-end.
# MAGIC
# MAGIC **Flujo:**
# MAGIC 1. Agente Topográfico → Procesar SRTM y calcular susceptibilidad
# MAGIC 2. Agente NLP → Extraer patrones de riesgo de relatos
# MAGIC 3. Agente Meteorológico → Ingestar datos y detectar gatillantes
# MAGIC 4. Agente Integrador → Fusionar, clasificar y generar boletín

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración del Pipeline

# COMMAND ----------

from datetime import datetime
import time

# Configuración de ejecución
PIPELINE_CONFIG = {
    "run_topo_agent": True,        # Agente 1: Topográfico
    "run_nlp_agent": True,          # Agente 2: NLP
    "run_weather_agent": True,      # Agente 3: Meteorológico
    "run_integrator_agent": True,   # Agente 4: Integrador
    "generate_bulletin": True,      # Generar boletín final
    "verbose": True                 # Logging detallado
}

print("⚙️  CONFIGURACIÓN DEL PIPELINE")
print("=" * 80)
for key, value in PIPELINE_CONFIG.items():
    status = "✅" if value else "❌"
    print(f"   {status} {key:25s}: {value}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Función de Logging

# COMMAND ----------

def log_step(agent_name, step, message, status="info"):
    """
    Logging estructurado del pipeline

    Args:
        agent_name: Nombre del agente
        step: Paso actual
        message: Mensaje
        status: info|success|warning|error
    """
    timestamp = datetime.now().strftime("%H:%M:%S")

    icons = {
        "info": "ℹ️",
        "success": "✅",
        "warning": "⚠️",
        "error": "❌",
        "start": "🚀",
        "finish": "🎉"
    }

    icon = icons.get(status, "•")

    if PIPELINE_CONFIG["verbose"]:
        print(f"[{timestamp}] {icon} {agent_name:20s} | Step {step} | {message}")

def log_separator():
    """Imprime separador visual"""
    print("\n" + "=" * 80 + "\n")

# Test de logging
log_step("Pipeline", "0", "Sistema de logging inicializado", "success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agente 1: Topográfico

# COMMAND ----------

def run_topo_agent():
    """Ejecuta Agente Topográfico"""
    agent_name = "AGENTE TOPOGRÁFICO"

    log_separator()
    log_step(agent_name, "1", "Iniciando procesamiento topográfico...", "start")

    start_time = time.time()

    try:
        # SRTM Processing
        log_step(agent_name, "1.1", "Procesando datos SRTM...", "info")
        %run ../01_Agent_Topografico/01_srtm_processing

        # Susceptibility Map
        log_step(agent_name, "1.2", "Generando mapa de susceptibilidad...", "info")
        %run ../01_Agent_Topografico/02_susceptibility_map

        elapsed = time.time() - start_time
        log_step(agent_name, "1", f"Completado en {elapsed:.1f}s", "success")

        return True

    except Exception as e:
        log_step(agent_name, "1", f"ERROR: {str(e)}", "error")
        return False

# Ejecutar si está habilitado
if PIPELINE_CONFIG["run_topo_agent"]:
    topo_success = run_topo_agent()
else:
    log_step("Pipeline", "1", "Agente Topográfico deshabilitado", "warning")
    topo_success = True  # Asumir éxito si está deshabilitado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agente 2: NLP

# COMMAND ----------

def run_nlp_agent():
    """Ejecuta Agente NLP"""
    agent_name = "AGENTE NLP"

    log_separator()
    log_step(agent_name, "2", "Iniciando análisis NLP...", "start")

    start_time = time.time()

    try:
        # Extract Risk Patterns
        log_step(agent_name, "2.1", "Extrayendo patrones de riesgo...", "info")
        %run ../02_Agent_NLP/01_extract_risk_patterns

        elapsed = time.time() - start_time
        log_step(agent_name, "2", f"Completado en {elapsed:.1f}s", "success")

        return True

    except Exception as e:
        log_step(agent_name, "2", f"ERROR: {str(e)}", "error")
        return False

# Ejecutar si está habilitado
if PIPELINE_CONFIG["run_nlp_agent"]:
    nlp_success = run_nlp_agent()
else:
    log_step("Pipeline", "2", "Agente NLP deshabilitado", "warning")
    nlp_success = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agente 3: Meteorológico

# COMMAND ----------

def run_weather_agent():
    """Ejecuta Agente Meteorológico"""
    agent_name = "AGENTE METEOROLÓGICO"

    log_separator()
    log_step(agent_name, "3", "Iniciando procesamiento meteorológico...", "start")

    start_time = time.time()

    try:
        # Weather Ingestion
        log_step(agent_name, "3.1", "Ingiriendo datos meteorológicos...", "info")
        %run ../03_Agent_Meteorologico/01_weather_ingestion

        # Trigger Detection
        log_step(agent_name, "3.2", "Detectando condiciones gatillantes...", "info")
        %run ../03_Agent_Meteorologico/02_trigger_detection

        elapsed = time.time() - start_time
        log_step(agent_name, "3", f"Completado en {elapsed:.1f}s", "success")

        return True

    except Exception as e:
        log_step(agent_name, "3", f"ERROR: {str(e)}", "error")
        return False

# Ejecutar si está habilitado
if PIPELINE_CONFIG["run_weather_agent"]:
    weather_success = run_weather_agent()
else:
    log_step("Pipeline", "3", "Agente Meteorológico deshabilitado", "warning")
    weather_success = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Agente 4: Integrador

# COMMAND ----------

def run_integrator_agent():
    """Ejecuta Agente Integrador"""
    agent_name = "AGENTE INTEGRADOR"

    log_separator()
    log_step(agent_name, "4", "Iniciando fusión y clasificación...", "start")

    start_time = time.time()

    try:
        # Risk Classification
        log_step(agent_name, "4.1", "Clasificando riesgo multi-factorial...", "info")
        %run ../04_Agent_Integrador/01_risk_classification

        # Bulletin Generation (si está habilitado)
        if PIPELINE_CONFIG["generate_bulletin"]:
            log_step(agent_name, "4.2", "Generando boletín EAWS...", "info")
            %run ../04_Agent_Integrador/02_boletin_generation

        elapsed = time.time() - start_time
        log_step(agent_name, "4", f"Completado en {elapsed:.1f}s", "success")

        return True

    except Exception as e:
        log_step(agent_name, "4", f"ERROR: {str(e)}", "error")
        return False

# Ejecutar si está habilitado
if PIPELINE_CONFIG["run_integrator_agent"]:
    integrator_success = run_integrator_agent()
else:
    log_step("Pipeline", "4", "Agente Integrador deshabilitado", "warning")
    integrator_success = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen de Ejecución

# COMMAND ----------

log_separator()

print("📊 RESUMEN DE EJECUCIÓN DEL PIPELINE")
print("=" * 80)

pipeline_status = {
    "Agente Topográfico": topo_success if PIPELINE_CONFIG["run_topo_agent"] else "Deshabilitado",
    "Agente NLP": nlp_success if PIPELINE_CONFIG["run_nlp_agent"] else "Deshabilitado",
    "Agente Meteorológico": weather_success if PIPELINE_CONFIG["run_weather_agent"] else "Deshabilitado",
    "Agente Integrador": integrator_success if PIPELINE_CONFIG["run_integrator_agent"] else "Deshabilitado"
}

all_success = all([
    topo_success if PIPELINE_CONFIG["run_topo_agent"] else True,
    nlp_success if PIPELINE_CONFIG["run_nlp_agent"] else True,
    weather_success if PIPELINE_CONFIG["run_weather_agent"] else True,
    integrator_success if PIPELINE_CONFIG["run_integrator_agent"] else True
])

for agent, status in pipeline_status.items():
    if status == "Deshabilitado":
        print(f"   ⏭️  {agent:25s}: DESHABILITADO")
    elif status:
        print(f"   ✅ {agent:25s}: ÉXITO")
    else:
        print(f"   ❌ {agent:25s}: ERROR")

print("=" * 80)

if all_success:
    print("\n🎉 PIPELINE COMPLETADO EXITOSAMENTE")
    print(f"   Zona: {PILOT_ZONE['name']}")
    print(f"   Timestamp: {datetime.now().isoformat()}")
    print("\n📊 Datos disponibles en Unity Catalog:")
    print(f"   • Topografía: {TABLE_TOPO_SUSCEPTIBILITY}")
    print(f"   • NLP: {TABLE_NLP_PATTERNS}")
    print(f"   • Meteorología: {TABLE_WEATHER_TRIGGERS}")
    print(f"   • Predicciones: {TABLE_RISK_PREDICTIONS}")
    if PIPELINE_CONFIG["generate_bulletin"]:
        print(f"   • Boletín: {TABLE_BOLETINES}")
    print("\n🎯 Sistema multi-agente funcionando end-to-end")
else:
    print("\n⚠️  PIPELINE COMPLETADO CON ERRORES")
    print("   Revisa los logs arriba para identificar problemas")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validación de Outputs

# COMMAND ----------

print("\n🔍 VALIDACIÓN DE OUTPUTS")
print("=" * 80)

from pyspark.sql import functions as F

# Verificar cada tabla
tables_to_check = [
    (TABLE_TOPO_SUSCEPTIBILITY, "Susceptibilidad Topográfica"),
    (TABLE_NLP_PATTERNS, "Patrones NLP"),
    (TABLE_WEATHER_TRIGGERS, "Triggers Meteorológicos"),
    (TABLE_RISK_PREDICTIONS, "Predicciones de Riesgo"),
    (TABLE_BOLETINES, "Boletines")
]

for table_name, description in tables_to_check:
    try:
        df = spark.table(table_name)
        count = df.count()

        if count > 0:
            print(f"   ✅ {description:30s}: {count:6,} registros")
        else:
            print(f"   ⚠️  {description:30s}: TABLA VACÍA")

    except Exception as e:
        print(f"   ❌ {description:30s}: NO EXISTE")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Estadísticas Finales

# COMMAND ----------

print("\n📈 ESTADÍSTICAS DEL SISTEMA")
print("=" * 80)

try:
    # Última predicción
    last_prediction = spark.table(TABLE_RISK_PREDICTIONS) \
        .orderBy(F.desc("prediction_timestamp")) \
        .first()

    if last_prediction:
        print(f"\n🎯 ÚLTIMA PREDICCIÓN:")
        print(f"   Zona: {last_prediction['zone_name']}")
        print(f"   Fecha pronóstico: {last_prediction['forecast_date']}")
        print(f"   Nivel EAWS: {last_prediction['eaws_level']} - {last_prediction['eaws_label']}")
        print(f"   Score de riesgo: {last_prediction['risk_score']:.3f}")
        print(f"   Confianza: {last_prediction['confidence']:.1%}")

        # Factores principales
        print(f"\n   Factores principales:")
        for factor in last_prediction['main_factors']:
            print(f"      • {factor}")

    # Último boletín
    if PIPELINE_CONFIG["generate_bulletin"]:
        last_bulletin = spark.table(TABLE_BOLETINES) \
            .orderBy(F.desc("creation_timestamp")) \
            .first()

        if last_bulletin:
            print(f"\n📝 ÚLTIMO BOLETÍN:")
            print(f"   ID: {last_bulletin['boletin_id']}")
            print(f"   Fecha emisión: {last_bulletin['issue_date']}")
            print(f"   Válido: {last_bulletin['valid_from']} a {last_bulletin['valid_to']}")
            print(f"   Nivel: {last_bulletin['eaws_level']} - {last_bulletin['eaws_label']}")
            print(f"   Generado por: {last_bulletin['generated_by']}")

except Exception as e:
    print(f"⚠️  No se pudieron obtener estadísticas: {e}")

print("\n" + "=" * 80)
print("✅ PIPELINE DE ORQUESTACIÓN COMPLETADO")
print("=" * 80)
