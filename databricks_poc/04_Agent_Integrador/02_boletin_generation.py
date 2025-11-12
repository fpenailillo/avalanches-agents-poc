# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 4: Generación de Boletines de Avalanchas
# MAGIC
# MAGIC Genera boletines automatizados en formato EAWS usando LLM.
# MAGIC
# MAGIC **Input:** `avalanche_predictions` (capa Gold)
# MAGIC **Output:** `avalanche_bulletins` (capa Gold)

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Cargar Predicciones

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import uuid
import json

print(f"📊 Cargando predicciones desde: {TABLE_RISK_PREDICTIONS}")

predictions_df = spark.table(TABLE_RISK_PREDICTIONS)

# Obtener última fecha de predicción
max_date = predictions_df.agg(F.max("forecast_date")).collect()[0][0]

# Filtrar predicciones del último forecast
latest_predictions = predictions_df.filter(F.col("forecast_date") == max_date)

print(f"✅ Predicciones cargadas: {latest_predictions.count()} para fecha {max_date}")

display(latest_predictions.orderBy(F.desc("risk_score")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurar Cliente LLM

# COMMAND ----------

pip install OpenAI

# COMMAND ----------

try:
    from openai import OpenAI

    # Obtener token de Databricks
    DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    llm_client = OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url=DATABRICKS_LLM_ENDPOINT
    )

    print("✅ Cliente LLM configurado")
    llm_available = True

except Exception as e:
    print(f"⚠️  LLM no disponible: {e}")
    print("   → Se usará generación de boletines basada en templates")
    llm_available = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Función de Generación con LLM

# COMMAND ----------

def generate_bulletin_with_llm(zone_name, forecast_date, predictions_data):
    """
    Genera boletín usando LLM (Llama 4).

    Args:
        zone_name: Nombre de la zona
        forecast_date: Fecha del pronóstico
        predictions_data: Lista de diccionarios con predicciones por banda

    Returns:
        Dict con contenido del boletín
    """
    if not llm_available:
        return None

    # Crear prompt estructurado
    predictions_summary = "\n".join([
        f"- {p['elevation_band']}: Nivel {p['eaws_level']} ({p['eaws_label']}), Score: {p['risk_score']:.2f}, Factores: {', '.join(p['main_factors'])}"
        for p in predictions_data
    ])

    prompt = f"""
Eres un experto en predicción de avalanchas. Genera un boletín oficial de peligro de avalanchas siguiendo el formato EAWS (European Avalanche Warning Services).

DATOS DE PREDICCIÓN:
Zona: {zone_name}
Fecha de emisión: {forecast_date}
Válido para: {(forecast_date + timedelta(days=1)).strftime('%Y-%m-%d')} a {(forecast_date + timedelta(days=2)).strftime('%Y-%m-%d')}

PREDICCIONES POR BANDA ALTITUDINAL:
{predictions_summary}

ESCALA EAWS:
1 - Débil: Nieve generalmente bien estabilizada
2 - Limitado: Nieve moderadamente estabilizada
3 - Notable: Nieve débilmente estabilizada en muchas pendientes
4 - Fuerte: Nieve débilmente estabilizada en mayoría de pendientes
5 - Muy Fuerte: Nieve inestable generalizada

GENERA UN BOLETÍN EN ESPAÑOL CON LA SIGUIENTE ESTRUCTURA (máximo 500 palabras):

1. RESUMEN EJECUTIVO (2-3 frases sobre nivel general y zonas críticas)

2. NIVEL DE PELIGRO
   - Nivel EAWS general
   - Distribución por elevación

3. DESCRIPCIÓN DEL PELIGRO
   - Problemas de avalanchas identificados
   - Ubicaciones y orientaciones afectadas
   - Tamaño y probabilidad de avalanchas

4. MANTO NIVOSO
   - Condiciones actuales
   - Factores desencadenantes

5. PRONÓSTICO METEOROLÓGICO
   - Condiciones esperadas (nevadas, viento, temperatura)

6. RECOMENDACIONES
   - Para montañistas y esquiadores
   - Zonas a evitar
   - Precauciones específicas

IMPORTANTE: Sé preciso, profesional y enfocado en la seguridad. Usa terminología técnica apropiada pero comprensible.
"""

    try:
        response = llm_client.chat.completions.create(
            model=LLM_MODEL_NAME,
            messages=[
                {"role": "system", "content": "Eres un experto en avalanchas y meteorología de montaña. Generas boletines profesionales siguiendo estándares internacionales EAWS."},
                {"role": "user", "content": prompt}
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=LLM_MAX_TOKENS
        )

        bulletin_text = response.choices[0].message.content.strip()

        return {
            "bulletin_text": bulletin_text,
            "generated_by": "LLM_Llama4",
            "generation_method": "ai_generated"
        }

    except Exception as e:
        print(f"❌ Error generando con LLM: {e}")
        return None

print("✅ Función LLM definida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Función de Generación con Template (Fallback)

# COMMAND ----------

def generate_bulletin_with_template(zone_name, forecast_date, predictions_data):
    """
    Genera boletín usando templates estructurados (fallback cuando LLM no disponible).
    """
    # Encontrar nivel máximo de peligro
    max_level = max([p['eaws_level'] for p in predictions_data])
    max_label = EAWS_SCALE[max_level]['level']

    # Bandas de mayor riesgo
    high_risk_bands = [p['elevation_band'] for p in predictions_data if p['eaws_level'] >= 3]

    # Factores principales
    all_factors = []
    for p in predictions_data:
        all_factors.extend(p['main_factors'])
    unique_factors = list(set(all_factors))[:5]

    # Generar texto del boletín
    bulletin_text = f"""
BOLETÍN DE PELIGRO DE AVALANCHAS
{zone_name.upper()}

Fecha de emisión: {forecast_date.strftime('%d/%m/%Y')}
Válido para: {(forecast_date + timedelta(days=1)).strftime('%d/%m/%Y')} a {(forecast_date + timedelta(days=2)).strftime('%d/%m/%Y')}

═══════════════════════════════════════════════════════════════

1. RESUMEN EJECUTIVO

El nivel de peligro de avalanchas para {zone_name} es {max_level} - {max_label.upper()}.
{'Se identificaron condiciones críticas en elevaciones ' + ', '.join(high_risk_bands) + '.' if high_risk_bands else 'Las condiciones son generalmente estables.'}

═══════════════════════════════════════════════════════════════

2. NIVEL DE PELIGRO (Escala EAWS)

Nivel general: {max_level} - {max_label}
Color: {EAWS_SCALE[max_level]['color']}

Distribución por elevación:
"""

    for pred in sorted(predictions_data, key=lambda x: x['risk_score'], reverse=True):
        bulletin_text += f"  • {pred['elevation_band']:15s}: Nivel {pred['eaws_level']} - {pred['eaws_label']}\n"

    bulletin_text += f"""
═══════════════════════════════════════════════════════════════

3. DESCRIPCIÓN DEL PELIGRO

Problemas de avalanchas identificados:
"""

    # Determinar tipos de problemas según factores
    problems = []
    for factor in unique_factors:
        if "nevada" in factor.lower():
            problems.append("- Nieve reciente: Acumulación de nieve nueva que puede deslizarse")
        elif "viento" in factor.lower():
            problems.append("- Nieve venteada: Formación de placas de viento en orientaciones de sotavento")
        elif "térmica" in factor.lower() or "temperatura" in factor.lower():
            problems.append("- Nieve húmeda: Debilitamiento del manto nivoso por temperaturas elevadas")
        elif "pendiente" in factor.lower():
            problems.append("- Terreno crítico: Presencia de pendientes susceptibles (30-45°)")

    if not problems:
        problems.append("- Condiciones estables en general")

    bulletin_text += "\n".join(list(set(problems)))

    bulletin_text += f"""

Ubicaciones afectadas:
  • Elevaciones: {'Sobre 2500m' if any('Alta' in p['elevation_band'] or 'Media' in p['elevation_band'] for p in predictions_data) else 'Todas las elevaciones'}
  • Orientaciones: Todas las orientaciones (especialmente sotavento con viento)
  • Pendientes: 30-45 grados principalmente

═══════════════════════════════════════════════════════════════

4. MANTO NIVOSO

Factores identificados:
"""

    for i, factor in enumerate(unique_factors[:5], 1):
        bulletin_text += f"  {i}. {factor}\n"

    bulletin_text += f"""
═══════════════════════════════════════════════════════════════

5. PRONÓSTICO METEOROLÓGICO

Se prevén las siguientes condiciones:
  • Nevadas: {'Moderadas a intensas' if any('Nevada' in f for f in unique_factors) else 'Leves o ausentes'}
  • Viento: {'Fuerte, con formación de placas' if any('Viento' in f for f in unique_factors) else 'Moderado'}
  • Temperatura: {'Fluctuante con riesgo de deshielo' if any('térmica' in f.lower() or 'Fluctuación' in f for f in unique_factors) else 'Estable'}

═══════════════════════════════════════════════════════════════

6. RECOMENDACIONES

Nivel {max_level} - {max_label}:
"""

    if max_level == 1:
        bulletin_text += """
  • Condiciones generalmente favorables
  • Mantener precauciones estándar
  • Evaluar pendientes individuales antes de exponerse
"""
    elif max_level == 2:
        bulletin_text += """
  • Condiciones mayormente favorables
  • Precaución en pendientes específicas
  • Evitar pendientes muy empinadas en zonas identificadas
"""
    elif max_level == 3:
        bulletin_text += """
  • Condiciones CRÍTICAS - Experiencia requerida
  • Evaluación cuidadosa del terreno ESENCIAL
  • Evitar pendientes empinadas en zonas de riesgo
  • Considerar postergar actividades en elevaciones críticas
"""
    elif max_level == 4:
        bulletin_text += """
  • Condiciones MUY CRÍTICAS - Solo expertos
  • Restricción a terreno simple SOLAMENTE
  • Evitar pendientes empinadas
  • Alejarse de zonas de salida de avalanchas
  • Considerar seriamente postergar actividades
"""
    else:  # Nivel 5
        bulletin_text += """
  • CONDICIONES EXTRAORDINARIAS - PELIGRO EXTREMO
  • EVITAR TODO TERRENO DE AVALANCHAS
  • POSTERGAR ACTIVIDADES
  • Permanecer en zonas seguras solamente
"""

    bulletin_text += f"""
Zonas a evitar:
  • {', '.join(high_risk_bands) if high_risk_bands else 'Evaluar según condiciones locales'}

═══════════════════════════════════════════════════════════════

Este boletín ha sido generado automáticamente por el Sistema de
Predicción de Avalanchas de Chile. Para más información contactar
a las autoridades locales de seguridad en montaña.

Próxima actualización: {(forecast_date + timedelta(days=1)).strftime('%d/%m/%Y')}
"""

    return {
        "bulletin_text": bulletin_text,
        "generated_by": "Template_System",
        "generation_method": "rule_based"
    }

print("✅ Función template definida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generar Boletines para Zona Piloto

# COMMAND ----------

print(f"📝 Generando boletín para {PILOT_ZONE['name']}...")

# Convertir predicciones a formato adecuado
predictions_list = latest_predictions.collect()
predictions_data = [
    {
        'elevation_band': row['elevation_band'],
        'eaws_level': row['eaws_level'],
        'eaws_label': row['eaws_label'],
        'risk_score': row['risk_score'],
        'main_factors': row['main_factors']
    }
    for row in predictions_list
]

# Intentar generar con LLM primero
bulletin_content = generate_bulletin_with_llm(
    PILOT_ZONE['name'],
    max_date,
    predictions_data
)

# Si LLM falla, usar template
if not bulletin_content:
    print("   → Usando generación con template")
    bulletin_content = generate_bulletin_with_template(
        PILOT_ZONE['name'],
        max_date,
        predictions_data
    )

print(f"✅ Boletín generado con: {bulletin_content['generated_by']}")

# Mostrar boletín
print("\n" + "="*70)
print(bulletin_content['bulletin_text'])
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Extraer Metadatos del Boletín

# COMMAND ----------

# Calcular nivel general (máximo)
max_level = max([p['eaws_level'] for p in predictions_data])
max_label = EAWS_SCALE[max_level]['level']

# Identificar problemas de avalanchas
avalanche_problems = []
all_factors = []
for p in predictions_data:
    all_factors.extend(p['main_factors'])

if any('nevada' in f.lower() or 'nieve reciente' in f.lower() for f in all_factors):
    avalanche_problems.append("Nieve reciente")
if any('viento' in f.lower() for f in all_factors):
    avalanche_problems.append("Placas de viento")
if any('térmica' in f.lower() or 'temperatura' in f.lower() or 'fluctuación' in f.lower() for f in all_factors):
    avalanche_problems.append("Nieve húmeda")
if any('pendiente' in f.lower() for f in all_factors):
    avalanche_problems.append("Terreno crítico")
if not avalanche_problems:
    avalanche_problems.append("Condiciones estables")

# Elevaciones afectadas
high_risk_bands = [p['elevation_band'] for p in predictions_data if p['eaws_level'] >= 3]
affected_elevations = ', '.join(high_risk_bands) if high_risk_bands else "Todas las elevaciones"

# Resumen corto
summary = f"Nivel {max_level} - {max_label} para {PILOT_ZONE['name']}. " + \
          (f"Condiciones críticas en {', '.join(high_risk_bands)}." if high_risk_bands else "Condiciones estables.")

# Descripción de peligro
danger_desc = EAWS_SCALE[max_level]['description']

# Recomendaciones
recommendations = EAWS_SCALE[max_level].get('recommendations', 'No recommendations available')

print("✅ Metadatos extraídos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Crear Registro de Boletín

# COMMAND ----------

from pyspark.sql.types import *

# Crear estructura del boletín
bulletin_record = {
    "boletin_id": f"boletin_{datetime.now().strftime('%Y%m%d%H%M%S')}_{PILOT_ZONE['name'].replace(' ', '_')}",
    "zone_name": PILOT_ZONE['name'],
    "issue_date": max_date,
    "valid_from": max_date + timedelta(days=1),
    "valid_to": max_date + timedelta(days=2),
    "eaws_level": max_level,
    "eaws_label": max_label,
    "summary": summary,
    "danger_description": danger_desc,
    "avalanche_problems": avalanche_problems,
    "affected_elevations": affected_elevations,
    "affected_aspects": "Todas las orientaciones",
    "recommendations": recommendations,
    "confidence": "Alta" if bulletin_content['generation_method'] == "ai_generated" else "Media",
    "bulletin_text": bulletin_content['bulletin_text'],
    "generated_by": bulletin_content['generated_by'],
    "creation_timestamp": datetime.now()
}

# Convertir a DataFrame
bulletin_df = spark.createDataFrame([bulletin_record])

print("✅ Registro de boletín creado")

display(bulletin_df.select(
    "boletin_id",
    "zone_name",
    "issue_date",
    "eaws_level",
    "eaws_label",
    "summary"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Guardar Boletín en Delta Lake (Capa Gold)

# COMMAND ----------

print(TABLE_BOLETINES)

# COMMAND ----------

# Ensure only one eaws_level column and cast to StringType
from pyspark.sql.types import StringType

# Cast eaws_level to StringType for consistency
bulletin_df = bulletin_df.withColumn(
    "eaws_level",
    bulletin_df["eaws_level"].cast(StringType())
)

bulletin_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .partitionBy("zone_name", "issue_date") \
    .saveAsTable(TABLE_BOLETINES)

# Verify table is not empty
bulletin_count_after = spark.table(TABLE_BOLETINES).count()
if bulletin_count_after == 0:
    raise Exception(f"❌ ERROR: Tabla {TABLE_BOLETINES} está VACÍA después de guardar")

print(f"✅ Boletín guardado exitosamente ({bulletin_count_after} total en tabla)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Exportar Boletín a Formato TXT

# COMMAND ----------

# Guardar boletín en volume para exportación
output_filename = f"boletin_{PILOT_ZONE['name'].replace(' ', '_')}_{max_date.strftime('%Y%m%d')}.txt"
output_path = f"{VOLUME_PATH}/outputs/boletines/{output_filename}"

try:
    dbutils.fs.put(output_path, bulletin_content['bulletin_text'], overwrite=True)
    print(f"✅ Boletín exportado a: {output_path}")
except Exception as e:
    print(f"⚠️  No se pudo exportar: {e}")
    print(f"   Contenido disponible en tabla: {TABLE_BOLETINES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Visualizar Boletines Históricos

# COMMAND ----------

print("📚 Boletines históricos:")

all_boletines = spark.table(TABLE_BOLETINES).orderBy(F.desc("issue_date"))

display(all_boletines.select(
    "boletin_id",
    "zone_name",
    "issue_date",
    "eaws_level",
    "eaws_label",
    "summary",
    "generated_by",
    "creation_timestamp"
))

print(f"\n📊 Total boletines emitidos: {all_boletines.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Resumen Final

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 RESUMEN: GENERACIÓN DE BOLETINES")
print("=" * 80)

print(f"""
📝 BOLETÍN GENERADO:
   • ID: {bulletin_record['boletin_id']}
   • Zona: {PILOT_ZONE['name']}
   • Fecha emisión: {max_date}
   • Válido: {bulletin_record['valid_from']} a {bulletin_record['valid_to']}

🎯 CLASIFICACIÓN:
   • Nivel EAWS: {max_level} - {max_label}
   • Color: {EAWS_SCALE[max_level]['color']}
   • Confianza: {bulletin_record['confidence']}

⚠️  PROBLEMAS IDENTIFICADOS:
   {chr(10).join(['• ' + p for p in avalanche_problems]) if avalanche_problems else '• Condiciones estables'}

📍 ZONAS AFECTADAS:
   • Elevaciones: {affected_elevations}
   • Aspectos: Todas las orientaciones

🤖 GENERACIÓN:
   • Método: {bulletin_content['generation_method']}
   • Sistema: {bulletin_content['generated_by']}

💾 ALMACENAMIENTO:
   • Tabla: {TABLE_BOLETINES}
   • Archivo: {output_filename if 'output_filename' in locals() else 'N/A'}

📊 ESTADÍSTICAS:
   • Total boletines históricos: {all_boletines.count()}

✅ AGENTE INTEGRADOR COMPLETADO
   → Boletín listo para distribución
   → Sistema multi-agente funcionando end-to-end
""")

print("=" * 80)
