# Databricks notebook source
# MAGIC %md
# MAGIC # Instalación de Dependencias - POC Avalanche Prediction
# MAGIC
# MAGIC **Ejecutar SOLO UNA VEZ al inicio** para instalar todas las dependencias necesarias.
# MAGIC
# MAGIC Este notebook instala:
# MAGIC - Librerías de procesamiento geoespacial (rasterio, gdal, shapely)
# MAGIC - Modelos NLP (transformers, torch, sentence-transformers)
# MAGIC - APIs meteorológicas (openmeteo-requests, requests-cache)
# MAGIC - Análisis de redes (networkx)
# MAGIC - Visualización (plotly, folium)
# MAGIC - LLM integration (openai)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Librerías Core de Python

# COMMAND ----------

# MAGIC %pip install --upgrade pip setuptools wheel

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Procesamiento Geoespacial

# COMMAND ----------

# MAGIC %pip install rasterio==1.3.9
# MAGIC %pip install GDAL==3.4.1
# MAGIC %pip install shapely==2.0.2
# MAGIC %pip install geopandas==0.14.1
# MAGIC %pip install pyproj==3.6.1
# MAGIC %pip install fiona==1.9.5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Google Earth Engine (opcional - requiere autenticación)

# COMMAND ----------

# MAGIC %pip install earthengine-api==0.1.384

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. APIs Meteorológicas

# COMMAND ----------

# MAGIC %pip install openmeteo-requests==1.1.0
# MAGIC %pip install requests-cache==1.1.1
# MAGIC %pip install retry-requests==2.0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Modelos de NLP y Machine Learning

# COMMAND ----------

# MAGIC %pip install transformers==4.36.2
# MAGIC %pip install torch==2.1.2
# MAGIC %pip install sentence-transformers==2.2.2
# MAGIC %pip install scikit-learn==1.3.2
# MAGIC %pip install scipy==1.11.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. LLM Integration

# COMMAND ----------

# MAGIC %pip install openai==1.12.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Análisis de Redes Sociales

# COMMAND ----------

# MAGIC %pip install networkx==3.2.1
# MAGIC %pip install python-louvain==0.16

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Visualización

# COMMAND ----------

# MAGIC %pip install plotly==5.18.0
# MAGIC %pip install matplotlib==3.8.2
# MAGIC %pip install seaborn==0.13.1
# MAGIC %pip install folium==0.15.1
# MAGIC %pip install wordcloud==1.9.3

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Utilidades Adicionales

# COMMAND ----------

# MAGIC %pip install tqdm==4.66.1
# MAGIC %pip install python-dotenv==1.0.0
# MAGIC %pip install pyyaml==6.0.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Reiniciar Python Kernel

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Verificación de Instalación

# COMMAND ----------

import sys
print("Python version:", sys.version)
print("\n" + "="*70)
print("VERIFICANDO INSTALACIÓN DE DEPENDENCIAS")
print("="*70)

# Diccionario de librerías a verificar
libraries_to_check = {
    "Procesamiento Geoespacial": ["rasterio", "gdal", "shapely", "geopandas"],
    "APIs Meteorológicas": ["openmeteo_requests", "requests_cache", "retry_requests"],
    "NLP y ML": ["transformers", "torch", "sentence_transformers", "sklearn"],
    "LLM": ["openai"],
    "Análisis de Redes": ["networkx"],
    "Visualización": ["plotly", "matplotlib", "seaborn", "folium"],
    "Google Earth Engine": ["ee"]
}

# Verificar cada categoría
all_ok = True
for category, libs in libraries_to_check.items():
    print(f"\n📦 {category}:")
    for lib in libs:
        try:
            if lib == "gdal":
                from osgeo import gdal
                version = gdal.__version__
            elif lib == "sklearn":
                import sklearn
                version = sklearn.__version__
            else:
                module = __import__(lib)
                version = getattr(module, '__version__', 'installed')

            print(f"   ✅ {lib:25s} → {version}")
        except ImportError as e:
            print(f"   ❌ {lib:25s} → NOT INSTALLED")
            all_ok = False

print("\n" + "="*70)
if all_ok:
    print("✅ TODAS LAS DEPENDENCIAS INSTALADAS CORRECTAMENTE")
else:
    print("⚠️  ALGUNAS DEPENDENCIAS FALTANTES (revisa arriba)")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Prueba de Importación Crítica

# COMMAND ----------

print("🧪 PRUEBA DE IMPORTACIONES CRÍTICAS\n")

try:
    # Geoespacial
    import rasterio
    from osgeo import gdal
    import geopandas as gpd
    print("✅ Módulos geoespaciales OK")

    # Weather
    import openmeteo_requests
    import requests_cache
    from retry_requests import retry
    print("✅ APIs meteorológicas OK")

    # NLP
    from transformers import AutoTokenizer, AutoModel, pipeline
    import torch
    from sentence_transformers import SentenceTransformer
    print("✅ Modelos NLP OK")

    # LLM
    from openai import OpenAI
    print("✅ LLM integration OK")

    # Networks
    import networkx as nx
    print("✅ NetworkX OK")

    # Visualization
    import plotly.graph_objects as go
    import matplotlib.pyplot as plt
    import seaborn as sns
    import folium
    print("✅ Visualización OK")

    # PySpark (ya viene con Databricks)
    from pyspark.sql import functions as F
    from pyspark.sql.types import *
    print("✅ PySpark OK")

    print("\n🎉 TODAS LAS IMPORTACIONES CRÍTICAS EXITOSAS")
    print("   → El entorno está listo para ejecutar el POC")

except Exception as e:
    print(f"\n❌ ERROR EN IMPORTACIONES: {e}")
    print("   → Revisa la instalación de dependencias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Configuración de Earth Engine (Opcional)

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTA:** Google Earth Engine requiere autenticación separada.
# MAGIC
# MAGIC Si necesitas usar GEE:
# MAGIC 1. Ejecuta: `earthengine authenticate` en un terminal
# MAGIC 2. O usa credenciales de service account
# MAGIC
# MAGIC Para este POC, el análisis SRTM puede hacerse con datos precargados.

# COMMAND ----------

def test_earth_engine():
    """Prueba conexión a Google Earth Engine"""
    try:
        import ee
        ee.Initialize()
        print("✅ Google Earth Engine inicializado correctamente")
        return True
    except Exception as e:
        print(f"⚠️  Google Earth Engine no disponible: {e}")
        print("   → No es crítico para el POC si usas datos SRTM precargados")
        return False

test_earth_engine()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Resumen Final

# COMMAND ----------

print("\n" + "="*70)
print("📋 RESUMEN DE INSTALACIÓN")
print("="*70)
print("""
✅ INSTALADO:
   • Procesamiento geoespacial (GDAL, Rasterio, GeoPandas)
   • APIs meteorológicas (Open-Meteo)
   • Modelos NLP (Transformers, Sentence-Transformers)
   • LLM Integration (OpenAI SDK para Databricks)
   • Análisis de redes (NetworkX)
   • Visualización (Plotly, Matplotlib, Folium)
   • PySpark (nativo en Databricks)

📝 PRÓXIMOS PASOS:
   1. Ejecutar: 00_environment_setup.py
   2. Ejecutar: 02_create_unity_catalog.py
   3. Iniciar ingesta de datos con cada agente

⚠️  NOTAS IMPORTANTES:
   • Google Earth Engine requiere autenticación adicional
   • Para Databricks Community Edition, algunas librerías pueden tener limitaciones
   • El POC está diseñado para funcionar con datos precargados si APIs fallan

🔗 DOCUMENTACIÓN:
   • Rasterio: https://rasterio.readthedocs.io
   • Transformers: https://huggingface.co/docs/transformers
   • Open-Meteo: https://open-meteo.com/en/docs
   • NetworkX: https://networkx.org/documentation/stable/
""")
print("="*70)

print("\n🎉 INSTALACIÓN COMPLETADA")
print("   → Reinicia el kernel si es necesario")
print("   → Continúa con la configuración de Unity Catalog\n")
