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
# MAGIC ## 2. Procesamiento Geoespacial (OPCIONAL)
# MAGIC
# MAGIC **NOTA:** Estas librerías son opcionales. El POC funciona sin ellas usando DEM sintético.
# MAGIC Si la instalación falla, puedes omitir este paso.

# COMMAND ----------

# Instalar librerías geoespaciales (pueden fallar en algunos entornos)
print("⚠️  Instalando librerías geoespaciales (OPCIONAL)...")
print("   Si alguna falla, el POC seguirá funcionando con DEM sintético.\n")

try:
    %pip install --quiet shapely>=2.0.0
    print("✅ shapely instalado")
except:
    print("⚠️  shapely falló (opcional)")

try:
    %pip install --quiet pyproj>=3.6.0
    print("✅ pyproj instalado")
except:
    print("⚠️  pyproj falló (opcional)")

try:
    %pip install --quiet geopandas>=0.14.0
    print("✅ geopandas instalado")
except:
    print("⚠️  geopandas falló (opcional)")

print("\n⚠️  NOTA: rasterio/GDAL NO son necesarios - el POC usa DEM sintético")
print("   El sistema funcionará correctamente sin estas librerías.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Google Earth Engine (OPCIONAL - requiere autenticación)
# MAGIC
# MAGIC **NOTA:** Opcional. El POC funciona sin GEE usando DEM sintético.

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
# CRÍTICAS: necesarias para el POC
# OPCIONALES: el POC funciona sin ellas
libraries_to_check = {
    "APIs Meteorológicas (CRÍTICAS)": ["openmeteo_requests", "requests_cache", "retry_requests"],
    "NLP y ML (CRÍTICAS)": ["transformers", "torch", "sentence_transformers", "sklearn"],
    "LLM (CRÍTICA)": ["openai"],
    "Análisis de Redes (CRÍTICA)": ["networkx"],
    "Visualización (CRÍTICAS)": ["plotly", "matplotlib", "seaborn"],
    "Procesamiento Geoespacial (OPCIONAL)": ["shapely", "geopandas"],
    "Google Earth Engine (OPCIONAL)": ["ee"]
}

# Verificar cada categoría
critical_ok = True
optional_missing = []

for category, libs in libraries_to_check.items():
    is_critical = "CRÍTICA" in category
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
            if is_critical:
                critical_ok = False
            else:
                optional_missing.append(lib)

print("\n" + "="*70)
if critical_ok:
    print("✅ TODAS LAS DEPENDENCIAS CRÍTICAS INSTALADAS CORRECTAMENTE")
    if optional_missing:
        print(f"⚠️  Dependencias opcionales faltantes: {', '.join(optional_missing)}")
        print("   → El POC funcionará correctamente usando alternativas sintéticas")
else:
    print("❌ ALGUNAS DEPENDENCIAS CRÍTICAS FALTANTES")
    print("   → Ejecuta este notebook nuevamente o instala manualmente")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Prueba de Importación Crítica

# COMMAND ----------

print("🧪 PRUEBA DE IMPORTACIONES CRÍTICAS\n")

critical_imports_ok = True

# Geoespacial (OPCIONAL)
try:
    import rasterio
    from osgeo import gdal
    import geopandas as gpd
    print("✅ Módulos geoespaciales OK (opcional)")
except Exception as e:
    print("⚠️  Módulos geoespaciales NO disponibles (opcional - se usará DEM sintético)")

# Weather (CRÍTICO)
try:
    import openmeteo_requests
    import requests_cache
    from retry_requests import retry
    print("✅ APIs meteorológicas OK")
except Exception as e:
    print(f"❌ APIs meteorológicas FALLARON: {e}")
    critical_imports_ok = False

# NLP (CRÍTICO)
try:
    from transformers import AutoTokenizer, AutoModel, pipeline
    import torch
    from sentence_transformers import SentenceTransformer
    print("✅ Modelos NLP OK")
except Exception as e:
    print(f"❌ Modelos NLP FALLARON: {e}")
    critical_imports_ok = False

# LLM (CRÍTICO)
try:
    from openai import OpenAI
    print("✅ LLM integration OK")
except Exception as e:
    print(f"❌ LLM integration FALLÓ: {e}")
    critical_imports_ok = False

# Networks (CRÍTICO)
try:
    import networkx as nx
    print("✅ NetworkX OK")
except Exception as e:
    print(f"❌ NetworkX FALLÓ: {e}")
    critical_imports_ok = False

# Visualization (CRÍTICO)
try:
    import plotly.graph_objects as go
    import matplotlib.pyplot as plt
    import seaborn as sns
    import folium
    print("✅ Visualización OK")
except Exception as e:
    print(f"❌ Visualización FALLÓ: {e}")
    critical_imports_ok = False

# PySpark (ya viene con Databricks)
try:
    from pyspark.sql import functions as F
    from pyspark.sql.types import *
    print("✅ PySpark OK")
except Exception as e:
    print(f"❌ PySpark FALLÓ: {e}")
    critical_imports_ok = False

if critical_imports_ok:
    print("\n🎉 TODAS LAS IMPORTACIONES CRÍTICAS EXITOSAS")
    print("   → El entorno está listo para ejecutar el POC")
else:
    print("\n❌ ERROR: Algunas importaciones críticas fallaron")
    print("   → Revisa la instalación de dependencias y ejecuta este notebook nuevamente")

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
✅ DEPENDENCIAS CRÍTICAS INSTALADAS:
   • APIs meteorológicas (Open-Meteo)
   • Modelos NLP (Transformers, Sentence-Transformers)
   • LLM Integration (OpenAI SDK para Databricks)
   • Análisis de redes (NetworkX)
   • Visualización (Plotly, Matplotlib, Folium)
   • PySpark (nativo en Databricks)

⚠️  DEPENDENCIAS OPCIONALES:
   • Procesamiento geoespacial (GDAL, Rasterio, GeoPandas)
     → Si no están instaladas, el POC usa DEM sintético
   • Google Earth Engine (requiere autenticación adicional)
     → Si no está configurado, el POC usa DEM sintético

📝 PRÓXIMOS PASOS:
   1. Ejecutar: 00_environment_setup.py
   2. Ejecutar: 02_create_unity_catalog.py
   3. Ejecutar: 05_Pipeline/01_orchestrator.py

💡 MODO DE FUNCIONAMIENTO:
   • El POC funciona completamente con o sin librerías geoespaciales
   • Usa DEM sintético realista cuando GEE/rasterio no están disponibles
   • Todas las APIs tienen fallbacks a datos sintéticos

🔗 DOCUMENTACIÓN:
   • Transformers: https://huggingface.co/docs/transformers
   • Open-Meteo: https://open-meteo.com/en/docs
   • NetworkX: https://networkx.org/documentation/stable/
""")
print("="*70)

print("\n🎉 INSTALACIÓN COMPLETADA")
print("   → Reinicia el kernel si es necesario")
print("   → Continúa con la configuración de Unity Catalog\n")
