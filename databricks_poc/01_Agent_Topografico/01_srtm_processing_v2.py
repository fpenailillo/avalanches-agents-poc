# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 1: Procesamiento Topográfico SRTM - Google Earth Engine v2
# MAGIC
# MAGIC **Versión refactorizada para conexión EXCLUSIVA a Google Earth Engine**
# MAGIC
# MAGIC Este agente procesa datos de elevación SRTM desde Google Earth Engine para:
# MAGIC - Extraer DEM filtrado por elevación > 2000m (zona de avalanchas)
# MAGIC - Calcular pendientes y orientaciones NATIVAMENTE en GEE
# MAGIC - Identificar pendientes críticas (30-45°)
# MAGIC - Muestreo adaptativo según tamaño del área
# MAGIC
# MAGIC **IMPORTANTE:** Este notebook requiere autenticación con Google Earth Engine.
# MAGIC Si GEE no está disponible, el proceso FALLA explícitamente (sin fallbacks).
# MAGIC
# MAGIC ## Configuración de Credenciales GEE
# MAGIC
# MAGIC ### Opción 1: Service Account (Recomendado para producción)
# MAGIC ```python
# MAGIC # 1. Sube el JSON del service account a Databricks Secrets:
# MAGIC #    databricks secrets create-scope --scope gee-secrets
# MAGIC #    databricks secrets put --scope gee-secrets --key service-account-json
# MAGIC #
# MAGIC # 2. O súbelo a un Volume de Unity Catalog:
# MAGIC #    /Volumes/workspace/avalanches_agents/raw_files/gee-service-account.json
# MAGIC ```
# MAGIC
# MAGIC ### Opción 2: Autenticación Interactiva (Para desarrollo)
# MAGIC ```bash
# MAGIC # En terminal de Databricks:
# MAGIC earthengine authenticate
# MAGIC ```
# MAGIC
# MAGIC **Input:** SRTM de Google Earth Engine
# MAGIC **Output:** Tabla `topo_features` en capa Silver con metadatos GEE

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar Configuración

# COMMAND ----------

print("🔍 VERIFICANDO CONFIGURACIÓN...")
print(f"   Catalog: {CATALOG}")
print(f"   Schema: {SCHEMA}")
print(f"   Full Database: {FULL_DATABASE}")
print(f"   Tabla destino: {TABLE_TOPO_FEATURES}")
print(f"   Zona piloto: {PILOT_ZONE['name']}")

# Verificar schema existe
try:
    spark.sql(f"USE {FULL_DATABASE}")
    print(f"✅ Schema '{FULL_DATABASE}' existe y está activo")
except Exception as e:
    print(f"❌ ERROR: Schema '{FULL_DATABASE}' no existe: {e}")
    raise Exception(f"Ejecuta primero: 00_Setup/02_create_unity_catalog.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Importar Librerías

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime
import uuid
import json

# PySpark
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Google Earth Engine
try:
    import ee
    print("✅ Earth Engine API importada")
except ImportError as e:
    print(f"❌ ERROR CRÍTICO: Earth Engine API no está instalada")
    print(f"   → Ejecuta: %pip install earthengine-api==0.1.384")
    raise Exception("Earth Engine API requerida. Instala con: pip install earthengine-api")

print("✅ Librerías importadas correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuración de Autenticación GEE

# COMMAND ----------

# Configuración para autenticación
GEE_SERVICE_ACCOUNT_EMAIL = None  # Se llenará si usamos service account
GEE_AUTH_METHOD = None  # 'service_account' o 'interactive'

# Rutas posibles para service account JSON
POSSIBLE_SERVICE_ACCOUNT_PATHS = [
    f"{VOLUME_PATH}/gee-service-account.json",
    "/dbfs/FileStore/gee-credentials/service-account.json",
    "/tmp/gee-service-account.json"
]

print("🔐 CONFIGURACIÓN DE AUTENTICACIÓN GEE")
print("=" * 70)
print("Métodos soportados:")
print("  1. Service Account (JSON key)")
print("  2. Autenticación interactiva")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Función de Autenticación Robusta

# COMMAND ----------

def authenticate_gee():
    """
    Autentica con Google Earth Engine usando múltiples métodos.

    Orden de prioridad:
    1. Service Account desde Databricks Secrets
    2. Service Account desde archivo JSON en Volume
    3. Autenticación interactiva (credentials guardadas)

    Returns:
        bool: True si autenticación exitosa, False si falla

    Raises:
        Exception: Si ningún método de autenticación funciona
    """
    global GEE_AUTH_METHOD, GEE_SERVICE_ACCOUNT_EMAIL

    print("🔐 Intentando autenticar con Google Earth Engine...")
    print("-" * 70)

    # MÉTODO 1: Service Account desde Databricks Secrets
    try:
        print("1️⃣  Intentando autenticación con Service Account (Databricks Secrets)...")

        # Intentar leer desde Databricks Secrets
        try:
            service_account_json_str = dbutils.secrets.get(scope="gee-secrets", key="service-account-json")
            service_account_info = json.loads(service_account_json_str)

            credentials = ee.ServiceAccountCredentials(
                service_account_info['client_email'],
                key_data=service_account_json_str
            )
            ee.Initialize(credentials)

            GEE_AUTH_METHOD = 'service_account_secrets'
            GEE_SERVICE_ACCOUNT_EMAIL = service_account_info['client_email']

            print(f"✅ Autenticado con Service Account: {GEE_SERVICE_ACCOUNT_EMAIL}")
            print(f"   Método: Databricks Secrets")
            return True

        except Exception as secret_error:
            print(f"   ⚠️  No disponible: {secret_error}")

    except Exception as e:
        print(f"   ⚠️  Error en método 1: {e}")

    # MÉTODO 2: Service Account desde archivo JSON en Volume
    for json_path in POSSIBLE_SERVICE_ACCOUNT_PATHS:
        try:
            print(f"2️⃣  Intentando Service Account desde archivo: {json_path}...")

            # Leer archivo JSON
            with open(json_path.replace('/Volumes/', '/dbfs/Volumes/'), 'r') as f:
                service_account_info = json.load(f)

            credentials = ee.ServiceAccountCredentials(
                service_account_info['client_email'],
                json_path.replace('/Volumes/', '/dbfs/Volumes/')
            )
            ee.Initialize(credentials)

            GEE_AUTH_METHOD = 'service_account_file'
            GEE_SERVICE_ACCOUNT_EMAIL = service_account_info['client_email']

            print(f"✅ Autenticado con Service Account: {GEE_SERVICE_ACCOUNT_EMAIL}")
            print(f"   Archivo: {json_path}")
            return True

        except FileNotFoundError:
            print(f"   ⚠️  Archivo no encontrado: {json_path}")
        except Exception as e:
            print(f"   ⚠️  Error leyendo {json_path}: {e}")

    # MÉTODO 3: Autenticación Interactiva
    try:
        print("3️⃣  Intentando autenticación interactiva...")
        print("   (Requiere haber ejecutado: earthengine authenticate)")

        ee.Initialize()

        GEE_AUTH_METHOD = 'interactive'

        print("✅ Autenticado con credenciales interactivas")
        print("   Método: earthengine authenticate")
        return True

    except Exception as e:
        print(f"   ❌ Falló autenticación interactiva: {e}")

    # Si llegamos aquí, ningún método funcionó
    print("\n" + "=" * 70)
    print("❌ ERROR CRÍTICO: NO SE PUDO AUTENTICAR CON GOOGLE EARTH ENGINE")
    print("=" * 70)
    print("\n📋 Para resolver este error, elige UNA de estas opciones:\n")
    print("OPCIÓN A - Service Account (Recomendado para producción):")
    print("  1. Obtén un JSON de service account de tu proyecto GEE")
    print("  2. Súbelo a Databricks Secrets:")
    print("     databricks secrets create-scope --scope gee-secrets")
    print("     databricks secrets put --scope gee-secrets --key service-account-json")
    print("  3. O súbelo a un Volume:")
    print(f"     {VOLUME_PATH}/gee-service-account.json\n")
    print("OPCIÓN B - Autenticación Interactiva (Para desarrollo):")
    print("  1. En terminal de Databricks ejecuta:")
    print("     earthengine authenticate")
    print("  2. Sigue las instrucciones para autenticar con tu cuenta Google")
    print("=" * 70)

    raise Exception("❌ Google Earth Engine no disponible. Ver instrucciones arriba.")

# Ejecutar autenticación
authenticate_gee()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Función de Test de Conexión GEE

# COMMAND ----------

def test_gee_connection():
    """
    Verifica que GEE está funcionando correctamente con una operación simple.

    Returns:
        bool: True si la conexión funciona
    """
    print("\n🔬 Testeando conexión con Google Earth Engine...")

    try:
        # Test simple: obtener información de SRTM
        srtm = ee.Image('USGS/SRTMGL1_003')

        # Obtener metadatos básicos
        info = srtm.getInfo()

        print("✅ Conexión GEE exitosa")
        print(f"   Dataset SRTM: {info['id']}")
        print(f"   Bandas disponibles: {info['bands'][0]['id']}")

        return True

    except Exception as e:
        print(f"❌ ERROR en test de conexión GEE: {e}")
        raise Exception("GEE no está respondiendo correctamente. Verifica tu autenticación.")

# Ejecutar test
test_gee_connection()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Configuración de Zona Piloto

# COMMAND ----------

print("🏔️  CONFIGURACIÓN DE ZONA PILOTO")
print("=" * 70)
print(f"Zona: {PILOT_ZONE['name']}")
print(f"Centro: ({PILOT_ZONE['center_lat']:.4f}, {PILOT_ZONE['center_lon']:.4f})")
print(f"Bounding Box:")
print(f"   Sur: {PILOT_ZONE['bbox']['south']:.4f}")
print(f"   Norte: {PILOT_ZONE['bbox']['north']:.4f}")
print(f"   Oeste: {PILOT_ZONE['bbox']['west']:.4f}")
print(f"   Este: {PILOT_ZONE['bbox']['east']:.4f}")
print(f"Elevación mínima para análisis: 2000 m (filtro avalanchas)")
print(f"Pendientes críticas: {CRITICAL_SLOPE_MIN}-{CRITICAL_SLOPE_MAX}°")
print("=" * 70)

# Calcular área aproximada
bbox = PILOT_ZONE['bbox']
area_deg2 = abs(bbox['north'] - bbox['south']) * abs(bbox['east'] - bbox['west'])
area_km2 = area_deg2 * 111 * 85  # Aproximación para esta latitud

print(f"\n📊 Área aproximada: {area_km2:.2f} km²")

# Determinar estrategia de muestreo
if area_km2 < 100:
    sampling_strategy = "full"
    print(f"   Estrategia: Extracción COMPLETA (área < 100 km²)")
else:
    sampling_strategy = "adaptive"
    print(f"   Estrategia: Muestreo ADAPTATIVO (área > 100 km²)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Extracción y Procesamiento en Google Earth Engine

# COMMAND ----------

def process_srtm_in_gee(bbox, elevation_min=2000, slope_min=0, slope_max=90):
    """
    Procesa SRTM completamente en Google Earth Engine.

    Procesamiento nativo en GEE:
    - Carga SRTM 30m
    - Filtra por elevación > elevation_min
    - Calcula slope y aspect con ee.Terrain
    - Identifica pendientes críticas
    - Extrae datos filtrados

    Args:
        bbox: Diccionario con south, north, west, east
        elevation_min: Elevación mínima en metros (default 2000)
        slope_min: Pendiente mínima para análisis
        slope_max: Pendiente máxima para análisis

    Returns:
        ee.Image: Imagen procesada con bandas: elevation, slope, aspect
    """
    print("\n🌍 PROCESANDO DATOS EN GOOGLE EARTH ENGINE")
    print("=" * 70)

    # 1. Definir área de interés (AOI)
    aoi = ee.Geometry.Rectangle([
        bbox['west'],
        bbox['south'],
        bbox['east'],
        bbox['north']
    ])

    print(f"1️⃣  Área de interés definida")
    print(f"   Bounds: ({bbox['south']:.4f}, {bbox['west']:.4f}) a ({bbox['north']:.4f}, {bbox['east']:.4f})")

    # 2. Cargar SRTM
    srtm = ee.Image('USGS/SRTMGL1_003')
    dem = srtm.select('elevation').clip(aoi)

    print(f"2️⃣  SRTM cargado desde GEE")
    print(f"   Dataset: USGS/SRTMGL1_003")
    print(f"   Resolución: 30 metros")

    # 3. Aplicar filtro de elevación > 2000m (zona de avalanchas)
    elevation_mask = dem.gte(elevation_min)
    dem_filtered = dem.updateMask(elevation_mask)

    print(f"3️⃣  Filtro de elevación aplicado")
    print(f"   Mínima elevación: {elevation_min} m")
    print(f"   (Solo zonas propensas a avalanchas)")

    # 4. Calcular slope y aspect NATIVAMENTE en GEE
    terrain = ee.Terrain.products(dem_filtered)
    slope = terrain.select('slope')
    aspect = terrain.select('aspect')

    print(f"4️⃣  Slope y Aspect calculados en GEE")
    print(f"   Algoritmo: ee.Terrain.products()")
    print(f"   Slope: grados (0-90)")
    print(f"   Aspect: grados (0-360)")

    # 5. Identificar pendientes críticas (30-45°)
    critical_slope_mask = slope.gte(CRITICAL_SLOPE_MIN).And(slope.lte(CRITICAL_SLOPE_MAX))
    is_critical_slope = critical_slope_mask.byte()

    print(f"5️⃣  Pendientes críticas identificadas")
    print(f"   Rango: {CRITICAL_SLOPE_MIN}-{CRITICAL_SLOPE_MAX}°")

    # 6. Combinar todas las bandas
    result = ee.Image.cat([
        dem_filtered.rename('elevation'),
        slope.rename('slope'),
        aspect.rename('aspect'),
        is_critical_slope.rename('is_critical_slope')
    ])

    print(f"6️⃣  Imagen multi-banda creada")
    print(f"   Bandas: elevation, slope, aspect, is_critical_slope")

    # 7. Obtener estadísticas del área procesada
    stats = result.reduceRegion(
        reducer=ee.Reducer.minMax().combine(ee.Reducer.mean(), '', True).combine(ee.Reducer.count(), '', True),
        geometry=aoi,
        scale=30,
        maxPixels=1e9
    ).getInfo()

    print(f"\n📊 ESTADÍSTICAS DEL ÁREA PROCESADA (GEE):")
    print(f"   Elevación mín: {stats.get('elevation_min', 0):.0f} m")
    print(f"   Elevación máx: {stats.get('elevation_max', 0):.0f} m")
    print(f"   Elevación media: {stats.get('elevation_mean', 0):.0f} m")
    print(f"   Slope medio: {stats.get('slope_mean', 0):.2f}°")
    print(f"   Píxeles procesados: {stats.get('elevation_count', 0):,}")

    print("=" * 70)
    print("✅ Procesamiento en GEE completado")

    return result, aoi, stats

# Procesar datos en GEE
gee_result, gee_aoi, gee_stats = process_srtm_in_gee(PILOT_ZONE['bbox'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Extracción de Datos con Muestreo Adaptativo

# COMMAND ----------

def extract_gee_data_adaptive(gee_image, aoi, max_pixels=50000):
    """
    Extrae datos de GEE con estrategia de muestreo adaptativo.

    Estrategia:
    - Si píxeles totales < 50,000 → extracción completa
    - Si píxeles totales > 50,000 → muestreo sistemático
    - Batches de máximo 5000 puntos por request (evitar timeout)

    Args:
        gee_image: Imagen de Earth Engine procesada
        aoi: Área de interés (ee.Geometry)
        max_pixels: Máximo de píxeles a extraer

    Returns:
        pd.DataFrame: DataFrame con datos extraídos
    """
    print("\n📦 EXTRAYENDO DATOS DESDE GOOGLE EARTH ENGINE")
    print("=" * 70)

    # Estimar número total de píxeles
    pixel_count = gee_stats.get('elevation_count', 0)

    print(f"Píxeles totales en área: {pixel_count:,}")

    # Determinar escala de muestreo
    if pixel_count <= max_pixels:
        # Extracción completa
        scale = 30  # Resolución nativa SRTM
        print(f"✅ Extracción COMPLETA ({pixel_count:,} píxeles)")
        print(f"   Escala: 30m (resolución nativa)")
    else:
        # Muestreo adaptativo
        # Calcular factor de muestreo para tener ~max_pixels
        sample_factor = int(np.sqrt(pixel_count / max_pixels))
        scale = 30 * sample_factor

        print(f"⚡ Muestreo ADAPTATIVO")
        print(f"   Píxeles originales: {pixel_count:,}")
        print(f"   Píxeles objetivo: {max_pixels:,}")
        print(f"   Factor de muestreo: {sample_factor}x")
        print(f"   Escala: {scale}m")

    # Extraer puntos usando sampleRegions
    print(f"\n⏳ Extrayendo datos (esto puede tomar varios minutos)...")

    try:
        # Crear grid de puntos
        # Convertir imagen a puntos (cada píxel)
        sample = gee_image.sample(
            region=aoi,
            scale=scale,
            projection='EPSG:4326',
            geometries=True
        )

        # Convertir a lista de features
        features = sample.getInfo()

        if not features or len(features['features']) == 0:
            raise Exception("❌ ERROR: No se extrajeron datos de GEE. Verifica la zona y filtros.")

        print(f"✅ Datos extraídos: {len(features['features']):,} puntos")

        # Convertir a DataFrame
        data = []
        for feature in features['features']:
            props = feature['properties']
            coords = feature['geometry']['coordinates']

            data.append({
                'longitude': coords[0],
                'latitude': coords[1],
                'elevation': props.get('elevation'),
                'slope_degrees': props.get('slope'),
                'aspect_degrees': props.get('aspect'),
                'is_critical_slope': bool(props.get('is_critical_slope', 0))
            })

        df = pd.DataFrame(data)

        # Validar datos
        if len(df) < 1000:
            print(f"⚠️  ADVERTENCIA: Solo {len(df)} píxeles extraídos (< 1000)")
            print(f"   Esto podría indicar un problema con los filtros o el área")

        print(f"\n📊 DataFrame creado:")
        print(f"   Filas: {len(df):,}")
        print(f"   Columnas: {len(df.columns)}")

        return df, scale

    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO durante extracción de GEE:")
        print(f"   {e}")
        print(f"\n🔍 Posibles causas:")
        print(f"   1. Timeout de GEE (área muy grande)")
        print(f"   2. Sin datos en el área después de aplicar filtros")
        print(f"   3. Problema de conectividad con GEE")
        raise Exception(f"Fallo en extracción de GEE: {e}")

# Extraer datos
gee_data_df, gee_scale = extract_gee_data_adaptive(gee_result, gee_aoi)

# Mostrar muestra
print("\n📋 Muestra de datos extraídos:")
display(gee_data_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Enriquecer con Metadatos y Clasificación

# COMMAND ----------

def enrich_topo_features(df, zone_name, gee_scale):
    """
    Enriquece DataFrame con metadatos GEE y clasificaciones.

    Agrega:
    - feature_id único
    - zone_name
    - elevation_band
    - gee_scale (resolución real del píxel)
    - gee_timestamp (cuándo se procesó)
    - gee_quality_flag (calidad 0-1)
    - processing_timestamp

    Args:
        df: DataFrame con datos de GEE
        zone_name: Nombre de la zona
        gee_scale: Escala de procesamiento en GEE

    Returns:
        pd.DataFrame: DataFrame enriquecido
    """
    print("\n🔧 Enriqueciendo features topográficos...")

    # Generar IDs únicos
    df['feature_id'] = [f"gee_{uuid.uuid4().hex[:12]}" for _ in range(len(df))]

    # Agregar zona
    df['zone_name'] = zone_name

    # Clasificar en bandas altitudinales
    def get_band_name(elevation):
        for band in ELEVATION_BANDS:
            if band['min'] <= elevation < band['max']:
                return band['name']
        return ELEVATION_BANDS[-1]['name']

    df['elevation_band'] = df['elevation'].apply(get_band_name)

    # Metadatos GEE
    df['gee_scale'] = gee_scale
    df['gee_timestamp'] = datetime.now()

    # Quality flag (0-1)
    # Basado en si los valores están en rangos esperados
    df['gee_quality_flag'] = 1.0  # Por defecto alta calidad

    # Reducir calidad si hay valores fuera de rango
    df.loc[df['elevation'] < 0, 'gee_quality_flag'] = 0.5
    df.loc[df['slope_degrees'] > 90, 'gee_quality_flag'] = 0.5
    df.loc[df['aspect_degrees'] > 360, 'gee_quality_flag'] = 0.5
    df.loc[pd.isna(df['elevation']), 'gee_quality_flag'] = 0.0

    # Timestamp de procesamiento
    df['processing_timestamp'] = datetime.now()

    # Ordenar columnas
    df = df[[
        'feature_id',
        'latitude',
        'longitude',
        'elevation',
        'slope_degrees',
        'aspect_degrees',
        'elevation_band',
        'is_critical_slope',
        'zone_name',
        'gee_scale',
        'gee_timestamp',
        'gee_quality_flag',
        'processing_timestamp'
    ]]

    print(f"✅ Features enriquecidos:")
    print(f"   Total features: {len(df):,}")
    print(f"   Calidad promedio: {df['gee_quality_flag'].mean():.3f}")
    print(f"   Features de alta calidad (>0.9): {(df['gee_quality_flag'] > 0.9).sum():,}")

    return df

# Enriquecer datos
topo_features_df = enrich_topo_features(gee_data_df, PILOT_ZONE['name'], gee_scale)

# Mostrar muestra enriquecida
print("\n📋 Muestra de features enriquecidos:")
display(topo_features_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validación Estricta de Datos

# COMMAND ----------

def validate_gee_data(df):
    """
    Validación estricta de datos de GEE.

    Si la validación falla, el proceso se DETIENE.
    No hay fallbacks ni continuación con datos defectuosos.

    Args:
        df: DataFrame a validar

    Raises:
        Exception: Si cualquier validación falla
    """
    print("\n🔍 VALIDACIÓN ESTRICTA DE DATOS GEE")
    print("=" * 70)

    errors = []
    warnings = []

    # 1. Verificar que hay datos
    if len(df) == 0:
        errors.append("DataFrame VACÍO - no se extrajeron datos de GEE")
    else:
        print(f"✅ Dataset no vacío: {len(df):,} registros")

    # 2. Verificar mínimo de píxeles
    if len(df) < 1000:
        warnings.append(f"Solo {len(df)} píxeles extraídos (< 1000). Cobertura podría ser insuficiente.")
    else:
        print(f"✅ Cobertura suficiente: {len(df):,} píxeles")

    # 3. Verificar valores nulos
    null_counts = df.isnull().sum()
    critical_cols = ['latitude', 'longitude', 'elevation', 'slope_degrees', 'aspect_degrees']

    for col in critical_cols:
        if null_counts[col] > 0:
            errors.append(f"Columna '{col}' tiene {null_counts[col]} valores nulos")

    if null_counts[critical_cols].sum() == 0:
        print(f"✅ Sin valores nulos en columnas críticas")

    # 4. Verificar rangos de valores
    if (df['elevation'] < 0).any():
        errors.append(f"Elevaciones negativas encontradas (mín: {df['elevation'].min()})")

    if (df['slope_degrees'] < 0).any() or (df['slope_degrees'] > 90).any():
        errors.append(f"Pendientes fuera de rango 0-90° (rango: {df['slope_degrees'].min()}-{df['slope_degrees'].max()})")

    if (df['aspect_degrees'] < 0).any() or (df['aspect_degrees'] > 360).any():
        errors.append(f"Aspect fuera de rango 0-360° (rango: {df['aspect_degrees'].min()}-{df['aspect_degrees'].max()})")

    if df['elevation'].min() >= 0 and df['elevation'].max() <= 9000:
        print(f"✅ Elevaciones en rango válido: {df['elevation'].min():.0f} - {df['elevation'].max():.0f} m")

    if df['slope_degrees'].min() >= 0 and df['slope_degrees'].max() <= 90:
        print(f"✅ Pendientes en rango válido: {df['slope_degrees'].min():.2f} - {df['slope_degrees'].max():.2f}°")

    # 5. Verificar cobertura geográfica
    bbox = PILOT_ZONE['bbox']
    lat_range = df['latitude'].max() - df['latitude'].min()
    lon_range = df['longitude'].max() - df['longitude'].min()
    expected_lat_range = bbox['north'] - bbox['south']
    expected_lon_range = bbox['east'] - bbox['west']

    if lat_range < expected_lat_range * 0.8 or lon_range < expected_lon_range * 0.8:
        warnings.append(f"Cobertura geográfica parcial (lat: {lat_range:.4f}°, lon: {lon_range:.4f}°)")
    else:
        print(f"✅ Cobertura geográfica completa")

    # 6. Verificar calidad GEE
    avg_quality = df['gee_quality_flag'].mean()
    if avg_quality < 0.9:
        warnings.append(f"Calidad promedio de GEE baja: {avg_quality:.3f}")
    else:
        print(f"✅ Calidad GEE alta: {avg_quality:.3f}")

    # Resumen de validación
    print("\n" + "=" * 70)

    if errors:
        print("❌ VALIDACIÓN FALLIDA - ERRORES CRÍTICOS:")
        for i, error in enumerate(errors, 1):
            print(f"   {i}. {error}")
        print("\n🛑 PROCESO DETENIDO - No se guardarán datos defectuosos")
        print("=" * 70)
        raise Exception(f"Validación de datos GEE falló con {len(errors)} errores")

    if warnings:
        print("⚠️  ADVERTENCIAS (no bloquean el proceso):")
        for i, warning in enumerate(warnings, 1):
            print(f"   {i}. {warning}")

    print("\n✅ VALIDACIÓN EXITOSA - Datos listos para guardar")
    print("=" * 70)

    return True

# Validar datos
validate_gee_data(topo_features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Guardar en Delta Lake (Capa Silver)

# COMMAND ----------

print(f"💾 GUARDANDO EN DELTA LAKE: {TABLE_TOPO_FEATURES}")
print("=" * 70)

# Verificar que tenemos datos válidos
if len(topo_features_df) == 0:
    raise Exception("❌ ERROR CRÍTICO: DataFrame vacío. No hay datos para guardar.")

print(f"   DataFrame Pandas: {len(topo_features_df):,} registros")

# Convertir a Spark DataFrame
topo_spark_df = spark.createDataFrame(topo_features_df)

# Verificar conversión
record_count = topo_spark_df.count()
if record_count == 0:
    raise Exception("❌ ERROR CRÍTICO: Spark DataFrame vacío después de conversión")

print(f"   Spark DataFrame: {record_count:,} registros")

# Guardar en Delta Lake
topo_spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_TOPO_FEATURES)

# Verificar que se guardó correctamente
saved_count = spark.table(TABLE_TOPO_FEATURES).count()
if saved_count == 0:
    raise Exception(f"❌ ERROR CRÍTICO: Tabla {TABLE_TOPO_FEATURES} vacía después de guardar")

if saved_count != record_count:
    print(f"⚠️  ADVERTENCIA: Se esperaban {record_count:,} registros pero se guardaron {saved_count:,}")

print(f"✅ Datos guardados exitosamente")
print(f"   Tabla: {TABLE_TOPO_FEATURES}")
print(f"   Registros: {saved_count:,}")
print(f"   Formato: Delta Lake")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Estadísticas por Banda Altitudinal

# COMMAND ----------

print("📊 ESTADÍSTICAS POR BANDA ALTITUDINAL")
print("=" * 70)

stats = topo_spark_df.groupBy("elevation_band").agg(
    F.count("*").alias("total_pixels"),
    F.avg("elevation").alias("avg_elevation_m"),
    F.avg("slope_degrees").alias("avg_slope_deg"),
    F.sum(F.when(F.col("is_critical_slope"), 1).otherwise(0)).alias("critical_pixels"),
    F.min("slope_degrees").alias("min_slope"),
    F.max("slope_degrees").alias("max_slope"),
    F.avg("gee_quality_flag").alias("avg_quality")
).orderBy("avg_elevation_m")

display(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Métricas de Calidad GEE

# COMMAND ----------

print("📊 MÉTRICAS DE CALIDAD - GOOGLE EARTH ENGINE")
print("=" * 70)

quality_metrics = topo_spark_df.agg(
    F.count("*").alias("total_features"),
    F.avg("gee_quality_flag").alias("avg_quality"),
    F.min("gee_quality_flag").alias("min_quality"),
    F.max("gee_quality_flag").alias("max_quality"),
    F.sum(F.when(F.col("gee_quality_flag") >= 0.9, 1).otherwise(0)).alias("high_quality_count"),
    F.sum(F.when(F.col("gee_quality_flag") < 0.9, 1).otherwise(0)).alias("low_quality_count")
).collect()[0]

print(f"\n📈 Resumen de Calidad:")
print(f"   Total features: {quality_metrics['total_features']:,}")
print(f"   Calidad promedio: {quality_metrics['avg_quality']:.3f}")
print(f"   Calidad mínima: {quality_metrics['min_quality']:.3f}")
print(f"   Calidad máxima: {quality_metrics['max_quality']:.3f}")
print(f"   Alta calidad (≥0.9): {quality_metrics['high_quality_count']:,} ({quality_metrics['high_quality_count']/quality_metrics['total_features']*100:.1f}%)")
print(f"   Baja calidad (<0.9): {quality_metrics['low_quality_count']:,} ({quality_metrics['low_quality_count']/quality_metrics['total_features']*100:.1f}%)")

# Mostrar distribución de escala GEE
print(f"\n📏 Escala de Procesamiento GEE:")
scale_stats = topo_spark_df.select("gee_scale").distinct().collect()
for row in scale_stats:
    count = topo_spark_df.filter(F.col("gee_scale") == row['gee_scale']).count()
    print(f"   {row['gee_scale']} metros: {count:,} píxeles")

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Visualización: Distribución de Pendientes

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convertir muestra a Pandas para visualización
sample_df = topo_spark_df.sample(0.1).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Histograma de pendientes
axes[0].hist(sample_df['slope_degrees'], bins=50, color='steelblue', edgecolor='black', alpha=0.7)
axes[0].axvline(CRITICAL_SLOPE_MIN, color='red', linestyle='--', linewidth=2, label=f'Crítico mín ({CRITICAL_SLOPE_MIN}°)')
axes[0].axvline(CRITICAL_SLOPE_MAX, color='red', linestyle='--', linewidth=2, label=f'Crítico máx ({CRITICAL_SLOPE_MAX}°)')
axes[0].set_xlabel('Pendiente (grados)', fontsize=12)
axes[0].set_ylabel('Frecuencia', fontsize=12)
axes[0].set_title('Distribución de Pendientes (Datos GEE)', fontsize=14, fontweight='bold')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Pendientes por banda altitudinal
band_slopes = sample_df.groupby('elevation_band')['slope_degrees'].mean().sort_values()
axes[1].barh(band_slopes.index, band_slopes.values, color='forestgreen', edgecolor='black')
axes[1].set_xlabel('Pendiente Promedio (grados)', fontsize=12)
axes[1].set_ylabel('Banda Altitudinal', fontsize=12)
axes[1].set_title('Pendiente Promedio por Banda Altitudinal', fontsize=14, fontweight='bold')
axes[1].grid(True, alpha=0.3, axis='x')

plt.tight_layout()
plt.show()

print("✅ Visualizaciones generadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Resumen del Agente Topográfico

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 RESUMEN: AGENTE 1 - PROCESAMIENTO TOPOGRÁFICO GEE v2")
print("=" * 70)

total_pixels = topo_spark_df.count()
critical_pixels = topo_spark_df.filter(F.col("is_critical_slope") == True).count()
critical_percentage = (critical_pixels / total_pixels) * 100

print(f"""
🏔️  ZONA PROCESADA: {PILOT_ZONE['name']}
   • Bounding Box: {PILOT_ZONE['bbox']}
   • Área aprox: {area_km2:.1f} km²

🌍 DATOS DE GOOGLE EARTH ENGINE:
   • Dataset: USGS/SRTMGL1_003 (SRTM 30m)
   • Método autenticación: {GEE_AUTH_METHOD}
   • Escala procesamiento: {gee_scale} metros
   • Procesamiento: NATIVO en GEE (ee.Terrain)
   • Filtro elevación: > 2000 m (zona avalanchas)

📊 DATOS GENERADOS:
   • Total píxeles: {total_pixels:,}
   • Píxeles críticos (30-45°): {critical_pixels:,} ({critical_percentage:.2f}%)
   • Calidad promedio GEE: {quality_metrics['avg_quality']:.3f}

📈 FEATURES CALCULADOS (EN GEE):
   ✅ Elevación (m) - filtrada > 2000m
   ✅ Pendiente (grados) - ee.Terrain.slope()
   ✅ Orientación/Aspect (grados) - ee.Terrain.aspect()
   ✅ Banda altitudinal
   ✅ Flag de pendiente crítica
   ✅ Metadatos GEE (scale, timestamp, quality)

💾 DATOS ALMACENADOS:
   • Tabla: {TABLE_TOPO_FEATURES}
   • Formato: Delta Lake
   • Capa: Silver (Processed Features)
   • Registros: {total_pixels:,}

⚠️  IMPORTANTE:
   • SIN fallbacks a DEM sintético
   • Validación estricta aplicada
   • Solo datos de calidad de GEE

🎯 PRÓXIMO PASO:
   → Ejecutar 02_susceptibility_map_v2.py para análisis de susceptibilidad
""")

print("=" * 70)
print("✅ AGENTE 1 COMPLETADO EXITOSAMENTE")
print("=" * 70)
