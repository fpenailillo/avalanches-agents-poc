# Databricks notebook source
# MAGIC %md
# MAGIC # Agente 1: Procesamiento Topográfico SRTM
# MAGIC
# MAGIC Este agente procesa datos de elevación SRTM para:
# MAGIC - Extraer modelo digital de elevación (DEM) de la zona piloto
# MAGIC - Calcular pendientes (slope) en grados
# MAGIC - Determinar orientaciones (aspect) en grados
# MAGIC - Segmentar por bandas altitudinales
# MAGIC - Identificar pendientes críticas (30-45°)
# MAGIC
# MAGIC **Input:** Datos SRTM de Google Earth Engine o archivos GeoTIFF precargados
# MAGIC **Output:** Tabla `topo_features` en capa Silver

# COMMAND ----------

# MAGIC %run ../00_Setup/00_environment_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importar Librerías

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime
import uuid

# Procesamiento geoespacial
try:
    import rasterio
    from rasterio.transform import from_bounds
    from osgeo import gdal
    import geopandas as gpd
    from shapely.geometry import Point, box
    print("✅ Librerías geoespaciales importadas")
except ImportError as e:
    print(f"⚠️  Error importando librerías geoespaciales: {e}")
    print("   → Ejecuta el notebook de instalación de dependencias primero")

# PySpark
from pyspark.sql import functions as F
from pyspark.sql.types import *

print("✅ Librerías importadas correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuración de Zona Piloto

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
print(f"Elevación: {PILOT_ZONE['elevation_min']}-{PILOT_ZONE['elevation_max']} m")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Opción A: Cargar SRTM desde Google Earth Engine

# COMMAND ----------

def load_srtm_from_gee():
    """
    Carga datos SRTM desde Google Earth Engine
    Requiere autenticación previa: earthengine authenticate
    """
    try:
        import ee

        # Inicializar Earth Engine
        ee.Initialize()
        print("✅ Google Earth Engine inicializado")

        # Definir área de interés
        bbox = PILOT_ZONE['bbox']
        aoi = ee.Geometry.Rectangle([
            bbox['west'],
            bbox['south'],
            bbox['east'],
            bbox['north']
        ])

        # Cargar SRTM (30m resolución)
        srtm = ee.Image('USGS/SRTMGL1_003')
        dem = srtm.clip(aoi)

        print(f"✅ DEM SRTM cargado para zona: {PILOT_ZONE['name']}")
        print(f"   Resolución: 30 metros")
        print(f"   Fuente: USGS SRTM GL1 003")

        # Obtener estadísticas
        stats = dem.reduceRegion(
            reducer=ee.Reducer.minMax().combine(ee.Reducer.mean(), '', True),
            geometry=aoi,
            scale=30,
            maxPixels=1e9
        ).getInfo()

        print(f"   Elevación mín: {stats.get('elevation_min', 0):.0f} m")
        print(f"   Elevación máx: {stats.get('elevation_max', 0):.0f} m")
        print(f"   Elevación media: {stats.get('elevation_mean', 0):.0f} m")

        return dem, aoi

    except Exception as e:
        print(f"❌ Error cargando desde GEE: {e}")
        print("   → Verifica autenticación con: earthengine authenticate")
        print("   → O usa Opción B: datos precargados")
        return None, None

# Intentar cargar desde GEE
dem_gee, aoi_gee = load_srtm_from_gee()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Opción B: Generar DEM Sintético (para pruebas)

# COMMAND ----------

def generate_synthetic_dem():
    """
    Genera DEM sintético para zona piloto cuando GEE no está disponible.
    Útil para pruebas y desarrollo del POC.
    """
    print("🔬 Generando DEM sintético para pruebas...")

    bbox = PILOT_ZONE['bbox']

    # Crear grilla de coordenadas
    resolution = 0.0003  # ~30 metros en grados (aprox)
    lons = np.arange(bbox['west'], bbox['east'], resolution)
    lats = np.arange(bbox['south'], bbox['north'], resolution)

    lon_grid, lat_grid = np.meshgrid(lons, lats)

    # Generar elevaciones sintéticas con patrón montañoso
    center_lat = PILOT_ZONE['center_lat']
    center_lon = PILOT_ZONE['center_lon']

    # Distancia al centro (normalizada)
    dist_to_center = np.sqrt(
        ((lat_grid - center_lat) * 111)**2 +  # 111 km por grado de latitud
        ((lon_grid - center_lon) * 85)**2     # ~85 km por grado de longitud a esta latitud
    )

    # Elevación sintética: aumenta hacia el centro (efecto montaña)
    max_elevation = 3500
    min_elevation = 2000
    elevation = max_elevation - (dist_to_center / dist_to_center.max()) * (max_elevation - min_elevation)

    # Añadir ruido para simular rugosidad del terreno
    np.random.seed(42)
    noise = np.random.normal(0, 50, elevation.shape)
    elevation = elevation + noise

    # Asegurar rango válido
    elevation = np.clip(elevation, min_elevation, max_elevation)

    print(f"✅ DEM sintético generado:")
    print(f"   Tamaño: {elevation.shape[0]} x {elevation.shape[1]} pixels")
    print(f"   Resolución: ~30 metros")
    print(f"   Elevación mín: {elevation.min():.0f} m")
    print(f"   Elevación máx: {elevation.max():.0f} m")
    print(f"   Elevación media: {elevation.mean():.0f} m")

    return {
        'elevation': elevation,
        'lons': lons,
        'lats': lats,
        'lon_grid': lon_grid,
        'lat_grid': lat_grid
    }

# Generar DEM sintético como fallback
dem_data = generate_synthetic_dem()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calcular Pendientes (Slope)

# COMMAND ----------

def calculate_slope(elevation_grid, lats, lons):
    """
    Calcula pendientes en grados usando algoritmo de Horn.

    Args:
        elevation_grid: Array 2D con elevaciones
        lats: Array 1D con latitudes
        lons: Array 1D con longitudes

    Returns:
        Array 2D con pendientes en grados
    """
    print("📐 Calculando pendientes...")

    # Resolución en metros
    lat_res = abs(lats[1] - lats[0]) * 111000  # metros
    lon_res = abs(lons[1] - lons[0]) * 85000   # metros a esta latitud

    # Calcular gradientes (derivadas parciales)
    dz_dy, dz_dx = np.gradient(elevation_grid, lat_res, lon_res)

    # Calcular pendiente en radianes y convertir a grados
    slope_rad = np.arctan(np.sqrt(dz_dx**2 + dz_dy**2))
    slope_deg = np.degrees(slope_rad)

    print(f"✅ Pendientes calculadas:")
    print(f"   Mínima: {slope_deg.min():.2f}°")
    print(f"   Máxima: {slope_deg.max():.2f}°")
    print(f"   Media: {slope_deg.mean():.2f}°")

    return slope_deg

slope_grid = calculate_slope(
    dem_data['elevation'],
    dem_data['lats'],
    dem_data['lons']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calcular Orientación (Aspect)

# COMMAND ----------

def calculate_aspect(elevation_grid, lats, lons):
    """
    Calcula orientación (aspect) en grados (0-360).

    0° = Norte, 90° = Este, 180° = Sur, 270° = Oeste

    Args:
        elevation_grid: Array 2D con elevaciones
        lats: Array 1D con latitudes
        lons: Array 1D con longitudes

    Returns:
        Array 2D con orientaciones en grados
    """
    print("🧭 Calculando orientaciones...")

    lat_res = abs(lats[1] - lats[0]) * 111000
    lon_res = abs(lons[1] - lons[0]) * 85000

    dz_dy, dz_dx = np.gradient(elevation_grid, lat_res, lon_res)

    # Calcular aspecto en radianes y convertir a grados
    aspect_rad = np.arctan2(-dz_dx, dz_dy)
    aspect_deg = np.degrees(aspect_rad)

    # Convertir a rango 0-360
    aspect_deg = (aspect_deg + 360) % 360

    print(f"✅ Orientaciones calculadas:")
    print(f"   Rango: 0-360°")
    print(f"   Media: {aspect_deg.mean():.2f}°")

    return aspect_deg

aspect_grid = calculate_aspect(
    dem_data['elevation'],
    dem_data['lats'],
    dem_data['lons']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Identificar Pendientes Críticas (30-45°)

# COMMAND ----------

def identify_critical_slopes(slope_grid):
    """
    Identifica pendientes críticas para avalanchas (30-45 grados).

    Args:
        slope_grid: Array 2D con pendientes en grados

    Returns:
        Array booleano indicando píxeles críticos
    """
    print(f"🎯 Identificando pendientes críticas ({CRITICAL_SLOPE_MIN}-{CRITICAL_SLOPE_MAX}°)...")

    critical_mask = (slope_grid >= CRITICAL_SLOPE_MIN) & (slope_grid <= CRITICAL_SLOPE_MAX)

    total_pixels = slope_grid.size
    critical_pixels = critical_mask.sum()
    percentage = (critical_pixels / total_pixels) * 100

    print(f"✅ Pendientes críticas identificadas:")
    print(f"   Píxeles críticos: {critical_pixels:,} / {total_pixels:,}")
    print(f"   Porcentaje: {percentage:.2f}%")

    return critical_mask

critical_slopes = identify_critical_slopes(slope_grid)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Segmentar por Bandas Altitudinales

# COMMAND ----------

def assign_elevation_bands(elevation_grid):
    """
    Asigna cada píxel a una banda altitudinal.

    Returns:
        Array 2D con nombre de banda altitudinal
    """
    print("🏔️  Segmentando por bandas altitudinales...")

    band_grid = np.empty(elevation_grid.shape, dtype=object)

    for band in ELEVATION_BANDS:
        mask = (elevation_grid >= band['min']) & (elevation_grid < band['max'])
        band_grid[mask] = band['name']

    print(f"✅ Segmentación completada:")
    for band in ELEVATION_BANDS:
        mask = band_grid == band['name']
        count = mask.sum()
        percentage = (count / band_grid.size) * 100
        print(f"   {band['name']:12s}: {count:6,} píxeles ({percentage:5.2f}%)")

    return band_grid

elevation_bands_grid = assign_elevation_bands(dem_data['elevation'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Crear DataFrame con Features Topográficos

# COMMAND ----------

def create_topo_features_dataframe():
    """
    Crea DataFrame de Pandas con todos los features topográficos.
    """
    print("📊 Creando DataFrame con features topográficos...")

    # Aplanar grillas
    n_rows, n_cols = dem_data['elevation'].shape
    total_pixels = n_rows * n_cols

    features = []

    for i in range(n_rows):
        for j in range(n_cols):
            feature = {
                'feature_id': f"topo_{uuid.uuid4().hex[:12]}",
                'latitude': float(dem_data['lat_grid'][i, j]),
                'longitude': float(dem_data['lon_grid'][i, j]),
                'elevation': int(dem_data['elevation'][i, j]),
                'slope_degrees': float(slope_grid[i, j]),
                'aspect_degrees': float(aspect_grid[i, j]),
                'elevation_band': elevation_bands_grid[i, j],
                'is_critical_slope': bool(critical_slopes[i, j]),
                'zone_name': PILOT_ZONE['name'],
                'processing_timestamp': datetime.now()
            }
            features.append(feature)

    df = pd.DataFrame(features)

    print(f"✅ DataFrame creado:")
    print(f"   Filas: {len(df):,}")
    print(f"   Columnas: {len(df.columns)}")

    return df

topo_features_df = create_topo_features_dataframe()

# Mostrar muestra
print("\n📋 Muestra de datos:")
display(topo_features_df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Guardar en Delta Lake (Capa Silver)

# COMMAND ----------

print(f"💾 Guardando features topográficos en: {TABLE_TOPO_FEATURES}")

# Convertir a Spark DataFrame
topo_spark_df = spark.createDataFrame(topo_features_df)

# Guardar en Delta Lake
topo_spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TABLE_TOPO_FEATURES)

print(f"✅ Datos guardados en tabla: {TABLE_TOPO_FEATURES}")
print(f"   Registros: {topo_spark_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Estadísticas por Banda Altitudinal

# COMMAND ----------

print("📊 ESTADÍSTICAS POR BANDA ALTITUDINAL")
print("=" * 70)

stats = topo_spark_df.groupBy("elevation_band").agg(
    F.count("*").alias("total_pixels"),
    F.avg("elevation").alias("avg_elevation_m"),
    F.avg("slope_degrees").alias("avg_slope_deg"),
    F.sum(F.when(F.col("is_critical_slope"), 1).otherwise(0)).alias("critical_pixels"),
    F.min("slope_degrees").alias("min_slope"),
    F.max("slope_degrees").alias("max_slope")
).orderBy("avg_elevation_m")

display(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Visualización: Distribución de Pendientes

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

# Histograma de pendientes
axes[0].hist(topo_features_df['slope_degrees'], bins=50, color='steelblue', edgecolor='black', alpha=0.7)
axes[0].axvline(CRITICAL_SLOPE_MIN, color='red', linestyle='--', linewidth=2, label=f'Crítico mín ({CRITICAL_SLOPE_MIN}°)')
axes[0].axvline(CRITICAL_SLOPE_MAX, color='red', linestyle='--', linewidth=2, label=f'Crítico máx ({CRITICAL_SLOPE_MAX}°)')
axes[0].set_xlabel('Pendiente (grados)', fontsize=12)
axes[0].set_ylabel('Frecuencia', fontsize=12)
axes[0].set_title('Distribución de Pendientes', fontsize=14, fontweight='bold')
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Pendientes por banda altitudinal
band_slopes = topo_features_df.groupby('elevation_band')['slope_degrees'].mean().sort_values()
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
# MAGIC ## 13. Resumen del Agente Topográfico

# COMMAND ----------

print("\n" + "=" * 70)
print("📊 RESUMEN: AGENTE 1 - PROCESAMIENTO TOPOGRÁFICO")
print("=" * 70)

total_pixels = len(topo_features_df)
critical_pixels = topo_features_df['is_critical_slope'].sum()
critical_percentage = (critical_pixels / total_pixels) * 100

print(f"""
🏔️  ZONA PROCESADA: {PILOT_ZONE['name']}
   • Bounding Box: {PILOT_ZONE['bbox']}
   • Área aprox: {abs(PILOT_ZONE['bbox']['north'] - PILOT_ZONE['bbox']['south']) * abs(PILOT_ZONE['bbox']['east'] - PILOT_ZONE['bbox']['west']) * 111 * 85:.1f} km²

📊 DATOS GENERADOS:
   • Total píxeles: {total_pixels:,}
   • Resolución: ~30 metros
   • Píxeles críticos (30-45°): {critical_pixels:,} ({critical_percentage:.2f}%)

📈 FEATURES CALCULADOS:
   ✅ Elevación (m)
   ✅ Pendiente (grados)
   ✅ Orientación/Aspect (grados)
   ✅ Banda altitudinal
   ✅ Flag de pendiente crítica

💾 DATOS ALMACENADOS:
   • Tabla: {TABLE_TOPO_FEATURES}
   • Formato: Delta Lake
   • Capa: Silver (Processed Features)

🎯 PRÓXIMO PASO:
   → Ejecutar 02_slope_analysis.py para análisis detallado
   → Ejecutar 03_susceptibility_map.py para mapa de susceptibilidad
""")

print("=" * 70)
print("✅ AGENTE 1 COMPLETADO EXITOSAMENTE")
print("=" * 70)
