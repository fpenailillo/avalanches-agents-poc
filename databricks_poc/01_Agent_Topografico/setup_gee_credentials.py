# Databricks notebook source
# MAGIC %md
# MAGIC # Setup de Credenciales GEE - EJECUTAR UNA SOLA VEZ
# MAGIC
# MAGIC **⚠️ IMPORTANTE: Este notebook contiene credenciales sensibles**
# MAGIC
# MAGIC **Instrucciones:**
# MAGIC 1. Pega tus credenciales GEE en la celda 3
# MAGIC 2. Ejecuta el notebook COMPLETO
# MAGIC 3. Verifica que las credenciales funcionan
# MAGIC 4. **BORRA O ARCHIVA este notebook** (no lo dejes en producción)
# MAGIC
# MAGIC **Método de almacenamiento:** Unity Catalog Volume
# MAGIC **Ubicación:** `/Volumes/workspace/avalanches_agents/raw_files/gee-service-account.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importar Librerías

# COMMAND ----------

import json
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurar Ruta de Destino

# COMMAND ----------

# Configuración del volume
CATALOG = "workspace"
SCHEMA = "avalanches_agents"
VOLUME = "raw_files"

# Ruta completa
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
CREDENTIALS_FILE = "gee-service-account.json"
FULL_PATH = f"{VOLUME_PATH}/{CREDENTIALS_FILE}"

print("📁 CONFIGURACIÓN DE ALMACENAMIENTO")
print("=" * 70)
print(f"   Catalog: {CATALOG}")
print(f"   Schema: {SCHEMA}")
print(f"   Volume: {VOLUME}")
print(f"   Archivo: {CREDENTIALS_FILE}")
print(f"   Ruta completa: {FULL_PATH}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ⚠️ PEGAR CREDENCIALES AQUÍ ⚠️
# MAGIC
# MAGIC **IMPORTANTE**: Después de ejecutar este notebook, BÓRRALO o ARCHÍVALO

# COMMAND ----------

# ⚠️ PEGA AQUÍ TU JSON DEL SERVICE ACCOUNT
# Reemplaza todo el contenido del diccionario gee_credentials con tu JSON

gee_credentials = {
    "type": "service_account",
    "project_id": "TU_PROJECT_ID",
    "private_key_id": "TU_PRIVATE_KEY_ID",
    "private_key": "-----BEGIN PRIVATE KEY-----\nTU_PRIVATE_KEY_AQUI\n-----END PRIVATE KEY-----\n",
    "client_email": "TU_SERVICE_ACCOUNT_EMAIL@TU_PROJECT.iam.gserviceaccount.com",
    "client_id": "TU_CLIENT_ID",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/TU_SERVICE_ACCOUNT_EMAIL",
    "universe_domain": "googleapis.com"
}

# Validar que se modificó el diccionario
if gee_credentials["project_id"] == "TU_PROJECT_ID":
    raise Exception("❌ ERROR: Debes reemplazar las credenciales con tus valores reales")

print("✅ Credenciales cargadas")
print(f"   Proyecto: {gee_credentials['project_id']}")
print(f"   Service Account: {gee_credentials['client_email']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar que el Volume existe

# COMMAND ----------

try:
    # Intentar listar el volumen
    files = dbutils.fs.ls(VOLUME_PATH)
    print(f"✅ Volume existe: {VOLUME_PATH}")
    print(f"   Archivos actuales: {len(files)}")
except Exception as e:
    print(f"⚠️  Volume no existe, creándolo...")
    try:
        dbutils.fs.mkdirs(VOLUME_PATH)
        print(f"✅ Volume creado: {VOLUME_PATH}")
    except Exception as e2:
        print(f"❌ ERROR creando volume: {e2}")
        print(f"\n🔧 Solución manual:")
        print(f"   1. Ve a Data Explorer en Databricks")
        print(f"   2. Navega a: {CATALOG}.{SCHEMA}")
        print(f"   3. Crea un Volume llamado: {VOLUME}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar Credenciales en el Volume

# COMMAND ----------

print("💾 GUARDANDO CREDENCIALES...")
print("=" * 70)

try:
    # Convertir ruta de Volume a ruta de filesystem
    fs_path = FULL_PATH.replace('/Volumes/', '/dbfs/Volumes/')

    # Guardar JSON
    with open(fs_path, 'w') as f:
        json.dump(gee_credentials, f, indent=2)

    print(f"✅ Credenciales guardadas exitosamente")
    print(f"   Ubicación: {FULL_PATH}")

    # Verificar que se guardó
    file_exists = os.path.exists(fs_path)
    file_size = os.path.getsize(fs_path) if file_exists else 0

    print(f"   Tamaño: {file_size} bytes")

    if file_size < 100:
        raise Exception("❌ ERROR: Archivo muy pequeño, posiblemente corrupto")

    print("=" * 70)

except Exception as e:
    print(f"❌ ERROR al guardar: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificar que se puede leer

# COMMAND ----------

print("🔍 VERIFICANDO LECTURA...")
print("=" * 70)

try:
    fs_path = FULL_PATH.replace('/Volumes/', '/dbfs/Volumes/')

    with open(fs_path, 'r') as f:
        loaded_credentials = json.load(f)

    print(f"✅ Archivo leído exitosamente")
    print(f"   Proyecto: {loaded_credentials['project_id']}")
    print(f"   Service Account: {loaded_credentials['client_email']}")
    print(f"   Campos presentes: {len(loaded_credentials)}")

    # Verificar campos críticos
    required_fields = ['type', 'project_id', 'private_key', 'client_email']
    missing_fields = [f for f in required_fields if f not in loaded_credentials]

    if missing_fields:
        raise Exception(f"❌ Faltan campos críticos: {missing_fields}")

    print(f"✅ Todos los campos críticos presentes")
    print("=" * 70)

except Exception as e:
    print(f"❌ ERROR al leer: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Test de Autenticación GEE

# COMMAND ----------

print("🔐 TESTEANDO AUTENTICACIÓN CON GOOGLE EARTH ENGINE...")
print("=" * 70)

try:
    import ee

    # Leer credenciales del archivo guardado
    fs_path = FULL_PATH.replace('/Volumes/', '/dbfs/Volumes/')

    with open(fs_path, 'r') as f:
        service_account_info = json.load(f)

    # Autenticar
    credentials = ee.ServiceAccountCredentials(
        service_account_info['client_email'],
        fs_path
    )
    ee.Initialize(credentials)

    print(f"✅ Autenticación exitosa")
    print(f"   Service Account: {service_account_info['client_email']}")
    print(f"   Proyecto: {service_account_info['project_id']}")

except Exception as e:
    print(f"❌ ERROR en autenticación: {e}")
    print(f"\n🔧 Posibles causas:")
    print(f"   1. El service account no tiene permisos en GEE")
    print(f"   2. El proyecto GEE no está habilitado")
    print(f"   3. La clave privada es incorrecta")
    print(f"\n📋 Verifica en Google Cloud Console:")
    print(f"   https://console.cloud.google.com/iam-admin/serviceaccounts")
    raise

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Test de Acceso a SRTM

# COMMAND ----------

print("🌍 TESTEANDO ACCESO A DATOS SRTM...")
print("=" * 70)

try:
    # Cargar dataset SRTM
    srtm = ee.Image('USGS/SRTMGL1_003')

    # Obtener información básica
    info = srtm.getInfo()

    print(f"✅ Acceso a SRTM exitoso")
    print(f"   Dataset ID: {info['id']}")
    print(f"   Tipo: {info['type']}")
    print(f"   Bandas: {info['bands'][0]['id']}")

    # Test de extracción en zona piloto
    bbox = ee.Geometry.Rectangle([-70.32, -33.40, -70.22, -33.30])  # La Parva
    dem_clip = srtm.clip(bbox)

    # Obtener estadísticas
    stats = dem_clip.reduceRegion(
        reducer=ee.Reducer.minMax().combine(ee.Reducer.mean(), '', True),
        geometry=bbox,
        scale=30,
        maxPixels=1e9
    ).getInfo()

    print(f"\n📊 Test en zona piloto (La Parva):")
    print(f"   Elevación mín: {stats.get('elevation_min', 0):.0f} m")
    print(f"   Elevación máx: {stats.get('elevation_max', 0):.0f} m")
    print(f"   Elevación media: {stats.get('elevation_mean', 0):.0f} m")

    print(f"\n✅ TODO FUNCIONAL - Puedes ejecutar los notebooks principales")

except Exception as e:
    print(f"❌ ERROR accediendo a SRTM: {e}")
    print(f"\n🔧 Verifica que el service account tiene acceso al dataset SRTM")
    raise

print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumen Final

# COMMAND ----------

print("\n" + "=" * 70)
print("✅ CONFIGURACIÓN DE CREDENCIALES GEE COMPLETADA")
print("=" * 70)

print(f"""
📁 UBICACIÓN DE CREDENCIALES:
   {FULL_PATH}

🔐 AUTENTICACIÓN:
   ✅ Service Account configurado
   ✅ Conexión GEE verificada
   ✅ Acceso a SRTM confirmado

📊 PROYECTO GEE:
   Proyecto: {gee_credentials['project_id']}
   Service Account: {gee_credentials['client_email']}

🎯 PRÓXIMOS PASOS:
   1. Ejecuta: 01_srtm_processing_v2.py
   2. Verifica que se generen datos en topo_features
   3. Ejecuta: 02_susceptibility_map_v2.py

⚠️  IMPORTANTE - SEGURIDAD:
   1. BORRA este notebook o muévelo a una carpeta privada
   2. NO lo commitees a Git
   3. NO lo compartas con otros usuarios
   4. Las credenciales están seguras en el Volume protegido

🔒 CÓMO ACCEDER A LAS CREDENCIALES:
   Los notebooks principales (01_srtm_processing_v2.py) están
   configurados para leer automáticamente desde:
   {FULL_PATH}

📞 SOPORTE:
   Si necesitas regenerar credenciales:
   1. Ve a Google Cloud Console
   2. IAM & Admin > Service Accounts
   3. Selecciona: {gee_credentials['client_email']}
   4. Crea nueva clave (JSON)
   5. Vuelve a ejecutar este notebook con la nueva clave
""")

print("=" * 70)
print("✅ SETUP COMPLETADO")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ RECORDATORIO FINAL
# MAGIC
# MAGIC **Después de ejecutar este notebook:**
# MAGIC
# MAGIC 1. ✅ Verifica que el test de GEE pasó exitosamente
# MAGIC 2. ✅ Ejecuta los notebooks principales para confirmar que funcionan
# MAGIC 3. ⚠️ **BORRA o ARCHIVA este notebook**
# MAGIC 4. ⚠️ **NO lo commitees a Git**
# MAGIC 5. ⚠️ **NO lo compartas con otros usuarios**
# MAGIC
# MAGIC Las credenciales están seguras en el Volume de Unity Catalog y solo
# MAGIC usuarios con permisos apropiados pueden acceder a ellas.
