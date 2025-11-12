# Configuración de Google Earth Engine para Databricks

Este documento explica cómo configurar el **service account de Google Earth Engine** en Databricks de forma segura para que el Agente Topográfico pueda acceder a datos SRTM.

## 📋 Requisitos Previos

- ✅ Service Account de GEE creado (JSON proporcionado)
- ✅ Proyecto GEE: `calculo-pendiente`
- ✅ Email del service account: `databricks@calculo-pendiente.iam.gserviceaccount.com`
- ✅ Permisos necesarios en el proyecto GEE

---

## 🔐 OPCIÓN 1: Databricks Secrets (Recomendado)

### Paso 1: Instalar Databricks CLI

```bash
pip install databricks-cli
```

### Paso 2: Configurar Databricks CLI

```bash
databricks configure --token
```

Te pedirá:
- **Databricks Host**: URL de tu workspace (ej: `https://dbc-xxxxx.cloud.databricks.com`)
- **Token**: Tu personal access token de Databricks

### Paso 3: Crear Scope de Secrets

```bash
databricks secrets create-scope --scope gee-secrets
```

### Paso 4: Almacenar el JSON del Service Account

**IMPORTANTE**: Guarda el JSON completo del service account en un archivo temporal llamado `gee-service-account.json`

```bash
# En tu máquina local, crea el archivo:
nano gee-service-account.json

# Pega el JSON completo del service account y guarda
```

### Paso 5: Subir el Secret a Databricks

```bash
databricks secrets put --scope gee-secrets --key service-account-json --string-value "$(cat gee-service-account.json)"
```

### Paso 6: Verificar que se guardó

```bash
databricks secrets list --scope gee-secrets
```

Deberías ver:
```
Key (Scope: gee-secrets)
------------------------
service-account-json
```

### Paso 7: Eliminar el archivo temporal

```bash
# MUY IMPORTANTE: Elimina el archivo temporal
rm gee-service-account.json
```

### Paso 8: Verificar en el Notebook

El notebook `01_srtm_processing_v2.py` ya está configurado para leer desde Databricks Secrets:

```python
# Esto ya está implementado en el notebook
service_account_json_str = dbutils.secrets.get(scope="gee-secrets", key="service-account-json")
service_account_info = json.loads(service_account_json_str)

credentials = ee.ServiceAccountCredentials(
    service_account_info['client_email'],
    key_data=service_account_json_str
)
ee.Initialize(credentials)
```

---

## 🗄️ OPCIÓN 2: Unity Catalog Volume

Si prefieres usar un archivo en lugar de secrets:

### Paso 1: Crear el archivo JSON

En Databricks, crea un notebook temporal:

```python
# EJECUTA ESTO UNA SOLA VEZ Y LUEGO BORRA EL NOTEBOOK

import json

# Pega aquí tu JSON del service account
gee_credentials = {
    "type": "service_account",
    "project_id": "calculo-pendiente",
    "private_key_id": "TU_PRIVATE_KEY_ID",
    "private_key": "-----BEGIN PRIVATE KEY-----\nTU_PRIVATE_KEY\n-----END PRIVATE KEY-----\n",
    "client_email": "databricks@calculo-pendiente.iam.gserviceaccount.com",
    "client_id": "TU_CLIENT_ID",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/databricks%40calculo-pendiente.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

# Guardar en Volume
volume_path = "/Volumes/workspace/avalanches_agents/raw_files"
json_path = f"{volume_path}/gee-service-account.json"

# Asegurarse que el directorio existe
dbutils.fs.mkdirs(volume_path)

# Guardar archivo
with open(json_path.replace('/Volumes/', '/dbfs/Volumes/'), 'w') as f:
    json.dump(gee_credentials, f, indent=2)

print(f"✅ Credenciales guardadas en: {json_path}")
print("⚠️  IMPORTANTE: BORRA ESTE NOTEBOOK DESPUÉS DE EJECUTARLO")
```

### Paso 2: Verificar que se creó

```python
# En otro notebook
volume_path = "/Volumes/workspace/avalanches_agents/raw_files"
json_path = f"{volume_path}/gee-service-account.json"

# Verificar que existe
display(dbutils.fs.ls(volume_path))
```

### Paso 3: Proteger el archivo

```python
# Opcional: Restringir permisos del volume
# Solo administradores y el service principal pueden leer
```

---

## 🧪 OPCIÓN 3: Autenticación Interactiva (Solo desarrollo)

Si estás en desarrollo local o en un cluster interactivo:

### Paso 1: Ejecutar en terminal del cluster

```bash
earthengine authenticate
```

### Paso 2: Seguir instrucciones

1. Se abrirá un navegador
2. Inicia sesión con tu cuenta Google que tiene acceso al proyecto GEE
3. Copia el código de autorización
4. Pégalo en la terminal

### Paso 3: Verificar

```python
import ee
ee.Initialize()  # Debería funcionar sin errores
```

---

## ✅ Verificar Configuración

Ejecuta este código en un notebook para verificar que todo funciona:

```python
# Databricks notebook source

import ee
import json

print("🔍 VERIFICANDO CONFIGURACIÓN DE GOOGLE EARTH ENGINE")
print("=" * 70)

# Test 1: Intentar autenticar con Secrets
try:
    print("\n1️⃣  Probando autenticación con Databricks Secrets...")
    service_account_json_str = dbutils.secrets.get(scope="gee-secrets", key="service-account-json")
    service_account_info = json.loads(service_account_json_str)

    credentials = ee.ServiceAccountCredentials(
        service_account_info['client_email'],
        key_data=service_account_json_str
    )
    ee.Initialize(credentials)

    print(f"   ✅ Autenticado con: {service_account_info['client_email']}")
    print(f"   ✅ Proyecto: {service_account_info['project_id']}")

except Exception as e:
    print(f"   ⚠️  No disponible: {e}")

    # Test 2: Intentar con archivo en Volume
    try:
        print("\n2️⃣  Probando autenticación con archivo en Volume...")
        json_path = "/Volumes/workspace/avalanches_agents/raw_files/gee-service-account.json"

        with open(json_path.replace('/Volumes/', '/dbfs/Volumes/'), 'r') as f:
            service_account_info = json.load(f)

        credentials = ee.ServiceAccountCredentials(
            service_account_info['client_email'],
            json_path.replace('/Volumes/', '/dbfs/Volumes/')
        )
        ee.Initialize(credentials)

        print(f"   ✅ Autenticado con: {service_account_info['client_email']}")
        print(f"   ✅ Proyecto: {service_account_info['project_id']}")

    except Exception as e2:
        print(f"   ⚠️  No disponible: {e2}")

        # Test 3: Autenticación interactiva
        try:
            print("\n3️⃣  Probando autenticación interactiva...")
            ee.Initialize()
            print(f"   ✅ Autenticado con credenciales interactivas")
        except Exception as e3:
            print(f"   ❌ Falló: {e3}")
            raise Exception("❌ No se pudo autenticar con ningún método")

# Test de conexión
print("\n🔬 TESTEANDO CONEXIÓN GEE...")
try:
    srtm = ee.Image('USGS/SRTMGL1_003')
    info = srtm.getInfo()
    print(f"   ✅ Conexión GEE exitosa")
    print(f"   ✅ Dataset SRTM accesible")
    print(f"   ✅ ID: {info['id']}")
except Exception as e:
    print(f"   ❌ Error: {e}")
    raise

print("\n" + "=" * 70)
print("✅ CONFIGURACIÓN COMPLETA Y FUNCIONAL")
print("   → Puedes ejecutar 01_srtm_processing_v2.py")
print("=" * 70)
```

---

## 🔒 Mejores Prácticas de Seguridad

### ✅ HACER:

1. **Usar Databricks Secrets** para credenciales (método recomendado)
2. **Restringir permisos** del scope de secrets a usuarios autorizados
3. **Rotar credenciales** periódicamente (cada 90 días)
4. **Eliminar archivos temporales** con credenciales
5. **Usar service accounts** específicos para cada entorno (dev, prod)
6. **Auditar accesos** a secrets regularmente

### ❌ NUNCA HACER:

1. ❌ Commitear credenciales al repositorio Git
2. ❌ Hardcodear credenciales en notebooks
3. ❌ Compartir credenciales por email/chat sin cifrar
4. ❌ Dejar archivos JSON con credenciales en notebooks de producción
5. ❌ Usar credenciales de producción en desarrollo
6. ❌ Compartir el mismo service account entre múltiples proyectos

---

## 🚨 Si las credenciales se comprometen

Si accidentalmente expusiste las credenciales:

1. **Inmediatamente revoca** el service account en Google Cloud Console
2. **Genera nuevas credenciales**
3. **Actualiza** los secrets en Databricks
4. **Revisa logs** de acceso para actividad sospechosa
5. **Notifica** al equipo de seguridad

---

## 📞 Soporte

**Proyecto GEE**: `calculo-pendiente`
**Service Account**: `databricks@calculo-pendiente.iam.gserviceaccount.com`

Para problemas con:
- **Autenticación GEE**: Verifica permisos del service account en Google Cloud Console
- **Databricks Secrets**: Contacta administrador del workspace
- **Errores del notebook**: Ver logs en `01_srtm_processing_v2.py`

---

## 📚 Referencias

- [Google Earth Engine Service Accounts](https://developers.google.com/earth-engine/guides/service_account)
- [Databricks Secrets Management](https://docs.databricks.com/security/secrets/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)

---

**Última actualización**: 2025-11-12
**Versión**: 1.0
