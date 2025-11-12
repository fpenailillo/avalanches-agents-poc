# 🚀 Instrucciones Rápidas de Setup - GEE

Tienes credenciales de Google Earth Engine listas. Aquí está la forma más rápida de configurarlas:

---

## ⚡ Método Rápido (5 minutos)

### Paso 1: Abrir Databricks

1. Ve a tu workspace de Databricks
2. Importa el notebook: `databricks_poc/01_Agent_Topografico/setup_gee_credentials.py`

### Paso 2: Pegar Credenciales

En el notebook `setup_gee_credentials.py`, en la **Celda 4** (CMD 4), reemplaza el contenido del diccionario `gee_credentials` con:

```python
# IMPORTANTE: Reemplaza este diccionario con tus credenciales reales
# Las credenciales están en el mensaje del usuario o en un archivo seguro

gee_credentials = {
  "type": "service_account",
  "project_id": "calculo-pendiente",
  "private_key_id": "TU_PRIVATE_KEY_ID_AQUI",
  "private_key": "-----BEGIN PRIVATE KEY-----\nTU_PRIVATE_KEY_COMPLETA_AQUI\n-----END PRIVATE KEY-----\n",
  "client_email": "databricks@calculo-pendiente.iam.gserviceaccount.com",
  "client_id": "TU_CLIENT_ID_AQUI",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/databricks%40calculo-pendiente.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# NOTA: Las credenciales reales fueron compartidas por el usuario.
# Revisa el chat/mensaje original para obtener los valores correctos.
```

### Paso 3: Ejecutar Notebook

1. Ejecuta **todas las celdas** del notebook `setup_gee_credentials.py`
2. Verifica que todos los tests pasen (✅ en cada paso)
3. **BORRA el notebook después de ejecutarlo** (o muévelo a una carpeta privada)

### Paso 4: Probar los Notebooks Principales

1. Abre y ejecuta: `01_srtm_processing_v2.py`
2. Debería autenticar automáticamente con GEE
3. Verificar que se generen datos en la tabla `topo_features`

---

## 🔒 Seguridad

### ✅ Las credenciales estarán seguras porque:

1. Se guardan en **Unity Catalog Volume** (protegido por permisos)
2. No se commitean a Git (protegido por `.gitignore`)
3. Solo usuarios autorizados del workspace pueden acceder

### ⚠️ Importante:

- **Borra el notebook `setup_gee_credentials.py`** después de ejecutarlo
- **NO compartas** el workspace con usuarios no autorizados
- **NO copies** las credenciales a otros lugares

---

## 📊 Información del Service Account

- **Proyecto GEE**: `calculo-pendiente`
- **Service Account**: `databricks@calculo-pendiente.iam.gserviceaccount.com`
- **Ubicación de credenciales**: `/Volumes/workspace/avalanches_agents/raw_files/gee-service-account.json`

---

## 🔧 Si algo falla

### Error: "Volume no existe"

1. Ve a Databricks Data Explorer
2. Navega a: `workspace.avalanches_agents`
3. Crea un Volume llamado: `raw_files`
4. Vuelve a ejecutar el notebook de setup

### Error: "Service account sin permisos"

1. Ve a Google Cloud Console: https://console.cloud.google.com
2. IAM & Admin > Service Accounts
3. Selecciona: `databricks@calculo-pendiente.iam.gserviceaccount.com`
4. Verifica que tiene rol: "Earth Engine Resource Writer"

### Error: "No se puede autenticar"

1. Verifica que el JSON está completo (incluyendo el `private_key`)
2. Verifica que no hay errores de formato en el JSON
3. Intenta regenerar la clave en Google Cloud Console

---

## 📞 Soporte

Para más detalles, consulta:
- `README_GEE_CONFIG.md` - Guía completa de configuración
- `01_srtm_processing_v2.py` - Notebook principal con procesamiento GEE

---

**Última actualización**: 2025-11-12
