# Guía de Inicio Rápido - POC Avalanche Prediction

## ⚡ Ejecución en 3 Pasos

### 📋 Pre-requisitos

Asegúrate de tener:
- ✅ Acceso a Databricks (Community Edition o superior)
- ✅ Workspace con Unity Catalog habilitado
- ✅ El código subido a `/Workspace/Users/tu-email/AvalanchePOC/`

---

## 🚀 Paso 1: Setup Inicial (SOLO UNA VEZ)

### 1.1 Instalar Dependencias

```python
# Ejecutar este notebook (tarda ~5 minutos)
%run 00_Setup/01_install_dependencies.py
```

**Resultado esperado:**
```
✅ TODAS LAS DEPENDENCIAS INSTALADAS CORRECTAMENTE
   → El entorno está listo para ejecutar el POC
```

⚠️ **IMPORTANTE:** El notebook reiniciará el kernel automáticamente.

---

### 1.2 Crear Unity Catalog

```python
# Ejecutar este notebook (tarda ~30 segundos)
%run 00_Setup/02_create_unity_catalog.py
```

**Resultado esperado:**
```
✅ UNITY CATALOG CONFIGURADO EXITOSAMENTE
   → El sistema está listo para ingesta de datos

📊 ESTRUCTURA DE DATOS (Medallion Architecture):

🥉 BRONZE LAYER (Raw Data):
   • ski_resort_locations
   • weather_daily
   • weather_hourly
   • srtm_raw

🥈 SILVER LAYER (Processed Features):
   • topo_features
   • topo_susceptibility
   • nlp_risk_patterns
   • weather_features
   • weather_triggers

🥇 GOLD LAYER (Analytics & Predictions):
   • avalanche_predictions
   • avalanche_bulletins
   • susceptibility_map
```

---

## 🎯 Paso 2: Ejecutar Pipeline Completo

```python
# Ejecutar el orquestador (tarda ~5-10 minutos)
%run 05_Pipeline/01_orchestrator.py
```

**Este notebook:**
1. ✅ Verifica que el schema existe
2. ✅ Ejecuta Agente 1 (Topográfico)
3. ✅ Ejecuta Agente 2 (NLP)
4. ✅ Ejecuta Agente 3 (Meteorológico)
5. ✅ Ejecuta Agente 4 (Integrador)
6. ✅ Genera boletín EAWS

**Resultado esperado:**
```
🎉 PIPELINE COMPLETADO EXITOSAMENTE
   Zona: La Parva
   Timestamp: 2025-01-09T...

📊 Datos disponibles en Unity Catalog:
   • Topografía: workspace.avalanches_agents.topo_susceptibility
   • NLP: workspace.avalanches_agents.nlp_risk_patterns
   • Meteorología: workspace.avalanches_agents.weather_triggers
   • Predicciones: workspace.avalanches_agents.avalanche_predictions
   • Boletín: workspace.avalanches_agents.avalanche_bulletins

🎯 Sistema multi-agente funcionando end-to-end
```

---

## 📊 Paso 3: Consultar Resultados

### Ver Última Predicción

```python
from pyspark.sql import functions as F

predictions = spark.table("workspace.avalanches_agents.avalanche_predictions")
latest = predictions.orderBy(F.desc("forecast_date")).limit(1)
display(latest)
```

### Ver Último Boletín

```python
bulletins = spark.table("workspace.avalanches_agents.avalanche_bulletins")
latest_bulletin = bulletins.orderBy(F.desc("issue_date")).limit(1)

# Mostrar metadata
display(latest_bulletin.select(
    "boletin_id",
    "zone_name",
    "issue_date",
    "eaws_level",
    "eaws_label",
    "summary"
))

# Mostrar texto completo del boletín
print(latest_bulletin.first()["bulletin_text"])
```

### Ver Triggers Meteorológicos

```python
triggers = spark.table("workspace.avalanches_agents.weather_triggers")

# Filtrar solo triggers críticos
critical = triggers.filter(F.col("is_critical") == True)
display(critical.orderBy(F.desc("date")))
```

---

## 🔧 Ejecución Individual de Agentes

Si quieres ejecutar agentes por separado para debugging:

### Agente 1: Topográfico

```python
%run 01_Agent_Topografico/01_srtm_processing.py
%run 01_Agent_Topografico/02_susceptibility_map.py
```

### Agente 2: NLP

```python
%run 02_Agent_NLP/01_extract_risk_patterns.py
```

### Agente 3: Meteorológico

```python
%run 03_Agent_Meteorologico/01_weather_ingestion.py
%run 03_Agent_Meteorologico/02_trigger_detection.py
```

### Agente 4: Integrador

```python
%run 04_Agent_Integrador/01_risk_classification.py
%run 04_Agent_Integrador/02_boletin_generation.py
```

---

## ❓ Troubleshooting

### Error: "Schema no existe"

```
❌ ERROR: Schema 'workspace.avalanches_agents' no existe
```

**Solución:**
```python
# Ejecutar setup de Unity Catalog
%run 00_Setup/02_create_unity_catalog.py
```

---

### Error: "Tablas no existen"

```
❌ Susceptibilidad Topográfica: NO EXISTE
```

**Solución:** Las tablas se crean vacías en el setup. El pipeline las llenará con datos.

```python
# Ejecutar pipeline completo
%run 05_Pipeline/01_orchestrator.py
```

---

### Error: "Dependencias faltantes"

```
ImportError: No module named 'transformers'
```

**Solución:**
```python
# Instalar dependencias
%run 00_Setup/01_install_dependencies.py

# Reiniciar kernel si es necesario
dbutils.library.restartPython()
```

---

### Sin datos meteorológicos (Open-Meteo falla)

**Síntoma:** El agente meteorológico usa datos sintéticos

**Solución:** Verificar conexión a internet del cluster. El POC funciona con datos sintéticos si la API falla.

---

### Google Earth Engine no disponible

**Síntoma:** "⚠️ Google Earth Engine no disponible"

**Solución:** El POC usa DEM sintético automáticamente. Para usar GEE real:

```python
import ee
ee.Authenticate()  # Seguir proceso de autenticación
ee.Initialize()
```

---

## 📈 Próximos Pasos

Una vez ejecutado exitosamente:

1. **Explorar datos:** Usa SQL Analytics para consultas
2. **Visualizar:** Notebooks en `06_Visualization/` (próximamente)
3. **Ajustar zona:** Modifica `config/zone_config.py`
4. **Personalizar pesos:** Ajusta fusión multi-factorial en Agente 4
5. **Expandir:** Agrega más zonas en `ski_resort_locations`

---

## 📚 Documentación Completa

Para más detalles técnicos, consulta:
- `README.md` - Documentación completa del sistema
- `config/zone_config.py` - Configuración detallada
- Notebooks individuales - Cada uno tiene documentación inline

---

## 🆘 Soporte

Si encuentras problemas:
1. Revisa el README.md completo
2. Verifica logs del orquestador
3. Ejecuta agentes individualmente para identificar el problema
4. Revisa errores en la sección de validación

---

**¡Listo!** 🎉 Con estos 3 pasos tendrás el POC funcionando completamente.
