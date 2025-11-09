# POC Sistema de Predicción de Avalanchas - Chile

## Sistema Inteligente Multi-Agente para Predicción de Riesgo de Avalanchas

**Autor:** Francisco Javier Peñailillo San Martín
**Programa:** Magíster en Tecnologías de la Información
**Universidad:** Universidad Técnica Federico Santa María
**Plataforma:** Databricks

---

## 📋 Tabla de Contenidos

1. [Descripción General](#descripción-general)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Requisitos y Configuración](#requisitos-y-configuración)
4. [Guía de Instalación](#guía-de-instalación)
5. [Guía de Uso](#guía-de-uso)
6. [Estructura de Datos](#estructura-de-datos)
7. [Agentes del Sistema](#agentes-del-sistema)
8. [Escala EAWS](#escala-eaws)
9. [Limitaciones y Trabajo Futuro](#limitaciones-y-trabajo-futuro)
10. [Referencias](#referencias)

---

## 🎯 Descripción General

Este POC demuestra la viabilidad técnica de un **sistema multi-agente** para predecir riesgo de avalanchas en Chile, integrando:

- 🏔️ **Análisis topográfico** (SRTM/Google Earth Engine)
- 📚 **Conocimiento experto** (análisis NLP de 4000+ relatos)
- 🌦️ **Pronóstico meteorológico** (APIs en tiempo real)
- 🤖 **Fusión inteligente** (clasificación EAWS + generación de boletines con LLM)

### Objetivos del POC

✅ Demostrar arquitectura multi-agente escalable
✅ Alcanzar clasificación de riesgo en Escala Europea (EAWS 1-5)
✅ Generar boletines automatizados para zona piloto (La Parva)
✅ Sentar bases para sistema nacional de predicción

### Zona Piloto

📍 **La Parva, Región Metropolitana**
- Coordenadas: -33.35°, -70.27°
- Elevación: 2500-3500 m.s.n.m.
- Área: ~10-20 km²

---

## 🏗️ Arquitectura del Sistema

### Arquitectura Medallion (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────┐
│                    CAPA GOLD (Analytics)                    │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ Predicciones │  │  Boletines   │  │ Susceptibilidad  │  │
│  │    EAWS      │  │  Generados   │  │   Mapa Final    │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
│                                                             │
│                 ▲ AGENTE 4: INTEGRADOR ▲                    │
└─────────────────────────────────────────────────────────────┘
                           │
                           │ Fusión Multi-factorial
                           │
┌─────────────────────────────────────────────────────────────┐
│                 CAPA SILVER (Processed Features)            │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  ┌──────────┐  │
│  │   Topo   │  │   NLP    │  │  Weather  │  │ Weather  │  │
│  │ Features │  │ Patterns │  │ Features  │  │ Triggers │  │
│  └──────────┘  └──────────┘  └───────────┘  └──────────┘  │
│                                                             │
│       ▲              ▲              ▲             ▲         │
│  AGENTE 1       AGENTE 2       AGENTE 3      AGENTE 3      │
└─────────────────────────────────────────────────────────────┘
       │              │              │             │
       │              │              │             │
┌─────────────────────────────────────────────────────────────┐
│                   CAPA BRONZE (Raw Data)                    │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │   SRTM   │  │  Relatos │  │ Weather  │  │ Weather  │   │
│  │   Raw    │  │ AndesHB  │  │  Daily   │  │  Hourly  │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
│                                                             │
│  FUENTES:  GEE      AndesHB    Open-Meteo   Open-Meteo     │
└─────────────────────────────────────────────────────────────┘
```

### Flujo de Datos

```
┌─────────────────────────────────────────────────────────────────┐
│                      FLUJO DE EJECUCIÓN                         │
└─────────────────────────────────────────────────────────────────┘

1. AGENTE TOPOGRÁFICO
   ├─ Procesar SRTM (30m resolución)
   ├─ Calcular pendientes (30-45° críticas)
   ├─ Segmentar por bandas altitudinales
   └─ Generar mapa de susceptibilidad
          │
          ▼
2. AGENTE NLP
   ├─ Cargar base de relatos (4000+)
   ├─ Extraer menciones de riesgo
   ├─ Identificar patrones históricos
   └─ Generar embeddings semánticos
          │
          ▼
3. AGENTE METEOROLÓGICO
   ├─ Ingestar datos Open-Meteo (16 días)
   ├─ Calcular features (nevadas 24h/72h, temp, viento)
   ├─ Detectar condiciones gatillantes
   └─ Clasificar severidad
          │
          ▼
4. AGENTE INTEGRADOR
   ├─ Fusionar outputs (3 agentes)
   ├─ Calcular score multi-factorial
   ├─ Clasificar nivel EAWS (1-5)
   ├─ Identificar factores principales
   └─ Generar boletín con LLM
          │
          ▼
   📄 BOLETÍN EAWS FINAL
```

---

## ⚙️ Requisitos y Configuración

### Plataforma

- **Databricks** (Community Edition o Standard)
- **Unity Catalog** habilitado (recomendado)
- Cluster con Python 3.9+

### Dependencias Principales

```python
# Procesamiento Geoespacial
rasterio==1.3.9
GDAL==3.4.1
geopandas==0.14.1

# APIs Meteorológicas
openmeteo-requests==1.1.0
requests-cache==1.1.1

# NLP y ML
transformers==4.36.2
torch==2.1.2
sentence-transformers==2.2.2

# LLM Integration
openai==1.12.0

# Análisis de Redes
networkx==3.2.1

# Visualización
plotly==5.18.0
matplotlib==3.8.2
seaborn==0.13.1
```

### APIs Externas

1. **Open-Meteo** (gratuito, sin API key)
   - URL: `https://api.open-meteo.com/v1/forecast`
   - Variables: temperatura, precipitación, nieve, viento

2. **Google Earth Engine** (opcional, requiere cuenta)
   - Para datos SRTM de alta calidad
   - El POC funciona con DEM sintético si GEE no disponible

3. **Databricks LLM** (Llama 4)
   - Requiere Databricks con Model Serving
   - Fallback a templates si no disponible

---

## 🚀 Guía de Instalación

### Paso 1: Clonar Repositorio

```bash
git clone https://github.com/tu-usuario/avalanches-agents-poc.git
cd avalanches-agents-poc/databricks_poc
```

### Paso 2: Subir a Databricks

1. En Databricks Workspace, crear directorio:
   ```
   /Workspace/Users/tu-email@domain.com/AvalanchePOC/
   ```

2. Subir carpeta completa `databricks_poc/`

### Paso 3: Ejecutar Setup (SOLO UNA VEZ)

Ejecutar notebooks en orden:

```
1. 00_Setup/01_install_dependencies.py
   → Instala todas las librerías necesarias
   → Reinicia kernel automáticamente

2. 00_Setup/02_create_unity_catalog.py
   → Crea catálogo, schema, volumes, tablas
   → Requiere permisos ADMIN

3. Verificar config/zone_config.py
   → Revisar configuración de zona piloto
```

### Paso 4: Verificar Configuración

Ejecutar:
```
00_Setup/00_environment_setup.py
```

Deberías ver:
```
✅ UNITY CATALOG:
   Catálogo: workspace
   Schema: avalanches_agents
   Database completo: workspace.avalanches_agents

✅ ZONA PILOTO:
   Nombre: La Parva
   Centro: (-33.35, -70.27)
```

---

## 📖 Guía de Uso

### Opción A: Ejecutar Pipeline Completo (Recomendado)

```python
# Ejecutar orquestador (ejecuta todos los agentes)
%run 05_Pipeline/01_orchestrator.py
```

Este notebook:
- ✅ Ejecuta los 4 agentes en secuencia
- ✅ Maneja errores automáticamente
- ✅ Genera logging detallado
- ✅ Produce boletín final

**Tiempo estimado:** 5-10 minutos

### Opción B: Ejecutar Agentes Individualmente

```python
# 1. Agente Topográfico
%run 01_Agent_Topografico/01_srtm_processing.py
%run 01_Agent_Topografico/02_susceptibility_map.py

# 2. Agente NLP
%run 02_Agent_NLP/01_extract_risk_patterns.py

# 3. Agente Meteorológico
%run 03_Agent_Meteorologico/01_weather_ingestion.py
%run 03_Agent_Meteorologico/02_trigger_detection.py

# 4. Agente Integrador
%run 04_Agent_Integrador/01_risk_classification.py
%run 04_Agent_Integrador/02_boletin_generation.py
```

### Opción C: Consultar Resultados Existentes

```python
# Cargar última predicción
predictions = spark.table("workspace.avalanches_agents.avalanche_predictions")
latest = predictions.orderBy(F.desc("forecast_date")).limit(1)
display(latest)

# Cargar último boletín
bulletins = spark.table("workspace.avalanches_agents.avalanche_bulletins")
latest_bulletin = bulletins.orderBy(F.desc("issue_date")).limit(1)
display(latest_bulletin)
```

---

## 📊 Estructura de Datos

### Capas de Datos (Medallion Architecture)

#### BRONZE LAYER (Raw Data)

| Tabla | Descripción | Particiones |
|-------|-------------|-------------|
| `weather_daily` | Datos meteorológicos diarios (Open-Meteo) | location_name, date |
| `weather_hourly` | Datos meteorológicos horarios | location_name, date |
| `srtm_raw` | Elevación SRTM raw | - |
| `andes_handbook_routes` | Relatos de montañismo | - |
| `ski_resort_locations` | Ubicaciones monitoreadas | - |

#### SILVER LAYER (Processed Features)

| Tabla | Descripción | Generada por |
|-------|-------------|--------------|
| `topo_features` | Features topográficos (pendiente, aspecto, banda) | Agente 1 |
| `topo_susceptibility` | Susceptibilidad agregada por zona/banda | Agente 1 |
| `nlp_risk_patterns` | Patrones de riesgo históricos | Agente 2 |
| `weather_features` | Features meteorológicos (nevadas, temp) | Agente 3 |
| `weather_triggers` | Condiciones gatillantes detectadas | Agente 3 |

#### GOLD LAYER (Analytics & Predictions)

| Tabla | Descripción | Generada por |
|-------|-------------|--------------|
| `avalanche_predictions` | Predicciones EAWS por zona/banda/fecha | Agente 4 |
| `avalanche_bulletins` | Boletines generados | Agente 4 |
| `susceptibility_map` | Mapa final de susceptibilidad | Agente 4 |

### Esquema de Predicción (Tabla Gold)

```sql
prediction_id          STRING       -- ID único
zone_name              STRING       -- Zona geográfica
forecast_date          DATE         -- Fecha del pronóstico
elevation_band         STRING       -- Banda altitudinal
eaws_level             INT          -- Nivel EAWS (1-5)
eaws_label             STRING       -- Etiqueta EAWS
risk_score             DOUBLE       -- Score normalizado 0-1
confidence             DOUBLE       -- Confianza 0-1
topo_contribution      DOUBLE       -- Contribución topografía
weather_contribution   DOUBLE       -- Contribución meteorología
nlp_contribution       DOUBLE       -- Contribución NLP
main_factors           ARRAY<STRING> -- Factores principales
prediction_timestamp   TIMESTAMP    -- Timestamp de predicción
```

---

## 🤖 Agentes del Sistema

### Agente 1: Topográfico

**Objetivo:** Identificar zonas topográficamente susceptibles

**Inputs:**
- SRTM DEM (30m resolución)
- Bounding box zona piloto

**Procesamiento:**
1. Calcular pendientes (algoritmo de Horn)
2. Calcular orientaciones (aspect 0-360°)
3. Identificar pendientes críticas (30-45°)
4. Segmentar por bandas altitudinales
5. Calcular score de susceptibilidad

**Outputs:**
- `topo_features`: Features por píxel
- `topo_susceptibility`: Susceptibilidad agregada

**Métricas:**
- Área con pendientes críticas (km²)
- Susceptibility score (0-1)

---

### Agente 2: NLP

**Objetivo:** Extraer conocimiento experto de relatos históricos

**Inputs:**
- Base de datos AndesHandbook (4000+ relatos)
- Vocabulario especializado en avalanchas

**Procesamiento:**
1. Filtrar relatos de zona piloto
2. Extraer menciones de riesgo (regex + keywords)
3. Identificar patrones de pendientes mencionadas
4. Clasificar nivel de dificultad
5. Análisis de sentimiento

**Outputs:**
- `nlp_risk_patterns`: Patrones por zona/banda

**Métricas:**
- Menciones de avalanchas
- Menciones de riesgo
- Score NLP (0-1)

---

### Agente 3: Meteorológico

**Objetivo:** Detectar condiciones gatillantes

**Inputs:**
- Open-Meteo API (pronóstico 16 días)
- Umbrales de decisión configurados

**Procesamiento:**
1. Ingestar datos horarios y diarios
2. Calcular features agregados:
   - Nevada 24h/48h/72h
   - Variación térmica
   - Viento máximo
   - Isoterma 0°C
3. Detectar 6 tipos de triggers:
   - Nevada intensa
   - Nevada acumulada
   - Fluctuación térmica
   - Viento fuerte
   - Isoterma crítica
   - Lluvia sobre nieve

**Outputs:**
- `weather_features`: Features agregados
- `weather_triggers`: Triggers detectados

**Métricas:**
- Triggers críticos detectados
- Nevada máxima (cm)
- Viento máximo (km/h)

---

### Agente 4: Integrador

**Objetivo:** Fusionar datos y generar predicción final

**Inputs:**
- `topo_susceptibility` (Agente 1)
- `nlp_risk_patterns` (Agente 2)
- `weather_triggers` (Agente 3)

**Procesamiento:**
1. **Fusión multi-factorial:**
   ```python
   risk_score = (0.35 * topo_score +
                 0.45 * weather_score +
                 0.20 * nlp_score)
   ```

2. **Clasificación EAWS:**
   - Score < 0.2 → Nivel 1 (Débil)
   - Score < 0.4 → Nivel 2 (Limitado)
   - Score < 0.6 → Nivel 3 (Notable)
   - Score < 0.8 → Nivel 4 (Fuerte)
   - Score ≥ 0.8 → Nivel 5 (Muy Fuerte)

3. **Generación de boletín:**
   - Usar LLM (Llama 4) si disponible
   - Fallback a templates estructurados

**Outputs:**
- `avalanche_predictions`: Predicciones EAWS
- `avalanche_bulletins`: Boletines generados

**Métricas:**
- Nivel EAWS final
- Confianza de predicción
- Factores principales identificados

---

## 📊 Escala EAWS (European Avalanche Warning Services)

| Nivel | Etiqueta | Color | Descripción | Recomendaciones |
|-------|----------|-------|-------------|-----------------|
| **1** | Débil | 🟢 Verde | Nieve generalmente bien estabilizada | Condiciones favorables |
| **2** | Limitado | 🟡 Amarillo | Nieve moderadamente estabilizada | Precaución en pendientes específicas |
| **3** | Notable | 🟠 Naranja | Nieve débilmente estabilizada en muchas pendientes | Experiencia y evaluación esencial |
| **4** | Fuerte | 🔴 Rojo | Nieve débilmente estabilizada en mayoría de pendientes | Restricción a terreno simple |
| **5** | Muy Fuerte | 🟣 Morado | Nieve inestable generalizada | Evitar todo terreno de avalanchas |

### Factores Considerados

- **Topografía:** Pendiente, orientación, elevación
- **Meteorología:** Nevada, viento, temperatura
- **Histórico:** Eventos previos, patrones conocidos

---

## 🔍 Limitaciones y Trabajo Futuro

### Limitaciones del POC

❗ **Datos:**
- DEM sintético si GEE no disponible
- Base NLP limitada a zona piloto
- Sin validación con datos observacionales

❗ **Modelos:**
- Reglas heurísticas simples (no ML avanzado)
- Pesos de fusión fijos (no optimizados)
- Sin calibración con eventos reales

❗ **Cobertura:**
- Solo zona piloto (La Parva)
- Sin análisis de múltiples zonas
- Sin validación temporal

### Trabajo Futuro

🚀 **Fase 2 - Mejoras ML:**
- Entrenar modelos PINNs para topografía
- Implementar Time-LLM para meteorología
- Optimizar pesos de fusión con ML
- Validar con datos históricos de Snowlab

🚀 **Fase 3 - Escalamiento:**
- Expandir a toda Región Metropolitana
- Integrar más centros de esquí
- Agregar Agente de Monitoreo Satelital (Sentinel-2)
- Dashboard interactivo en tiempo real

🚀 **Fase 4 - Producción:**
- API REST para consultas
- Integración con sistemas de emergencia
- App móvil para montañistas
- Sistema de alertas automáticas

---

## 📚 Referencias

1. **EAWS (European Avalanche Warning Services)**
   - https://www.avalanches.org

2. **SRTM (Shuttle Radar Topography Mission)**
   - https://www2.jpl.nasa.gov/srtm/

3. **Open-Meteo API**
   - https://open-meteo.com/en/docs

4. **AndesHandbook**
   - https://www.andeshandbook.org

5. **Databricks Documentation**
   - https://docs.databricks.com

6. **Unity Catalog**
   - https://docs.databricks.com/unity-catalog/

---

## 👤 Autor

**Francisco Javier Peñailillo San Martín**
- Estudiante de Magíster en Tecnologías de la Información
- Universidad Técnica Federico Santa María
- Especialización: IA aplicada a seguridad en montaña

---

## 📄 Licencia

Este proyecto es un POC académico para tesis de magíster.

---

## 🙏 Agradecimientos

- **AndesHandbook** por base de datos de relatos
- **Open-Meteo** por API meteorológica gratuita
- **Google Earth Engine** por acceso a SRTM
- **Databricks** por plataforma de desarrollo
- **Comunidad montañista chilena** por inspiración

---

**Última actualización:** 2025-01-09

