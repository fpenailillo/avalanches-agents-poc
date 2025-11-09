# avalanches-agents-poc

Prompt para Claude Code - POC Sistema de Predicción de Avalanchas
Necesito desarrollar una Prueba de Concepto (POC) para mi tesis de magíster: un sistema inteligente de predicción de riesgo de avalanchas para Chile basado en arquitectura multi-agente.

## CONTEXTO DEL PROYECTO

Estoy desarrollando el primer sistema nacional de predicción de avalanchas para Chile, que integra 4 agentes de IA especializados:
1. Agente Topográfico-Nival (análisis SRTM con PINNs)
2. Agente de Monitoreo Satelital (Vision Transformers con Sentinel-2)
3. Agente de Predicción Meteorológica (Time-LLM/LSTM)
4. Agente Integrador de Riesgos (fusión con NLP y generación de boletines)

El objetivo es alcanzar >75% de precisión usando la Escala Europea de Peligro de Aludes (5 niveles).

## TRABAJO YA REALIZADO

Tengo los siguientes componentes desarrollados y datos disponibles:

### ✅ Datos de Relatos NLP (4000+ relatos)
- Base de datos procesada de Andeshandbook
- Extracción geográfica completa (latitud, longitud, sector, altitud)
- Clasificación de riesgos y patrones técnicos
- 8 categorías estructuradas: resumen, ubicación, características técnicas, condiciones temporales, equipamiento, riesgos, aspectos emocionales, metadatos

### ✅ Análisis Topográfico SRTM
- Conexión establecida con Google Earth Engine
- Pipeline funcional para extracción de pendientes críticas (30-45°)
- Mapas generados para Región Metropolitana
- Segmentación altitudinal implementada
- Cálculos de orientación, altitud y gradiente térmico

### ✅ Integración Meteorológica
- Acceso configurado a APIs meteorológicas
- Experiencia con datos de temperatura, precipitación e isoterma 0°C
- Conexión con NASA Earth Data y Copernicus

## OBJETIVO DEL POC

Crear un prototipo funcional end-to-end que demuestre viabilidad técnica de la arquitectura multi-agente, integrando los 3-4 agentes en pipeline coordinado y generando un boletín de ejemplo para una zona piloto (ej: La Parva, Región Metropolitana).

**IMPORTANTE**: El desarrollo se realizará en **Databricks**, aprovechando su capacidad de procesamiento distribuido y gestión de datos a gran escala.

## REQUISITOS TÉCNICOS

### Plataforma:
- **Databricks** (ambiente principal de desarrollo)
- PySpark para procesamiento distribuido
- MLflow para tracking de experimentos
- Delta Lake para gestión de datos

### Datos disponibles (YA TENGO):
- ✅ SRTM DEM (30m) - pipeline funcional en Google Earth Engine
- ✅ Base de datos NLP de 4000+ relatos preprocesados
- ✅ Acceso a APIs meteorológicas configurado
- ✅ Opcional: acceso a Sentinel-2 vía Google Earth Engine

### Tecnologías sugeridas:
- Python 3.9+
- PySpark (Databricks)
- PyTorch para modelos ML
- GDAL/Rasterio para datos geoespaciales (compatible con Databricks)
- LangChain para coordinación multi-agente
- MLflow para experimentación
- APIs: OpenWeatherMap, snowforecast, o similar (ya configuradas)
- Google Earth Engine (ya tengo acceso)

### Salida esperada:
- Pipeline funcional en Databricks con 3-4 agentes coordinados
- Clasificación de riesgo en Escala Europea (1-5 niveles) para zona piloto
- Boletín automatizado en texto con factores explicativos
- Visualización básica (mapa de susceptibilidad + pronóstico)
- Notebooks Databricks documentados y reproducibles

## TAREAS ESPECÍFICAS

Por favor, ayúdame a:

1. **Diseñar la arquitectura del POC en Databricks**: 
   - Estructura modular de agentes usando notebooks o jobs
   - Flujo de datos con Delta Lake
   - Coordinación entre agentes
   - Estrategia de almacenamiento (raw/bronze → silver → gold)

2. **Implementar Agente 1 (Topográfico)**: 
   - Adaptar mi pipeline existente de SRTM/Google Earth Engine a Databricks
   - Cargar/procesar SRTM para zona piloto (33°S-34°S)
   - Calcular pendientes críticas (30-45°) con procesamiento distribuido si es necesario
   - Segmentar por altitud (4 bandas: <2500m, 2500-3200m, 3200-3500m, >3500m)
   - Generar mapa base de susceptibilidad
   - Almacenar resultados en Delta Lake

3. **Implementar Agente 2 (NLP - Integración de Relatos)**:
   - Cargar mi base de datos de 4000+ relatos en Databricks
   - Filtrar relatos relevantes para zona piloto
   - Extraer patrones de riesgo históricos
   - Identificar zonas de actividad frecuente
   - Generar embeddings semánticos para similaridad
   - Output: conocimiento experto estructurado por zona

4. **Implementar Agente 3 (Meteorológico)**:
   - Integrar mis APIs meteorológicas ya configuradas en Databricks
   - Obtener datos de temperatura, precipitación, isoterma 0°C
   - Identificar condiciones gatillantes (>30cm/24h, fluctuaciones térmicas)
   - Generar pronósticos por horizonte temporal (24h/48h/72h)
   - Output: probabilidad de condiciones críticas por banda altitudinal

5. **Implementar Agente 4 (Integrador)**:
   - Fusionar outputs de agentes 1, 2 y 3
   - Aplicar reglas heurísticas basadas en Escala Europea
   - Clasificar nivel de peligro (1-Débil a 5-Muy Fuerte)
   - Usar LLM (vía API o modelo local) para generar boletín textual estructurado
   - Incluir: nivel de peligro, factores contribuyentes, zonas afectadas, recomendaciones
   - Formato compatible con estándares EAWS/SLF

6. **Pipeline de coordinación en Databricks**: 
   - Workflows o jobs que ejecuten agentes secuencialmente
   - Manejo de errores y logging
   - Versionado de datos con Delta Lake
   - Tracking de experimentos con MLflow

7. **Visualización y validación**:
   - Dashboard básico en Databricks SQL o notebook
   - Mapa de susceptibilidad + pronóstico superpuesto
   - Métricas de confianza por agente
   - Comparación con pronósticos manuales (si disponibles)

8. **Documentación**: 
   - README con arquitectura del sistema
   - Instrucciones de setup en Databricks
   - Dependencias y configuración de APIs
   - Guía de ejecución paso a paso
   - Limitaciones del POC y trabajo futuro

## RESTRICCIONES

- POC debe ser ejecutable en Databricks Community Edition (o cluster básico)
- Aprovechar procesamiento distribuido solo donde sea necesario (SRTM grande, análisis masivo NLP)
- Priorizar simplicidad sobre precisión en esta fase (reglas heurísticas + ML básico está OK)
- Si faltan datos específicos, usar estructura realista con valores mock
- Tiempo estimado: POC funcional en 2-4 semanas de trabajo

## ZONA PILOTO SUGERIDA

La Parva, Región Metropolitana (33.35°S, 70.27°W):
- Altitud: 2.500-3.500 msnm
- Terreno conocido con datos de validación (Snowlab)
- Área compacta (~10-20 km²)
- Tengo relatos NLP para esta zona
- Cobertura SRTM completa
- Datos meteorológicos disponibles vía API

## ESTRUCTURA SUGERIDA EN DATABRICKS
/Workspace/AvalanchePOC/
├── 00_Setup/
│   ├── install_dependencies.py
│   ├── config_apis.py
│   └── data_ingestion.py
├── 01_Agent_Topografico/
│   ├── srtm_processing.py
│   ├── slope_analysis.py
│   └── susceptibility_map.py
├── 02_Agent_NLP/
│   ├── load_relatos.py
│   ├── filter_zona.py
│   └── extract_patterns.py
├── 03_Agent_Meteorologico/
│   ├── api_integration.py
│   ├── forecast_processing.py
│   └── trigger_detection.py
├── 04_Agent_Integrador/
│   ├── fusion_logic.py
│   ├── risk_classification.py
│   └── boletin_generation.py
├── 05_Pipeline/
│   ├── orchestrator.py
│   └── workflow_definition.json
├── 06_Visualization/
│   ├── dashboard.py
│   └── maps.py
└── README.md
