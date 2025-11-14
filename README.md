# 🏔️ POC Sistema de Predicción de Avalanchas - Chile

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Ready-orange.svg)](https://databricks.com/)
[![License](https://img.shields.io/badge/License-Academic-green.svg)](LICENSE)

> **Sistema Inteligente Multi-Agente para Predicción de Riesgo de Avalanchas**
> Tesis de Magíster en Tecnologías de la Información
> Universidad Técnica Federico Santa María

## 📋 Descripción

Prueba de Concepto (POC) de un sistema inteligente de predicción de riesgo de avalanchas para Chile, basado en arquitectura multi-agente. Este proyecto integra análisis topográfico, procesamiento de lenguaje natural, datos meteorológicos y modelos de inteligencia artificial para generar boletines de riesgo automatizados según la Escala Europea de Peligro de Aludes (EAWS).

### 🎯 Objetivos del Proyecto

- **Arquitectura Multi-Agente Escalable**: 4 agentes especializados trabajando de forma coordinada
- **Predicción EAWS**: Clasificación de riesgo en 5 niveles según estándares europeos
- **Automatización**: Generación de boletines de avalanchas sin intervención manual
- **Fundamento para Sistema Nacional**: Base técnica para escalar a todo Chile

### 🤖 Agentes del Sistema

1. **Agente Topográfico-Nival**: Análisis SRTM con Google Earth Engine
2. **Agente NLP**: Extracción de conocimiento de 4000+ relatos de montañismo
3. **Agente Meteorológico**: Predicción con datos de Open-Meteo API
4. **Agente Integrador**: Fusión multi-factorial y generación de boletines con LLM

## 📁 Estructura del Proyecto

```
avalanches-agents-poc/
├── databricks_poc/              # Código principal del POC
│   ├── 00_Setup/                # Scripts de configuración inicial
│   │   ├── 00_environment_setup.py
│   │   ├── 01_install_dependencies.py
│   │   └── 02_create_unity_catalog.py
│   ├── 01_Agent_Topografico/    # Agente de análisis topográfico
│   │   ├── 01_srtm_processing.py
│   │   └── 02_susceptibility_map.py
│   ├── 02_Agent_NLP/            # Agente de procesamiento de relatos
│   │   └── 01_extract_risk_patterns.py
│   ├── 03_Agent_Meteorologico/  # Agente meteorológico
│   │   ├── 01_weather_ingestion.py
│   │   └── 02_trigger_detection.py
│   ├── 04_Agent_Integrador/     # Agente integrador y generador de boletines
│   │   ├── 01_risk_classification.py
│   │   └── 02_boletin_generation.py
│   ├── 05_Pipeline/             # Orquestación del sistema
│   │   └── 01_orchestrator.py
│   ├── config/                  # Configuraciones del proyecto
│   │   └── zone_config.py
│   ├── README.md                # Documentación técnica completa
│   └── QUICKSTART.md            # Guía de inicio rápido
├── google_srtm                  # Script GEE para análisis SRTM
├── 01_weather_data_ingestion.py # Script de ingesta meteorológica
├── 4.Analisis de redes con modelos NLP y Llms.ipynb  # Análisis exploratorio
├── requirements.txt             # Dependencias del proyecto
├── .gitignore                   # Archivos ignorados por Git
└── README.md                    # Este archivo
```

## 🚀 Inicio Rápido

### Prerrequisitos

- Python 3.9 o superior
- Cuenta de Databricks (Community Edition o superior)
- Unity Catalog habilitado (recomendado)
- Acceso a internet para APIs meteorológicas

### Instalación

1. **Clonar el repositorio**
```bash
git clone https://github.com/fpenailillo/avalanches-agents-poc.git
cd avalanches-agents-poc
```

2. **Instalar dependencias locales** (opcional, para desarrollo)
```bash
pip install -r requirements.txt
```

3. **Configurar Databricks**
   - Subir la carpeta `databricks_poc/` a tu workspace
   - Ejecutar `00_Setup/01_install_dependencies.py`
   - Ejecutar `00_Setup/02_create_unity_catalog.py`

4. **Ejecutar el POC**
```python
# En Databricks
%run 05_Pipeline/01_orchestrator.py
```

Para más detalles, consulta [databricks_poc/QUICKSTART.md](databricks_poc/QUICKSTART.md)

## 🏗️ Arquitectura Técnica

### Arquitectura Medallion (Bronze → Silver → Gold)

El sistema implementa la arquitectura Medallion para gestión de datos:

- **🥉 Bronze Layer**: Datos raw (SRTM, relatos, datos meteorológicos)
- **🥈 Silver Layer**: Features procesados (topografía, patrones NLP, triggers meteorológicos)
- **🥇 Gold Layer**: Predicciones EAWS y boletines generados

### Flujo de Ejecución

1. **Agente Topográfico** → Analiza SRTM y calcula susceptibilidad
2. **Agente NLP** → Extrae patrones de riesgo de relatos históricos
3. **Agente Meteorológico** → Detecta condiciones gatillantes
4. **Agente Integrador** → Fusiona datos y genera predicción EAWS

## ✅ Estado del Proyecto

### Componentes Implementados

- ✅ **Análisis Topográfico SRTM**: Pipeline funcional con Google Earth Engine
- ✅ **Base de Datos NLP**: 4000+ relatos procesados de AndesHandbook
- ✅ **Integración Meteorológica**: API Open-Meteo configurada
- ✅ **Sistema Multi-Agente**: 4 agentes coordinados en Databricks
- ✅ **Arquitectura Medallion**: Delta Lake con capas Bronze/Silver/Gold
- ✅ **Generación de Boletines**: Con LLM (Llama 4) o templates
- ✅ **Orquestación**: Pipeline automatizado con manejo de errores

## 🛠️ Tecnologías Utilizadas

### Plataforma Principal
- **Databricks**: Ambiente de desarrollo y ejecución
- **PySpark**: Procesamiento distribuido de datos
- **Delta Lake**: Arquitectura Medallion para gestión de datos
- **MLflow**: Tracking de experimentos (preparado para futuras mejoras)

### Análisis Geoespacial
- **Google Earth Engine**: Procesamiento SRTM
- **GDAL/Rasterio**: Manipulación de datos geoespaciales
- **GeoPandas**: Análisis vectorial

### Machine Learning y NLP
- **PyTorch**: Framework de deep learning
- **Transformers**: Modelos de procesamiento de lenguaje
- **Sentence Transformers**: Embeddings semánticos
- **NLTK**: Procesamiento de texto

### APIs y Datos Externos
- **Open-Meteo API**: Datos meteorológicos (gratuita)
- **Google Earth Engine**: Datos SRTM
- **AndesHandbook**: Base de relatos (4000+ documentos)

### Generación de Boletines
- **OpenAI/LangChain**: Integración con LLMs
- **Databricks LLM**: Llama 4 para generación de texto

## 📊 Datos del Proyecto

### Fuentes de Datos

| Tipo | Fuente | Descripción | Cobertura |
|------|--------|-------------|-----------|
| **Topografía** | SRTM/GEE | DEM 30m resolución | Región Metropolitana |
| **Meteorología** | Open-Meteo | Pronóstico 16 días | Zona piloto |
| **NLP** | AndesHandbook | 4000+ relatos | Chile completo |
| **Ubicaciones** | Manual | Centros de esquí | RM (expandible) |

### Zona Piloto

📍 **La Parva, Región Metropolitana**
- **Coordenadas**: -33.35°S, -70.27°W
- **Elevación**: 2500-3500 m.s.n.m.
- **Área**: ~10-20 km²
- **Justificación**: Datos de validación disponibles, terreno conocido

## 🎨 Características Principales

### Sistema Multi-Agente Coordinado
- ✅ **4 agentes especializados** trabajando de forma coordinada
- ✅ **Orquestación automatizada** con manejo de errores
- ✅ **Logging detallado** para debugging y monitoreo

### Arquitectura de Datos Robusta
- ✅ **Arquitectura Medallion** (Bronze → Silver → Gold)
- ✅ **Delta Lake** para versionado y confiabilidad
- ✅ **Unity Catalog** para gobernanza de datos

### Análisis Inteligente
- ✅ **Análisis topográfico** con detección de pendientes críticas (30-45°)
- ✅ **Extracción de patrones NLP** de experiencias históricas
- ✅ **Detección de triggers meteorológicos** (6 tipos de condiciones)
- ✅ **Fusión multi-factorial** con pesos configurables

### Generación Automatizada de Boletines
- ✅ **Clasificación EAWS** (5 niveles de peligro)
- ✅ **Generación con LLM** (Llama 4) o templates estructurados
- ✅ **Formato profesional** compatible con estándares internacionales

## 📈 Escala EAWS (European Avalanche Warning Services)

| Nivel | Etiqueta | Descripción | Color |
|-------|----------|-------------|-------|
| **1** | Débil | Nieve generalmente bien estabilizada | 🟢 Verde |
| **2** | Limitado | Nieve moderadamente estabilizada | 🟡 Amarillo |
| **3** | Notable | Nieve débilmente estabilizada en muchas pendientes | 🟠 Naranja |
| **4** | Fuerte | Nieve débilmente estabilizada en mayoría de pendientes | 🔴 Rojo |
| **5** | Muy Fuerte | Nieve inestable generalizada | 🟣 Morado |

## 🔮 Trabajo Futuro

### Fase 2: Mejoras de Modelos ML
- [ ] Implementar PINNs (Physics-Informed Neural Networks) para topografía
- [ ] Integrar Time-LLM para predicción meteorológica
- [ ] Optimizar pesos de fusión con aprendizaje automático
- [ ] Validar con datos históricos de Snowlab

### Fase 3: Escalamiento
- [ ] Expandir a toda la Región Metropolitana
- [ ] Agregar más centros de esquí y zonas de montaña
- [ ] Implementar Agente de Monitoreo Satelital (Sentinel-2)
- [ ] Dashboard interactivo en tiempo real

### Fase 4: Producción
- [ ] API REST para consultas externas
- [ ] Integración con sistemas de emergencia
- [ ] Aplicación móvil para montañistas
- [ ] Sistema de alertas automáticas vía SMS/email

## ⚠️ Limitaciones del POC

Este proyecto es una Prueba de Concepto con las siguientes limitaciones:

### Datos
- DEM sintético si Google Earth Engine no está disponible
- Base NLP limitada a zona piloto (La Parva)
- Sin validación con datos observacionales de avalanchas reales
- Sin datos de campo para calibración

### Modelos
- Reglas heurísticas simples (no ML avanzado en esta fase)
- Pesos de fusión fijos (no optimizados mediante aprendizaje)
- Sin calibración con eventos históricos
- Generación de boletines con templates si LLM no disponible

### Cobertura
- Enfocado en zona piloto única
- Sin análisis de múltiples zonas simultáneas
- Sin validación temporal extensa
- Limitado a Región Metropolitana

### Infraestructura
- Diseñado para Databricks Community Edition o cluster básico
- Procesamiento distribuido limitado
- Sin API REST para consultas externas
- Sin sistema de alertas en tiempo real

## 📚 Documentación

- **[README Principal](README.md)**: Este archivo
- **[README Técnico](databricks_poc/README.md)**: Documentación completa del sistema
- **[Guía Rápida](databricks_poc/QUICKSTART.md)**: Inicio rápido en 3 pasos
- **[Configuración](databricks_poc/config/zone_config.py)**: Parámetros del sistema

## 🤝 Contribuciones

Este es un proyecto académico de tesis de magíster. Si tienes sugerencias o encuentras problemas:

1. Abre un issue en GitHub
2. Describe el problema o sugerencia con detalle
3. Incluye ejemplos o capturas si es posible

## 📝 Licencia

Este proyecto es un POC académico desarrollado como parte de una tesis de Magíster en Tecnologías de la Información en la Universidad Técnica Federico Santa María.

## 👤 Autor

**Francisco Javier Peñailillo San Martín**
- 🎓 Estudiante de Magíster en Tecnologías de la Información
- 🏫 Universidad Técnica Federico Santa María
- 🔬 Especialización: IA aplicada a seguridad en montaña
- 📧 Contacto: [GitHub](https://github.com/fpenailillo)

## 🙏 Agradecimientos

- **AndesHandbook** - Base de datos de relatos de montañismo
- **Open-Meteo** - API meteorológica gratuita
- **Google Earth Engine** - Acceso a datos SRTM
- **Databricks** - Plataforma de desarrollo
- **Comunidad montañista chilena** - Inspiración y contexto del proyecto
- **EAWS** - Estándares de clasificación de peligro de avalanchas

## 📖 Referencias

1. **EAWS (European Avalanche Warning Services)** - https://www.avalanches.org
2. **SRTM (Shuttle Radar Topography Mission)** - https://www2.jpl.nasa.gov/srtm/
3. **Open-Meteo API** - https://open-meteo.com/en/docs
4. **AndesHandbook** - https://www.andeshandbook.org
5. **Databricks Documentation** - https://docs.databricks.com

---

**Última actualización**: Noviembre 2024

⭐ Si este proyecto te resulta útil o interesante, por favor considera darle una estrella en GitHub
