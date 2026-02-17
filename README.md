# pipeline_cgspace
ver en qué tendencia van los briefs uploaded to cgspace
# CGSpace Briefs — Análisis de Metadatos

Proyecto personal para explorar y entender el universo de **Briefs** publicados en [CGSpace](https://cgspace.cgiar.org) (repositorio institucional del CGIAR). El objetivo es comprender cómo se conciben estos documentos: qué temas abordan, cómo están organizados temáticamente, qué lenguaje usan, y cómo evolucionan en el tiempo.

---

## Objetivo

Construir una base de datos a partir de los Briefs disponibles en CGSpace para responder cinco preguntas:

1. **Tendencia temporal:** ¿qué temas crecen o caen por trimestre?
2. **Agenda institucional:** ¿qué centros o programas publican más Briefs y sobre qué?
3. **Geografía:** ¿qué países y regiones aparecen más, y cómo cambia eso?
4. **Colecciones:** ¿hay series o colecciones que concentran un enfoque temático?
5. **Señales de línea:** ¿qué keywords son nuevas, qué lenguaje aparece en abstracts recientes?

---

## Estado del proyecto

| Fase | Estado |
|---|---|
| 0 — Cosecha exploratoria (muestra pequeña) | ⬜ Pendiente |
| A — Cosecha OAI-PMH completa (24 meses) | ⬜ Pendiente |
| B — Transformación y staging | ⬜ Pendiente |
| C — Carga SQLite | ⬜ Pendiente |
| D — Análisis y outputs | ⬜ Pendiente |

---

## Alcance

- **Ventana temporal:** últimos 24 meses. Regla de corte: `issued_date >= (hoy − 24 meses)`.
- **Granularidad temporal:** trimestre (Q1–Q4). Campos derivados: `year`, `quarter`, `year_quarter` (ej. `2025Q3`).
- **Definición de Brief:** ítem con `dcterms.type` igual a `Brief` o `Policy Brief`. El campo `type_raw` siempre se conserva.
- **Fuente:** OAI-PMH de CGSpace. Sin scraping de la interfaz web.

---

## Pipeline ETL

### Fase 0 — Cosecha exploratoria (primer paso obligatorio)

Cosechar una muestra pequeña (200–500 registros) para inspeccionar:

- Valores reales de `dcterms.type`
- Presencia y completitud de campos CG (`cg.coverage.*`, `cg.contributor.*`)
- Calidad de abstracts, keywords y fechas

Esta fase define el diccionario de mapeo real.

### Fase A — Cosecha OAI-PMH

- Descarga de registros XML con paginación por `resumptionToken`
- `datestamp` OAI guardado para cosechas incrementales
- Pausas, reintentos con backoff, caché local

Salida: `data/raw/oai_records_YYYYMMDD_*.xml` y `data/logs/harvest.log`

### Fase B — Transformación

- XML → estructura larga (key/value repetibles)
- Mapeo de campos DC y CG Core al diccionario interno
- Campos de fecha derivados: `issued_date`, `year`, `quarter`, `year_quarter`
- Conservar siempre valores `*_raw`

Salida: `data/staging/records_long.parquet`

### Fase C — Carga SQLite

Base de datos: `data/db/cgspace_briefs.sqlite` con tablas normalizadas e índices.

### Fase D — Análisis y outputs

- Top keywords por trimestre
- Términos nuevos en abstracts (comparación por frecuencia, sin modelos)
- Top países y regiones por trimestre
- Ranking institucional por volumen
- Notas narrativas por colección o serie

---

## Esquema de base de datos

### `briefs` — tabla principal

| Campo | Tipo | Descripción |
|---|---|---|
| `brief_id` | TEXT PK | Handle o URI estable |
| `title` | TEXT | Título |
| `issued_date` | TEXT ISO | Fecha de emisión |
| `year` | INTEGER | Año derivado |
| `quarter` | INTEGER | Trimestre (1–4) |
| `year_quarter` | TEXT | Ej. `2025Q3` |
| `type_raw` | TEXT | Tipo original |
| `brief_flag` | INTEGER | 1 si es Brief |
| `abstract` | TEXT | Resumen |
| `language` | TEXT | Idioma |
| `publisher` | TEXT | Editorial |
| `datestamp` | TEXT | Datestamp OAI |
| `last_harvested_at` | TEXT | Timestamp del pipeline |

Tablas relacionadas: `authors`, `brief_authors`, `keywords`, `brief_keywords`, `geo`, `brief_geo`, `funding_entities`, `brief_funding`, `series`, `brief_series`.

---

## Campos y mapeo

> ⚠️ Diccionario a completar tras la Fase 0.

### Campos core

| Campo interno | Fuente OAI |
|---|---|
| Identificador | `dc.identifier.*` |
| Título | `dc.title` |
| Fecha | `dc.date.issued` |
| Tipo | `dc.type` / `dcterms.type` |
| Autores | `dc.contributor.author` |
| Abstract | `dc.description.abstract` |
| Keywords | `dc.subject` |
| Idioma | `dc.language.iso` |
| Publisher | `dc.publisher` |

### Campos CG

| Campo interno | Fuente OAI |
|---|---|
| País | `cg.coverage.country` |
| Región | `cg.coverage.region` |
| Donor | `cg.contributor.donor` |
| Proyecto | `cg.identifier.project` |
| Initiative | `cg.contributor.initiative` |
| Program | `cg.contributor.programAccelerator` |
| CRP | `cg.contributor.crp` |
| Serie | `dcterms.isPartOf` |

---

## Normalización

- `*_raw`: valor original, siempre conservado.
- `*_norm`: trim + colapsar espacios + minúsculas.
- Keywords: diccionario de sinónimos manual e incremental.
- Países: catálogo controlado (ISO o manual).

---

## Métricas de completitud

Por trimestre, calcular el % de briefs con cada campo clave (`abstract`, keyword, `country`, `donor`, `initiative`, `series`). Sirve para distinguir cambios de agenda de cambios en el llenado de metadatos.

---

## Análisis previstos

### 1. Tendencia temporal
Conteo y top keywords por trimestre. Keywords emergentes: aparecen por primera vez y crecen ≥ 2 trimestres seguidos.

### 2. Agenda institucional
Jerarquía: initiative/program → collection → afiliación. Volumen, keywords y países por institución.

### 3. Geografía
Países y regiones por trimestre. Diversidad geográfica (nº países únicos).

### 4. Colecciones y series
Volumen y top keywords por serie. Concentración temática: share de los 3 temas principales.

### 5. Señales de línea (versión simplificada)
1. Keywords emergentes (nuevas en últimos 2 trimestres)
2. Entidades emergentes (donors, proyectos, iniciativas nuevas)
3. Términos nuevos en abstracts (comparación de frecuencia entre trimestres, sin modelos)

---

## Estructura de carpetas
```
project/
├── README.md
├── data/
│   ├── raw/
│   ├── staging/
│   ├── db/
│   └── logs/
├── scripts/
│   ├── 00_explore_oai.py
│   ├── 01_harvest_oai.py
│   ├── 02_parse_transform.py
│   ├── 03_load_sqlite.py
│   ├── 04_build_marts.py
│   └── 05_analysis_outputs.py
└── outputs/
    ├── tables/
    ├── figures/
    └── notes/
```

---

## Próximos pasos

- [ ] Ejecutar cosecha exploratoria con 200–500 registros
- [ ] Inspeccionar valores reales de `dcterms.type` y campos CG
- [ ] Completar diccionario de mapeo
- [ ] Implementar `brief_flag` y `year_quarter`
- [ ] Cargar tablas core en SQLite
- [ ] Producir primeros outputs: top keywords y países por trimestre

---

## Notas técnicas

- Lenguaje: Python
- Base de datos: SQLite
- Formato intermedio: Parquet o CSV
- Pipeline reproducible, auditable y respetuoso con el servidor

---

*Proyecto en desarrollo. Última actualización: febrero 2026.*
