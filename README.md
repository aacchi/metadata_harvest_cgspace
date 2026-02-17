# pipeline_cgspace
Análisis de metadatos de Briefs publicados en CGSpace (CGIAR)

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
| 0 — Cosecha exploratoria OAI-PMH | ✅ Completada |
| 1 — Cosecha REST API (ventana 2024–2025) | ✅ Completada — 377 briefs |
| 2 — Carga SQLite | ✅ Completada |
| 3 — Exploración y primeros outputs | ✅ Completada |
| 4 — Ampliar ventana a 24 meses reales (2023–2025) | ⬜ Pendiente |
| 5 — Análisis trimestral completo | ⬜ Pendiente |
| 6 — Outputs narrativos | ⬜ Pendiente |

---

## Decisiones técnicas tomadas

### Fuente de datos
Se descartó OAI-PMH como fuente principal por dos razones:
- El endpoint OAI (`/oai/request`) devuelve todos los tipos de documentos mezclados, sin posibilidad de filtrar por tipo directamente.
- El formato `oai_dc` no expone los campos CG específicos (`cg.coverage.*`, `cg.contributor.*`).

**Se usa la REST API de DSpace 7** (`/server/api/discover/search/objects`) con el filtro `f.itemtype=Brief,equals`. Ventajas:
- Filtra directamente los 11,242 Briefs del repositorio.
- Devuelve JSON con todos los campos CG completos.
- Paginación simple por `page` y `size`.

**Nota importante:** Los parámetros con comas (ej. `Brief,equals`) no deben pasarse como dict a `requests` — hay que construir la URL como string para evitar re-encoding.

### Endpoint correcto
```
https://cgspace.cgiar.org/server/oai/request     ← OAI-PMH (descartado)
https://cgspace.cgiar.org/server/api/discover/search/objects  ← REST API (usado)
```

### Formatos OAI disponibles (para referencia)
`oai_dc`, `qdc`, `dim`, `xoai`, `uketd_dc`, `didl`

El formato `xoai` expone campos CG pero requiere parseo XML complejo. La REST API es superior para este proyecto.

---

## Alcance

- **Ventana temporal:** últimos 24 meses. Regla de corte: `issued_date >= (hoy − 24 meses)`.
- **Granularidad temporal:** trimestre (Q1–Q4). Campos derivados: `year`, `quarter`, `year_quarter` (ej. `2025Q3`).
- **Definición de Brief:** ítem con `dcterms.type = "Brief"`. El campo `type_raw` siempre se conserva.
- **Total en repositorio:** 11,242 Briefs (al 17/02/2026).

---

## Pipeline

### Script 00 — Exploración OAI-PMH (`00_explore_oai.py`)
Cosecha exploratoria de 100 registros vía OAI-PMH para inspeccionar estructura de campos. **Completado.** Sirvió para descubrir que los campos CG no están disponibles en `oai_dc` y que el tipo `Brief` no aparece en una muestra aleatoria pequeña.

### Script 01 — Cosecha REST (`01_harvest_rest.py`)
Cosecha de Briefs vía REST API con filtro `f.itemtype=Brief,equals`. Guarda cada página como JSON en `data/raw/` y produce un Parquet consolidado en `data/staging/`.

**Parámetros actuales:**
- `PAGE_SIZE = 100`
- `PAUSE_SECS = 3`
- `CUTOFF_DATE = hoy − 730 días`

**Resultado primera cosecha (17/02/2026):**
- 377 briefs capturados (2024–2025)
- 4,523 descartados por ventana temporal
- Archivo: `data/staging/briefs_raw_20260217.parquet`

**Pendiente:** corregir filtro de ventana temporal. Actualmente compara solo el año (`issued_raw[:4] >= CUTOFF_DATE[:4]`), lo que excluye briefs de 2023. Debe cambiarse a comparación de fecha completa (`issued_raw[:10] >= CUTOFF_DATE`).

### Script 02 — Carga SQLite (`02_load_sqlite.py`)
Toma el Parquet más reciente de `data/staging/` y carga las tablas normalizadas en SQLite.

**Resultado primera carga (17/02/2026):**
- briefs: 377
- keywords: 559
- geo: 95
- authors: 949
- funding_entities: 286
- brief_keywords: 1,537
- brief_geo: 900
- brief_funding: 1,463

### Script 03 — Exploración (`03_explore_db.py`)
Consultas exploratorias sobre la base SQLite. Responde las 5 preguntas con los datos disponibles.

---

## Hallazgos principales (primera cosecha)

### Temas dominantes (top keywords)
1. climate change (75)
2. livestock (50)
3. food systems (42)
4. gender (38)
5. agriculture (29)
6. food security (22)
7. farmers / nutrition / value chains (21 c/u)

### Geografía
- **Top países:** Kenya (50), Ethiopia (29), India (22), Tanzania (19), Colombia (15)
- **Top regiones:** Africa (132), Eastern Africa (95), Asia (88), Southern Asia (57)

### Instituciones más activas
- Sustainable Animal Productivity (44 briefs)
- Livestock and Climate (35)
- Gender Equality (30)
- Rethinking Food Markets (18)
- Climate Resilience (15)

### Donors principales
- CGIAR Trust Fund (291 — dominante)
- Bill & Melinda Gates Foundation (7)
- USAID (7)

### SDGs más frecuentes
- SDG 13 - Climate action (104)
- SDG 2 - Zero hunger (61)
- SDG 1 - No poverty (49)

### Series más frecuentes
- IPSR Innovation Profile (23)
- Biennial Review (10)
- CGIAR Climate Impact Platform Daily Briefs SB60 (9)

### Completitud de metadatos
| Campo | % con dato |
|---|---|
| access_rights | 100% |
| language | 96.3% |
| abstract | 67.9% |
| series | 33.2% |

---

## Esquema de base de datos

### `briefs` — tabla principal

| Campo | Tipo | Descripción |
|---|---|---|
| `brief_id` | TEXT PK | Handle DSpace |
| `uuid` | TEXT | UUID interno |
| `uri` | TEXT | URL hdl.handle.net |
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
| `series_raw` | TEXT | Serie (dcterms.isPartOf) |
| `access_rights` | TEXT | Open/Limited Access |
| `license` | TEXT | Licencia CC |
| `cg_number` | TEXT | Número de serie |
| `cg_review_status` | TEXT | Peer Review / Internal |
| `last_harvested_at` | TEXT | Timestamp del pipeline |

Tablas relacionadas: `authors`, `brief_authors`, `keywords`, `brief_keywords`, `geo`, `brief_geo`, `funding_entities`, `brief_funding`, `brief_tags`.

---

## Campos y mapeo (confirmados en producción)

### Campos core

| Campo interno | Fuente REST API |
|---|---|
| Identificador | `handle` (campo raíz del item) |
| UUID | `uuid` (campo raíz) |
| Título | `dc.title` |
| Fecha | `dcterms.issued` |
| Tipo | `dcterms.type` |
| Autores | `dc.contributor.author` |
| Abstract | `dcterms.abstract` |
| Keywords | `dcterms.subject` |
| Idioma | `dcterms.language` |
| Publisher | `dcterms.publisher` |
| Serie | `dcterms.isPartOf` |
| Acceso | `dcterms.accessRights` |
| Licencia | `dcterms.license` |

### Campos CG (confirmados presentes)

| Campo interno | Fuente REST API |
|---|---|
| País | `cg.coverage.country` |
| Región | `cg.coverage.region` |
| Subregión | `cg.coverage.subregion` |
| Donor | `cg.contributor.donor` |
| Proyecto | `cg.identifier.project` |
| Initiative | `cg.contributor.initiative` |
| Program/Accelerator | `cg.contributor.programAccelerator` |
| CRP | `cg.contributor.crp` |
| Afiliación | `cg.contributor.affiliation` |
| Impact Area | `cg.subject.impactArea` |
| Action Area | `cg.subject.actionArea` |
| SDG | `cg.subject.sdg` |
| Número de serie | `cg.number` |
| Review status | `cg.reviewStatus` |

---

## Normalización

- `*_raw`: valor original, siempre conservado.
- `*_norm`: trim + colapsar espacios + minúsculas.
- Campos multi-valor separados por ` | ` en staging; normalizados en tablas relacionales en SQLite.
- Keywords: diccionario de sinónimos pendiente (ej. `gender` ≈ `gender equality` ≈ `gender equity`).
- Países: catálogo controlado pendiente.

---

## Próximos pasos

- [ ] Corregir filtro de ventana temporal en `01_harvest_rest.py` (fecha completa, no solo año)
- [ ] Re-cosechar para capturar briefs de 2023
- [ ] Agregar filtro de fecha en la URL de la API: `f.dateIssued=[2023 TO 2026],equals`
- [ ] Análisis de keywords emergentes por trimestre
- [ ] Análisis de narrativa en abstracts (comparación de frecuencia de términos)
- [ ] Visualizaciones: evolución temporal de keywords, mapa de países

---

## Estructura de carpetas
```
project/
├── README.md
├── data/
│   ├── raw/            # JSON por página (cosecha REST)
│   ├── staging/        # Parquet consolidado
│   ├── db/             # SQLite
│   └── logs/           # Logs de cosecha
├── scripts/
│   ├── 00_explore_oai.py       # Exploración OAI-PMH (completado)
│   ├── 01_harvest_rest.py      # Cosecha REST API
│   ├── 02_load_sqlite.py       # Carga SQLite
│   ├── 03_explore_db.py        # Consultas exploratorias
│   ├── 04_build_marts.py       # Tablas de análisis (pendiente)
│   └── 05_analysis_outputs.py  # Outputs finales (pendiente)
└── outputs/
    ├── tables/
    ├── figures/
    └── notes/
```

---

## Notas técnicas

- Lenguaje: Python 3.x
- Entorno: Git Bash + VSCode en Windows
- Base de datos: SQLite (portable, sin servidor)
- Formato intermedio: Parquet (via pandas + pyarrow)
- Pipeline reproducible y respetuoso con el servidor (pausas de 3s entre páginas)
- Dependencias: `requests`, `pandas`, `lxml`, `pyarrow`

---

*Proyecto en desarrollo. Última actualización: 17 febrero 2026.*
