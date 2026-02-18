# pipeline_cgspace
Análisis de metadatos de Briefs publicados en CGSpace (CGIAR)

## Objetivo

Construir una base de datos a partir de los Briefs disponibles en [CGSpace](https://cgspace.cgiar.org) (repositorio institucional del CGIAR) para responder cinco preguntas:

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
| 1 — Cosecha REST API con filtros de fecha | ✅ Completada — 1,709 briefs |
| 2 — Carga SQLite | ✅ Completada |
| 3 — Normalización de datos | ✅ Completada |
| 4 — Exploración y primeros outputs | ✅ Completada |
| 5 — Análisis temporal y temático | ⬜ Pendiente |
| 6 — Outputs narrativos y visualizaciones | ⬜ Pendiente |

*Proyecto personal de investigación — última actualización: 17 febrero 2026*
