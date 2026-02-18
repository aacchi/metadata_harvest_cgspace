"""
01_harvest_rest.py
Cosecha de Briefs desde la REST API de DSpace 7 (CGSpace).
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# ── Configuración ──────────────────────────────────────────────
BASE_URL    = "https://cgspace.cgiar.org/server/api/discover/search/objects"
PAGE_SIZE   = 100
PAUSE_SECS  = 3
RETRY_WAIT  = 60
MAX_RETRIES = 5

CUTOFF_DATE = (datetime.today() - timedelta(days=730)).strftime("%Y-%m-%d")

RAW_DIR     = Path("data/raw")
STAGING_DIR = Path("data/staging")
LOG_DIR     = Path("data/logs")
TODAY       = datetime.today().strftime("%Y%m%d")

FIELDS_TO_EXTRACT = [
    "dc.title",
    "dcterms.type",
    "dcterms.issued",
    "dcterms.abstract",
    "dcterms.language",
    "dcterms.publisher",
    "dcterms.isPartOf",
    "dcterms.accessRights",
    "dcterms.license",
    "dcterms.subject",
    "dc.contributor.author",
    "dc.identifier.uri",
    "cg.coverage.country",
    "cg.coverage.region",
    "cg.coverage.subregion",
    "cg.contributor.donor",
    "cg.contributor.initiative",
    "cg.contributor.programAccelerator",
    "cg.contributor.crp",
    "cg.contributor.affiliation",
    "cg.identifier.project",
    "cg.subject.actionArea",
    "cg.subject.impactArea",
    "cg.subject.sdg",
    "cg.number",
    "cg.reviewStatus",
]

# ── Logging ────────────────────────────────────────────────────
LOG_PATH = LOG_DIR / f"harvest_{TODAY}.log"

def log(msg):
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ── HTTP con reintentos ────────────────────────────────────────
def get_json(url):
    """GET con reintentos. La URL se pasa completa para evitar
    que requests re-codifique las comas de los filtros."""
    headers = {"User-Agent": "pipeline_cgspace/0.1 (investigacion personal)"}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 429:
                wait = RETRY_WAIT * attempt
                log(f"  429. Esperando {wait}s... (intento {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            log(f"  Error intento {attempt}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(10)
    raise Exception(f"Fallo tras {MAX_RETRIES} intentos en {url}")

# ── Extraer campos de un registro ─────────────────────────────
def extract_record(item):
    meta = item.get("metadata", {})
    row  = {
        "brief_id" : item.get("handle", ""),
        "uuid"     : item.get("uuid", ""),
        "uri"      : "",
    }

    uri_list = meta.get("dc.identifier.uri", [])
    if uri_list:
        row["uri"] = uri_list[0].get("value", "")

    for field in FIELDS_TO_EXTRACT:
        values    = meta.get(field, [])
        extracted = [v.get("value", "") for v in values if v.get("value")]

        if field == "dc.title":
            row["title"] = extracted[0] if extracted else ""
        elif field == "dcterms.type":
            row["type_raw"] = extracted[0] if extracted else ""
        elif field == "dcterms.issued":
            row["issued_date"] = extracted[0] if extracted else ""
        elif field == "dcterms.abstract":
            row["abstract"] = extracted[0] if extracted else ""
        elif field == "dcterms.language":
            row["language"] = extracted[0] if extracted else ""
        elif field == "dcterms.publisher":
            row["publisher"] = " | ".join(extracted)
        elif field == "dcterms.isPartOf":
            row["series_raw"] = " | ".join(extracted)
        elif field == "dcterms.accessRights":
            row["access_rights"] = extracted[0] if extracted else ""
        elif field == "dcterms.license":
            row["license"] = extracted[0] if extracted else ""
        elif field == "dc.identifier.uri":
            pass
        else:
            key = field.replace(".", "_").replace("-", "_")
            row[key] = " | ".join(extracted)

    return row

# ── Parsear fecha ──────────────────────────────────────────────
def parse_issued_date(date_str):
    if not date_str:
        return None, None, None, None
    for fmt, length in [("%Y-%m-%d", 10), ("%Y-%m", 7), ("%Y", 4)]:
        try:
            d       = datetime.strptime(date_str[:length], fmt)
            year    = d.year
            quarter = (d.month - 1) // 3 + 1
            return date_str, year, quarter, f"{year}Q{quarter}"
        except ValueError:
            continue
    return date_str, None, None, None

# ── Cosecha principal ──────────────────────────────────────────
def harvest():
    log("=" * 60)
    log(f"Inicio cosecha REST — cutoff: {CUTOFF_DATE}")
    log("=" * 60)

    all_rows    = []
    page_num    = 0
    total_pages = None
    total_items = None
    skipped     = 0

    time.sleep(PAUSE_SECS)

    while True:
        # Construir URL completa sin que requests toque los parámetros
        cutoff_year = CUTOFF_DATE[:4]
        url = (f"{BASE_URL}"
               f"?f.itemtype=Brief,equals"
                f"&f.dateIssued=[{cutoff_year} TO 2026],equals"
               f"&size={PAGE_SIZE}"
               f"&page={page_num}")

        log(f"\nPágina {page_num + 1}" +
            (f"/{total_pages}" if total_pages else "") +
            f" — acumulados: {len(all_rows)}")

        data = get_json(url)

        search_result = data.get("_embedded", {}).get("searchResult", {})
        page_info     = search_result.get("page", {})

        if total_pages is None:
            total_pages = page_info.get("totalPages", 0)
            total_items = page_info.get("totalElements", 0)
            log(f"  Total Briefs: {total_items} | Páginas: {total_pages}")

        # Guardar JSON crudo
        raw_path = RAW_DIR / f"briefs_p{page_num:04d}_{TODAY}.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Procesar registros
        objects = (search_result
                   .get("_embedded", {})
                   .get("objects", []))

        if not objects:
            log("  Sin registros. Fin.")
            break

        page_added   = 0
        page_skipped = 0

        for obj in objects:
            item = obj.get("_embedded", {}).get("indexableObject", {})
            if not item:
                continue

            row        = extract_record(item)
            issued_raw = row.get("issued_date", "")
            _, year, quarter, year_quarter = parse_issued_date(issued_raw)

            # Filtro ventana temporal
            in_window = False
            if issued_raw and len(issued_raw) >= 4:
                in_window = issued_raw[:10] >= CUTOFF_DATE

            if in_window:
                row["year"]              = year
                row["quarter"]           = quarter
                row["year_quarter"]      = year_quarter
                row["brief_flag"]        = 1
                row["last_harvested_at"] = datetime.now().isoformat()
                all_rows.append(row)
                page_added += 1
            else:
                page_skipped += 1
                skipped += 1

        log(f"  Añadidos: {page_added} | Fuera de ventana: {page_skipped}")

        # Parar si toda la página está fuera de ventana
        if page_skipped == len(objects) and page_num > 5:
            log("  Página completa fuera de ventana. Deteniendo.")
            break

        if page_num + 1 >= total_pages:
            log("  Última página alcanzada.")
            break

        page_num += 1
        log(f"  Pausando {PAUSE_SECS}s...")
        time.sleep(PAUSE_SECS)

    # ── Guardar staging ────────────────────────────────────────
    log(f"\n{'='*60}")
    log(f"Cosecha completada: {len(all_rows)} registros | {skipped} descartados")

    if all_rows:
        df       = pd.DataFrame(all_rows)
        out_path = STAGING_DIR / f"briefs_raw_{TODAY}.parquet"
        df.to_parquet(out_path, index=False)
        log(f"Guardado: {out_path}")
        log(f"Columnas: {list(df.columns)}")
        log(f"Rango fechas: {df['issued_date'].min()} → {df['issued_date'].max()}")
        log(f"Registros por año:\n"
            f"{df['year'].value_counts().sort_index().to_string()}")
    else:
        log("⚠ Sin registros para guardar.")

if __name__ == "__main__":
    harvest()