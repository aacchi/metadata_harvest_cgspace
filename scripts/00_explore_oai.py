"""
00_explore_oai.py — v2
Cosecha exploratoria usando formato xoai, que expone campos CG completos.
Objetivo: ver todos los campos disponibles incluyendo cg.coverage.*, 
cg.contributor.*, y confirmar valores reales de dcterms.type.
"""

import requests
import time
from lxml import etree

# ── Configuración ──────────────────────────────────────────────
BASE_URL        = "https://cgspace.cgiar.org/server/oai/request"
METADATA_PREFIX = "xoai"          # cambiado de oai_dc a xoai
MAX_RECORDS     = 100
PAUSE_SECS      = 3
RETRY_WAIT      = 30
MAX_RETRIES     = 3

def fetch_page(url, params):
    """Hace una petición GET con reintentos ante error 429."""
    headers = {
        "User-Agent": "pipeline_cgspace/0.1 (investigacion personal)"
    }
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"  Fetching... (intento {attempt}/{MAX_RETRIES})")
        response = requests.get(url, params=params, headers=headers, timeout=30)

        if response.status_code == 429:
            wait = RETRY_WAIT * attempt
            print(f"  ⚠ 429. Esperando {wait}s...")
            time.sleep(wait)
            continue

        response.raise_for_status()
        return response.content   # devolvemos bytes para parsear luego

    raise Exception(f"Fallo tras {MAX_RETRIES} intentos.")

def parse_xoai(xml_bytes):
    """
    Parsea el XML xoai y extrae todos los campos como dict:
    { "dc.title": ["valor1"], "cg.coverage.country": ["Kenya", "Ethiopia"], ... }
    """
    tree = etree.fromstring(xml_bytes)

    # Namespace OAI
    OAI_NS  = "http://www.openarchives.org/OAI/2.0/"
    XOAI_NS = "http://www.lyncode.com/xoai"

    records_data = []

    for record in tree.findall(f".//{{{OAI_NS}}}record"):
        # Saltar registros eliminados
        header = record.find(f"{{{OAI_NS}}}header")
        if header is not None and header.get("status") == "deleted":
            continue

        metadata = record.find(f"{{{OAI_NS}}}metadata")
        if metadata is None:
            continue

        # xoai structure: <metadata><repository><metadata><element name="dc">
        #   <element name="title"><field name="value">...</field>
        fields = {}

        # Buscar todos los elementos xoai
        repo_meta = metadata.find(f".//{{{XOAI_NS}}}metadata")
        if repo_meta is None:
            continue

        def walk(element, prefix=""):
            """Recorre recursivamente el árbol xoai construyendo nombres de campo."""
            name = element.get("name", "")
            current = f"{prefix}.{name}" if prefix else name

            # Si tiene hijos <element>, seguir bajando
            children = element.findall(f"{{{XOAI_NS}}}element")
            if children:
                for child in children:
                    walk(child, current)
            else:
                # Nodo hoja: extraer valores de <field name="value">
                for field in element.findall(f"{{{XOAI_NS}}}field"):
                    if field.get("name") == "value" and field.text:
                        value = field.text.strip()
                        if value:
                            if current not in fields:
                                fields[current] = []
                            fields[current].append(value)

        for top_element in repo_meta.findall(f"{{{XOAI_NS}}}element"):
            walk(top_element)

        if fields:
            records_data.append(fields)

    return records_data

def explore():
    print("=== Cosecha exploratoria CGSpace — formato xoai ===\n")
    print(f"Esperando {PAUSE_SECS}s antes de la primera petición...")
    time.sleep(PAUSE_SECS)

    params = {
        "verb":           "ListRecords",
        "metadataPrefix": METADATA_PREFIX,
    }

    all_records  = []
    field_index  = {}   # campo → set de valores únicos
    page_num     = 0

    while len(all_records) < MAX_RECORDS:
        page_num += 1
        print(f"\nPágina {page_num} — registros acumulados: {len(all_records)}")

        xml_bytes = fetch_page(BASE_URL, params)
        records   = parse_xoai(xml_bytes)
        print(f"  Registros parseados esta página: {len(records)}")

        for rec in records:
            if len(all_records) >= MAX_RECORDS:
                break
            all_records.append(rec)
            for field, values in rec.items():
                if field not in field_index:
                    field_index[field] = set()
                for v in values:
                    field_index[field].add(v)

        # Paginación
        tree     = etree.fromstring(xml_bytes)
        OAI_NS   = "http://www.openarchives.org/OAI/2.0/"
        token_el = tree.find(f".//{{{OAI_NS}}}resumptionToken")
        if token_el is None or not token_el.text:
            print("  No hay más páginas.")
            break

        params = {"verb": "ListRecords", "resumptionToken": token_el.text}
        print(f"  Pausando {PAUSE_SECS}s...")
        time.sleep(PAUSE_SECS)

    # ── Reporte general ────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"REPORTE — {len(all_records)} registros, {len(field_index)} campos únicos")
    print(f"{'='*60}\n")

    # Ordenar: primero campos CG, luego el resto
    cg_fields  = sorted([f for f in field_index if f.startswith("cg")])
    dc_fields  = sorted([f for f in field_index if not f.startswith("cg")])

    print("── Campos CG (específicos CGIAR) ──────────────────────")
    for field in cg_fields:
        values = field_index[field]
        print(f"\n[{field}] — {len(values)} valores únicos")
        for v in list(values)[:8]:
            print(f"    · {v}")

    print("\n── Campos DC / otros ───────────────────────────────────")
    for field in dc_fields:
        values = field_index[field]
        print(f"\n[{field}] — {len(values)} valores únicos")
        for v in list(values)[:5]:
            print(f"    · {v}")

    # ── Foco: valores de type ──────────────────────────────────
    print(f"\n{'='*60}")
    print("FOCO: valores de dcterms.type y dc.type")
    print(f"{'='*60}")
    for field in ["dcterms.type", "dc.type"]:
        if field in field_index:
            print(f"\n[{field}]")
            for v in sorted(field_index[field]):
                print(f"    · {v}")
        else:
            print(f"\n[{field}] — no encontrado en esta muestra")

if __name__ == "__main__":
    explore()