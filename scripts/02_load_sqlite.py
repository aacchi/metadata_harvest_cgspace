"""
02_load_sqlite.py
Carga el Parquet de staging a SQLite.
Crea las tablas normalizadas: briefs, keywords, geo, authors,
funding_entities con sus tablas de relación.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Rutas ──────────────────────────────────────────────────────
STAGING_DIR = Path("data/staging")
DB_PATH     = Path("data/db/cgspace_briefs.sqlite")
TODAY       = datetime.today().strftime("%Y%m%d")

# Tomar el Parquet más reciente
parquet_files = sorted(STAGING_DIR.glob("briefs_raw_*.parquet"))
if not parquet_files:
    raise FileNotFoundError("No hay archivos Parquet en data/staging/")
PARQUET_PATH = parquet_files[-1]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── Crear esquema ──────────────────────────────────────────────
SCHEMA = """
-- Tabla principal
CREATE TABLE IF NOT EXISTS briefs (
    brief_id            TEXT PRIMARY KEY,
    uuid                TEXT,
    uri                 TEXT,
    title               TEXT,
    issued_date         TEXT,
    year                INTEGER,
    quarter             INTEGER,
    year_quarter        TEXT,
    type_raw            TEXT,
    brief_flag          INTEGER DEFAULT 1,
    abstract            TEXT,
    language            TEXT,
    publisher           TEXT,
    series_raw          TEXT,
    access_rights       TEXT,
    license             TEXT,
    cg_number           TEXT,
    cg_review_status    TEXT,
    last_harvested_at   TEXT
);

-- Keywords
CREATE TABLE IF NOT EXISTS keywords (
    keyword_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword_raw  TEXT UNIQUE,
    keyword_norm TEXT
);

CREATE TABLE IF NOT EXISTS brief_keywords (
    brief_id   TEXT REFERENCES briefs(brief_id),
    keyword_id INTEGER REFERENCES keywords(keyword_id),
    PRIMARY KEY (brief_id, keyword_id)
);

-- Geografía
CREATE TABLE IF NOT EXISTS geo (
    geo_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    geo_type     TEXT,  -- country | region | subregion
    value_raw    TEXT,
    value_norm   TEXT,
    UNIQUE (geo_type, value_raw)
);

CREATE TABLE IF NOT EXISTS brief_geo (
    brief_id TEXT REFERENCES briefs(brief_id),
    geo_id   INTEGER REFERENCES geo(geo_id),
    PRIMARY KEY (brief_id, geo_id)
);

-- Autores
CREATE TABLE IF NOT EXISTS authors (
    author_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    author_name_raw  TEXT UNIQUE,
    author_name_norm TEXT
);

CREATE TABLE IF NOT EXISTS brief_authors (
    brief_id    TEXT REFERENCES briefs(brief_id),
    author_id   INTEGER REFERENCES authors(author_id),
    author_order INTEGER,
    PRIMARY KEY (brief_id, author_id)
);

-- Entidades de financiación / programa
CREATE TABLE IF NOT EXISTS funding_entities (
    entity_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT,  -- donor | initiative | programAccelerator | crp | project | affiliation
    entity_raw  TEXT,
    entity_norm TEXT,
    UNIQUE (entity_type, entity_raw)
);

CREATE TABLE IF NOT EXISTS brief_funding (
    brief_id  TEXT REFERENCES briefs(brief_id),
    entity_id INTEGER REFERENCES funding_entities(entity_id),
    PRIMARY KEY (brief_id, entity_id)
);

-- SDGs e impact areas (campos simples multi-valor)
CREATE TABLE IF NOT EXISTS brief_tags (
    brief_id  TEXT REFERENCES briefs(brief_id),
    tag_type  TEXT,  -- sdg | impactArea | actionArea
    tag_value TEXT,
    PRIMARY KEY (brief_id, tag_type, tag_value)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_briefs_year_quarter  ON briefs(year_quarter);
CREATE INDEX IF NOT EXISTS idx_briefs_year          ON briefs(year);
CREATE INDEX IF NOT EXISTS idx_briefs_issued        ON briefs(issued_date);
CREATE INDEX IF NOT EXISTS idx_brief_keywords_bid   ON brief_keywords(brief_id);
CREATE INDEX IF NOT EXISTS idx_brief_keywords_kid   ON brief_keywords(keyword_id);
CREATE INDEX IF NOT EXISTS idx_keywords_norm        ON keywords(keyword_norm);
CREATE INDEX IF NOT EXISTS idx_brief_geo_bid        ON brief_geo(brief_id);
CREATE INDEX IF NOT EXISTS idx_geo_type_norm        ON geo(geo_type, value_norm);
CREATE INDEX IF NOT EXISTS idx_brief_funding_bid    ON brief_funding(brief_id);
CREATE INDEX IF NOT EXISTS idx_funding_type         ON funding_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_brief_tags           ON brief_tags(tag_type, tag_value);
"""

# ── Helpers ────────────────────────────────────────────────────
def split_multi(value):
    """Divide campos multi-valor separados por ' | '."""
    if not value or pd.isna(value):
        return []
    return [v.strip() for v in str(value).split("|") if v.strip()]

def norm(text):
    """Normalización básica: minúsculas + trim."""
    if not text:
        return ""
    return str(text).strip().lower()

# ── Carga ──────────────────────────────────────────────────────
def load(conn, df):
    cur = conn.cursor()

    log(f"Cargando {len(df)} registros...")

    for _, row in df.iterrows():
        bid = row.get("brief_id", "")
        if not bid:
            continue

        # ── briefs ──────────────────────────────────────────
        cur.execute("""
            INSERT OR REPLACE INTO briefs
            (brief_id, uuid, uri, title, issued_date, year, quarter,
             year_quarter, type_raw, brief_flag, abstract, language,
             publisher, series_raw, access_rights, license,
             cg_number, cg_review_status, last_harvested_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            bid,
            row.get("uuid", ""),
            row.get("uri", ""),
            row.get("title", ""),
            row.get("issued_date", ""),
            row.get("year"),
            row.get("quarter"),
            row.get("year_quarter", ""),
            row.get("type_raw", ""),
            row.get("brief_flag", 1),
            row.get("abstract", ""),
            row.get("language", ""),
            row.get("publisher", ""),
            row.get("series_raw", ""),
            row.get("access_rights", ""),
            row.get("license", ""),
            row.get("cg_number", ""),
            row.get("cg_reviewStatus", ""),
            row.get("last_harvested_at", ""),
        ))

        # ── keywords ────────────────────────────────────────
        for kw in split_multi(row.get("dcterms_subject", "")):
            kw_norm = norm(kw)
            cur.execute("""
                INSERT OR IGNORE INTO keywords (keyword_raw, keyword_norm)
                VALUES (?, ?)
            """, (kw, kw_norm))
            cur.execute("SELECT keyword_id FROM keywords WHERE keyword_raw = ?", (kw,))
            kid = cur.fetchone()[0]
            cur.execute("""
                INSERT OR IGNORE INTO brief_keywords (brief_id, keyword_id)
                VALUES (?, ?)
            """, (bid, kid))

        # ── geografía ────────────────────────────────────────
        for geo_type, col in [
            ("country",    "cg_coverage_country"),
            ("region",     "cg_coverage_region"),
            ("subregion",  "cg_coverage_subregion"),
        ]:
            for val in split_multi(row.get(col, "")):
                val_norm = norm(val)
                cur.execute("""
                    INSERT OR IGNORE INTO geo (geo_type, value_raw, value_norm)
                    VALUES (?, ?, ?)
                """, (geo_type, val, val_norm))
                cur.execute("""
                    SELECT geo_id FROM geo
                    WHERE geo_type = ? AND value_raw = ?
                """, (geo_type, val))
                gid = cur.fetchone()[0]
                cur.execute("""
                    INSERT OR IGNORE INTO brief_geo (brief_id, geo_id)
                    VALUES (?, ?)
                """, (bid, gid))

        # ── autores ──────────────────────────────────────────
        for order, author in enumerate(split_multi(row.get("dc_contributor_author", ""))):
            author_norm = norm(author)
            cur.execute("""
                INSERT OR IGNORE INTO authors (author_name_raw, author_name_norm)
                VALUES (?, ?)
            """, (author, author_norm))
            cur.execute("""
                SELECT author_id FROM authors WHERE author_name_raw = ?
            """, (author,))
            aid = cur.fetchone()[0]
            cur.execute("""
                INSERT OR IGNORE INTO brief_authors (brief_id, author_id, author_order)
                VALUES (?, ?, ?)
            """, (bid, aid, order))

        # ── entidades de financiación ─────────────────────────
        funding_cols = {
            "donor"              : "cg_contributor_donor",
            "initiative"         : "cg_contributor_initiative",
            "programAccelerator" : "cg_contributor_programAccelerator",
            "crp"                : "cg_contributor_crp",
            "project"            : "cg_identifier_project",
            "affiliation"        : "cg_contributor_affiliation",
        }
        for etype, col in funding_cols.items():
            for val in split_multi(row.get(col, "")):
                val_norm = norm(val)
                cur.execute("""
                    INSERT OR IGNORE INTO funding_entities
                    (entity_type, entity_raw, entity_norm)
                    VALUES (?, ?, ?)
                """, (etype, val, val_norm))
                cur.execute("""
                    SELECT entity_id FROM funding_entities
                    WHERE entity_type = ? AND entity_raw = ?
                """, (etype, val))
                eid = cur.fetchone()[0]
                cur.execute("""
                    INSERT OR IGNORE INTO brief_funding (brief_id, entity_id)
                    VALUES (?, ?)
                """, (bid, eid))

        # ── tags: SDG, impactArea, actionArea ─────────────────
        tag_cols = {
            "sdg"        : "cg_subject_sdg",
            "impactArea" : "cg_subject_impactArea",
            "actionArea" : "cg_subject_actionArea",
        }
        for tag_type, col in tag_cols.items():
            for val in split_multi(row.get(col, "")):
                cur.execute("""
                    INSERT OR IGNORE INTO brief_tags (brief_id, tag_type, tag_value)
                    VALUES (?, ?, ?)
                """, (bid, tag_type, val))

    conn.commit()

# ── Main ───────────────────────────────────────────────────────
def main():
    log(f"Leyendo: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    log(f"Registros en staging: {len(df)}")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    log(f"Conectando a: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)

    load(conn, df)
    conn.close()

    log("✓ Carga completada.")

    # Reporte rápido
    conn = sqlite3.connect(DB_PATH)
    for table in ["briefs", "keywords", "geo", "authors",
                  "funding_entities", "brief_keywords",
                  "brief_geo", "brief_funding"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log(f"  {table}: {n} filas")
    conn.close()

if __name__ == "__main__":
    main()