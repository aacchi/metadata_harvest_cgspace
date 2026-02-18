"""
03_explore_db.py
Primeras consultas exploratorias sobre la base SQLite.
Responde las 5 preguntas del proyecto con los datos disponibles.
"""

import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/db/cgspace_briefs.sqlite")

def q(conn, sql, title=""):
    """Ejecuta una query y muestra el resultado."""
    if title:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
    df = pd.read_sql_query(sql, conn)
    print(df.to_string(index=False))
    return df

def main():
    conn = sqlite3.connect(DB_PATH)

    # ── 1. Volumen por trimestre ───────────────────────────────
    q(conn, """
        SELECT year_quarter,
               COUNT(*) as n_briefs
        FROM   briefs
        WHERE  year_quarter IS NOT NULL
        GROUP  BY year_quarter
        ORDER  BY year_quarter
    """, "1. Briefs por trimestre")

    # ── 2. Top 20 keywords ────────────────────────────────────
    q(conn, """
        SELECT k.keyword_norm,
               COUNT(*) as n_briefs
        FROM   brief_keywords bk
        JOIN   keywords k ON bk.keyword_id = k.keyword_id
        GROUP  BY k.keyword_norm
        ORDER  BY n_briefs DESC
        LIMIT  20
    """, "2. Top 20 keywords")

    # ── 3. Top 15 países ──────────────────────────────────────
    q(conn, """
        SELECT g.value_norm as country,
               COUNT(*) as n_briefs
        FROM   brief_geo bg
        JOIN   geo g ON bg.geo_id = g.geo_id
        WHERE  g.geo_type = 'country'
        GROUP  BY g.value_norm
        ORDER  BY n_briefs DESC
        LIMIT  15
    """, "3. Top 15 países")

    # ── 4. Top 10 regiones ────────────────────────────────────
    q(conn, """
        SELECT g.value_norm as region,
               COUNT(*) as n_briefs
        FROM   brief_geo bg
        JOIN   geo g ON bg.geo_id = g.geo_id
        WHERE  g.geo_type = 'region'
        GROUP  BY g.value_norm
        ORDER  BY n_briefs DESC
        LIMIT  10
    """, "4. Top 10 regiones")

    # ── 5. Top iniciativas ────────────────────────────────────
    q(conn, """
        SELECT fe.entity_raw,
               COUNT(*) as n_briefs
        FROM   brief_funding bf
        JOIN   funding_entities fe ON bf.entity_id = fe.entity_id
        WHERE  fe.entity_type = 'initiative'
        GROUP  BY fe.entity_raw
        ORDER  BY n_briefs DESC
        LIMIT  15
    """, "5. Top iniciativas")

    # ── 6. Top programas/aceleradores ─────────────────────────
    q(conn, """
        SELECT fe.entity_raw,
               COUNT(*) as n_briefs
        FROM   brief_funding bf
        JOIN   funding_entities fe ON bf.entity_id = fe.entity_id
        WHERE  fe.entity_type = 'programAccelerator'
        GROUP  BY fe.entity_raw
        ORDER  BY n_briefs DESC
        LIMIT  10
    """, "6. Top programas/aceleradores")

    # ── 7. Top donors ─────────────────────────────────────────
    q(conn, """
        SELECT fe.entity_raw,
               COUNT(*) as n_briefs
        FROM   brief_funding bf
        JOIN   funding_entities fe ON bf.entity_id = fe.entity_id
        WHERE  fe.entity_type = 'donor'
        GROUP  BY fe.entity_raw
        ORDER  BY n_briefs DESC
        LIMIT  10
    """, "7. Top donors")

    # ── 8. Top SDGs ───────────────────────────────────────────
    q(conn, """
        SELECT tag_value,
               COUNT(*) as n_briefs
        FROM   brief_tags
        WHERE  tag_type = 'sdg'
        GROUP  BY tag_value
        ORDER  BY n_briefs DESC
    """, "8. SDGs")

    # ── 9. Top impact areas ───────────────────────────────────
    q(conn, """
        SELECT tag_value,
               COUNT(*) as n_briefs
        FROM   brief_tags
        WHERE  tag_type = 'impactArea'
        GROUP  BY tag_value
        ORDER  BY n_briefs DESC
    """, "9. Impact areas")

    # ── 10. Keywords por trimestre (top 5 por trimestre) ──────
    q(conn, """
        SELECT b.year_quarter,
               k.keyword_norm,
               COUNT(*) as n
        FROM   brief_keywords bk
        JOIN   briefs b  ON bk.brief_id   = b.brief_id
        JOIN   keywords k ON bk.keyword_id = k.keyword_id
        WHERE  b.year_quarter IS NOT NULL
        GROUP  BY b.year_quarter, k.keyword_norm
        HAVING COUNT(*) >= 3
        ORDER  BY b.year_quarter, n DESC
    """, "10. Keywords por trimestre (frecuencia >= 3)")

    # ── 11. Series más frecuentes ─────────────────────────────
    q(conn, """
        SELECT series_raw,
               COUNT(*) as n_briefs
        FROM   briefs
        WHERE  series_raw IS NOT NULL AND series_raw != ''
        GROUP  BY series_raw
        ORDER  BY n_briefs DESC
        LIMIT  15
    """, "11. Series más frecuentes")

    # ── 12. Completitud de metadatos ──────────────────────────
    q(conn, """
        SELECT
            COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN abstract     != '' THEN 1 END) / COUNT(*), 1) as pct_abstract,
            ROUND(100.0 * SUM(CASE WHEN language     != '' THEN 1 END) / COUNT(*), 1) as pct_language,
            ROUND(100.0 * SUM(CASE WHEN series_raw   != '' THEN 1 END) / COUNT(*), 1) as pct_series,
            ROUND(100.0 * SUM(CASE WHEN access_rights!= '' THEN 1 END) / COUNT(*), 1) as pct_access
        FROM briefs
    """, "12. Completitud general de metadatos")

    q(conn, """
        SELECT
            b.year,
            COUNT(DISTINCT b.brief_id)                                          as total_briefs,
            COUNT(DISTINCT bg.brief_id)                                         as con_pais,
            COUNT(DISTINCT bf.brief_id)                                         as con_funding,
            COUNT(DISTINCT bk.brief_id)                                         as con_keywords
        FROM      briefs b
        LEFT JOIN brief_geo      bg ON b.brief_id = bg.brief_id
        LEFT JOIN brief_funding  bf ON b.brief_id = bf.brief_id
        LEFT JOIN brief_keywords bk ON b.brief_id = bk.brief_id
        GROUP BY  b.year
        ORDER BY  b.year
    """, "13. Completitud por año")

    conn.close()

if __name__ == "__main__":
    main()