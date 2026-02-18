"""
04_normalize.py
Normalización de valores inconsistentes en la base SQLite.
Corrige: SDGs, impact areas, action areas y keywords duplicadas.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/cgspace_briefs.sqlite")

def log(msg):
    print(msg)

# ── Diccionarios de normalización ──────────────────────────────

# SDGs: mapear variantes al valor canónico
SDG_MAP = {
    # Mayúsculas/minúsculas
    "SDG 1 - No Poverty"                                  : "SDG 1 - No poverty",
    "SDG 2 - Zero Hunger"                                 : "SDG 2 - Zero hunger",
    "SDG 3 - Good Health and Well-Being"                  : "SDG 3 - Good health and well-being",
    "SDG 4 - Quality Education"                           : "SDG 4 - Quality education",
    "SDG 5 - Gender Equality"                             : "SDG 5 - Gender equality",
    "SDG 6 - Clean Water and Sanitation"                  : "SDG 6 - Clean water and sanitation",
    "SDG 7 - Affordable and Clean Energy"                 : "SDG 7 - Affordable and clean energy",
    "SDG 8 - Decent Work and Economic Growth"             : "SDG 8 - Decent work and economic growth",
    "SDG 9 - Industry, Innovation and Infrastructure"     : "SDG 9 - Industry, innovation and infrastructure",
    "SDG 10 - Reduce Inequalities"                        : "SDG 10 - Reduced inequalities",
    "SDG 10 - Reduced Inequality"                         : "SDG 10 - Reduced inequalities",
    "SDG 11 - Sustainable Cities and Communities"         : "SDG 11 - Sustainable cities and communities",
    "SDG 12 - Responsible Consumption and Production"     : "SDG 12 - Responsible consumption and production",
    "SDG 12 - Responsible production and consumption"     : "SDG 12 - Responsible consumption and production",
    "SDG 13 - Climate Action"                             : "SDG 13 - Climate action",
    "SDG 14 - Life Below Water"                           : "SDG 14 - Life below water",
    "SDG 15 - Life on Land"                               : "SDG 15 - Life on land",
    "SDG 16 - Peace, Justice and Strong Institutions"     : "SDG 16 - Peace, justice and strong institutions",
    "SDG 17 - Partnerships for the Goals"                 : "SDG 17 - Partnerships for the goals",
}

# Impact areas: mapear variantes al valor canónico
IMPACT_MAP = {
    "Climate adaptation & mitigation"           : "Climate adaptation and mitigation",
    "Environmental health & biodiversity"       : "Environmental health and biodiversity",
    "Nutrition, health & food security"         : "Nutrition, health and food security",
    "Nutrition, health, and food security"      : "Nutrition, health and food security",
    "Poverty reduction, livelihoods & jobs"     : "Poverty reduction, livelihoods and jobs",
    "Gender equality, youth & social inclusion" : "Gender equality, youth and social inclusion",
}

# Keywords: fusionar variantes del mismo concepto
KEYWORD_MAP = {
    # climate change
    "climatic change"                           : "climate change",
    "cambio climático"                          : "climate change",
    "climate change impacts"                    : "climate change",
    "climate change impact"                     : "climate change",
    # climate smart agriculture
    "climate-smart agriculture"                 : "climate smart agriculture",
    "climate smart agriculture-climate smart agriculture" : "climate smart agriculture",
    # gender
    "gender equity"                             : "gender equality",
    "gender mainstreaming"                      : "gender equality",
    "gender-responsive approaches"              : "gender equality",
    "gender-transformative approaches"          : "gender equality",
    # food security
    "seguridad alimentaria"                     : "food security",
    "food insecurity"                           : "food security",
    # agrifood
    "agrifood system"                           : "agrifood systems",
    "sistema alimentario"                       : "agrifood systems",
    # climate resilience
    "resiliencia al clima"                      : "climate resilience",
    # value chains
    "cadena de valor"                           : "value chains",
    # livestock
    "ganadería"                                 : "livestock",
    "livestock production"                      : "livestock",
    "livestock systems"                         : "livestock",
    # sustainability
    "sostenibilidad"                            : "sustainability",
    "sustainable development"                   : "sustainability",
    # agroecology
    "agroecología"                              : "agroecology",
    # deforestation
    "deforestación"                             : "deforestation",
    # nutrition
    "nutrición"                                 : "nutrition",
    "malnutrition"                              : "nutrition",
    # capacity
    "capacity building"                         : "capacity development",
    "capacity development-capacity building"    : "capacity development",
    # innovation scaling
    "innovation scaling"                        : "scaling of innovations",
    "innovation scaling-scaling of innovations" : "scaling of innovations",
    # climate services
    "climate services-climate information services" : "climate services",
    # climate finance
    "financiación relacionada con el cambio climático" : "climate finance",
    # monitoring
    "seguimiento y evaluación"                  : "monitoring and evaluation",
    # evaluation
    "evaluación"                                : "evaluation",
    "evaluación de capacidades"                 : "capacity assessment",
    # mitigation
    "mitigación del cambio climático"           : "climate change mitigation",
    "gas de efecto invernadero"                 : "greenhouse gas emissions",
    # project
    "proyecto"                                  : "project design",
    # women
    "women farmers"                             : "women",
    "women's empowerment"                       : "empowerment",
    "women's participation"                     : "women",
    # decision making
    "decision-making"                           : "decision making",
    "decision support systems"                  : "decision-support systems",
    # milk
    "leche"                                     : "milk",
}

# ── Aplicar normalizaciones ────────────────────────────────────

def normalize_tags(conn, tag_type, mapping):
    """Actualiza brief_tags reemplazando variantes por valor canónico."""
    cur = conn.cursor()
    fixed = 0
    for variant, canonical in mapping.items():
        # Buscar si existe la variante
        cur.execute("""
            SELECT COUNT(*) FROM brief_tags
            WHERE tag_type = ? AND tag_value = ?
        """, (tag_type, variant))
        n = cur.fetchone()[0]
        if n == 0:
            continue

        # Para cada brief que tiene la variante, insertar el canónico
        # (si no lo tiene ya) y luego borrar la variante
        cur.execute("""
            SELECT brief_id FROM brief_tags
            WHERE tag_type = ? AND tag_value = ?
        """, (tag_type, variant))
        brief_ids = [r[0] for r in cur.fetchall()]

        for bid in brief_ids:
            cur.execute("""
                INSERT OR IGNORE INTO brief_tags (brief_id, tag_type, tag_value)
                VALUES (?, ?, ?)
            """, (bid, tag_type, canonical))

        cur.execute("""
            DELETE FROM brief_tags
            WHERE tag_type = ? AND tag_value = ?
        """, (tag_type, variant))

        fixed += n
        log(f"  [{tag_type}] '{variant}' → '{canonical}' ({n} registros)")

    return fixed

def normalize_keywords(conn, mapping):
    """Fusiona keywords variantes en la tabla keywords y brief_keywords."""
    cur = conn.cursor()
    fixed = 0

    for variant, canonical in mapping.items():
        # Verificar que la variante existe
        cur.execute("""
            SELECT keyword_id FROM keywords WHERE keyword_norm = ?
        """, (variant,))
        row = cur.fetchone()
        if not row:
            continue
        variant_id = row[0]

        # Obtener o crear el keyword canónico
        cur.execute("""
            SELECT keyword_id FROM keywords WHERE keyword_norm = ?
        """, (canonical,))
        row = cur.fetchone()
        if row:
            canonical_id = row[0]
        else:
            cur.execute("""
                INSERT INTO keywords (keyword_raw, keyword_norm)
                VALUES (?, ?)
            """, (canonical, canonical))
            canonical_id = cur.lastrowid

        # Reasignar brief_keywords de variante a canónico
        cur.execute("""
            SELECT brief_id FROM brief_keywords WHERE keyword_id = ?
        """, (variant_id,))
        brief_ids = [r[0] for r in cur.fetchall()]

        n = len(brief_ids)
        for bid in brief_ids:
            cur.execute("""
                INSERT OR IGNORE INTO brief_keywords (brief_id, keyword_id)
                VALUES (?, ?)
            """, (bid, canonical_id))

        # Borrar relaciones y keyword variante
        cur.execute("""
            DELETE FROM brief_keywords WHERE keyword_id = ?
        """, (variant_id,))
        cur.execute("""
            DELETE FROM keywords WHERE keyword_id = ?
        """, (variant_id,))

        fixed += n
        if n > 0:
            log(f"  [keyword] '{variant}' → '{canonical}' ({n} briefs)")

    return fixed

def report(conn):
    """Muestra conteos post-normalización."""
    cur = conn.cursor()

    log("\n── SDGs post-normalización ─────────────────────────────")
    cur.execute("""
        SELECT tag_value, COUNT(*) as n
        FROM   brief_tags
        WHERE  tag_type = 'sdg'
        GROUP  BY tag_value
        ORDER  BY n DESC
    """)
    for row in cur.fetchall():
        log(f"  {row[1]:4d}  {row[0]}")

    log("\n── Impact areas post-normalización ─────────────────────")
    cur.execute("""
        SELECT tag_value, COUNT(*) as n
        FROM   brief_tags
        WHERE  tag_type = 'impactArea'
        GROUP  BY tag_value
        ORDER  BY n DESC
    """)
    for row in cur.fetchall():
        log(f"  {row[1]:4d}  {row[0]}")

    log("\n── Top 30 keywords post-normalización ──────────────────")
    cur.execute("""
        SELECT k.keyword_norm, COUNT(*) as n
        FROM   brief_keywords bk
        JOIN   keywords k ON bk.keyword_id = k.keyword_id
        GROUP  BY k.keyword_norm
        ORDER  BY n DESC
        LIMIT  30
    """)
    for row in cur.fetchall():
        log(f"  {row[1]:4d}  {row[0]}")

def main():
    conn = sqlite3.connect(DB_PATH)

    log("=== Normalización de SDGs ===")
    n = normalize_tags(conn, "sdg", SDG_MAP)
    log(f"Total SDG corregidos: {n}")

    log("\n=== Normalización de Impact Areas ===")
    n = normalize_tags(conn, "impactArea", IMPACT_MAP)
    log(f"Total Impact Areas corregidas: {n}")

    log("\n=== Normalización de Keywords ===")
    n = normalize_keywords(conn, KEYWORD_MAP)
    log(f"Total keyword relaciones reasignadas: {n}")

    conn.commit()

    log("\n=== Reporte post-normalización ===")
    report(conn)

    conn.close()
    log("\n✓ Normalización completada.")

if __name__ == "__main__":
    main()