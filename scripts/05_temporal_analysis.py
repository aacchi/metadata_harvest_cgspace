"""
05_temporal_analysis.py
Análisis temporal de keywords: identificar términos emergentes,
en declive, y patrones de co-ocurrencia por trimestre.
"""

import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
from datetime import datetime

DB_PATH = Path("data/db/cgspace_briefs.sqlite")
OUT_DIR = Path("outputs/tables")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── 1. Keywords por trimestre ──────────────────────────────────
def keywords_by_quarter(conn):
    """
    Matriz de keywords × trimestres con frecuencias.
    """
    log("Construyendo matriz keywords × trimestre...")
    
    df = pd.read_sql_query("""
        SELECT 
            b.year_quarter,
            k.keyword_norm,
            COUNT(*) as n_briefs
        FROM   brief_keywords bk
        JOIN   briefs b  ON bk.brief_id = b.brief_id
        JOIN   keywords k ON bk.keyword_id = k.keyword_id
        WHERE  b.year_quarter IS NOT NULL
        GROUP  BY b.year_quarter, k.keyword_norm
    """, conn)
    
    # Pivot: keywords como filas, trimestres como columnas
    pivot = df.pivot_table(
        index='keyword_norm',
        columns='year_quarter',
        values='n_briefs',
        fill_value=0
    )
    
    # Ordenar columnas por trimestre
    quarters = sorted(pivot.columns)
    pivot = pivot[quarters]
    
    log(f"  Matriz: {len(pivot)} keywords × {len(quarters)} trimestres")
    
    return pivot

# ── 2. Identificar emergentes ──────────────────────────────────
def identify_emerging(pivot, min_recent=5, min_growth=3):
    """
    Keywords emergentes: ausentes en primeros trimestres,
    frecuentes en últimos trimestres.
    
    Criterios:
    - Frecuencia en últimos 2 trimestres >= min_recent
    - Frecuencia en primeros 3 trimestres < min_growth
    """
    log("\nIdentificando keywords emergentes...")
    
    quarters = list(pivot.columns)
    if len(quarters) < 5:
        log("  ⚠ Insuficientes trimestres para análisis de emergencia")
        return pd.DataFrame()
    
    recent = quarters[-2:]      # últimos 2 trimestres
    early  = quarters[:3]       # primeros 3 trimestres
    
    pivot['recent_freq'] = pivot[recent].sum(axis=1)
    pivot['early_freq']  = pivot[early].sum(axis=1)
    pivot['growth']      = pivot['recent_freq'] - pivot['early_freq']
    
    emerging = pivot[
        (pivot['recent_freq'] >= min_recent) &
        (pivot['early_freq'] < min_growth)
    ].copy()
    
    emerging = emerging.sort_values('growth', ascending=False)
    
    log(f"  Keywords emergentes: {len(emerging)}")
    
    return emerging

# ── 3. Identificar en declive ──────────────────────────────────
def identify_declining(pivot, min_early=5, max_recent=2):
    """
    Keywords en declive: frecuentes en primeros trimestres,
    raras en últimos trimestres.
    """
    log("\nIdentificando keywords en declive...")
    
    quarters = list(pivot.columns)
    if len(quarters) < 5:
        return pd.DataFrame()
    
    recent = quarters[-2:]
    early  = quarters[:3]
    
    if 'recent_freq' not in pivot.columns:
        pivot['recent_freq'] = pivot[recent].sum(axis=1)
        pivot['early_freq']  = pivot[early].sum(axis=1)
        pivot['decline']     = pivot['early_freq'] - pivot['recent_freq']
    else:
        pivot['decline'] = pivot['early_freq'] - pivot['recent_freq']
    
    declining = pivot[
        (pivot['early_freq'] >= min_early) &
        (pivot['recent_freq'] <= max_recent)
    ].copy()
    
    declining = declining.sort_values('decline', ascending=False)
    
    log(f"  Keywords en declive: {len(declining)}")
    
    return declining

# ── 4. Co-ocurrencia por trimestre ─────────────────────────────
def cooccurrence_by_quarter(conn, quarter, min_freq=3):
    """
    Pares de keywords que co-ocurren en el mismo brief
    para un trimestre específico.
    """
    log(f"\nCalculando co-ocurrencia para {quarter}...")
    
    df = pd.read_sql_query("""
        SELECT 
            bk1.brief_id,
            k1.keyword_norm as kw1,
            k2.keyword_norm as kw2
        FROM   brief_keywords bk1
        JOIN   brief_keywords bk2 ON bk1.brief_id = bk2.brief_id
        JOIN   keywords k1 ON bk1.keyword_id = k1.keyword_id
        JOIN   keywords k2 ON bk2.keyword_id = k2.keyword_id
        JOIN   briefs b ON bk1.brief_id = b.brief_id
        WHERE  b.year_quarter = ?
          AND  k1.keyword_norm < k2.keyword_norm
    """, conn, params=(quarter,))
    
    # Contar pares
    pairs = df.groupby(['kw1', 'kw2']).size().reset_index(name='n_cooccur')
    pairs = pairs[pairs['n_cooccur'] >= min_freq]
    pairs = pairs.sort_values('n_cooccur', ascending=False)
    
    log(f"  Pares con freq >= {min_freq}: {len(pairs)}")
    
    return pairs

# ── 5. Top keywords estables ───────────────────────────────────
def identify_stable(pivot, min_avg=10):
    """
    Keywords estables: presentes consistentemente a lo largo
    de todos los trimestres con frecuencia alta.
    """
    log("\nIdentificando keywords estables...")
    
    pivot['avg_freq'] = pivot.mean(axis=1)
    pivot['cv']       = pivot.std(axis=1) / (pivot['avg_freq'] + 1e-10)
    
    stable = pivot[pivot['avg_freq'] >= min_avg].copy()
    stable = stable.sort_values('avg_freq', ascending=False)
    
    log(f"  Keywords estables (freq promedio >= {min_avg}): {len(stable)}")
    
    return stable

# ── 6. Guardar outputs ─────────────────────────────────────────
def save_outputs(pivot, emerging, declining, stable):
    """
    Guarda todos los resultados como CSV.
    """
    log("\nGuardando outputs...")
    
    # Matriz completa
    pivot.to_csv(OUT_DIR / "keywords_by_quarter.csv")
    log(f"  ✓ {OUT_DIR}/keywords_by_quarter.csv")
    
    # Emergentes
    if not emerging.empty:
        emerging.to_csv(OUT_DIR / "keywords_emerging.csv")
        log(f"  ✓ {OUT_DIR}/keywords_emerging.csv")
    
    # En declive
    if not declining.empty:
        declining.to_csv(OUT_DIR / "keywords_declining.csv")
        log(f"  ✓ {OUT_DIR}/keywords_declining.csv")
    
    # Estables
    if not stable.empty:
        stable[['avg_freq', 'cv']].to_csv(OUT_DIR / "keywords_stable.csv")
        log(f"  ✓ {OUT_DIR}/keywords_stable.csv")

# ── 7. Reporte de hallazgos ────────────────────────────────────
def report(emerging, declining, stable, pivot):
    """
    Reporte narrativo de hallazgos.
    """
    log("\n" + "="*60)
    log("ANÁLISIS TEMPORAL — HALLAZGOS")
    log("="*60)
    
    if not emerging.empty:
        log("\n📈 TOP 10 KEYWORDS EMERGENTES:")
        for idx, row in emerging.head(10).iterrows():
            log(f"  • {idx:30s} crecimiento: +{int(row['growth'])}")
    
    if not declining.empty:
        log("\n📉 TOP 10 KEYWORDS EN DECLIVE:")
        for idx, row in declining.head(10).iterrows():
            log(f"  • {idx:30s} declive: -{int(row['decline'])}")
    
    if not stable.empty:
        log("\n🔄 TOP 10 KEYWORDS ESTABLES:")
        for idx, row in stable.head(10).iterrows():
            log(f"  • {idx:30s} freq promedio: {row['avg_freq']:.1f}")
    
    # Variabilidad temporal general
    log("\n📊 VARIABILIDAD TEMPORAL:")
    # Solo usar columnas de trimestres (no las derivadas)
    quarter_cols = [col for col in pivot.columns if 'Q' in str(col)]
    total_per_q = pivot[quarter_cols].sum(axis=0)
    log(f"  Trimestre con más keywords: {total_per_q.idxmax()} ({int(total_per_q.max())} menciones)")
    log(f"  Trimestre con menos keywords: {total_per_q.idxmin()} ({int(total_per_q.min())} menciones)")

# ── Main ───────────────────────────────────────────────────────
def main():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Construir matriz
    pivot = keywords_by_quarter(conn)
    
    # 2. Identificar patrones
    emerging  = identify_emerging(pivot)
    declining = identify_declining(pivot)
    stable    = identify_stable(pivot)

    # 3. Co-ocurrencia para último trimestre
    quarter_cols = [col for col in pivot.columns if 'Q' in str(col)]
    last_q = sorted(quarter_cols)[-1]
    cooccur = cooccurrence_by_quarter(conn, last_q, min_freq=5)
    cooccur.to_csv(OUT_DIR / f"cooccurrence_{last_q}.csv", index=False)
    log(f"  ✓ {OUT_DIR}/cooccurrence_{last_q}.csv")
    
    # 4. Guardar
    save_outputs(pivot, emerging, declining, stable)
    
    # 5. Reporte
    report(emerging, declining, stable, pivot)
    
    conn.close()
    log("\n✓ Análisis completado.")

if __name__ == "__main__":
    main()