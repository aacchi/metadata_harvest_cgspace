"""
06_visualizations.py
Genera visualizaciones del análisis temporal:
- Evolución de keywords emergentes
- Heatmap de keywords × trimestres
- Top keywords por trimestre
- Distribución de briefs por trimestre
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime

# Configuración
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

DB_PATH = Path("data/db/cgspace_briefs.sqlite")
TAB_DIR = Path("outputs/tables")
FIG_DIR = Path("outputs/figures")
FIG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── 1. Evolución de keywords emergentes ────────────────────────
def plot_emerging_trends():
    """
    Líneas de tiempo para top 10 keywords emergentes.
    """
    log("\nGraficando evolución de keywords emergentes...")
    
    # Cargar datos
    matrix = pd.read_csv(TAB_DIR / "keywords_by_quarter.csv", index_col=0)
    emerging = pd.read_csv(TAB_DIR / "keywords_emerging.csv", index_col=0)
    
    # Top 10 emergentes
    top10 = emerging.head(10).index.tolist()
    
    # Filtrar solo trimestres (no columnas derivadas)
    quarter_cols = [col for col in matrix.columns if 'Q' in str(col) and 'recent' not in col and 'early' not in col]
    data = matrix.loc[top10, quarter_cols]
    
    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    for keyword in data.index:
        ax.plot(data.columns, data.loc[keyword], marker='o', label=keyword, linewidth=2)
    
    ax.set_xlabel('Trimestre', fontsize=12)
    ax.set_ylabel('Frecuencia (# briefs)', fontsize=12)
    ax.set_title('Evolución de Keywords Emergentes (Top 10)', fontsize=14, fontweight='bold')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    out_path = FIG_DIR / "emerging_trends.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    log(f"  ✓ {out_path}")

# ── 2. Heatmap de keywords × trimestres ────────────────────────
def plot_heatmap():
    """
    Heatmap de top 30 keywords por trimestre.
    """
    log("\nGenerando heatmap keywords × trimestres...")
    
    # Cargar datos
    matrix = pd.read_csv(TAB_DIR / "keywords_by_quarter.csv", index_col=0)
    
    # Filtrar solo trimestres
    quarter_cols = [col for col in matrix.columns if 'Q' in str(col) and 'recent' not in col]
    data = matrix[quarter_cols]
    
    # Top 30 por frecuencia total
    data['total'] = data.sum(axis=1)
    top30 = data.nlargest(30, 'total').drop('total', axis=1)
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 12))
    
    sns.heatmap(
        top30,
        cmap='YlOrRd',
        annot=False,
        fmt='g',
        cbar_kws={'label': 'Frecuencia'},
        linewidths=0.5,
        ax=ax
    )
    
    ax.set_title('Heatmap: Top 30 Keywords por Trimestre', fontsize=14, fontweight='bold')
    ax.set_xlabel('Trimestre', fontsize=12)
    ax.set_ylabel('Keyword', fontsize=12)
    plt.tight_layout()
    
    out_path = FIG_DIR / "heatmap_keywords.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    log(f"  ✓ {out_path}")

# ── 3. Distribución de briefs por trimestre ────────────────────
def plot_briefs_distribution():
    """
    Barras: cantidad de briefs por trimestre.
    """
    log("\nGraficando distribución de briefs por trimestre...")
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT year_quarter, COUNT(*) as n_briefs
        FROM   briefs
        WHERE  year_quarter IS NOT NULL
        GROUP  BY year_quarter
        ORDER  BY year_quarter
    """, conn)
    conn.close()
    
    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    bars = ax.bar(df['year_quarter'], df['n_briefs'], color='steelblue', alpha=0.7, edgecolor='black')
    
    # Resaltar Q4s
    for i, (q, n) in enumerate(zip(df['year_quarter'], df['n_briefs'])):
        if 'Q4' in q:
            bars[i].set_color('coral')
            bars[i].set_alpha(0.8)
    
    ax.set_xlabel('Trimestre', fontsize=12)
    ax.set_ylabel('Número de Briefs', fontsize=12)
    ax.set_title('Distribución de Briefs por Trimestre', fontsize=14, fontweight='bold')
    ax.grid(True, axis='y', alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    out_path = FIG_DIR / "briefs_distribution.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    log(f"  ✓ {out_path}")

# ── 4. Top 5 keywords por trimestre (small multiples) ──────────
def plot_top5_per_quarter():
    """
    Grid de barras: top 5 keywords en cada trimestre.
    """
    log("\nGraficando top 5 keywords por trimestre...")
    
    # Cargar datos
    matrix = pd.read_csv(TAB_DIR / "keywords_by_quarter.csv", index_col=0)
    
    # Filtrar trimestres
    quarter_cols = [col for col in matrix.columns if 'Q' in str(col) and 'recent' not in col]
    data = matrix[quarter_cols]
    
    # Crear grid
    n_quarters = len(quarter_cols)
    ncols = 3
    nrows = (n_quarters + ncols - 1) // ncols
    
    fig, axes = plt.subplots(nrows, ncols, figsize=(15, 4 * nrows))
    axes = axes.flatten() if n_quarters > 1 else [axes]
    
    for i, quarter in enumerate(quarter_cols):
        if i >= len(axes):
            break
        
        ax = axes[i]
        top5 = data[quarter].nlargest(5)
        
        ax.barh(range(len(top5)), top5.values, color='teal', alpha=0.7)
        ax.set_yticks(range(len(top5)))
        ax.set_yticklabels(top5.index, fontsize=9)
        ax.set_xlabel('Frecuencia', fontsize=9)
        ax.set_title(quarter, fontsize=11, fontweight='bold')
        ax.invert_yaxis()
        ax.grid(True, axis='x', alpha=0.3)
    
    # Ocultar axes sobrantes
    for j in range(i + 1, len(axes)):
        axes[j].axis('off')
    
    plt.suptitle('Top 5 Keywords por Trimestre', fontsize=16, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    
    out_path = FIG_DIR / "top5_per_quarter.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    log(f"  ✓ {out_path}")

# ── 5. Comparación emergentes vs estables ──────────────────────
def plot_emerging_vs_stable():
    """
    Comparación de evolución: emergentes vs estables.
    """
    log("\nComparando keywords emergentes vs estables...")
    
    # Cargar datos
    matrix = pd.read_csv(TAB_DIR / "keywords_by_quarter.csv", index_col=0)
    emerging = pd.read_csv(TAB_DIR / "keywords_emerging.csv", index_col=0)
    stable = pd.read_csv(TAB_DIR / "keywords_stable.csv", index_col=0)
    
    # Filtrar trimestres
    quarter_cols = [col for col in matrix.columns if 'Q' in str(col) and 'recent' not in col]
    
    # Top 3 emergentes y top 3 estables
    top_emerging = emerging.head(3).index.tolist()
    top_stable = stable.head(3).index.tolist()
    
    # Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Emergentes
    for kw in top_emerging:
        ax1.plot(quarter_cols, matrix.loc[kw, quarter_cols], marker='o', label=kw, linewidth=2)
    ax1.set_title('Keywords Emergentes (Top 3)', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Trimestre')
    ax1.set_ylabel('Frecuencia')
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # Estables
    for kw in top_stable:
        ax2.plot(quarter_cols, matrix.loc[kw, quarter_cols], marker='s', label=kw, linewidth=2)
    ax2.set_title('Keywords Estables (Top 3)', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Trimestre')
    ax2.set_ylabel('Frecuencia')
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.3)
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    
    out_path = FIG_DIR / "emerging_vs_stable.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    log(f"  ✓ {out_path}")

# ── Main ───────────────────────────────────────────────────────
def main():
    log("="*60)
    log("GENERANDO VISUALIZACIONES")
    log("="*60)
    
    plot_briefs_distribution()
    plot_emerging_trends()
    plot_heatmap()
    plot_top5_per_quarter()
    plot_emerging_vs_stable()
    
    log("\n" + "="*60)
    log("✓ Todas las visualizaciones generadas")
    log(f"✓ Ver en: {FIG_DIR}")
    log("="*60)

if __name__ == "__main__":
    main()