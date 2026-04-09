"""
Quick metrics analysis - reads already-saved per-element CSVs directly,
no need to reload the heavy net Excel file.
"""
import sys, pandas as pd, numpy as np
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import config

METRICS_DIR = config.METRICS_DIR

per_l = METRICS_DIR / "metric_3ph_line_loading_per_element.csv"
per_t = METRICS_DIR / "metric_3ph_trafo_loading_per_element.csv"

if not per_l.exists() or not per_t.exists():
    print("[ERROR] Per-element CSV files not found. Wait for metrics_3ph_loading.py to finish.")
    sys.exit(1)

line_df  = pd.read_csv(per_l)
trafo_df = pd.read_csv(per_t)

for label, df in [("LINES", line_df), ("TRAFOS", trafo_df)]:
    print(f"\n{'='*60}")
    print(f"== {label} ({len(df)} rows, {df['phase'].nunique()} phases, "
          f"{df['dss_name'].nunique()} elements)")
    print(f"{'='*60}")
    for ph in ("a", "b", "c"):
        sub = df[df["phase"] == ph]
        if sub.empty:
            continue
        print(f"\n  Phase {ph.upper()}:  n={len(sub)}")
        print(f"    MAE   (mean-of-element-MAEs): {sub['mae'].mean():.3f}%")
        print(f"    RMSE  (mean-of-element-RMSEs): {sub['rmse'].mean():.3f}%")
        print(f"    Max AE (worst single element) : {sub['max_ae'].max():.3f}%")
        print(f"    MBE   (mean bias, + = PP over) : {sub['mbe'].mean():.3f}%")
        # percentile of per-element MAE
        print(f"    Percentiles of element MAE:")
        for p in [50, 75, 90, 95, 99]:
            print(f"      P{p:2d}: {sub['mae'].quantile(p/100):.2f}%")
    print()
    print(f"  Top-10 worst (by max_ae) – Phase A:")
    sub_a = df[df["phase"] == "a"].nlargest(10, "max_ae")
    for _, row in sub_a.iterrows():
        print(f"    {row['dss_name']:<40s}  mae={row['mae']:7.2f}%  max_ae={row['max_ae']:7.2f}%")
    print()
    print(f"  Error distribution (Phase A, by element MAE):")
    sub_a_all = df[df["phase"] == "a"]
    for lo, hi in [(0,1),(1,5),(5,10),(10,20),(20,50),(50,200),(200,9999)]:
        n = len(sub_a_all[(sub_a_all["mae"] >= lo) & (sub_a_all["mae"] < hi)])
        pct = 100*n/len(sub_a_all) if len(sub_a_all) > 0 else 0
        print(f"    MAE [{lo:4d},{hi:5d})%:  {n:4d} ({pct:.1f}%)")

print("\nDone.")
