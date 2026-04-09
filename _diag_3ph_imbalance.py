"""
Διερευνώ τους 3-phase Dyn worst trafos:
- Φορτώνω τα DSS loading CSVs (για τους 254 matched trafos)
- Υπολογίζω mean loading ανά φάση + imbalance ratio
- Σόρτάρω κατά MAE (από metrics per-element CSV)
- Βλέπω αν υπάρχει correlation μεταξύ imbalance και σφάλματος
"""
import sys
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, '.')
import config

# ── 1. Φόρτωση exclusion lists ──────────────────────────────────────────────
ll_names = set(pd.read_csv(str(config.DSS_TRAFO_LL_NAMES))["trafo_name"].str.lower().str.strip())
yy_names = set(pd.read_csv(str(config.DSS_TRAFO_1PH_YY_NAMES))["trafo_name"].str.lower().str.strip())
excluded = ll_names | yy_names

# ── 2. Φόρτωση DSS trafo loading CSVs ──────────────────────────────────────
dss_a = pd.read_csv(str(config.DSS_TRAFO_LOADING_A), sep=";", index_col=0)
dss_b = pd.read_csv(str(config.DSS_TRAFO_LOADING_B), sep=";", index_col=0)
dss_c = pd.read_csv(str(config.DSS_TRAFO_LOADING_C), sep=";", index_col=0)

# Κρατάω μόνο τους trafos που δεν εξαιρούνται
valid_cols = [c for c in dss_a.columns if c.lower() not in excluded]
dss_a = dss_a[valid_cols]
dss_b = dss_b[valid_cols]
dss_c = dss_c[valid_cols]

print(f"Valid 3-phase trafos in DSS CSVs: {len(valid_cols)}")

# ── 3. Φόρτωση per-element metrics CSV ──────────────────────────────────────
per_elem = pd.read_csv(str(config.METRICS_DIR / "metric_3ph_trafo_loading_per_element.csv"))

# ── 4. Ανά trafo: mean DSS loading / imbalance ratio ────────────────────────
results = []
for col in valid_cols:
    mean_a = dss_a[col].mean(skipna=True)
    mean_b = dss_b[col].mean(skipna=True)
    mean_c = dss_c[col].mean(skipna=True)

    phases_loaded = [x for x in [mean_a, mean_b, mean_c] if not np.isnan(x)]
    if not phases_loaded:
        continue

    max_ph = max(phases_loaded)
    min_ph = min(phases_loaded)
    imbalance = (max_ph - min_ph) / (max_ph + 1e-9)  # 0=balanced, 1=totally unbalanced

    # Αντίστοιχα MAE από per-element
    for phase, mean_dss in zip(["a", "b", "c"], [mean_a, mean_b, mean_c]):
        row = per_elem[(per_elem["dss_name"] == col) & (per_elem["phase"] == phase)]
        mae = row["mae"].values[0] if len(row) else np.nan

        results.append({
            "trafo": col,
            "phase": phase,
            "mean_dss": mean_dss,
            "imbalance": imbalance,
            "mae": mae,
        })

df = pd.DataFrame(results).dropna(subset=["mae"])

# ── 5. Summary: imbalance vs MAE correlation ─────────────────────────────────
print("\n--- Correlation: imbalance vs MAE ---")
print(f"  Pearson r = {df['imbalance'].corr(df['mae']):.3f}")

# ── 6. Breakdown κατά imbalance bucket ───────────────────────────────────────
bins = [0, 0.3, 0.6, 0.8, 0.95, 1.01]
labels = ["0-30%", "30-60%", "60-80%", "80-95%", "95-100%"]
df["imb_bin"] = pd.cut(df["imbalance"], bins=bins, labels=labels)

print("\n--- MAE ανά imbalance bucket ---")
print(f"  {'Imbalance':<12} {'N trafos':>9} {'mean MAE':>9} {'max MAE':>9}")
for lbl, grp in df.groupby("imb_bin", observed=True):
    n_trafos = grp["trafo"].nunique()
    print(f"  {lbl:<12} {n_trafos:>9} {grp['mae'].mean():>8.1f}% {grp['mae'].max():>8.1f}%")

# ── 7. Worst 10 ────────────────────────────────────────────────────────────────
print("\n--- Top-10 worst trafos (by max MAE across phases) ---")
worst = df.groupby("trafo")["mae"].max().sort_values(ascending=False).head(10)
for name, mae in worst.items():
    imb = df[df["trafo"] == name]["imbalance"].iloc[0]
    mean_loading = df[df["trafo"] == name]["mean_dss"].mean()
    print(f"  {name:<40s}  mae={mae:6.1f}%  imb={imb:.2f}  mean_dss={mean_loading:.1f}%")

# ── 8. Πόσοι trafos έχουν imbalance > 80% ───────────────────────────────────
high_imb = df[df["imbalance"] > 0.80]["trafo"].nunique()
total = df["trafo"].nunique()
print(f"\nTrafos με imbalance > 80%: {high_imb}/{total}")

# ── 9. Correlation: mean_dss loading vs MAE ─────────────────────────────────
# MAE (% loading) = absolute error in percentage points (since DSS is also in %)
# Low-loaded trafos → even small absolute error → large % error
print("\n--- Correlation: mean_dss loading vs MAE ---")
print(f"  r(mean_dss, MAE) = {df['mean_dss'].corr(df['mae']):.3f}")

# ── 10. MAE bins per loading level ───────────────────────────────────────────
bins2 = [0, 5, 10, 20, 40, 100]
labels2 = ["<5%", "5-10%", "10-20%", "20-40%", ">40%"]
df["load_bin"] = pd.cut(df["mean_dss"], bins=bins2, labels=labels2)
print("\n--- MAE ανά μέσο DSS loading ---")
print(f"  {'DSS loading':<12} {'N trafos':>9} {'mean MAE':>9} {'max MAE':>9}")
for lbl, grp in df.groupby("load_bin", observed=True):
    n = grp["trafo"].nunique()
    print(f"  {lbl:<12} {n:>9} {grp['mae'].mean():>8.1f}% {grp['mae'].max():>8.1f}%")

# ── 11. Επιλογές αποκλεισμού ─────────────────────────────────────────────────
print("\n--- Επιλογές αποκλεισμού ---")
print(f"  {'Κριτήριο':<30} {'Εξαιρεί':>8} {'Κρατά':>8}  MAE A    MAE B    MAE C")
for imb_th in [0.95, 0.80, 0.60]:
    excl = df[df["imbalance"] > imb_th]["trafo"].unique()
    keep = df[~df["trafo"].isin(excl)]
    n_keep  = keep["trafo"].nunique()
    n_excl  = total - n_keep
    a = keep[keep["phase"] == "a"]["mae"].mean()
    b = keep[keep["phase"] == "b"]["mae"].mean()
    c = keep[keep["phase"] == "c"]["mae"].mean()
    crit = f"imb > {imb_th:.0%}"
    print(f"  {crit:<30} {n_excl:>8} {n_keep:>8}  {a:6.1f}%  {b:6.1f}%  {c:6.1f}%")

# ── 12. Τι συμβαίνει με τα moderate-imbalance, high MAE trafos; ──────────────
# Αυτά έχουν imb=0.4-0.6 αλλά MAE>200% -- κάτι άλλο συμβαίνει
print("\n--- Worst trafos με moderate imbalance (imb < 0.80, MAE > 150%) ---")
moderate = df[(df["imbalance"] < 0.80) & (df["mae"] > 150)]
top = moderate.groupby("trafo")["mae"].max().sort_values(ascending=False)
for name, mae in top.items():
    imb = df[df["trafo"] == name]["imbalance"].iloc[0]
    mean_d = df[df["trafo"] == name]["mean_dss"].mean()
    mae_by_ph = df[df["trafo"] == name].set_index("phase")["mae"].to_dict()
    print(f"  {name:<42s}  imb={imb:.2f}  mean_dss={mean_d:.1f}%  "
          f"A={mae_by_ph.get('a', float('nan')):.0f}%  "
          f"B={mae_by_ph.get('b', float('nan')):.0f}%  "
          f"C={mae_by_ph.get('c', float('nan')):.0f}%")

# ── 12. (placeholder - removed) ──────────────────────────────────────────────
