import pandas as pd
import numpy as np
import re
from pandapower import from_excel

# =========================
# PATHS / SETTINGS
# =========================
NET_XLSX = r"C:\Users\anton\Desktop\nando_pp\excels\net_pp.xlsx"

# Pandapower line results
PP_LINE_CSV = r"C:\Users\anton\Desktop\nando_pp\results\res_line\loading_percent.csv"

# OpenDSS ALL-lines loading csv
DSS_CSV = r"C:\Users\anton\Desktop\nando_pp\excels\all_lines_loading_percent.csv"

# Output: metrics only
OUT_XLSX = r"C:\Users\anton\Desktop\nando_pp\metrics\all_line_loading_metrics.xlsx"

SEP = ";"


# =========================
# NAME NORMALIZER
# =========================
def norm_name(x: str) -> str:
    s = str(x).strip().lower()
    s = re.sub(r"^line\.", "", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


# =========================
# BUILD PP NAME MAP
# =========================
def build_pp_name_map(net, pp_columns):
    mapping = {}

    for line_idx, row in net.line.iterrows():
        if "in_service" in net.line.columns and not bool(row["in_service"]):
            continue

        pp_col = str(line_idx)
        if pp_col not in pp_columns:
            continue

        mapping[pp_col] = str(row["name"]).strip()

    return mapping


# =========================
# LOAD NETWORK
# =========================
net = from_excel(NET_XLSX)
net.line["name"] = net.line["name"].astype(str)


# =========================
# READ PP loading_percent.csv
# =========================
pp = pd.read_csv(PP_LINE_CSV, sep=SEP, engine="python")

if "time_step" in pp.columns:
    pp = pp.set_index("time_step")
else:
    pp = pp.set_index(pp.columns[0])

pp = pp.apply(pd.to_numeric, errors="coerce")
pp.columns = [str(c).strip() for c in pp.columns]


# =========================
# READ DSS all-lines CSV
# =========================
dss = pd.read_csv(DSS_CSV)

time_col_found = None
for c in dss.columns:
    if str(c).strip().lower() in ["time", "timestamp", "datetime"]:
        time_col_found = c
        break

if time_col_found is not None:
    dss = dss.set_index(time_col_found)
else:
    dss = dss.set_index(dss.columns[0])

dss = dss.apply(pd.to_numeric, errors="coerce")
dss.columns = [str(c).strip() for c in dss.columns]


# =========================
# BUILD NAME MAPS
# =========================
pp_name_map = build_pp_name_map(net, pp.columns)

pp_norm_map = {}
for pp_col, pp_name in pp_name_map.items():
    k = norm_name(pp_name)
    if k not in pp_norm_map:
        pp_norm_map[k] = pp_col

dss_norm_map = {}
for dss_col in dss.columns:
    k = norm_name(dss_col)
    if k not in dss_norm_map:
        dss_norm_map[k] = dss_col


# =========================
# MATCH COMMON LINES + METRICS
# =========================
summary_cols = ["line_name", "pp_line_idx", "dss_col", "N", "MAE_pp", "Bias_pp", "MaxAbs_pp"]
rows = []

common_keys = sorted(set(pp_norm_map.keys()) & set(dss_norm_map.keys()))

for k in common_keys:
    pp_col = pp_norm_map[k]
    dss_col = dss_norm_map[k]

    line_name = pp_name_map[pp_col]

    s_pp = pp[pp_col].to_numpy(dtype=float)
    s_dss = dss[dss_col].to_numpy(dtype=float)

    n = min(len(s_pp), len(s_dss))
    if n == 0:
        continue

    a_pp = s_pp[:n]
    a_dss = s_dss[:n]

    mask = (~np.isnan(a_pp)) & (~np.isnan(a_dss))
    if not np.any(mask):
        continue

    diff_pp = a_pp[mask] - a_dss[mask]

    rows.append({
        "line_name": line_name,
        "pp_line_idx": pp_col,
        "dss_col": str(dss_col),
        "N": int(diff_pp.size),
        "MAE_pp": float(np.mean(np.abs(diff_pp))),
        "Bias_pp": float(np.mean(diff_pp)),
        "MaxAbs_pp": float(np.max(np.abs(diff_pp))),
    })


# =========================
# SUMMARY
# =========================
summary = pd.DataFrame(rows, columns=summary_cols)

if not summary.empty:
    summary = summary.sort_values("MAE_pp", ascending=False).reset_index(drop=True)

    global_row = pd.DataFrame([{
        "line_name": "=== GLOBAL MEAN (all matched lines) ===",
        "pp_line_idx": "",
        "dss_col": "",
        "N": int(summary["N"].sum()),
        "MAE_pp": float(summary["MAE_pp"].mean()),
        "Bias_pp": float(summary["Bias_pp"].mean()),
        "MaxAbs_pp": float(summary["MaxAbs_pp"].max()),
    }], columns=summary_cols)

    summary = pd.concat([global_row, summary], ignore_index=True)


# =========================
# EXPORT METRICS ONLY
# =========================
with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
    summary.to_excel(writer, index=False, sheet_name="summary")

print("DONE")
print(f"Matched common lines: {len(rows)}")
print(f"Saved metrics: {OUT_XLSX}")