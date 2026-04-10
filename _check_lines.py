import sys, pandas as pd, re
sys.path.insert(0, r'C:\Users\anton\Desktop\project\Nando_pp')
import config
from pandapower import from_excel

net = from_excel(str(config.NET_PP_XLSX))
pp_names = list(net.line['name'].astype(str).str.strip())

dss = pd.read_csv(str(config.DSS_LINE_LOADING_CSV))
dss = dss.set_index(dss.columns[0])
dss_names = list(dss.columns.astype(str).str.strip())

def norm(x):
    s = str(x).strip().lower()
    s = re.sub(r'^line\.', '', s)
    s = re.sub(r'[^a-z0-9]+', '', s)
    return s

pp_norm  = {norm(n): n for n in pp_names}
dss_norm = {norm(n): n for n in dss_names}

matched  = set(pp_norm.keys()) & set(dss_norm.keys())
pp_only  = set(pp_norm.keys()) - set(dss_norm.keys())
dss_only = set(dss_norm.keys()) - set(pp_norm.keys())

print(f'PP lines total:   {len(pp_names)}')
print(f'DSS lines total:  {len(dss_names)}')
print(f'Matched:          {len(matched)}')
print(f'PP only (no DSS): {len(pp_only)}')
print(f'DSS only (no PP): {len(dss_only)}')
print()
print('PP  sample (raw):', sorted(pp_names)[:5])
print('DSS sample (raw):', sorted(dss_names)[:5])
print()
if pp_only:
    orig = [pp_norm[k] for k in sorted(pp_only)[:5]]
    print('PP unmatched sample (orig):', orig)
if dss_only:
    orig = [dss_norm[k] for k in sorted(dss_only)[:5]]
    print('DSS unmatched sample (orig):', orig)
