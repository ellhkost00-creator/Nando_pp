"""
Ελέγχω το συνολικό φορτίο downstream των worst trafos (LV network traversal).
"""
import sys, math
sys.path.insert(0, '.')
import config
from pandapower import from_excel
import pandas as pd

net = from_excel(str(config.NET_3PH_XLSX))

def get_downstream_buses(net, start_bus):
    """BFS to collect all buses reachable downstream of start_bus via lines."""
    visited = set([start_bus])
    queue = [start_bus]
    while queue:
        b = queue.pop()
        # lines where from_bus = b (downstream)
        children = net.line.loc[net.line['from_bus'] == b, 'to_bus'].tolist()
        for c in children:
            if c not in visited:
                visited.add(c)
                queue.append(c)
    return visited

worst = ['mv_f0_lv_dudley_6', 'mv_f0_lv_dudley_8', 'mv_f0_lv_bushmans_7', 'mv_f0_lv_sand_wash_1']

# Load the timeseries profile factor at t=24 for loads connected at step 24
# (pp_timeseries_3ph scales loads per step)
pp_a = pd.read_csv(str(config.RESULTS_DIR / 'res_trafo_3ph/loading_a_percent.csv'), sep=';', index_col=0)

for name in worst:
    row = net.trafo[net.trafo['name'].str.lower() == name]
    if row.empty: continue
    r = row.iloc[0]
    lv_bus = r.lv_bus

    down_buses = get_downstream_buses(net, lv_bus)
    loads = net.asymmetric_load[net.asymmetric_load['bus'].isin(down_buses)]

    total_p = (loads['p_a_mw'].sum() + loads['p_b_mw'].sum() + loads['p_c_mw'].sum()) * 1000
    pa = loads['p_a_mw'].sum() * 1000
    pb = loads['p_b_mw'].sum() * 1000
    pc = loads['p_c_mw'].sum() * 1000

    i_rated = r.sn_mva / (math.sqrt(3) * r.vn_lv_kv)

    # What a 1.0 pu loading would look like in real power
    s_rated_kva = r.sn_mva * 1000

    trafo_idx = row.index[0]
    pp_loading_24 = pp_a[str(trafo_idx)].iloc[24] if str(trafo_idx) in pp_a.columns else float('nan')

    print(f"\n{name}:")
    print(f"  transformer: {s_rated_kva:.0f} kVA,  I_rated={i_rated*1000:.1f} A,  vn_lv={r.vn_lv_kv:.3f} kV")
    print(f"  downstream buses: {len(down_buses)},  loads: {len(loads)}")
    print(f"  STATIC load total: {total_p:.3f} kW  [{pa:.3f}/{pb:.3f}/{pc:.3f} kW] = {total_p/s_rated_kva*100:.1f}% of sn")
    print(f"  load balance: A={pa/total_p*100:.0f}% B={pb/total_p*100:.0f}% C={pc/total_p*100:.0f}%")
    print(f"  PP loading t=24: {pp_loading_24:.1f}%")
    print(f"  Effective load being pushed: {pp_loading_24/100 * s_rated_kva:.1f} kVA")
