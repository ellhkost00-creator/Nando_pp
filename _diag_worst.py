import sys, math
sys.path.insert(0, '.')
import config
from pandapower import from_excel

net = from_excel(str(config.NET_3PH_XLSX))

worst = ['mv_f0_lv_dudley_6', 'mv_f0_lv_dudley_8', 'mv_f0_lv_bushmans_7', 'mv_f0_lv_sand_wash_1']
for name in worst:
    row = net.trafo[net.trafo['name'].str.lower() == name]
    if row.empty:
        print(f"{name}: NOT FOUND")
        continue
    r = row.iloc[0]
    hv_bus = net.bus.loc[r.hv_bus, 'name']
    lv_bus = net.bus.loc[r.lv_bus, 'name']
    lv_loads = net.asymmetric_load[net.asymmetric_load['bus'] == r.lv_bus]
    i_rated = r.sn_mva / (math.sqrt(3) * r.vn_lv_kv)
    print(f"\n{name}:")
    print(f"  hv_bus={hv_bus}({r.hv_bus}), lv_bus={lv_bus}({r.lv_bus})")
    print(f"  sn={r.sn_mva*1000:.0f} kVA, vn_lv={r.vn_lv_kv:.3f} kV, I_rated_lv={i_rated*1000:.1f} A")
    if not lv_loads.empty:
        l = lv_loads.iloc[0]
        total_p = (l.p_a_mw + l.p_b_mw + l.p_c_mw) * 1000
        print(f"  load: p_a={l.p_a_mw*1e6:.1f}W, p_b={l.p_b_mw*1e6:.1f}W, p_c={l.p_c_mw*1e6:.1f}W  total={total_p:.3f} kW")
        print(f"  load as % of sn: {total_p/(r.sn_mva*1000)*100:.1f}%")
        print(f"  load imbalance: a/total={l.p_a_mw/(total_p/1000)*100:.0f}% b={l.p_b_mw/(total_p/1000)*100:.0f}% c={l.p_c_mw/(total_p/1000)*100:.0f}%")
    else:
        print("  no asymmetric_load on LV bus! (loads may be on downstream LV nodes)")
    # lines connected to LV bus
    lv_lines_from = net.line[net.line['from_bus'] == r.lv_bus]
    lv_lines_to   = net.line[net.line['to_bus'] == r.lv_bus]
    print(f"  LV bus lines: {len(lv_lines_from)} leaving, {len(lv_lines_to)} entering")
