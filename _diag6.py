import sys; sys.path.insert(0,'.')
import config, pandapower as pp
from pandapower import from_excel
import pandapower.topology as top
import networkx as nx

net = from_excel(str(config.NET_PP_XLSX))

# Include ALL elements (even out of service) to check raw topology
mg_all = top.create_nxgraph(net, include_out_of_service=True)
comps_all = list(nx.connected_components(mg_all))
print(f'With ALL elements: {len(comps_all)} components, sizes: {sorted([len(c) for c in comps_all], reverse=True)[:10]}')

# Find source component
source_idx = net.bus[net.bus['name']=='sourcebus'].index[0]
for i,c in enumerate(comps_all):
    if source_idx in c:
        print(f'Source in component {i} (size {len(c)})')
        break

# Check the "3849" entry bus
idx_3849 = net.bus[net.bus['name']=='mv_f0_n3849'].index[0]
for i,c in enumerate(comps_all):
    if idx_3849 in c:
        print(f'mv_f0_n3849 in component {i} (size {len(c)})')
        break

# Find disconnected MV sub-networks - pick 2 isolated MV buses and trace
isolated_mv = [b for b in comps_all[1] if net.bus.loc[b,'name'].startswith('mv_f0_n')][:3]
print('Sample isolated MV buses:', [net.bus.loc[b,'name'] for b in isolated_mv])

# Check what lines/trafos connect TO these isolated buses
for bus_idx in isolated_mv[:1]:
    bname = net.bus.loc[bus_idx,'name']
    lines = net.line[(net.line.from_bus==bus_idx)|(net.line.to_bus==bus_idx)]
    print(f'{bname}: {len(lines)} lines connecting')
    trafos = net.trafo[(net.trafo.hv_bus==bus_idx)|(net.trafo.lv_bus==bus_idx)]
    print(f'{bname}: {len(trafos)} trafos connecting')
    switches = net.switch[(net.switch.bus==bus_idx)|(net.switch.element==bus_idx)]
    print(f'{bname}: {len(switches)} switches connecting')
print()
# Find the ROOT of each component (most connected bus)
for i, comp in enumerate(sorted(comps_all, key=len, reverse=True)[:4]):
    mv_in_comp = [b for b in comp if net.bus.loc[b,'name'].startswith('mv_f0_n')]
    lv_in_comp = [b for b in comp if net.bus.loc[b,'name'].startswith(('mv_f0_lv', 'lv_f0_'))]
    print(f'Component {i} (size {len(comp)}): {len(mv_in_comp)} MV buses, {len(lv_in_comp)} LV buses')
