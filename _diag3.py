import sys; sys.path.insert(0,'.')
import config, pandapower as pp
from pandapower import from_excel

net = from_excel(str(config.NET_PP_XLSX))
pp.runpp(net, numba=False)

res = net.res_bus
connected = res[res['vm_pu'].notna()]
print(f'Connected buses: {len(connected)}/{len(net.bus)}')

idx_3849 = net.bus[net.bus['name']=='mv_f0_n3849'].index[0]
lines_at_3849 = net.line[(net.line.from_bus==idx_3849)|(net.line.to_bus==idx_3849)]
print(f'Lines at mv_f0_n3849 (bus {idx_3849}): {len(lines_at_3849)}')
sw_at_3849 = net.switch[(net.switch.bus==idx_3849)|(net.switch.element==idx_3849)]
print(f'Switches at mv_f0_n3849: {len(sw_at_3849)}')
print(f'Total switches: {len(net.switch)}')
print(f'Open switches: {(~net.switch.closed).sum()}')

# Check which connected bus names start with mv_f0_n
connected_names = net.bus.loc[connected.index, 'name']
mv_connected = connected_names[connected_names.str.startswith('mv_f0_n')]
print(f'Connected MV buses: {len(mv_connected)}')

# Check if there's a disconnect - find MV buses adjacent to mv_f0_n3849
neighbors = set()
for _, row in net.line[(net.line.from_bus==idx_3849)|(net.line.to_bus==idx_3849)].iterrows():
    neighbors.add(row.from_bus)
    neighbors.add(row.to_bus)
for _, row in net.switch[(net.switch.bus==idx_3849)|(net.switch.element==idx_3849)].iterrows():
    neighbors.add(row.bus)
    neighbors.add(row.element)
neighbors.discard(idx_3849)
print(f'Direct neighbors of mv_f0_n3849: {[net.bus.loc[n,"name"] for n in list(neighbors)[:5]]}')

# Are they connected?
for n in list(neighbors)[:3]:
    print(f'  Bus {n} ({net.bus.loc[n,"name"]}): vm_pu = {net.res_bus.loc[n,"vm_pu"]}')
