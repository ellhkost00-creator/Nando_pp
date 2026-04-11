import sys; sys.path.insert(0,'.')
import config, pandapower as pp
from pandapower import from_excel
import networkx as nx
import pandapower.topology as top

net = from_excel(str(config.NET_PP_XLSX))

print('Switch et types:', net.switch['et'].unique())
print('Switch types (CB etc):', net.switch['type'].unique())
print('Switch element dtype:', net.switch['element'].dtype)
print('Sample switches:')
print(net.switch.head(5)[['bus','element','et','closed','type']])

# Try including out-of-service in graph
mg_all = top.create_nxgraph(net, include_out_of_service=True)
comps_all = list(nx.connected_components(mg_all))
print(f'\nWith out-of-service: {len(comps_all)} components, sizes: {sorted([len(c) for c in comps_all], reverse=True)[:5]}')

# Without out-of-service (ie with switches)
mg_normal = top.create_nxgraph(net, include_out_of_service=False)
comps_normal = list(nx.connected_components(mg_normal))
print(f'Without out-of-service: {len(comps_normal)} components, sizes: {sorted([len(c) for c in comps_normal], reverse=True)[:5]}')

# Find which lines connect component 3 (6738) to others
comps_sorted = sorted(comps_normal, key=len, reverse=True)
c_source_idx = None
for i, c in enumerate(comps_sorted):
    for node in c:
        if net.bus.loc[node,'name'] == 'sourcebus':
            c_source_idx = i
            break
print(f'\nSource bus is in component {c_source_idx} (size {len(comps_sorted[c_source_idx])})')

# Check if there are lines (in_service=False) between the source component and the 4788 component
source_comp = comps_sorted[c_source_idx]
other_mv_comp = comps_sorted[1 if c_source_idx != 1 else 2]  # the 4788
print(f'Other large MV component size: {len(other_mv_comp)}')

# Find lines between components
cross_lines = net.line[
    ((net.line.from_bus.isin(source_comp)) & (net.line.to_bus.isin(other_mv_comp))) |
    ((net.line.from_bus.isin(other_mv_comp)) & (net.line.to_bus.isin(source_comp)))
]
print(f'Lines between source and MV component: {len(cross_lines)}')
print(f'  in_service=True: {cross_lines.in_service.sum()}')
print(f'  in_service=False: {(~cross_lines.in_service).sum()}')

# Find switches between components  
cross_sw = net.switch[
    ((net.switch.bus.isin(source_comp)) & (net.switch.element.isin(other_mv_comp))) |
    ((net.switch.bus.isin(other_mv_comp)) & (net.switch.element.isin(source_comp)))
]
print(f'Switches between source and MV component: {len(cross_sw)}')
