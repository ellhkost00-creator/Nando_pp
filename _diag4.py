import sys; sys.path.insert(0,'.')
import config, pandapower as pp
from pandapower import from_excel

net = from_excel(str(config.NET_PP_XLSX))

# Check connected components using topology
import pandapower.topology as top
mg = top.create_nxgraph(net, include_out_of_service=False)
import networkx as nx
components = list(nx.connected_components(mg))
print(f'Connected components: {len(components)}')
sizes = sorted([len(c) for c in components], reverse=True)
print(f'Top 10 component sizes: {sizes[:10]}')
print(f'Total buses in largest component: {sizes[0]}')
print(f'Buses in small components (<=5): {sum(s for s in sizes if s<=5)}')
print(f'Buses in medium components (6-50): {sum(s for s in sizes if 6<=s<=50)}')
print(f'Buses in large components (>50): {sum(s for s in sizes if s>50)}')

# Find what's in the 2nd largest component (if any)
if len(components) > 1:
    second = sorted(components, key=len, reverse=True)[1]
    names = [net.bus.loc[b,'name'] for b in list(second)[:5]]
    print(f'2nd component ({len(second)} buses) sample names: {names}')
