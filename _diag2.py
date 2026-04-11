import sys; sys.path.insert(0,'.')
import config, pandapower as pp
from pandapower import from_excel

net = from_excel(str(config.NET_PP_XLSX))

# Check mv_f0_n3849
hits = net.bus[net.bus['name']=='mv_f0_n3849']
print('mv_f0_n3849 in net:', len(hits), 'rows', hits.index.tolist())

# Check main transformer
mask = net.trafo['name'].str.contains('KLO14|SMR8', na=False)
print('Main trafo:')
print(net.trafo[mask][['name','hv_bus','lv_bus','in_service']])

# Source bus
sb = net.bus[net.bus['name']=='sourcebus']
print('Source bus:', sb.index.tolist(), sb[['name','vn_kv']].values)

# Lines touching source bus area
src_idx = sb.index[0] if len(sb) > 0 else None
print('Lines from source:', len(net.line[(net.line.from_bus==src_idx)|(net.line.to_bus==src_idx)]))
print('Trafos at source:', len(net.trafo[(net.trafo.hv_bus==src_idx)|(net.trafo.lv_bus==src_idx)]))

# Check how many MV buses are connected
mv_buses = net.bus[net.bus['name'].str.startswith('mv_f0_n')]
print(f'MV buses total: {len(mv_buses)}')

# Run PF and check
pp.runpp(net, numba=False)
mv_nan = net.res_bus.loc[mv_buses.index, 'vm_pu'].isna().sum()
print(f'MV buses with NaN vm_pu: {mv_nan}/{len(mv_buses)}')
