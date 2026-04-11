import pandapower as pp
from pandapower import from_excel
import sys
sys.path.insert(0, '.')
import config

print('Network:', config.NETWORK_OPTION, config._NET_SUBDIR)
net = from_excel(str(config.NET_PP_XLSX))
print('Total buses:', len(net.bus))
print('Total lines:', len(net.line))
print('Total trafos (all):', len(net.trafo))
print('Total loads:', len(net.load))

try:
    pp.runpp(net, numba=False)
    nan_buses = net.res_bus['vm_pu'].isna().sum()
    total_buses = len(net.res_bus)
    print(f'After runpp -> NaN buses: {nan_buses}/{total_buses}')
    # show which bus types have NaN
    nan_idx = net.res_bus[net.res_bus['vm_pu'].isna()].index
    names = net.bus.loc[nan_idx, 'name'].head(20).tolist()
    print('First 20 NaN bus names:', names)
except Exception as e:
    print('runpp error:', e)

try:
    unsupplied = pp.topology.unsupplied_buses(net)
    print('Unsupplied buses:', len(unsupplied))
    names2 = net.bus.loc[list(unsupplied)[:20], 'name'].tolist()
    print('First 20 unsupplied names:', names2)
except Exception as e:
    print('topology error:', e)
