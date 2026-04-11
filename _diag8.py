import sys; sys.path.insert(0,'.')
import config
import re

# Build adjacency from DSS lines only
bus_adj = {}
with open(str(config.MV_LINES_DSS), 'r') as f:
    for raw in f:
        line = raw.strip()
        if not re.match(r'(?i)^new\s+line\.', line): continue
        b1_m = re.search(r'bus1=([^\s.]+)', line, re.I)
        b2_m = re.search(r'bus2=([^\s.]+)', line, re.I)
        if b1_m and b2_m:
            b1 = b1_m.group(1).lower()
            b2 = b2_m.group(1).lower()
            bus_adj.setdefault(b1, set()).add(b2)
            bus_adj.setdefault(b2, set()).add(b1)

# Build component map
def get_components(adj):
    visited = {}
    comp_id = 0
    for start in adj:
        if start in visited: continue
        queue = [start]
        while queue:
            b = queue.pop()
            if b in visited: continue
            visited[b] = comp_id
            for nb in adj.get(b, []):
                if nb not in visited:
                    queue.append(nb)
        comp_id += 1
    return visited

comp_map = get_components(bus_adj)
comp_of_3849 = comp_map.get('mv_f0_n3849', -1)
print(f'Component of mv_f0_n3849: {comp_of_3849}')
print(f'Total MV components: {max(comp_map.values())+1}')

# Check regulators - do they bridge components?
reg_bridges = 0
with open(str(config.REGS_DSS), 'r') as f:
    content = f.read()
    
# Find all N reactor entries to map jumper buses
# Reactor.Jumper_X_E: bus1=mv_f0_n[b1] -> jumper input
# Reactor.Jumper_X_O: bus1=Jumper_X.1 -> mv_f0_n[b2] downstream
jumper_upstream = {}  # jumper_base -> mv bus
jumper_downstream = {}

for m in re.finditer(r'New Reactor\.(\S+)\s+.*?bus1=(\S+)\s+bus2=(\S+)', content, re.I):
    rname, b1, b2 = m.group(1), m.group(2).lower(), m.group(3).lower()
    base = re.sub(r'_[abc]_(e|o)$', '', rname, flags=re.I)
    base = re.sub(r'_(e|o)$', '', base, flags=re.I)
    
    if rname.lower().endswith('_e'):
        mv_bus = b1.split('.')[0] if b1.split('.')[0].startswith('mv_f0_n') else b2.split('.')[0]
        if mv_bus.startswith('mv_f0_n'):
            jumper_upstream[base] = mv_bus
    elif rname.lower().endswith('_o'):
        mv_bus = b2.split('.')[0] if b2.split('.')[0].startswith('mv_f0_n') else b1.split('.')[0]
        if mv_bus.startswith('mv_f0_n'):
            jumper_downstream[base] = mv_bus

print(f'\nJumpers found - upstream: {len(jumper_upstream)}, downstream: {len(jumper_downstream)}')
print(f'Common keys: {len(set(jumper_upstream.keys()) & set(jumper_downstream.keys()))}')

# Check if any regulator bridges different components
bridges = 0
for key in set(jumper_upstream.keys()) & set(jumper_downstream.keys()):
    up = jumper_upstream[key]
    dn = jumper_downstream[key]
    c1 = comp_map.get(up, -1)
    c2 = comp_map.get(dn, -1)
    if c1 != c2:
        bridges += 1
        if bridges <= 5:
            print(f'  Bridge: {key}: {up}(comp {c1}) -> {dn}(comp {c2})')

print(f'Regulators bridging different MV components: {bridges}')

# How many lines connect to mv_f0_n3849?
print(f'\nLines adjacent to mv_f0_n3849: {len(bus_adj.get("mv_f0_n3849", []))}')
print(f'Adjacent: {list(bus_adj.get("mv_f0_n3849", []))[:5]}')
