import sys; sys.path.insert(0,'.')
import config
import re

# Check if 04_MV_Lines.dss has lines NOT connected to the main tree
# by looking at what buses are mentioned in lines

bus_to_lines = {}
with open(str(config.MV_LINES_DSS), 'r') as f:
    for raw in f:
        line = raw.strip()
        if not re.match(r'(?i)^new\s+line\.', line): continue
        b1_m = re.search(r'bus1=([^\s.]+)', line, re.I)
        b2_m = re.search(r'bus2=([^\s.]+)', line, re.I)
        if b1_m and b2_m:
            b1 = b1_m.group(1).lower()
            b2 = b2_m.group(1).lower()
            bus_to_lines.setdefault(b1, []).append(b2)
            bus_to_lines.setdefault(b2, []).append(b1)

all_buses = set(bus_to_lines.keys())
print(f'DSS MV buses: {len(all_buses)}')

# BFS from sourcebus/mv_f0_n3849
reachable = set()
queue = ['mv_f0_n3849']
while queue:
    bus = queue.pop()
    if bus in reachable: continue
    reachable.add(bus)
    for neighbor in bus_to_lines.get(bus, []):
        if neighbor not in reachable:
            queue.append(neighbor)

print(f'Reachable from mv_f0_n3849: {len(reachable)}')
unreachable = all_buses - reachable
print(f'Unreachable MV buses in DSS: {len(unreachable)}')
if unreachable:
    print('Sample unreachable:', list(unreachable)[:10])
    # Find root buses of unreachable components
    visited = set()
    comp_roots = []
    for start in unreachable:
        if start in visited: continue
        comp = set()
        q2 = [start]
        while q2:
            b = q2.pop()
            if b in comp: continue
            comp.add(b)
            visited.add(b)
            for nb in bus_to_lines.get(b, []):
                if nb not in comp:
                    q2.append(nb)
        comp_roots.append((len(comp), list(comp)[:3]))
    comp_roots.sort(reverse=True)
    print(f'Unreachable sub-components (top 5):')
    for sz, samples in comp_roots[:5]:
        print(f'  size={sz}, samples={samples}')
