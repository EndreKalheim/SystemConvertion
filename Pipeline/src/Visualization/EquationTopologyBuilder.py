import json
import os
from collections import deque

def build_eq_topology():
    eq_path = os.path.join(os.path.dirname(__file__), '../../output/diagrams/TSA_Equations.json')
    map_path = os.path.join(os.path.dirname(__file__), '../../data/extracted/KSpiceSystemMap.json')
    out_html = os.path.join(os.path.dirname(__file__), '../../output/diagrams/System_TSA_State_Topology.html')
    
    with open(eq_path, 'r', encoding='utf-8') as f:
        equations = json.load(f)
        
    with open(map_path, 'r', encoding='utf-8') as f:
        kspice_data = json.load(f)
        
    # Build raw connection map (bypassing noise)
    ignore_types = ['Alarm', 'Transmitter', 'Indicator', 'SignalSwitch']
    def is_noise(n, t):
        l = n.lower()
        if any(i in t for i in ignore_types) or l.endswith('_m') or l.endswith('_view'): return True
        if l.startswith('pv_') or l.startswith('network-') or 'fe0' in l or 'fit0' in l or 'tit0' in l or 'pit0' in l: return True
        return False
        
    # Build complete raw_ins and raw_outs across ALL nodes to allow tracing through noise
    raw_ins = {}
    raw_outs = {}
    base_types = {}
    is_noise_map = {}
    
    for m in kspice_data.get('Models', []):
        bn = m['Name'].replace('_pf', '')
        ktype = m['KSpiceType']
        is_n = is_noise(bn, ktype) and not 'ControlValve' in ktype
        is_noise_map[bn] = is_n
        
        if not is_n:
            base_types[bn] = ktype
            
        if bn not in raw_ins: raw_ins[bn] = []
        if bn not in raw_outs: raw_outs[bn] = []
        
        for i in m.get('Inputs', []):
            if i['Source']: 
                parts = i['Source'].split(':')
                src = parts[0].replace('_pf', '')
                port = parts[1] if len(parts) > 1 else ''
                raw_ins[bn].append((src, port))
                if src not in raw_outs: raw_outs[src] = []
                raw_outs[src].append((bn, port))

    # Undirected adjacency for tracing measurements through PipeVolume / transmitters / _pf aliases
    adj = {}
    def link(a, b):
        a = a.replace('_pf', '')
        b = b.replace('_pf', '')
        if not a or not b:
            return
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
    for a, pairs in raw_ins.items():
        a = a.replace('_pf', '')
        for src, _ in pairs:
            link(a, src)
    for src, pairs in raw_outs.items():
        src = src.replace('_pf', '')
        for dst, _ in pairs:
            link(src, dst)

    kspice_type_by_name = {}
    for m in kspice_data.get('Models', []):
        kspice_type_by_name[m['Name'].replace('_pf', '')] = m.get('KSpiceType', '')

    def get_upstream(node, flow_only=False):
        res = []
        vis = set()
        def t(curr, best_port=''):
            if curr in vis: return
            vis.add(curr)
            for u, port in raw_ins.get(curr, []):
                # Do not jump thermal boundaries / profile bounds if we just want fluid flow
                if flow_only and ('Profile' in port or 'Factor' in port or 'Conductance' in port or 'Temperature' in port or 'Control' in port or 'Measured' in port or 'Speed' in port):
                    continue
                # Block upstream traversal into Controllers across signals if flow_only is active
                if flow_only and ('ASC' in base_types.get(u, '') or 'Controller' in base_types.get(u, '')):
                    continue
                    
                # Save the specific measurement port (like LevelHeavyPhaseFeedSideWeir) instead of generic Value/Output
                cur_port = port if port and 'Value' not in port and 'Output' not in port and 'Out' not in port else best_port
                if u in base_types: res.append((u, cur_port))
                else: t(u, cur_port)
        t(node)
        return res

    def get_downstream(node, flow_only=False):
        res = []
        vis = set()
        def t(curr):
            if curr in vis: return
            vis.add(curr)
            for d, port in raw_outs.get(curr, []):
                # Do not jump thermal boundaries / profile bounds if we just want fluid flow
                if flow_only and ('Profile' in port or 'Factor' in port or 'Conductance' in port or 'Temperature' in port or 'Control' in port or 'Measured' in port or 'Speed' in port):
                    continue
                if flow_only and ('ASC' in base_types.get(d, '') or 'Controller' in base_types.get(d, '')):
                    continue
                if d in base_types: res.append(d)
                else: t(d)
        t(node)
        return res

    tsa_states = []
    edges = []
    
    # Pass-Through Pruning: Remove models for purely internal passive blocks (ESV, MA, HV) to avoid adding zero-value math overhead
    filtered_equations = []
    for eq in equations:
        comp = eq['Component']
        is_passive_type = any(t in comp.upper() for t in ['ESV', 'MA', 'HV', 'PSV', 'CV'])
        
        # Determine if endpoint structurally (has nothing upstream or nothing downstream flow-wise)
        ups = [u for u in get_upstream(comp, flow_only=True) if (u[0] if isinstance(u, tuple) else u) != comp]
        downs = [d for d in get_downstream(comp, flow_only=True) if d != comp]
        is_endpoint = (len(ups) == 0 or len(downs) == 0)
        
        # Determine if it's a direct 1-to-1 pass-through (if it combines feeds, it's a manifold and must be kept)
        is_true_pass_through = (len(ups) == 1 and len(downs) == 1)
        
        # Keep if it has controllers tied to it, too (like a ControlValve)
        is_control_valve = 'ControlValve' in base_types.get(comp, '') and not is_passive_type
        
        if is_passive_type and is_true_pass_through and not is_control_valve:
            # Drop it physically from equations list => equations will jump over it
            continue
        filtered_equations.append(eq)
        
    equations = filtered_equations

    C_FLOW = "#a3d2ca"
    C_PRES = "#f5d787"
    C_TEMP = "#f3a683"
    C_LVL  = "#81ecec"
    C_CTRL = "#ff7675"

    for eq in equations:
        cid = eq['ID']
        comp = eq['Component']
        st = eq['State']
        role = eq['Role']
        
        color = C_FLOW
        if st == 'Pressure': color = C_PRES
        if st == 'Temperature': color = C_TEMP
        if 'Level' in st: color = C_LVL
        if role == 'Controller': color = C_CTRL
        
        lbl = f"{comp}\n({st})\n\nEq: {eq['Formula']}"
        if 'Param' in eq: lbl += f"\n[{eq['Param']}]"
        
        tsa_states.append({"id": cid, "label": lbl, "color": color, "shape": "box", "font": {"multi": "html", "size": 11}})

    # Helpers to dynamically find the nearest valid component that provides a modeled state
    def find_nearest_state(start_node, target_state, traverse_upstream):
        res = []
        vis = set()
        queue = [start_node]
        while queue:
            curr = queue.pop(0)
            if curr in vis: continue
            vis.add(curr)
            
            if curr != start_node:
                has_state = any(e['Component'] == curr and e['State'] == target_state for e in equations)
                if has_state:
                    res.append(curr)
                    continue # Stop traversing this specific flow branch further
                    
            next_comps = get_upstream(curr, flow_only=True) if traverse_upstream else get_downstream(curr, flow_only=True)
            for n in next_comps:
                comp_name = n[0] if isinstance(n, tuple) else n
                queue.append(comp_name)
                
        # Deduplicate while preserving BFS order (closest first)
        seen = set()
        return [x for x in res if not (x in seen or seen.add(x))]

    equation_ids = {e['ID'] for e in equations}

    def split_comp_state(sid):
        if '_' not in sid:
            return sid, ''
        return sid.rsplit('_', 1)

    # Interpret the abstract equation requirements into literal model wiring!
    added_edges_map = {}
    asc_wired_cids = set()

    def add_edge(frm, to, lbl):
        if frm == to:
            return
        fc, fs = split_comp_state(frm)
        tc, ts = split_comp_state(to)
        if fc == tc and fs == ts:
            return
        
        pair_id = f"{frm}->{to}"
        if pair_id in added_edges_map:
            edge = added_edges_map[pair_id]
            if lbl not in edge["label"]:
                edge["label"] += f" & {lbl}"
        else:
            e = {"from": frm, "to": to, "label": lbl, "arrows": "to", "font": {"size": 8}}
            edges.append(e)
            added_edges_map[pair_id] = e

    def _type_rank_for_flow(comp_name):
        kt = (base_types.get(comp_name, '') or '').lower()
        if 'compressor' in kt or 'pump' in kt:
            return 100
        if 'valve' in kt:
            return 20
        if 'pipe' in kt or 'flow' in kt:
            return 10
        return 0

    def _type_rank_for_pressure(comp_name, dest=''):
        kt = (base_types.get(comp_name, '') or '').lower()
        dl = (dest or '').lower()
        
        is_sep = 'separator' in kt or 'tank' in kt or 'volume' in kt
        is_comp = 'compressor' in kt or 'pump' in kt
        
        if 'inlet' in dl:
            if is_sep: return 200
            if is_comp: return -100
        elif 'outlet' in dl:
            if is_comp: return 200
            if is_sep: return -100
            
        # Default behavior if not explicitly inlet/outlet
        if is_sep: return 50
        return 0

    def bfs_best_modeled_state(start_block, state_suffix, rank_fn=None):
        """Shortest-path BFS to a modeled {comp}_{state_suffix}; tie-break with rank_fn(comp)."""
        rank_fn = rank_fn or (lambda c: 0)
        start = start_block.replace('_pf', '')
        q = deque([start])
        dist = {start: 0}
        hits = []
        while q:
            cur = q.popleft()
            d = dist[cur]
            tid = f"{cur}_{state_suffix}"
            if tid in equation_ids:
                hits.append((d - rank_fn(cur), tid))
            for nb in adj.get(cur, ()):
                if nb not in dist:
                    dist[nb] = d + 1
                    q.append(nb)
        if not hits:
            return None
        hits.sort()
        return hits[0][1]

    def map_asc_measurement_suffix(dest, src_port):
        dl = (dest or '').lower()
        pl = (src_port or '').lower()
        if 'compressor' in dl or 'performancedata' in dl:
            return 'MassFlow'
        if 'flow' in dl or 'dp' in dl or 'deltap' in pl:
            return 'MassFlow'
        if 'pressure' in dl or 'measuredvalue' in dl or pl.endswith('pressure') or '.p' in pl or 'inlet' in dl or 'outlet' in dl:
            return 'Pressure'
        return None

    def wire_asc_from_declared_sources(cid, comp):
        if cid in asc_wired_cids:
            return
        if 'GenericASC' not in kspice_type_by_name.get(comp, ''):
            return
        asc_mdl = None
        for m in kspice_data.get('Models', []):
            if m.get('Name', '').replace('_pf', '') == comp:
                asc_mdl = m
                break
        if not asc_mdl:
            asc_wired_cids.add(cid)
            return
        for kin in asc_mdl.get('Inputs', []) or []:
            src_full = kin.get('Source') or ''
            dest = kin.get('Destination') or ''
            if not src_full or ':' not in src_full:
                continue
            blk, sport = src_full.split(':', 1)
            sfx = map_asc_measurement_suffix(dest, sport)
            if not sfx:
                continue
            rank_fn = (lambda c, _sfx=sfx, _dst=dest: _type_rank_for_flow(c) if _sfx == 'MassFlow' else _type_rank_for_pressure(c, _dst))
            tid = bfs_best_modeled_state(blk, sfx, rank_fn=rank_fn)
            if tid:
                add_edge(tid, cid, f"KSpice:{dest}")
        asc_wired_cids.add(cid)

    for eq in equations:
        cid = eq['ID']
        comp = eq['Component']
        up_comps = get_upstream(comp)
        up_flow_comps = get_upstream(comp, flow_only=True)
        
        # Auto-Wire abstract inputs based on formula demands!
        for inp in eq['Inputs']:
            if inp == "UPSTREAM_FLOW":
                for f_node in find_nearest_state(comp, 'MassFlow', True): add_edge(f"{f_node}_MassFlow", cid, "m_in")
            elif inp == "DOWNSTREAM_FLOW":
                for f_node in find_nearest_state(comp, 'MassFlow', False): add_edge(f"{f_node}_MassFlow", cid, "m_out")
            elif inp == "DOWNSTREAM_FLOW_SUM":
                for f_node in find_nearest_state(comp, 'MassFlow', False): add_edge(f"{f_node}_MassFlow", cid, "m_sum")
            elif inp == "MACRO_MASS_FLOW_TRUNK":
                # Connect the flow of the upstream component directly to this flow to show the trunk line
                for (u, port) in up_flow_comps:
                    if 'Separator' not in base_types.get(u, '') and 'Tank' not in base_types.get(u, ''):
                        add_edge(f"{u}_MassFlow", cid, "MACRO_TRUNK")
            elif inp == "UPSTREAM_PRESSURE":
                for p_node in find_nearest_state(comp, 'Pressure', True): add_edge(f"{p_node}_Pressure", cid, "P_in")
            elif inp == "DOWNSTREAM_PRESSURE":
                for p_node in find_nearest_state(comp, 'Pressure', False): add_edge(f"{p_node}_Pressure", cid, "P_out")
            elif inp == "UPSTREAM_TEMP":
                for t_node in find_nearest_state(comp, 'Temperature', True): add_edge(f"{t_node}_Temperature", cid, "T_in")
            elif inp == "COOLING_TEMP":
                for (u, port) in get_upstream(comp, flow_only=False):
                    if 'Temperature' in port:
                        add_edge(f"{u}_Temperature", cid, "T_cool")
            elif inp == "LOCAL_CONTROL":
                for (u, port) in up_comps:
                    if 'Control' in base_types.get(u, ''): add_edge(f"{u}_Control", cid, "U(t)")
                    elif 'ASC' in base_types.get(u, ''): add_edge(f"{u}_Control", cid, "U(t)")
            elif inp == "MEASURED_FLOW":
                if 'GenericASC' in kspice_type_by_name.get(comp, ''):
                    wire_asc_from_declared_sources(cid, comp)
                else:
                    for f_node in find_nearest_state(comp, 'MassFlow', True)[:1]: add_edge(f"{f_node}_MassFlow", cid, "y_flow")
            elif inp == "MEASURED_PRESSURE":
                if 'GenericASC' in kspice_type_by_name.get(comp, ''):
                    wire_asc_from_declared_sources(cid, comp)
                else:
                    for p_node in find_nearest_state(comp, 'Pressure', True): add_edge(f"{p_node}_Pressure", cid, "y_pres")
                    for p_node in find_nearest_state(comp, 'Pressure', False): add_edge(f"{p_node}_Pressure", cid, "y_pres")
            elif inp == "MEASURED_STATE":
                for (u, port) in up_comps:
                    ut = base_types.get(u, '')
                    vol = 'Separator' in ut or 'Tank' in ut
                    p_lower = (port or '').lower()
                    if vol:
                        if 'LIC' in comp:
                            if 'water' in p_lower or 'heavy' in p_lower:
                                add_edge(f"{u}_WaterLevel", cid, "y_meas")
                            elif 'oil' in p_lower or 'light' in p_lower or 'overflow' in p_lower:
                                add_edge(f"{u}_OilLevel", cid, "y_meas")
                            else:
                                add_edge(f"{u}_TotalLevel", cid, "y_meas")
                        if 'PIC' in comp:
                            add_edge(f"{u}_Pressure", cid, "y_meas")
                        if 'TIC' in comp:
                            add_edge(f"{u}_Temperature", cid, "y_meas")
                    else:
                        if 'PIC' in comp:
                            add_edge(f"{u}_Pressure", cid, "y_meas")
                        elif 'TIC' in comp:
                            add_edge(f"{u}_Temperature", cid, "y_meas")
                        elif 'LIC' in comp:
                            add_edge(f"{u}_TotalLevel", cid, "y_meas")
                        elif 'F' in comp or 'ASC' in comp:
                            add_edge(f"{u}_MassFlow", cid, "y_meas")
                        else:
                            add_edge(f"{u}_MassFlow", cid, "y_meas")
            else:
                add_edge(inp, cid, "local_var")

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><title>Equation-Driven MISO Topology</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        #mynetwork {{ width: 100vw; height: 100vh; border: 1px solid lightgray; background:#fafafa; }}
        body {{ margin: 0; padding: 0; overflow: hidden; font-family: monospace; }}
    </style>
    </head>
    <body>
    <div id="mynetwork"></div>
    <script type="text/javascript">
        var nodes = new vis.DataSet({json.dumps(tsa_states)});
        var edges = new vis.DataSet({json.dumps(edges)});
        var network = new vis.Network(document.getElementById('mynetwork'), {{nodes: nodes, edges: edges}}, {{
            physics: {{ stabilization: true, barnesHut: {{ springLength: 260, springConstant: 0.03 }} }},
            layout: {{ hierarchical: false }}
        }});
    </script>
    </body></html>
    """
    with open(out_html, 'w') as f:
        f.write(html_content)
        
    explicit_out = os.path.join(os.path.dirname(__file__), '../../output/diagrams/TSA_Explicit_Topology.json')
    with open(explicit_out, 'w') as f:
        json.dump({"nodes": tsa_states, "edges": edges}, f, indent=2)
        
    print(f"Topology successfully derived from Mathematical Equations at {out_html}")

if __name__ == '__main__':
    build_eq_topology()
