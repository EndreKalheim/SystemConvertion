import json
import os

def build_eq_topology():
    base = os.path.join(os.path.dirname(__file__), '..', '..')
    eq_path   = os.path.normpath(os.path.join(base, 'output', 'diagrams', 'TSA_Equations.json'))
    map_path  = os.path.normpath(os.path.join(base, 'data',   'extracted', 'KSpiceSystemMap.json'))
    out_html  = os.path.normpath(os.path.join(base, 'output', 'diagrams', 'System_TSA_State_Topology.html'))
    out_json  = os.path.normpath(os.path.join(base, 'output', 'diagrams', 'TSA_Explicit_Topology.json'))

    sig_map_path = os.path.normpath(os.path.join(base, 'output', 'diagrams', 'SignalMapping.json'))

    with open(eq_path,  'r', encoding='utf-8') as f:
        equations = json.load(f)
    with open(map_path, 'r', encoding='utf-8') as f:
        kspice_data = json.load(f)
    signal_map = {}
    if os.path.exists(sig_map_path):
        with open(sig_map_path, 'r', encoding='utf-8') as f:
            signal_map = json.load(f)

    # ── Build raw connection graph (traverse through noise) ──────────────────

    ignore_types = ['Alarm', 'Transmitter', 'Indicator', 'SignalSwitch', 'ProfileViewer']

    def is_noise(name, ktype):
        n = name.lower()
        if any(t in ktype for t in ignore_types): return True
        if n.endswith('_m') or n.endswith('_view'): return True
        # pv_* (PipeVolume) and pf_* (standalone PipeFlow) are routing components — make
        # them transparent so the BFS walks past them to the actual upstream/downstream
        # equipment (compressors, valves, separators). Without this, a pipe between
        # VA0001 and KA0001 would block the gas-outflow edge.
        if n.startswith('pv') or n.startswith('pf_') or n.startswith('network-'): return True
        if 'fe0' in n or 'fit0' in n or 'tit0' in n or 'pit0' in n: return True
        return False

    raw_ins   = {}   # node -> [(src, port)]
    raw_outs  = {}   # node -> [(dst, port)]
    base_types = {}  # clean component name -> KSpiceType

    for m in kspice_data.get('Models', []):
        bn    = m['Name'].replace('_pf', '')
        ktype = m['KSpiceType']
        noise = is_noise(bn, ktype) and 'ControlValve' not in ktype

        if not noise:
            base_types[bn] = ktype

        raw_ins.setdefault(bn, [])
        raw_outs.setdefault(bn, [])

        for inp in m.get('Inputs', []):
            if not inp.get('Source'):
                continue
            parts = inp['Source'].split(':')
            src   = parts[0].replace('_pf', '')
            port  = parts[1] if len(parts) > 1 else ''
            raw_ins[bn].append((src, port))
            raw_outs.setdefault(src, []).append((bn, port))

    # ── Upstream / downstream traversal (skips noise blocks) ────────────────

    def get_upstream(node, flow_only=False):
        """Return list of (comp, port) pairs that feed into node, skipping noise."""
        res = []
        vis = set()
        def _trace(curr, best_port=''):
            if curr in vis: return
            vis.add(curr)
            for u, port in raw_ins.get(curr, []):
                if flow_only and any(tok in port for tok in
                        ('Profile', 'Factor', 'Conductance', 'Temperature',
                         'Control', 'Measured', 'Speed')):
                    continue
                if flow_only and any(t in base_types.get(u, '') for t in ('ASC', 'Controller')):
                    continue
                # Prefer a descriptive port name over generic Output/Value/Out
                cur_port = port if (port and not any(g in port for g in ('Value', 'Output', 'Out'))) else best_port
                if u in base_types:
                    res.append((u, cur_port))
                else:
                    _trace(u, cur_port)
        _trace(node)
        return res

    def get_downstream(node, flow_only=False):
        """Return list of component names that node feeds into, skipping noise."""
        res = []
        vis = set()
        def _trace(curr):
            if curr in vis: return
            vis.add(curr)
            for d, port in raw_outs.get(curr, []):
                if flow_only and any(tok in port for tok in
                        ('Profile', 'Factor', 'Conductance', 'Temperature',
                         'Control', 'Measured', 'Speed')):
                    continue
                if flow_only and any(t in base_types.get(d, '') for t in ('ASC', 'Controller')):
                    continue
                if d in base_types:
                    res.append(d)
                else:
                    _trace(d)
        _trace(node)
        return res

    # ── Pass-through pruning ─────────────────────────────────────────────────
    # Passive valves that are simple 1-to-1 conduits (ESV, HV, MA, PSV, CV)
    # are dropped from the equation graph — the topology jumps over them.

    filtered = []
    for eq in equations:
        comp = eq['Component']
        is_passive = any(t in comp.upper() for t in ('ESV', 'MA', 'HV', 'PSV', 'CV'))

        ups   = [u for u in get_upstream(comp, flow_only=True)   if (u[0] if isinstance(u, tuple) else u) != comp]
        downs = [d for d in get_downstream(comp, flow_only=True) if d != comp]

        is_pass_through  = (len(ups) == 1 and len(downs) == 1)
        is_control_valve = 'ControlValve' in base_types.get(comp, '') and not is_passive

        if is_passive and is_pass_through and not is_control_valve:
            continue
        filtered.append(eq)

    equations = filtered

    # ── Build node list (one vis-network node per equation state) ────────────

    C_FLOW = "#a3d2ca"
    C_PRES = "#f5d787"
    C_TEMP = "#f3a683"
    C_LVL  = "#81ecec"
    C_CTRL = "#ff7675"

    tsa_states = []
    for eq in equations:
        cid  = eq['ID']
        comp = eq['Component']
        st   = eq['State']
        role = eq['Role']

        color = C_FLOW
        if st == 'Pressure':      color = C_PRES
        if st == 'Temperature':   color = C_TEMP
        if 'Level' in st:         color = C_LVL
        if role == 'Controller':  color = C_CTRL

        label = f"{comp}\n({st})\n\nEq: {eq['Formula']}"
        if 'Param' in eq:
            label += f"\n[{eq['Param']}]"

        tsa_states.append({
            "id":    cid,
            "label": label,
            "color": color,
            "shape": "box",
            "group": comp,   # same-component nodes share a group for clustering
            "font":  {"multi": "html", "size": 11}
        })

    # ── BFS: find nearest component with a given modelled state ─────────────

    equation_ids = {e['ID'] for e in equations}

    def find_nearest_state(start_comp, target_state, traverse_upstream,
                           pass_through_types=None, skip_comps=None):
        """BFS over the component graph; returns component names that model target_state.

        pass_through_types: list of KSpiceType substrings to skip rather than stop at.
        skip_comps: list of name substrings; any component whose name contains one of
            these strings is excluded from the BFS entirely (not visited, not returned).
            Used by SUCTION_PRESSURE to block anti-surge recirculation paths (UV valves)
            that would otherwise lead back to the discharge-side pressure.
        """
        found = []
        visited = set()
        queue   = [start_comp]
        while queue:
            curr = queue.pop(0)
            if curr in visited:
                continue
            # Skip components whose name matches a skip pattern.
            if skip_comps and curr != start_comp:
                if any(s.upper() in curr.upper() for s in skip_comps):
                    continue
            visited.add(curr)
            if curr != start_comp and f"{curr}_{target_state}" in equation_ids:
                curr_ktype = base_types.get(curr, '')
                if pass_through_types and any(pt in curr_ktype for pt in pass_through_types):
                    pass  # don't stop; continue BFS through this component type
                else:
                    found.append(curr)
                    continue   # stop going further along this branch
            if traverse_upstream:
                nexts = [u for u, _ in get_upstream(curr, flow_only=True)]
            else:
                nexts = get_downstream(curr, flow_only=True)
            queue.extend(n for n in nexts if n not in visited)
        # deduplicate preserving BFS order
        seen = set()
        return [x for x in found if not (x in seen or seen.add(x))]

    # ── Build edge list ──────────────────────────────────────────────────────

    edges = []
    added = set()

    def add_edge(frm, to, lbl):
        if frm == to:
            return
        # Prevent self-loops on the exact same state node
        fc, _, fs = frm.rpartition('_')
        tc, _, ts = to.rpartition('_')
        if fc == tc and fs == ts:
            return
        eid = f"{frm}->{to}"
        if eid not in added:
            edges.append({"from": frm, "to": to, "label": lbl,
                          "arrows": "to", "font": {"size": 8}})
            added.add(eid)

    for eq in equations:
        cid  = eq['ID']
        comp = eq['Component']
        up_comps      = get_upstream(comp)
        up_flow_comps = get_upstream(comp, flow_only=True)

        for inp in eq['Inputs']:

            # ── Abstract placeholders resolved via BFS ───────────────────────
            if inp == "UPSTREAM_FLOW":
                for n in find_nearest_state(comp, 'MassFlow', True):
                    add_edge(f"{n}_MassFlow", cid, "m_in")

            elif inp == "DOWNSTREAM_FLOW":
                for n in find_nearest_state(comp, 'MassFlow', False):
                    add_edge(f"{n}_MassFlow", cid, "m_out")

            elif inp == "DOWNSTREAM_EXPORT_FLOW":
                # Like DOWNSTREAM_FLOW but traverses PAST compressors/pumps to find
                # the true export valve. Used for separator gas pressure: the compressor
                # total flow includes recirculation (UV0001), so the real gas-export
                # signal is the downstream valve (ESV0005), not the compressor suction.
                for n in find_nearest_state(comp, 'MassFlow', False,
                                            pass_through_types=['Compressor', 'Pump']):
                    add_edge(f"{n}_MassFlow", cid, "m_out")

            elif inp == "DOWNSTREAM_FLOW_SUM":
                for n in find_nearest_state(comp, 'MassFlow', False):
                    add_edge(f"{n}_MassFlow", cid, "m_sum")

            elif inp == "MACRO_MASS_FLOW_TRUNK":
                for u, _ in up_flow_comps:
                    if 'Separator' not in base_types.get(u, '') and 'Tank' not in base_types.get(u, ''):
                        add_edge(f"{u}_MassFlow", cid, "trunk")

            elif inp == "UPSTREAM_PRESSURE":
                # Cap to nearest 1: prevents redundant edges when BFS reaches the same
                # physical pressure via multiple paths (e.g. HX0001 and KA0001 for UV valves).
                for n in find_nearest_state(comp, 'Pressure', True)[:1]:
                    add_edge(f"{n}_Pressure", cid, "P_in")

            elif inp == "SUCTION_PRESSURE":
                # Like UPSTREAM_PRESSURE but skips anti-surge / recirculation valves
                # (UV-named components) so the BFS only follows the main suction path
                # from the separator to the compressor inlet — blocking the discharge-side
                # path that would otherwise find HX0001_Pressure (circular for P_out model).
                for n in find_nearest_state(comp, 'Pressure', True, skip_comps=['UV', 'ASC']):
                    add_edge(f"{n}_Pressure", cid, "P_in")

            elif inp == "DOWNSTREAM_PRESSURE":
                for n in find_nearest_state(comp, 'Pressure', False):
                    add_edge(f"{n}_Pressure", cid, "P_out")

            elif inp == "CONTAINER_PRESSURE":
                # UV anti-surge valve: gas discharges back to the compressor suction separator.
                # Traverse upstream, skip UV valves and FE flow elements, and pass THROUGH
                # HeatExchanger and Compressor/Pump to find the suction Separator/Tank pressure.
                # BFS path: UV → HX (pass-through) → KA (pass-through) → VA (Separator, stop).
                for n in find_nearest_state(comp, 'Pressure', True,
                                            skip_comps=['UV', 'FE'],
                                            pass_through_types=['HeatExchanger', 'Compressor', 'Pump'])[:1]:
                    add_edge(f"{n}_Pressure", cid, "P_out")

            elif inp == "UPSTREAM_TEMP":
                for n in find_nearest_state(comp, 'Temperature', True):
                    add_edge(f"{n}_Temperature", cid, "T_in")

            elif inp == "COOLING_TEMP":
                for u, port in get_upstream(comp, flow_only=False):
                    if 'Temperature' in port:
                        add_edge(f"{u}_Temperature", cid, "T_cool")

            elif inp == "PARTNER_FLOW":
                # A heat-exchanger side's outlet temperature depends on how much hot/cold
                # stream the partner side is pushing through (Q ~ m_partner * Cp * dT).
                # The partner is identified generically: a non-self component connected
                # via a thermal-coupling port (Wall/Temperature/Heat/Conductance) that
                # has its own MassFlow equation. Works for any K-Spice HX pair regardless
                # of naming convention because the link is encoded in K-Spice's own
                # Inputs[].Source references.
                thermal_tokens = ('Wall', 'Temperature', 'Heat', 'Conductance', 'Profile')
                seen_partners = set()
                for u, port in raw_ins.get(comp, []):
                    if u == comp or u in seen_partners:
                        continue
                    if u not in base_types:
                        continue
                    if not any(tok in port for tok in thermal_tokens):
                        continue
                    if f"{u}_MassFlow" not in equation_ids:
                        continue
                    add_edge(f"{u}_MassFlow", cid, "m_partner")
                    seen_partners.add(u)

            elif inp == "LOCAL_CONTROL":
                for u, _ in up_comps:
                    kt = base_types.get(u, '')
                    if ('Control' in kt or 'ASC' in kt) and f"{u}_Control" in equation_ids:
                        add_edge(f"{u}_Control", cid, "U(t)")
                # Note: no downstream-controller fallback for pumps here.
                # Pump flow is captured by DOWNSTREAM_FLOW; the downstream valve's level
                # controller (LIC) is not a causal driver of pump speed or flow.

            elif inp == "PUMP_SPEED":
                # Wire the pump's Speed state as an input to its Pressure equation.
                # If a Speed equation was generated by KSpiceModelFactory (the normal
                # case now that pumps always get a Speed state), speed_key is already
                # in equation_ids and we just add the edge.  Only falls back to a
                # boundary node when no Speed CSV column exists in the signal map.
                speed_key = f"{comp}_Speed"
                if speed_key in signal_map:
                    if speed_key not in equation_ids:
                        tsa_states.append({"id": speed_key,
                                           "label": f"{comp}\n(Speed)\n[boundary]",
                                           "shape": "box", "color": "#D9EAD3"})
                        equation_ids.add(speed_key)
                    add_edge(speed_key, cid, "speed")

            elif inp == "MEASURED_FLOW":
                # For MEASURED_FLOW (used by ASC), we want the main process flow.
                # Skip UV (recirculation valves).
                # We do NOT pass through Compressors because we want to measure the compressor's flow.
                # If the flow element path hits a BlockValve first (like 23ESV0002), we shouldn't stop there
                # because we don't always model mass flow for pass-through valves tightly.
                candidates = find_nearest_state(comp, 'MassFlow', True,
                                            skip_comps=['UV'],
                                            pass_through_types=['Valve', 'BlockValve', 'ControlValve'])
                if candidates:
                    # Prefer the Compressor if available (e.g. 23KA0001 instead of a feed valve upstream)
                    best = next((n for n in candidates if 'Compressor' in base_types.get(n, '')), candidates[0])
                    add_edge(f"{best}_MassFlow", cid, "y_flow")

            elif inp == "MEASURED_PRESSURE":
                # Trace all connected pressures (e.g. from PTs). AscIdentifier sorts them by mean 
                # to separate suction (low) from discharge (high).
                for n in find_nearest_state(comp, 'Pressure', True):
                    add_edge(f"{n}_Pressure", cid, "y_pres")
                for n in find_nearest_state(comp, 'Pressure', False):
                    add_edge(f"{n}_Pressure", cid, "y_pres")

            elif inp == "MEASURED_STATE":
                kt = base_types.get(comp.split('_')[0], '')
                if 'Control' in kt and 'ASC' not in comp:
                    # K-Spice generic controllers have an explicit Measurement mapped to CSV
                    comp_bare = comp.split('_')[0]
                    meas_id = f"{comp_bare}:Measurement"
                    add_edge(meas_id, cid, "y_meas")
                    # Optionally add a visual node so the graph renders nicely
                    if meas_id not in equation_ids:
                        tsa_states.append({"id": meas_id, "label": meas_id, "shape": "box", "color": "#EAD1DC"})
                        equation_ids.add(meas_id)
                else:
                    for u, port in up_comps:
                        u_kt    = base_types.get(u, '')
                        is_vol  = 'Separator' in u_kt or 'Tank' in u_kt
                        p_lower = (port or '').lower()

                        if is_vol:
                            # Fallback logic
                            if 'LIC' in comp:
                                if 'water' in p_lower or 'heavy' in p_lower:
                                    add_edge(f"{u}_WaterLevel", cid, "y_meas")
                                elif 'oil' in p_lower or 'light' in p_lower or 'overflow' in p_lower:
                                    add_edge(f"{u}_OilLevel",   cid, "y_meas")
                                else:
                                    add_edge(f"{u}_TotalLevel", cid, "y_meas")
                            if 'PIC' in comp:
                                add_edge(f"{u}_Pressure",    cid, "y_meas")
                            if 'TIC' in comp:
                                add_edge(f"{u}_Temperature", cid, "y_meas")
                        else:
                            if 'ASC' in comp:
                                add_edge(f"{u}_MassFlow", cid, "y_meas")

            else:
                # Literal state ID passed directly (e.g. "23KA0001_MassFlow" -> intra-component edge)
                if inp in equation_ids:
                    add_edge(inp, cid, "local_var")

    # ── Cascade setpoint detection ────────────────────────────────────────────
    # When one PID/ASC's output port feeds another controller's setpoint input
    # (e.g. 23PIC0001:ControllerOutput → 23SIC0001:ExternalSetpoint), add a
    # "cascade_sp" edge so ClosedLoopRunner can substitute the prediction.
    for eq in equations:
        if eq.get('Role') != 'Controller':
            continue
        comp_bare = eq['Component']
        cid_ctrl  = eq['ID']
        for src, port in raw_ins.get(comp_bare, []):
            if not any(tok in port for tok in ('Output', 'ControllerOutput')):
                continue
            src_ktype = base_types.get(src, '')
            if not ('pid' in src_ktype.lower()
                    or 'GenericASC' in src_ktype
                    or 'Controller' in src_ktype):
                continue
            src_ctrl_id = f"{src}_Control"
            if src_ctrl_id in equation_ids and src_ctrl_id != cid_ctrl:
                add_edge(src_ctrl_id, cid_ctrl, "cascade_sp")

    # ── Render HTML with vis-network ─────────────────────────────────────────

    html = f"""<!DOCTYPE html>
<html>
<head>
  <title>K-Spice Equation Topology</title>
  <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
  <style>
    body  {{ margin: 0; padding: 0; overflow: hidden; font-family: monospace; background: #fafafa; }}
    #net  {{ width: 100vw; height: 100vh; border: 1px solid #ccc; }}
    #info {{ position: fixed; top: 8px; left: 8px; background: rgba(255,255,255,0.85);
             padding: 6px 10px; border-radius: 4px; font-size: 12px; z-index: 99; }}
  </style>
</head>
<body>
<div id="info">
  <b>K-Spice Equation Topology</b><br>
  {len(tsa_states)} states &nbsp;|&nbsp; {len(edges)} edges<br>
  <span style="color:#a3d2ca">&#9632;</span> MassFlow &nbsp;
  <span style="color:#f5d787">&#9632;</span> Pressure &nbsp;
  <span style="color:#f3a683">&#9632;</span> Temperature &nbsp;
  <span style="color:#81ecec">&#9632;</span> Level &nbsp;
  <span style="color:#ff7675">&#9632;</span> Controller
</div>
<div id="net"></div>
<script>
  var nodes = new vis.DataSet({json.dumps(tsa_states, indent=None)});
  var edges = new vis.DataSet({json.dumps(edges,      indent=None)});
  var net   = new vis.Network(document.getElementById('net'),
    {{nodes: nodes, edges: edges}},
    {{
      physics: {{
        stabilization: {{iterations: 300}},
        barnesHut: {{springLength: 220, springConstant: 0.04, damping: 0.12}}
      }},
      edges: {{
        smooth: {{type: 'curvedCW', roundness: 0.2}},
        font:   {{size: 9, align: 'middle'}}
      }},
      nodes: {{font: {{size: 11}}}},
      interaction: {{hover: true, tooltipDelay: 100}}
    }}
  );
  net.on('click', function(p) {{
    if (p.nodes.length) {{
      var n = nodes.get(p.nodes[0]);
      document.getElementById('info').innerHTML =
        '<b>' + n.id + '</b><br>' + (n.label || '').replace(/\\n/g,'<br>');
    }}
  }});
</script>
</body>
</html>"""

    os.makedirs(os.path.dirname(out_html), exist_ok=True)
    with open(out_html, 'w', encoding='utf-8') as f:
        f.write(html)

    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump({"nodes": tsa_states, "edges": edges}, f, indent=2)

    print(f"[SUCCESS] Topology: {out_html}")
    print(f"          States: {len(tsa_states)},  Edges: {len(edges)}")


if __name__ == '__main__':
    build_eq_topology()
