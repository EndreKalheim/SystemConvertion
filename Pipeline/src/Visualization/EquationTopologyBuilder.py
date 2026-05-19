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

    def _flow_siblings(src, exclude, raw_outs, base_types, equation_ids):
        """Return MassFlow equations sharing the same inter-stage space as src.
        Traverses through noise/pipe nodes; excludes HeatExchanger secondary sides.
        PipeFlow components (UV valves) are not filtered — they are valid siblings."""
        result = []
        visited = set()
        queue = [dst for dst, _ in raw_outs.get(src, [])]
        while queue:
            node = queue.pop(0)
            if node in visited or node == exclude:
                continue
            visited.add(node)
            if node in base_types:
                kt = base_types.get(node, '')
                if any(t in kt for t in ('Transmitter', 'Alarm', 'HeatExchanger')):
                    continue
                if f"{node}_MassFlow" in equation_ids and node not in result:
                    result.append(node)
            else:
                queue.extend(dst for dst, _ in raw_outs.get(node, []))
        return result

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
                if 'HeatExchanger' in base_types.get(comp, ''):
                    # HX is in series with the upstream compressor (mass conservation).
                    # Wiring upstream KA_MassFlow → HX_MassFlow avoids the circular
                    # dependency created when DOWNSTREAM_FLOW_SUM reaches the discharge
                    # header (PV_0001) which itself depends on HX_MassFlow:
                    #   PV[t] = HX[t] + HX1[t] - UV[t] - UV1[t]
                    #   HX[t] = UV[t] + PV[t-1]  → PV[t] = 2*PV[t-1] → diverges.
                    found_upstream_ka = False
                    for n in find_nearest_state(comp, 'MassFlow', True):
                        if 'Compressor' in base_types.get(n, '') or 'Pump' in base_types.get(n, ''):
                            add_edge(f"{n}_MassFlow", cid, "m_sum")
                            found_upstream_ka = True
                            break
                    if not found_upstream_ka:
                        for n in find_nearest_state(comp, 'MassFlow', False):
                            add_edge(f"{n}_MassFlow", cid, "m_sum")
                else:
                    for n in find_nearest_state(comp, 'MassFlow', False):
                        add_edge(f"{n}_MassFlow", cid, "m_sum")

            elif inp == "MACRO_MASS_FLOW_TRUNK":
                for u, _ in up_flow_comps:
                    if 'Separator' not in base_types.get(u, '') and 'Tank' not in base_types.get(u, ''):
                        add_edge(f"{u}_MassFlow", cid, "trunk")

            elif inp == "UPSTREAM_PRESSURE":
                for n in find_nearest_state(comp, 'Pressure', True):
                    add_edge(f"{n}_Pressure", cid, "P_in")

            elif inp == "SUCTION_PRESSURE":
                # Like UPSTREAM_PRESSURE but skips anti-surge / recirculation valves
                # (UV-named components) so the BFS only follows the main suction path
                # from the separator to the compressor inlet — blocking the discharge-side
                # path that would otherwise find HX0001_Pressure (circular for P_out model).
                for n in find_nearest_state(comp, 'Pressure', True, skip_comps=['UV', 'ASC']):
                    add_edge(f"{n}_Pressure", cid, "P_in")

            elif inp == "ANTISURGE_FLOW":
                # UV recirculation entering compressor suction inlet pipe.
                # BFS upstream from compressor through BlockValve/PipeFlow to find UV valves
                # (by name 'UV' or by having an ASC controller as input).
                visited_asg = set()
                queue_asg = [comp]
                while queue_asg:
                    c_asg = queue_asg.pop(0)
                    if c_asg in visited_asg:
                        continue
                    visited_asg.add(c_asg)
                    for u_asg, _ in get_upstream(c_asg, flow_only=True):
                        kt_asg = base_types.get(u_asg, '')
                        if f"{u_asg}_MassFlow" in equation_ids:
                            is_uv = 'UV' in u_asg.upper()
                            if not is_uv:
                                for ctrl, _ in raw_ins.get(u_asg, []):
                                    if 'ASC' in base_types.get(ctrl, ''):
                                        is_uv = True
                                        break
                            if is_uv:
                                add_edge(f"{u_asg}_MassFlow", cid, "uv_recirc")
                        if any(t in kt_asg for t in ('BlockValve', 'PipeFlow', 'Transmitter')):
                            queue_asg.append(u_asg)

            elif inp == "COMPRESSOR_SPEED":
                # Use the compressor's own Speed state (RPM) as input to MassFlow.
                # Avoids LOCAL_CONTROL (SIC→PIC cascade) which has inconsistent units
                # between training (RPM) and testset (%) CSV files.
                speed_id = f"{comp}_Speed"
                if speed_id in equation_ids:
                    add_edge(speed_id, cid, "local_var")

            elif inp == "DOWNSTREAM_PRESSURE":
                found = find_nearest_state(comp, 'Pressure', False)
                if found:
                    first_type = base_types.get(found[0], '')
                    if 'Compressor' in first_type or 'Pump' in first_type:
                        # BFS overshot to a compressor discharge pressure.
                        # Determine whether this valve is on the discharge side (has a
                        # HeatExchanger upstream) or the suction side (no HX upstream).
                        upstream_press = find_nearest_state(comp, 'Pressure', True)
                        is_discharge = any(
                            'HeatExchanger' in base_types.get(u, '')
                            for u in upstream_press
                        )
                        if is_discharge:
                            # Discharge-side internal valve (e.g. inter-stage ESV between
                            # LP-HX outlet and HP suction). Use mass balance:
                            #   F_valve = F_hx - F_uv - F_other_parallel
                            sources = find_nearest_state(comp, 'MassFlow', True)
                            for src in sources:
                                add_edge(f"{src}_MassFlow", cid, "m_in")
                                for sibling in _flow_siblings(
                                        src, comp, raw_outs, base_types, equation_ids):
                                    add_edge(f"{sibling}_MassFlow", cid, "m_out")
                        else:
                            # Suction-side valve with no inter-stage separator model.
                            # Use the compressor's own suction separator pressure as P_out
                            # (same traversal strategy as CONTAINER_PRESSURE for UV valves).
                            suction = find_nearest_state(
                                found[0], 'Pressure', True,
                                pass_through_types=['Compressor', 'Pump'])[:1]
                            if suction:
                                add_edge(f"{suction[0]}_Pressure", cid, "P_out")
                            # else: fall through to signal_map boundary below
                    else:
                        for n in found:
                            add_edge(f"{n}_Pressure", cid, "P_out")
                else:
                    # No modeled downstream Pressure equation — check signal_map for a
                    # direct downstream pressure signal (e.g. export boundary).
                    fallback_key = f"{comp}_DownstreamPressure"
                    if fallback_key in signal_map:
                        if fallback_key not in equation_ids:
                            tsa_states.append({"id": fallback_key,
                                               "label": f"{comp}\n(DownstreamPressure)\n[boundary]",
                                               "shape": "box", "color": "#D9EAD3"})
                            equation_ids.add(fallback_key)
                        add_edge(fallback_key, cid, "P_out")

            elif inp == "CONTAINER_PRESSURE":
                # Find P_out for UV valve: pressure at its recirculation outlet.
                # UV feeds the compressor inlet pipe directly (not the separator).
                # Follow UV outlet downstream to find which compressor it serves,
                # then BFS upstream from that compressor (no pass-throughs, skip UV/FE)
                # to find the suction-side pressure.
                #   LP UV → KA0001 upstream → VA0001_Pressure (LP suction separator)
                #   HP UV → KA2001 upstream → HX0001_Pressure (LP discharge = HP suction)
                found_pout = False
                for target in get_downstream(comp, flow_only=True)[:3]:
                    suction = [s for s in find_nearest_state(target, 'Pressure', True,
                                                             skip_comps=['UV', 'FE'])
                               if f"{s}_Pressure" in equation_ids][:1]
                    if suction:
                        add_edge(f"{suction[0]}_Pressure", cid, "P_out")
                        found_pout = True
                        break
                if not found_pout:
                    # Fallback: original upstream-through-HX+Compressor approach
                    for n in [n for n in find_nearest_state(comp, 'Pressure', True,
                                                            skip_comps=['UV', 'FE'],
                                                            pass_through_types=['HeatExchanger', 'Compressor', 'Pump'])
                              if f"{n}_Pressure" in equation_ids][:1]:
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

            elif inp == "ANTISURGE_INFLOW":
                # UV recirculation returns gas to the compressor inlet pipe, not the separator.
                # Mass balance on inlet pipe: VA_outflow = KA_flow - UV_flow
                # -> UV_flow acts as positive inflow (m_in) in VA0001 pressure balance.
                # Use find_nearest_state with pass_through_types so pruned ESV valves
                # (no MassFlow equation) AND unpruned non-compressor flow valves (e.g.
                # ESV1002 with a MassFlow equation) are both traversed past to reach the
                # actual compressor. get_downstream alone stops at the first base_type node.
                for ds_comp in find_nearest_state(comp, 'MassFlow', False,
                                                   pass_through_types=['BlockValve', 'PipeFlow',
                                                                        'ControlValve', 'SafetyValve']):
                    if 'Compressor' not in base_types.get(ds_comp, ''):
                        continue
                    for u, _ in get_upstream(ds_comp, flow_only=True):
                        if f"{u}_MassFlow" not in equation_ids:
                            continue
                        is_uv = 'UV' in u.upper()
                        if not is_uv:
                            for ctrl_comp, _ in raw_ins.get(u, []):
                                if 'ASC' in base_types.get(ctrl_comp, ''):
                                    is_uv = True
                                    break
                        if is_uv:
                            add_edge(f"{u}_MassFlow", cid, "m_in")

            elif inp == "DOWNSTREAM_COMPRESSOR_FLOW":
                # For separator/tank gas pressure: wire only actual compressor flows as
                # m_out, bypassing intermediate valves (ESV, PipeFlow, ControlValve).
                # This avoids the ESV/KA collinearity that arises when an ESV valve has
                # its own MassFlow equation and appears alongside KA in the OLS fit —
                # OLS assigns large compensating gains (~±260) that amplify small errors
                # ~260× in closed-loop → divergence.
                for n in find_nearest_state(comp, 'MassFlow', False,
                                             pass_through_types=['BlockValve', 'PipeFlow',
                                                                  'ControlValve', 'SafetyValve']):
                    if 'Compressor' in base_types.get(n, ''):
                        add_edge(f"{n}_MassFlow", cid, "m_out")

            elif inp == "DOWNSTREAM_LIQUID_OUTFLOWS":
                # Oil/water liquid outflows from separator/tank (LV valves, overflow valves).
                # find_nearest_state (no pass_through) returns the first MassFlow states
                # reachable downstream — these include both liquid valves (LV/MV) and gas
                # path block valves (ESV). Exclude gas-path nodes by checking whether the
                # node eventually leads to a Compressor downstream.
                for n in find_nearest_state(comp, 'MassFlow', False):
                    if f"{n}_MassFlow" not in equation_ids:
                        continue
                    kt_n = base_types.get(n, '')
                    if 'Compressor' in kt_n:
                        continue
                    # Exclude gas-path valves (BlockValve/PipeFlow ESVs that lead to KA).
                    is_gas = any('Compressor' in base_types.get(d, '')
                                 for d in find_nearest_state(n, 'MassFlow', False,
                                                              pass_through_types=['BlockValve', 'PipeFlow',
                                                                                  'ControlValve', 'SafetyValve']))
                    if not is_gas:
                        add_edge(f"{n}_MassFlow", cid, "m_out")

            elif inp == "MEASURED_FLOW":
                ctype = eq.get("ControllerType", "")
                if ctype == "ASC":
                    # ASC should measure the compressor it protects, not the UV valve flow.
                    # Follow: ASC controls UV valve -> UV feeds inlet pipe -> Compressor.
                    found_flow = False
                    for uv in get_downstream(comp):
                        if 'UV' not in uv.upper():
                            continue
                        for ka in get_downstream(uv, flow_only=True):
                            if 'Compressor' in base_types.get(ka, '') and f"{ka}_MassFlow" in equation_ids:
                                add_edge(f"{ka}_MassFlow", cid, "y_flow")
                                found_flow = True
                                break
                        if found_flow:
                            break
                    if not found_flow:
                        # Fallback: skip UV in BFS to avoid circular dependency
                        for n in find_nearest_state(comp, 'MassFlow', True, skip_comps=['UV'])[:1]:
                            add_edge(f"{n}_MassFlow", cid, "y_flow")
                else:
                    for n in find_nearest_state(comp, 'MassFlow', True)[:1]:
                        add_edge(f"{n}_MassFlow", cid, "y_flow")

            elif inp == "MEASURED_PRESSURE":
                ctype = eq.get("ControllerType", "")
                if ctype == "ASC":
                    # ASC only needs inlet and outlet pressure of the protected compressor.
                    added_pres = False
                    for uv in get_downstream(comp):
                        if 'UV' not in uv.upper():
                            continue
                        for ka in get_downstream(uv, flow_only=True):
                            if 'Compressor' not in base_types.get(ka, ''):
                                continue
                            for p in find_nearest_state(ka, 'Pressure', True,
                                                         skip_comps=['UV', 'FE'])[:1]:
                                add_edge(f"{p}_Pressure", cid, "y_pres")
                            if f"{ka}_Pressure" in equation_ids:
                                add_edge(f"{ka}_Pressure", cid, "y_pres")
                            added_pres = True
                            break
                        if added_pres:
                            break
                    if not added_pres:
                        for n in find_nearest_state(comp, 'Pressure', True):
                            add_edge(f"{n}_Pressure", cid, "y_pres")
                        for n in find_nearest_state(comp, 'Pressure', False):
                            add_edge(f"{n}_Pressure", cid, "y_pres")
                else:
                    for n in find_nearest_state(comp, 'Pressure', True):
                        add_edge(f"{n}_Pressure", cid, "y_pres")
                    for n in find_nearest_state(comp, 'Pressure', False):
                        add_edge(f"{n}_Pressure", cid, "y_pres")

            elif inp == "MEASURED_STATE":
                kt = base_types.get(comp.split('_')[0], '')
                comp_bare = comp.split('_')[0]
                if 'Control' in kt and 'ASC' not in comp:
                    if 'PIC' in comp:
                        # Pressure controllers: trace measurement chain via raw_ins.
                        # find_nearest_state uses flow_only=True which filters 'Measured'
                        # ports, so BFS through flow connections fails for PIC→PT→pv chains.
                        # Instead walk raw_ins directly:
                        #   PIC → PT (Measurement port) → pv_node → BFS upstream in raw_ins
                        #   skipping UV components to avoid reaching discharge HX via recirc.
                        found_pic = False
                        for src, port in raw_ins.get(comp_bare, []):
                            if found_pic:
                                break
                            if 'Measurement' not in port and 'Measured' not in port:
                                continue
                            for psrc, _ in raw_ins.get(src, []):
                                if found_pic:
                                    break
                                bfs_vis = set()
                                bfs_q = [psrc]
                                while bfs_q and not found_pic:
                                    bfs_curr = bfs_q.pop(0)
                                    if bfs_curr in bfs_vis:
                                        continue
                                    if 'UV' in bfs_curr.upper():
                                        continue
                                    bfs_vis.add(bfs_curr)
                                    if f"{bfs_curr}_Pressure" in equation_ids:
                                        add_edge(f"{bfs_curr}_Pressure", cid, "y_meas")
                                        found_pic = True
                                        break
                                    for usrc, _ in raw_ins.get(bfs_curr, []):
                                        bfs_q.append(usrc)
                        if not found_pic:
                            meas_id = f"{comp_bare}:Measurement"
                            add_edge(meas_id, cid, "y_meas")
                            if meas_id not in equation_ids:
                                tsa_states.append({"id": meas_id, "label": meas_id, "shape": "box", "color": "#EAD1DC"})
                                equation_ids.add(meas_id)
                    else:
                        # K-Spice generic controllers (LIC, TIC, SIC, etc.) use the
                        # Measurement virtual node resolved via SignalEquivalenceMap.
                        meas_id = f"{comp_bare}:Measurement"
                        add_edge(meas_id, cid, "y_meas")
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
                                    add_edge(f"{u}_OilLevel", cid, "y_meas")
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

    # ── Cascade PID setpoints ────────────────────────────────────────────────
    # Scan K-Spice wiring: when a controller's ExternalSetpoint (or Setpoint)
    # input comes from another controller's ControllerOutput, add a cascade_sp
    # edge so the topological sort evaluates the master before the slave and
    # ClosedLoopRunner uses the master's prediction instead of the CSV value.
    for m in kspice_data.get('Models', []):
        comp = m['Name'].replace('_pf', '')
        ctrl_id = f"{comp}_Control"
        if ctrl_id not in equation_ids:
            continue
        for inp in m.get('Inputs', []):
            dst = inp.get('Destination', '')
            if 'Setpoint' not in dst:
                continue
            src_full = inp.get('Source', '')
            if not src_full:
                continue
            src_comp = src_full.split(':')[0].replace('_pf', '')
            src_ctrl_id = f"{src_comp}_Control"
            if src_ctrl_id in equation_ids and src_ctrl_id != ctrl_id:
                add_edge(src_ctrl_id, ctrl_id, "cascade_sp")

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