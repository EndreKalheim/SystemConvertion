import json
import os
import argparse
import re

def build_state_topology(input_json, output_json, output_html):
    with open(input_json, 'r', encoding='utf-8') as f:
        data = json.load(f)

    tsa_states = []
    edges = []
    base_components = {}
    
    # 1. Component Filtering & Base Names
    # We strip out noise like pv_, FE, network-, etc.
    ignore_types = ['Alarm', 'Transmitter', 'Indicator', 'SignalSwitch', 'ProfileViewer']
    
    def is_noise_block(name, btype):
        nl = name.lower()
        if any(itype in btype for itype in ignore_types): return True
        if nl.endswith('_m') or nl.endswith('_view'): return True
        if nl.startswith('network-') or nl.startswith('pv'): return True
        if 'fe0' in nl or 'fit0' in nl or 'tit0' in nl or 'pit0' in nl: return True 
        return False
        
    def fix_name(name):
        return name.replace('_pf', '')

    raw_node_inputs = {}
    
    # First Pass: Collect everything
    for model in data.get('Models', []):
        m_name = model.get('Name')
        m_type = model.get('KSpiceType')
        base_name = fix_name(m_name)
        
        if base_name not in raw_node_inputs:
            raw_node_inputs[base_name] = []
            
        for inp in model.get('Inputs', []):
            src_raw = inp.get('Source', '')
            if src_raw:
                src_base = fix_name(src_raw.split(':')[0])
                raw_node_inputs[base_name].append((src_base, src_raw))
        
        if is_noise_block(m_name, m_type) or is_noise_block(base_name, m_type):
            continue
            
        if base_name not in base_components:
            base_components[base_name] = {'types': set(), 'params': model.get('Parameters', {})}
        base_components[base_name]['types'].add(m_type)
        
    # Function to resolve upstream nodes bypassing noise blocks
    def resolve_upstream(start_node):
        resolved = []
        visited = set()
        def trace(current_node, original_sig):
            if current_node in visited: return
            visited.add(current_node)
            if current_node not in raw_node_inputs: return
            for upstream, raw_sig in raw_node_inputs[current_node]:
                active_sig = original_sig if original_sig else raw_sig
                if upstream in base_components:
                    resolved.append((upstream, active_sig))
                else:
                    trace(upstream, active_sig)
        trace(start_node, None)
        return resolved

    # 2. Categorize Physical Roles & Instantiate Explicit States
    state_nodes = {}
    def add_state(b_name, suffix, role_color, shape="ellipse"):
        s_id = f"{b_name}_{suffix}"
        if s_id not in state_nodes:
            state_nodes[s_id] = True
            
            # Find parameters for the label
            params = base_components[b_name].get('params', {})
            info = ""
            if "Valve" in str(base_components[b_name]['types']) and 'M' in params:
                info = f"Cv: {params['M']:.2f}"
            elif "Separator" in str(base_components[b_name]['types']) and 'Diameter' in params:
                info = f"D: {params.get('Diameter', 0)}m L: {params.get('Length', 0)}m"
                
            label = f"{b_name}\n({suffix})"
            if info: label += f"\n[{info}]"
            
            tsa_states.append({"id": s_id, "label": label, "color": role_color, "shape": shape})

    C_FLOW = "#a3d2ca"
    C_PRES = "#f5d787"
    C_TEMP = "#f3a683"
    C_LVL  = "#81ecec"
    C_CTRL = "#ff7675"

    node_roles = {}
    
    for base_name, comp_data in base_components.items():
        types = comp_data['types']
        
        if 'Separator' in types or 'Tank' in types or 'Volume' in types:
            node_roles[base_name] = 'Volume'
            add_state(base_name, "Pressure", C_PRES)
            add_state(base_name, "Temperature", C_TEMP)
            add_state(base_name, "Level", C_LVL)
            
        elif any(t in types for t in ['PipeFlow', 'ControlValve', 'ChokeValve', 'CentrifugalCompressor', 'Pump', 'Source']):
            node_roles[base_name] = 'FlowEquipment'
            add_state(base_name, "MassFlow", C_FLOW)
            add_state(base_name, "Temperature", C_TEMP)
            add_state(base_name, "Pressure", C_PRES)
                
        elif 'PIDController' in types or 'PID' in types or 'GenericASC' in types:
            node_roles[base_name] = 'Controller'
            suffix = "Control" if 'PID' in types else "Output"
            add_state(base_name, suffix, C_CTRL, "box")
            if 'GenericASC' in types:
                add_state(base_name, "SurgeLimit", C_FLOW)
                
        else:
            node_roles[base_name] = 'Generic'
            add_state(base_name, "MassFlow", C_FLOW)
            add_state(base_name, "Pressure", C_PRES)
            add_state(base_name, "Temperature", C_TEMP)

    # 3. Exhaustive MISO Mapping & User Filtering Logic
    added_edges = set()
    def add_edge(frm, to, lbl=""):
        if frm in state_nodes and to in state_nodes:
            edge_id = f"{frm}->{to}"
            if edge_id not in added_edges:
                edges.append({"from": frm, "to": to, "label": lbl, "arrows": "to", "font": {"size": 8, "align": "middle"}})
                added_edges.add(edge_id)

    # Parse ALL real connections from K-Spice first
    for base_name, comp_data in base_components.items():
        upstream_nodes = resolve_upstream(base_name)
        role = node_roles[base_name]
        
        for up, raw_sig in upstream_nodes:
            if up == base_name: continue
            up_role = node_roles.get(up, 'Generic')
            sig_lower = raw_sig.lower()
            
            # -----------------------------------------------------------------
            # 1. PIDs & Controllers: Pure Measurement / Output logic
            # -----------------------------------------------------------------
            if role == 'Controller':
                if 'asc' in base_name.lower():
                    # Map the first 3 inputs accurately for ASC based on original signals
                    if 'pt' in up.lower() or 'pressure' in sig_lower:
                        add_edge(f"{up}_Pressure", f"{base_name}_Output", "ASC_Pressure")
                    elif 'fe' in up.lower() or 'flow' in sig_lower or 'dp' in sig_lower:
                        add_edge(f"{up}_MassFlow", f"{base_name}_Output", "ASC_Flow")
                    else:
                        add_edge(f"{up}_Pressure", f"{base_name}_Output", "ASC_Signal")
                    continue
                    
                if 'level' in sig_lower or 'lic' in base_name.lower():
                    add_edge(f"{up}_Level", f"{base_name}_Control", "Measure")
                elif 'pressure' in sig_lower or 'pic' in base_name.lower():
                    add_edge(f"{up}_Pressure", f"{base_name}_Control", "Measure")
                elif 'temp' in sig_lower or 'tic' in base_name.lower():
                    add_edge(f"{up}_Temperature", f"{base_name}_Control", "Measure")
                elif 'flow' in sig_lower:
                    add_edge(f"{up}_MassFlow", f"{base_name}_Control", "Measure")
                continue
                
            if up_role == 'Controller':
                up_suffix = "Output" if 'asc' in up.lower() else "Control"
                add_edge(f"{up}_{up_suffix}", f"{base_name}_MassFlow", "Command")
                continue

            # -----------------------------------------------------------------
            # 2. Physics Logic: Map everything, then filter (as requested)
            # -----------------------------------------------------------------
            
            # A. Homologous State Chains (Everything passes what it has)
            # Upstream passes its Temp/MassFlow to downstream.
            add_edge(f"{up}_Temperature", f"{base_name}_Temperature", "energy")
            if role != 'Volume': 
                # Volumes aggregate massflow, so direct massflow-to-massflow usually is only between pipes
                add_edge(f"{up}_MassFlow", f"{base_name}_MassFlow", "flow")

            # B. Separator / Tank specific mapping
            # "seperator get all the states ... massflow and tempreture from everything leaving or going in to it."
            if role == 'Volume':
                # Stuff going IN to the Separator
                add_edge(f"{up}_MassFlow", f"{base_name}_Pressure", "mass_in")
                add_edge(f"{up}_MassFlow", f"{base_name}_Level", "mass_in")
                # Do not invent same-node Level→Temperature edges here; equation-driven topology carries inventory→T when needed.
            elif up_role == 'Volume':
                # Stuff coming OUT of the Separator (FlowEquipment downstream)
                # "pressure dont realy care about massflow that often unless it is the seperator (tank)"
                # The Volume's Pressure drives the Flow downstream
                add_edge(f"{up}_Pressure", f"{base_name}_MassFlow", "drives_flow")
                # Flow downstream drains the Volume
                add_edge(f"{base_name}_MassFlow", f"{up}_Pressure", "mass_out_drain")
                add_edge(f"{base_name}_MassFlow", f"{up}_Level", "mass_out_drain")

    # Render HTML
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><title>TSA Exhaustive MISO State Topology</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        #mynetwork {{ width: 100vw; height: 100vh; border: 1px solid lightgray; background:#f4f4f4; }}
        body {{ margin: 0; padding: 0; overflow: hidden; font-family: sans-serif; }}
    </style>
    </head>
    <body>
    <div id="mynetwork"></div>
    <script type="text/javascript">
        var nodes = new vis.DataSet({json.dumps(tsa_states)});
        var edges = new vis.DataSet({json.dumps(edges)});
        var network = new vis.Network(document.getElementById('mynetwork'), {{nodes: nodes, edges: edges}}, {{
            physics: {{ stabilization: true, barnesHut: {{ springLength: 220, springConstant: 0.04 }} }},
            layout: {{ hierarchical: false }}
        }});
    </script>
    </body></html>
    """
    
    os.makedirs(os.path.dirname(output_html), exist_ok=True)
    with open(output_html, 'w') as f:
        f.write(html_content)
        
    print(f"TSA Exhaustive State Topology generated at {output_html}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", required=True)
    parser.add_argument("--out_json", required=False)
    parser.add_argument("--out_html", required=True)
    args = parser.parse_args()
    build_state_topology(args.json, args.out_json, args.out_html)
