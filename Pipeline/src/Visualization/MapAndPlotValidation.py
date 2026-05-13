import pandas as pd
import json
import os
import matplotlib.pyplot as plt

def generate_validation_plots():
    csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../KspiceSim.csv'))
    topology_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../output/diagrams/TSA_Explicit_Topology.json'))
    out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../output/validation_plots/'))

    os.makedirs(out_dir, exist_ok=True)
    
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Strip whitespace from headers
    df.columns = [c.strip() for c in df.columns]

    print(f"Loading explicit topology map...")
    with open(topology_path, 'r') as f:
        topology = json.load(f)
        
    nodes = {n['id']: n for n in topology['nodes']}
    edges = topology['edges']
    
    # Build adyacency maps
    inputs_map = {n_id: [] for n_id in nodes}
    for e in edges:
        if e['to'] in inputs_map:
            inputs_map[e['to']].append({'source': e['from'], 'label': e['label']})
            
    # Naive fallback mapping from State -> KSpice CSV column pattern
    # The actual column name in K-Spice often looks like "23LV0002_pf:MassFlow" or "23VA0001:Temperature"
    def find_column(node_id):
        comp = node_id.split('_')[0]
        state = node_id.split('_')[1]
        
        # Possible variations
        candidates = [
            f"{comp}:{state}",
            f"{comp}_pf:{state}",
            f"{comp}_m:{state}"
        ]
        
        # State remapping
        if state == "WaterLevel":
            candidates.append(f"{comp}:LevelHeavyPhaseFeedSideWeir")
        elif state == "OilLevel":
            candidates.append(f"{comp}:LevelOverflowLiquid")
        elif state == "OilLevel":
            candidates.append(f"{comp}:Level")
            candidates.append(f"{comp}:VolumeTotal")
        elif state == "Control":
            candidates.append(f"{comp}:ControllerOutput")
            candidates.append(f"{comp}:Output")
            
        # Hardcoded bug fixes for name mismatches between old architecture and new CSV
        if comp == "23VA001": 
            comp = "23VA0001"
            candidates.append(f"{comp}:{state}")
            if state == "WaterLevel": candidates.append(f"{comp}:LevelHeavyPhaseFeedSideWeir")
            elif state == "OilLevel": candidates.append(f"{comp}:LevelOverflowLiquid")
            elif state == "OilLevel": candidates.append(f"{comp}:VolumeTopWeirOverflowSide")
        
        if comp == "23LIC002":
            comp = "23LIC0002"
            candidates.append(f"{comp}:ControllerOutput")
        
        for c in candidates:
            if c in df.columns:
                return c
        
        # Fallback partial match
        for col in df.columns:
            if comp in col and state in col:
                return col
        return None

    unmapped = []
    
    for n_id, node in nodes.items():
        tgt_col = find_column(n_id)
        if not tgt_col:
            unmapped.append(f"{n_id} (Target not found)")
            continue
            
        in_cols = []
        for e in inputs_map[n_id]:
            src_node = e['source']
            src_col = find_column(src_node)
            if src_col:
                in_cols.append((src_node, src_col, e['label']))
                
        if not in_cols:
            # Skip plotting bounds or empty models
            continue
            
        # Plotting
        plt.figure(figsize=(10, 6))
        
        # Plot target Output
        plt.subplot(2, 1, 1)
        plt.plot(df['ModelTime'], df[tgt_col], label=f"OUTPUT: {tgt_col}", color='black')
        plt.title(f"Model ID: {n_id}")
        plt.legend(loc='upper right')
        plt.grid()
        
        # Plot Inputs
        plt.subplot(2, 1, 2)
        for (sid, s_col, lbl) in in_cols:
            plt.plot(df['ModelTime'], df[s_col], label=f"IN [{lbl}]: {s_col}")
        
        plt.title(f"Required Inputs for {n_id}")
        plt.legend(loc='upper right')
        plt.grid()
        plt.tight_layout()
        
        save_path = os.path.join(out_dir, f"{n_id}.png")
        plt.savefig(save_path)
        plt.close()
        
        print(f"Generated validation plot: {n_id}")

    if unmapped:
        print("\nCould not find mapped CSV columns for:")
        for x in unmapped:
            print(" -", x)

if __name__ == '__main__':
    generate_validation_plots()