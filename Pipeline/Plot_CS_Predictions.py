import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def plot_validation():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    predictions_csv = os.path.join(base_dir, "output", "CS_Predictions.csv")
    kspice_csv = os.path.join(base_dir, "data", "raw", "KspiceSim.csv")
    mapping_json = os.path.join(base_dir, "output", "diagrams", "SignalMapping.json")
    params_json = os.path.join(base_dir, "output", "CS_Identified_Parameters.json")
    topology_json = os.path.join(base_dir, "output", "diagrams", "TSA_Explicit_Topology.json")
    out_dir = os.path.join(base_dir, "output", "validation_plots")

    os.makedirs(out_dir, exist_ok=True)

    # Load Data
    try:
        df_pred = pd.read_csv(predictions_csv)
        df_raw = pd.read_csv(kspice_csv)
        df_raw.columns = [c.strip() for c in df_raw.columns]
        
        with open(mapping_json, 'r') as f:
            signal_map = json.load(f)
            
        params = {}
        if os.path.exists(params_json):
            with open(params_json, 'r') as f:
                params = json.load(f)
                
        # Load topology for input edge lookup
        topology = {}
        if os.path.exists(topology_json):
            with open(topology_json, 'r') as f:
                topology = json.load(f)
                
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # Build input edge map from topology
    input_edges = {}
    for edge in topology.get('edges', []):
        to_node = edge['to']
        from_node = edge['from']
        label = edge.get('label', '')
        if to_node not in input_edges:
            input_edges[to_node] = []
        input_edges[to_node].append({'from': from_node, 'label': label})

    plot_count = 0

    # Create plots for each modeled component
    for col in df_pred.columns:
        if col.endswith("_Predicted"):
            model_id = col.replace("_Predicted", "")
            true_col = f"{model_id}_True"
            
            if true_col not in df_pred.columns:
                continue

            # Component tags like 23VA001; state may contain underscores (e.g. TotalLevel)
            if '_' in model_id:
                comp, state = model_id.split('_', 1)
            else:
                comp, state = model_id, ''
            
            # One subplot per raw CSV column (dedupe); label shows first topology edge for that column.
            inputs_by_csv = {}

            if model_id in input_edges:
                for edge_info in input_edges[model_id]:
                    src_node = edge_info['from']
                    edge_label = edge_info['label']
                    if src_node in signal_map:
                        csv_col = signal_map[src_node]
                        if csv_col in df_raw.columns:
                            display_name = f"{edge_label}: {src_node}"
                            if csv_col not in inputs_by_csv:
                                inputs_by_csv[csv_col] = display_name
                            elif edge_label not in inputs_by_csv[csv_col]:
                                inputs_by_csv[csv_col] = inputs_by_csv[csv_col] + f" | {edge_label}: {src_node}"

            input_signals_to_plot = {label: csv for csv, label in inputs_by_csv.items()}

            # Build title
            title_str = f"Model: {model_id}\n"
            if model_id in params:
                p = params[model_id]
                if "Formula" in p:
                    title_str += f"Eq: {p['Formula']}"
                if "FitScore" in p:
                    title_str += f" | Fit: {p['FitScore']:.1f}%"
                if "Kp" in p:
                    title_str += f" | Kp={p['Kp']:.3f}, Ti={p['Ti']:.3f}"
                if "Cv" in p:
                    title_str += f" | Cv={p['Cv']:.4f}"
                if "TimeConstant_s" in p:
                    title_str += f" | Tc={p['TimeConstant_s']:.2f}s"

            num_inputs = len(input_signals_to_plot)
            num_plots = 1 + num_inputs
            
            fig, axes = plt.subplots(num_plots, 1, figsize=(10, 3 + 2 * num_plots), sharex=True)
            if num_plots == 1:
                axes = [axes]
            
            # Top plot: Output comparison
            ax1 = axes[0]
            ax1.plot(df_pred['Time'], df_pred[true_col], label="K-Spice (Solution)", color='black', linestyle='--', linewidth=2)
            ax1.plot(df_pred['Time'], df_pred[col], label="TSA Model", color='blue', alpha=0.7)
            ax1.set_title(title_str, fontweight='bold', fontsize=9)
            ax1.set_ylabel("Output")
            ax1.legend(loc='upper right', fontsize=8)
            ax1.grid(True)
            
            # Input subplots
            min_len = min(len(df_pred), len(df_raw))
            for i, (label_name, raw_col_name) in enumerate(input_signals_to_plot.items(), 1):
                if i < len(axes):
                    ax = axes[i]
                    ax.plot(df_pred['Time'][:min_len], df_raw[raw_col_name][:min_len], label=raw_col_name, color='tab:blue')
                    ax.set_ylabel(label_name, fontsize=8)
                    ax.legend(loc='upper right', fontsize=7)
                    ax.grid(True)
            
            axes[-1].set_xlabel("Time (s)")
            
            plt.tight_layout()
            
            # Use a cleaner filename: CompName_State_Validation.png
            safe_id = model_id.replace('/', '_').replace('\\', '_')
            plot_file = os.path.join(out_dir, f"{safe_id}_Validation.png")
            plt.savefig(plot_file, dpi=120)
            plt.close()
            
            plot_count += 1
            print(f"Generated plot: {safe_id}")

    print(f"\n[SUMMARY] Generated {plot_count} validation plots in {out_dir}")

if __name__ == "__main__":
    plot_validation()
