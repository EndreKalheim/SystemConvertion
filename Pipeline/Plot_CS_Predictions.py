import os
import glob
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


def plot_validation():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    predictions_csv = os.path.join(base_dir, "output", "CS_Predictions.csv")
    kspice_csv      = os.path.join(base_dir, "data", "raw", "KspiceSim.csv")
    mapping_json    = os.path.join(base_dir, "output", "diagrams", "SignalMapping.json")
    params_json     = os.path.join(base_dir, "output", "CS_Identified_Parameters.json")
    topology_json   = os.path.join(base_dir, "output", "diagrams", "TSA_Explicit_Topology.json")
    out_dir         = os.path.join(base_dir, "output", "validation_plots")

    # ── Clean up old plots so stale files never accumulate ───────────────────
    if os.path.exists(out_dir):
        for old_file in glob.glob(os.path.join(out_dir, "*.png")):
            os.remove(old_file)
    os.makedirs(out_dir, exist_ok=True)

    # ── Load data ─────────────────────────────────────────────────────────────
    try:
        df_pred = pd.read_csv(predictions_csv)
        df_raw  = pd.read_csv(kspice_csv)
        df_raw.columns = [c.strip() for c in df_raw.columns]

        with open(mapping_json, 'r') as f:
            signal_map = json.load(f)

        params = {}
        if os.path.exists(params_json):
            with open(params_json, 'r') as f:
                params = json.load(f)

        topology = {}
        if os.path.exists(topology_json):
            with open(topology_json, 'r') as f:
                topology = json.load(f)
    except Exception as e:
        print(f"[ERROR] Loading files: {e}")
        return

    # ── Build input-edge map from topology ───────────────────────────────────
    input_edges = {}
    for edge in topology.get('edges', []):
        to_node   = edge['to']
        from_node = edge['from']
        label     = edge.get('label', '')
        input_edges.setdefault(to_node, []).append({'from': from_node, 'label': label})

    # ── Plot each modelled state ──────────────────────────────────────────────
    plotted = skipped_boundary = skipped_fallback = 0

    for col in df_pred.columns:
        if not col.endswith("_Predicted"):
            continue

        model_id = col.replace("_Predicted", "")
        true_col = f"{model_id}_True"
        if true_col not in df_pred.columns:
            continue

        # Component / state split on the first underscore (names like 23VA0001_TotalLevel)
        first_under = model_id.find('_')
        comp  = model_id[:first_under] if first_under > 0 else model_id
        state = model_id[first_under+1:] if first_under > 0 else ''

        # ── Model quality gate ────────────────────────────────────────────────
        p          = params.get(model_id, {})
        model_type = p.get('ModelType', 'Unknown')

        if model_type == 'Boundary':
            skipped_boundary += 1
            continue   # Boundary sources are inputs, not model outputs to validate

        is_fallback = (model_type == 'Fallback')
        if is_fallback:
            skipped_fallback += 1
            continue   # Nothing was fitted — skip rather than show a misleading plot

        # ── Gather input signals from topology ────────────────────────────────
        inputs_by_csv = {}
        for edge_info in input_edges.get(model_id, []):
            src_node   = edge_info['from']
            edge_label = edge_info['label']
            csv_col    = signal_map.get(src_node) or signal_map.get(src_node.replace('_pf', ''))
            if csv_col and csv_col in df_raw.columns:
                display = f"{edge_label}: {src_node}"
                if csv_col not in inputs_by_csv:
                    inputs_by_csv[csv_col] = display
                else:
                    inputs_by_csv[csv_col] += f" | {edge_label}"

        # csv_col → display_label mapping for subplot rows
        input_signals = {lbl: csv for csv, lbl in inputs_by_csv.items()}

        # ── Build title ───────────────────────────────────────────────────────
        fit_score = p.get('FitScore')
        fit_str   = f"  |  Fit: {fit_score:.1f}%" if fit_score is not None else ""
        tc        = p.get('TimeConstant_s')
        tc_str    = f"  Tc={tc:.2f}s" if tc is not None else ""
        formula   = p.get('Formula', '')
        title_str = f"{model_id}  [{model_type}{fit_str}{tc_str}]\n{formula}"

        # ── Layout ────────────────────────────────────────────────────────────
        n_inputs  = len(input_signals)
        n_subplots = 1 + n_inputs
        fig, axes = plt.subplots(n_subplots, 1,
                                 figsize=(11, 3 + 2.2 * n_subplots),
                                 sharex=True)
        if n_subplots == 1:
            axes = [axes]

        # Top plot: prediction vs measurement
        ax0 = axes[0]
        y_true = df_pred[true_col].values
        y_pred = df_pred[col].values

        ax0.plot(df_pred['Time'], y_true,
                 label="K-Spice (measurement)", color='black', linestyle='--', linewidth=1.8)
        ax0.plot(df_pred['Time'], y_pred,
                 label="TSA model", color='steelblue', alpha=0.85, linewidth=1.4)

        # Shade residual area to make fit quality immediately visible
        ax0.fill_between(df_pred['Time'], y_true, y_pred,
                         alpha=0.15, color='red', label='residual')

        ax0.set_title(title_str, fontweight='bold', fontsize=9)
        ax0.set_ylabel(state or "Output")
        ax0.legend(loc='upper right', fontsize=8)
        ax0.grid(True, alpha=0.4)

        # Input subplots — one row per unique CSV column
        min_len = min(len(df_pred), len(df_raw))
        for i, (display_lbl, csv_col) in enumerate(input_signals.items(), start=1):
            if i >= len(axes):
                break
            ax = axes[i]
            ax.plot(df_pred['Time'].values[:min_len],
                    df_raw[csv_col].values[:min_len],
                    color='darkorange', linewidth=1.2)
            ax.set_ylabel(display_lbl, fontsize=7.5)
            ax.set_title(f"Input: {csv_col}", fontsize=8)
            ax.grid(True, alpha=0.4)

        axes[-1].set_xlabel("Time (s)")
        plt.tight_layout()

        safe_id   = model_id.replace('/', '_').replace('\\', '_')
        plot_file = os.path.join(out_dir, f"{safe_id}_Validation.png")
        plt.savefig(plot_file, dpi=120)
        plt.close()
        plotted += 1
        print(f"  Plotted: {safe_id}  [{model_type}{fit_str}]")

    print(f"\n[SUMMARY] {plotted} validation plots written to {out_dir}")
    print(f"          Skipped: {skipped_boundary} boundary sources, {skipped_fallback} failed fits")


if __name__ == "__main__":
    plot_validation()
