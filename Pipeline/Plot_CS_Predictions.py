import os
import glob
import json
import argparse
from collections import deque
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


def _build_physical_adjacency(system_map_path):
    """Bidirectional component adjacency from K-Spice stream connections."""
    if not os.path.exists(system_map_path):
        return {}

    def _norm(name):
        if name.endswith('_pf'): return name[:-3]
        if name.endswith('_m'):  return name[:-2]
        return name

    with open(system_map_path) as f:
        smap = json.load(f)

    adj = {}
    for model in smap.get('Models', []):
        comp = _norm(model['Name'])
        for inp in model.get('Inputs', []):
            if 'Stream' not in inp.get('Destination', ''):
                continue
            src_raw = inp.get('Source', '')
            colon   = src_raw.find(':')
            if colon <= 0:
                continue
            src_comp = _norm(src_raw[:colon])
            if src_comp == comp:
                continue
            adj.setdefault(comp, set()).add(src_comp)
            adj.setdefault(src_comp, set()).add(comp)
    return adj


def _find_proxy_signal(missing_comp, state_suffix, parent_comp, adj, signal_map, dataset_cols, max_hops=8):
    """BFS downstream (excluding parent_comp) to find nearest series neighbour
    that has {neighbour}_{state_suffix} in signal_map with a CSV column.
    Returns (proxy_key, csv_col) or (None, None)."""
    visited = {missing_comp}
    if parent_comp:
        visited.add(parent_comp)

    queue = deque([(missing_comp, 0)])
    while queue:
        comp, depth = queue.popleft()
        if depth >= max_hops:
            continue
        for nb in adj.get(comp, set()):
            if nb in visited:
                continue
            visited.add(nb)
            candidate = f"{nb}_{state_suffix}"
            csv_col   = signal_map.get(candidate)
            if csv_col and csv_col in dataset_cols:
                return candidate, csv_col
            queue.append((nb, depth + 1))
    return None, None


def plot_validation(predictions_csv=None, kspice_csv=None, out_dir=None):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if predictions_csv is None:
        predictions_csv = os.path.join(base_dir, "output", "CS_Predictions.csv")
    if kspice_csv is None:
        kspice_csv = os.path.join(base_dir, "data", "raw", "KspiceSim.csv")
    if out_dir is None:
        out_dir = os.path.join(base_dir, "output", "validation_plots")
    mapping_json    = os.path.join(base_dir, "output", "diagrams", "SignalMapping.json")
    params_json     = os.path.join(base_dir, "output", "CS_Identified_Parameters.json")
    topology_json   = os.path.join(base_dir, "output", "diagrams", "TSA_Explicit_Topology.json")
    system_map_json = os.path.join(base_dir, "data", "extracted", "KSpiceSystemMap.json")

    # Optional sidecar fit-scores file. Written by OpenLoopTestRunner / ClosedLoopRunner
    # next to their predictions CSV. When present, plot titles show the test/closed-loop
    # fit instead of the training fit (which would otherwise be misleading on test data).
    # Sidecar naming: predictions "CS_Predictions_<Tag>.csv" → fits "<Tag>_FitScores.json".
    fits_label, override_fits = None, {}
    pred_dir, pred_file = os.path.split(predictions_csv)
    sidecar = None
    if pred_file.startswith("CS_Predictions_") and pred_file.endswith(".csv"):
        tag = pred_file[len("CS_Predictions_"):-len(".csv")]
        if tag:
            sidecar = os.path.join(pred_dir, f"{tag}_FitScores.json")
            # Friendly label for the plot title — covers TestSet, ClosedLoop, ClosedLoop_Train.
            if   tag == "TestSet":           fits_label = "Test-set fit"
            elif tag == "ClosedLoop":        fits_label = "Closed-loop fit (test data)"
            elif tag == "ClosedLoop_Train":  fits_label = "Closed-loop fit (training data)"
            else:                            fits_label = f"{tag} fit"
    if sidecar and os.path.exists(sidecar):
        try:
            with open(sidecar) as f:
                override_fits = json.load(f)
        except Exception:
            override_fits = {}

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

    phys_adj = _build_physical_adjacency(system_map_json)

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
        # parent component = part of model_id before the last underscore
        _pi = model_id.rfind('_')
        parent_comp = model_id[:_pi] if _pi > 0 else None

        inputs_by_csv = {}

        # For controllers (PID), always inject Setpoint + Measurement first so
        # every controller plot shows the exact two signals fed to PidIdentifier
        # — independent of whatever y_meas edges happen to exist in the topology.
        # This unifies the visual layout across LIC/PIC/TIC and lets you verify
        # that identification is using the right signals.
        if model_type == "PID":
            sp_csv   = signal_map.get(f"{comp}_Setpoint")
            meas_csv = signal_map.get(f"{comp}_Measurement")
            if sp_csv and sp_csv in df_raw.columns:
                inputs_by_csv[sp_csv] = f"Setpoint: {sp_csv}"
            if meas_csv and meas_csv in df_raw.columns:
                inputs_by_csv[meas_csv] = f"Measurement: {meas_csv}"
        elif model_type == "ValvePhysicsModel":
            # ValvePhysicsModel only contributes the local pipe outlet pressure as a unique
            # signal — P_in (upstream comp) and U(t) (controller output) come from the
            # topology edge iteration below.
            for in_name in p.get('InputNames', []):
                if in_name and in_name in df_raw.columns and in_name not in inputs_by_csv:
                    label = "Outlet Pressure" if in_name.endswith("OutletStream.p") or in_name.endswith("OutletPressure") else in_name
                    inputs_by_csv[in_name] = f"{label}: {in_name}"

        for edge_info in input_edges.get(model_id, []):
            src_node   = edge_info['from']
            edge_label = edge_info['label']

            # For PIDs, skip topology y_meas edges — the canonical measurement
            # signal (the one actually fed to PidIdentifier) is already shown
            # via SignalMapping above. Topology y_meas can point to a different
            # internal state signal and would just clutter the plot.
            if model_type == "PID" and edge_label == "y_meas":
                continue

            csv_col    = signal_map.get(src_node) or signal_map.get(src_node.replace('_pf', ''))

            if not (csv_col and csv_col in df_raw.columns):
                # Proxy resolution — only for MassFlow (conserved in series connections)
                _li = src_node.rfind('_')
                if _li > 0 and src_node[_li + 1:] == 'MassFlow':
                    src_comp = src_node[:_li]
                    proxy_key, csv_col = _find_proxy_signal(
                        src_comp, 'MassFlow', parent_comp,
                        phys_adj, signal_map, df_raw.columns
                    )
                    if proxy_key:
                        print(f"  [PROXY] {src_node} -> {proxy_key}")
                        src_node = f"{src_node} [proxy->{proxy_key}]"

            if csv_col and csv_col in df_raw.columns:
                display = f"{edge_label}: {src_node}"
                if csv_col not in inputs_by_csv:
                    inputs_by_csv[csv_col] = display
                else:
                    inputs_by_csv[csv_col] += f" | {edge_label}"

        # csv_col → display_label mapping for subplot rows
        input_signals = {lbl: csv for csv, lbl in inputs_by_csv.items()}

        # ── Build gain lookup: csv_col -> gain (from identified parameters) ─────
        gain_by_csv = {}
        input_names  = p.get('InputNames', [])
        linear_gains = p.get('LinearGains', [])
        if input_names and linear_gains:
            for iname, igain in zip(input_names, linear_gains):
                # Display name format: "resolvedKey(label)" or "resolvedKey(label)_neg"
                paren = iname.find('(')
                resolved = iname[:paren].strip() if paren > 0 else iname.strip()
                if resolved.endswith('_neg'):
                    resolved = resolved[:-4]
                csv_c = signal_map.get(resolved)
                if csv_c:
                    gain_by_csv[csv_c] = igain

        # ── Build title ───────────────────────────────────────────────────────
        # Prefer the test/closed-loop fit when a sidecar JSON has it — those scores
        # describe the actual experiment being plotted; the training fit on the
        # params JSON would be misleading.
        if model_id in override_fits:
            fit_score = override_fits[model_id]
            fit_str   = f"  |  {fits_label}: {fit_score:.1f}%"
        else:
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
            signal_data = df_raw[csv_col].values[:min_len]
            ax.plot(df_pred['Time'].values[:min_len], signal_data,
                    color='darkorange', linewidth=1.2)

            gain = gain_by_csv.get(csv_col)
            if gain is not None:
                # Colour-code: near-zero gain = grey (useless), significant = green
                is_useful = abs(gain) > 1e-6
                gcolor = 'green' if is_useful else 'grey'
                gain_annotation = f"gain={gain:.4g}"
                ax.annotate(gain_annotation,
                            xy=(0.99, 0.95), xycoords='axes fraction',
                            ha='right', va='top', fontsize=8,
                            color=gcolor,
                            bbox=dict(boxstyle='round,pad=0.2', fc='white', alpha=0.7))
                subplot_title = f"Input: {csv_col}  ({'useful' if is_useful else 'near-zero gain'})"
            else:
                subplot_title = f"Input: {csv_col}"

            ax.set_ylabel(display_lbl, fontsize=7.5)
            ax.set_title(subplot_title, fontsize=8)
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
    ap = argparse.ArgumentParser()
    ap.add_argument('--predictions', help='CS_Predictions CSV path (default: output/CS_Predictions.csv)')
    ap.add_argument('--rawcsv',      help='Raw KSpice CSV with input signals (default: data/raw/KspiceSim.csv)')
    ap.add_argument('--outdir',      help='Output directory for validation plots (default: output/validation_plots)')
    a = ap.parse_args()
    plot_validation(predictions_csv=a.predictions, kspice_csv=a.rawcsv, out_dir=a.outdir)
