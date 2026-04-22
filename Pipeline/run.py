#!/usr/bin/env python3
"""
K-Spice -> TSA Pipeline — single entry-point orchestrator
==========================================================
Usage:
  python run.py                        # interactive: pick model, run all phases
  python run.py --model Rev3B          # partial model name match
  python run.py --phase extract        # run only one phase
  python run.py --csv path/to/sim.csv  # override simulation CSV
  python run.py --list                 # list available models and exit

Phases (run in order when --phase all):
  1. extract    - Parse .mdl/.prm -> KSpiceSystemMap.json
  2. equations  - C# engine builds plant equations -> TSA_Equations.json
  3. topology   - Python builds wired topology -> TSA_Explicit_Topology.json
  4. simulate   - C# runs SysID on CSV data -> CS_Predictions.csv
  5. visualize  - Generate HTML diagrams + validation plots
"""
import os
import sys
import glob
import argparse
import subprocess

HERE         = os.path.dirname(os.path.abspath(__file__))
KSPICE_DIR   = os.path.abspath(os.path.join(HERE, '..', 'kspicefiles'))
DATA_RAW     = os.path.join(HERE, 'data', 'raw')
DATA_EXT     = os.path.join(HERE, 'data', 'extracted')
OUT_DIAG     = os.path.join(HERE, 'output', 'diagrams')
CSHARP_PROJ  = os.path.join(HERE, 'src', 'CSharp_Engine', 'KSpiceEngine')
SRC_VIS      = os.path.join(HERE, 'src', 'Visualization')
SRC_PARSER   = os.path.join(HERE, 'src', 'Parser', 'KSpiceParser.py')

MAP_JSON     = os.path.join(DATA_EXT, 'KSpiceSystemMap.json')
DEFAULT_CSV  = os.path.join(DATA_RAW,  'KspiceSim.csv')


# ---------------------------------------------------------------------------
# Model discovery
# ---------------------------------------------------------------------------

def find_mdl_files():
    """Return all non-backup .mdl files in the kspicefiles directory."""
    pattern = os.path.join(KSPICE_DIR, '**', '*.mdl')
    return sorted(
        f for f in glob.glob(pattern, recursive=True)
        if not f.endswith('.bak')
    )


def pick_model(model_arg):
    """Return (mdl_path, prm_path) for the chosen model."""
    models = find_mdl_files()
    if not models:
        sys.exit(f"[ERROR] No .mdl files found in:\n  {KSPICE_DIR}")

    if model_arg:
        hits = [m for m in models if model_arg.lower() in os.path.basename(m).lower()]
        if len(hits) == 1:
            mdl = hits[0]
        elif len(hits) > 1:
            print("Multiple matches for '{}':".format(model_arg))
            for i, m in enumerate(hits, 1):
                print(f"  {i}. {os.path.basename(m)}")
            mdl = hits[int(input("Select: ").strip()) - 1]
        else:
            sys.exit(f"[ERROR] No model matching '{model_arg}'.\nRun with --list to see available models.")
    else:
        print("\nAvailable K-Spice models in kspicefiles/:")
        for i, m in enumerate(models, 1):
            print(f"  {i:2}. {os.path.basename(m)}")
        choice = input("\nSelect model (number): ").strip()
        mdl = models[int(choice) - 1]

    # Auto-find matching .prm next to the .mdl
    prm = mdl[:-4] + '.prm'
    prm = prm if os.path.exists(prm) else None

    print(f"\n  Model  : {os.path.basename(mdl)}")
    print(f"  Params : {os.path.basename(prm) if prm else '(none found)'}")
    return mdl, prm


def pick_csv(csv_arg, mdl_path):
    """Return path to KSpice simulation CSV, with fallback search."""
    if csv_arg:
        if os.path.exists(csv_arg):
            return os.path.abspath(csv_arg)
        sys.exit(f"[ERROR] CSV not found: {csv_arg}")

    if os.path.exists(DEFAULT_CSV):
        return DEFAULT_CSV

    # Try to find a CSV next to the .mdl file
    nearby = glob.glob(os.path.join(os.path.dirname(mdl_path), '*.csv'))
    if nearby:
        print(f"  [WARN] Default CSV not found, using: {nearby[0]}")
        return nearby[0]

    print(f"  [WARN] No CSV found — simulation phase will be skipped")
    return None


# ---------------------------------------------------------------------------
# Phase runners
# ---------------------------------------------------------------------------

def _run(cmd, label=None):
    if label:
        print(f"  > {label}")
    print("  $", " ".join(str(c) for c in cmd))
    subprocess.run(cmd, check=True)


def phase_extract(mdl, prm):
    print("\n-- Phase 1 | Extract K-Spice model ---------------------------")
    cmd = [sys.executable, SRC_PARSER, '--mdl', mdl, '--out', MAP_JSON]
    if prm:
        cmd += ['--prm', prm]
    _run(cmd)


def phase_equations(csv_path):
    print("\n-- Phase 2 | Generate equations (C# engine) -------------------")
    cmd = ['dotnet', 'run', '--project', CSHARP_PROJ, '--',
           '--mode', 'equations',
           '--map', MAP_JSON,
           '--csv', csv_path or '']
    _run(cmd)


def phase_topology():
    print("\n-- Phase 3 | Build equation topology --------------------------")
    _run([sys.executable, os.path.join(SRC_VIS, 'EquationTopologyBuilder.py')])


def phase_simulate(csv_path):
    print("\n-- Phase 4 | Simulate & identify (C# engine) ------------------")
    if not csv_path or not os.path.exists(csv_path):
        print("  [SKIP] No CSV available — skipping simulation phase")
        return
    cmd = ['dotnet', 'run', '--project', CSHARP_PROJ, '--',
           '--mode', 'simulate',
           '--map', MAP_JSON,
           '--csv', csv_path]
    _run(cmd)


def phase_visualize():
    print("\n-- Phase 5 | Generate visualizations --------------------------")
    _run([sys.executable, os.path.join(SRC_VIS, 'TopologyVisualizer.py'),
          '--json', MAP_JSON,
          '--out',  os.path.join(OUT_DIAG, 'System_Topology_V2.html')])

    _run([sys.executable, os.path.join(SRC_VIS, 'StateTopologyBuilder.py'),
          '--json',     MAP_JSON,
          '--out_html', os.path.join(OUT_DIAG, 'System_TSA_State_Topology.html')])

    _run([sys.executable, os.path.join(HERE, 'Plot_CS_Predictions.py')])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

PHASE_CHOICES = ['all', 'extract', 'equations', 'topology', 'simulate', 'visualize']


def main():
    p = argparse.ArgumentParser(
        description='K-Spice -> TSA pipeline orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    p.add_argument('--model', '-m',
                   help='Partial name of .mdl file in kspicefiles/ (e.g. "Rev3B")')
    p.add_argument('--csv',
                   help='Path to KSpice simulation CSV (default: data/raw/KspiceSim.csv)')
    p.add_argument('--phase', choices=PHASE_CHOICES, default='all',
                   help='Which phase to run (default: all)')
    p.add_argument('--list', action='store_true',
                   help='List available models and exit')
    args = p.parse_args()

    if args.list:
        models = find_mdl_files()
        print(f"Available models in {KSPICE_DIR}:")
        for m in models:
            print(f"  {os.path.basename(m)}")
        return

    mdl, prm = pick_model(args.model)
    csv_path = pick_csv(args.csv, mdl)
    if csv_path:
        print(f"  CSV    : {os.path.basename(csv_path)}")

    ph       = args.phase
    run_all  = (ph == 'all')

    try:
        if run_all or ph == 'extract':    phase_extract(mdl, prm)
        if run_all or ph == 'equations':  phase_equations(csv_path)
        if run_all or ph == 'topology':   phase_topology()
        if run_all or ph == 'simulate':   phase_simulate(csv_path)
        if run_all or ph == 'visualize':  phase_visualize()
    except subprocess.CalledProcessError as e:
        sys.exit(f"\n[ERROR] Phase failed (exit code {e.returncode}). See output above.")

    print("\n[DONE] Pipeline complete.")
    print(f"       Outputs in: {os.path.join(HERE, 'output')}")


if __name__ == '__main__':
    main()
