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
# Held-out test CSV (lives one level above Pipeline/). Used by the testset and
# closedloop phases — same plant, different operating trajectory, never seen
# during identification. Override with --testcsv.
DEFAULT_TEST_CSV = os.path.abspath(os.path.join(HERE, '..', 'KspiceSimTestdata.csv'))


# ---------------------------------------------------------------------------
# Model discovery
# ---------------------------------------------------------------------------

def find_mdl_files():
    """Return .mdl files in kspicefiles/. For models where only a .mdl.bak exists
    (no clean .mdl), include the .bak as a fallback so those models still appear."""
    clean = {
        f for f in glob.glob(os.path.join(KSPICE_DIR, '**', '*.mdl'), recursive=True)
        if not f.endswith('.bak')
    }
    # Include .mdl.bak only when no corresponding clean .mdl exists
    for bak in glob.glob(os.path.join(KSPICE_DIR, '**', '*.mdl.bak'), recursive=True):
        if bak[:-4] not in clean:   # bak[:-4] strips ".bak" → ".mdl" path
            clean.add(bak)
    return sorted(clean)


def _prm_for(mdl_path):
    """Return the matching .prm (or .prm.bak) path for a .mdl or .mdl.bak file."""
    if mdl_path.endswith('.mdl.bak'):
        base = mdl_path[:-8]           # strip '.mdl.bak'
        for candidate in (base + '.prm', base + '.prm.bak'):
            if os.path.exists(candidate):
                return candidate
    else:
        candidate = mdl_path[:-4] + '.prm'   # strip '.mdl', add '.prm'
        if os.path.exists(candidate):
            return candidate
    return None


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

    prm = _prm_for(mdl)
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


def phase_equations(csv_path, mdl=None):
    print("\n-- Phase 2 | Generate equations (C# engine) -------------------")
    cmd = ['dotnet', 'run', '--project', CSHARP_PROJ, '--',
           '--mode', 'equations',
           '--map', MAP_JSON,
           '--csv', csv_path or '']
    if mdl:
        cmd += ['--kspicemdl', mdl]
    _run(cmd)


def phase_topology():
    print("\n-- Phase 3 | Build equation topology --------------------------")
    _run([sys.executable, os.path.join(SRC_VIS, 'EquationTopologyBuilder.py')])


LAST_CSV_FILE = os.path.join(HERE, 'output', 'last_train_csv.txt')


def _save_last_csv(csv_path):
    """Persist the training CSV path so later phases (visualize, tests) know which
    CSV to use without requiring --csv on every invocation."""
    os.makedirs(os.path.dirname(LAST_CSV_FILE), exist_ok=True)
    with open(LAST_CSV_FILE, 'w') as f:
        f.write(os.path.abspath(csv_path))


def _load_last_csv():
    """Return the previously saved training CSV path, or None if not found."""
    if os.path.exists(LAST_CSV_FILE):
        path = open(LAST_CSV_FILE).read().strip()
        return path if os.path.exists(path) else None
    return None


def phase_simulate(csv_path, mdl=None):
    print("\n-- Phase 4 | Simulate & identify (C# engine) ------------------")
    if not csv_path or not os.path.exists(csv_path):
        print("  [SKIP] No CSV available — skipping simulation phase")
        return
    cmd = ['dotnet', 'run', '--project', CSHARP_PROJ, '--',
           '--mode', 'simulate',
           '--map', MAP_JSON,
           '--csv', csv_path]
    if mdl:
        cmd += ['--kspicemdl', mdl]
    _run(cmd)
    _save_last_csv(csv_path)


def phase_visualize(csv_path=None):
    print("\n-- Phase 5 | Generate visualizations --------------------------")
    # Resolve the training CSV: explicit arg > saved state > hardcoded default.
    raw_csv = csv_path or _load_last_csv() or DEFAULT_CSV
    if raw_csv != (csv_path or DEFAULT_CSV):
        print(f"  (using saved training CSV: {os.path.basename(raw_csv)})")

    # Raw K-Spice component connectivity (overview)
    _run([sys.executable, os.path.join(SRC_VIS, 'TopologyVisualizer.py'),
          '--json', MAP_JSON,
          '--out',  os.path.join(OUT_DIAG, 'System_Topology_V2.html')])

    # Validation plots (uses TSA_Explicit_Topology.json from phase 3)
    _run([sys.executable, os.path.join(HERE, 'Plot_CS_Predictions.py'),
          '--rawcsv', raw_csv])


def phase_testset(test_csv, mdl=None):
    """Test 2 — apply frozen identified models to a held-out CSV (open-loop)."""
    print("\n-- Phase 6 | Test set (frozen models on held-out CSV) ---------")
    if not test_csv or not os.path.exists(test_csv):
        print(f"  [SKIP] Test CSV not found: {test_csv}")
        return
    cmd = ['dotnet', 'run', '--project', CSHARP_PROJ, '--',
           '--mode', 'testset',
           '--map', MAP_JSON,
           '--testcsv', test_csv]
    if mdl:
        cmd += ['--kspicemdl', mdl]
    _run(cmd)
    # Plot test-set predictions to a separate folder
    _run([sys.executable, os.path.join(HERE, 'Plot_CS_Predictions.py'),
          '--predictions', os.path.join(HERE, 'output', 'CS_Predictions_TestSet.csv'),
          '--rawcsv',      test_csv,
          '--outdir',      os.path.join(HERE, 'output', 'validation_plots_testset')])


def phase_closedloop(test_csv, mdl=None):
    """Test 1 — closed-loop simulation on the held-out CSV (or training CSV if none)."""
    print("\n-- Phase 7 | Closed-loop simulation ---------------------------")
    csv_for_run = test_csv if test_csv and os.path.exists(test_csv) else None
    cmd = ['dotnet', 'run', '--project', CSHARP_PROJ, '--',
           '--mode', 'closedloop',
           '--map', MAP_JSON]
    if csv_for_run:
        cmd += ['--testcsv', csv_for_run]
    if mdl:
        cmd += ['--kspicemdl', mdl]
    _run(cmd)
    # Plot closed-loop predictions to a separate folder
    raw_csv = csv_for_run or DEFAULT_CSV
    _run([sys.executable, os.path.join(HERE, 'Plot_CS_Predictions.py'),
          '--predictions', os.path.join(HERE, 'output', 'CS_Predictions_ClosedLoop.csv'),
          '--rawcsv',      raw_csv,
          '--outdir',      os.path.join(HERE, 'output', 'validation_plots_closedloop')])


def phase_closedloop_train(csv_path, mdl=None):
    """Test 3 — closed-loop simulation driven by the TRAINING CSV. Same machinery
    as phase_closedloop, but writes to *_Train suffixed files so the user can
    compare closed-loop performance on training vs held-out trajectories side by
    side. Models are still the frozen identified params from phase 4."""
    print("\n-- Phase 8 | Closed-loop simulation (training data) -----------")
    if not csv_path or not os.path.exists(csv_path):
        print(f"  [SKIP] Training CSV not found: {csv_path}")
        return
    cmd = ['dotnet', 'run', '--project', CSHARP_PROJ, '--',
           '--mode', 'closedloop',
           '--map', MAP_JSON,
           '--csv', csv_path,
           # No --testcsv — the C# runner falls through to --csv (training).
           '--suffix', '_Train']
    if mdl:
        cmd += ['--kspicemdl', mdl]
    _run(cmd)
    _run([sys.executable, os.path.join(HERE, 'Plot_CS_Predictions.py'),
          '--predictions', os.path.join(HERE, 'output', 'CS_Predictions_ClosedLoop_Train.csv'),
          '--rawcsv',      csv_path,
          '--outdir',      os.path.join(HERE, 'output', 'validation_plots_closedloop_train')])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

PHASE_CHOICES = ['all', 'extract', 'equations', 'topology', 'simulate', 'visualize',
                 'testset', 'closedloop', 'closedloop-train', 'tests']


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
    p.add_argument('--testcsv',
                   help='Path to held-out test CSV used by testset/closedloop phases '
                        f'(default: {DEFAULT_TEST_CSV})')
    p.add_argument('--phase', choices=PHASE_CHOICES, default='all',
                   help='Which phase to run (default: all). The "all" option runs the '
                        'original five phases only — use --phase testset / closedloop '
                        'explicitly, or use "tests" to run all evaluation tests.')
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
    test_csv = args.testcsv or (DEFAULT_TEST_CSV if os.path.exists(DEFAULT_TEST_CSV) else None)

    ph       = args.phase
    run_all  = (ph == 'all')

    try:
        if run_all or ph == 'extract':    phase_extract(mdl, prm)
        if run_all or ph == 'equations':  phase_equations(csv_path, mdl)
        if run_all or ph == 'topology':   phase_topology()
        if run_all or ph == 'simulate':   phase_simulate(csv_path, mdl)
        if run_all or ph == 'visualize':  phase_visualize(csv_path)
        if ph in ('testset', 'tests'):             phase_testset(test_csv, mdl)
        if ph in ('closedloop', 'tests'):          phase_closedloop(test_csv, mdl)
        if ph in ('closedloop-train', 'tests'):    phase_closedloop_train(csv_path, mdl)
    except subprocess.CalledProcessError as e:
        sys.exit(f"\n[ERROR] Phase failed (exit code {e.returncode}). See output above.")

    print("\n[DONE] Pipeline complete.")
    print(f"       Outputs in: {os.path.join(HERE, 'output')}")


if __name__ == '__main__':
    main()
