# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands run from the `Pipeline/` directory.

**Run the full pipeline (interactive model selection):**
```
python run.py
python run.py --model Rev3B          # skip interactive model prompt
python run.py --phase simulate       # single phase only
python run.py --phase tests          # run all three evaluation tests (testset, closedloop, closedloop-train)
python run.py --list                 # list available K-Spice models
```

**Run individual phases:**
```
python run.py --phase extract        # Phase 1: parse .mdl/.prm → KSpiceSystemMap.json
python run.py --phase equations      # Phase 2: C# → TSA_Equations.json + SignalMapping.json
python run.py --phase topology       # Phase 3: Python → TSA_Explicit_Topology.json
python run.py --phase simulate       # Phase 4: C# system ID → CS_Predictions.csv + CS_Identified_Parameters.json
python run.py --phase visualize      # Phase 5: HTML diagrams + validation plots
python run.py --phase testset        # Test 2: open-loop on held-out CSV
python run.py --phase closedloop     # Test 1: closed-loop on held-out CSV
python run.py --phase closedloop-train  # Test 3: closed-loop on training CSV
```

**Build and run the C# engine directly:**
```
dotnet build src/CSharp_Engine/KSpiceEngine/KSpiceEngine.csproj
dotnet run --project src/CSharp_Engine/KSpiceEngine -- --mode simulate --map data/extracted/KSpiceSystemMap.json --csv data/raw/KspiceSim.csv
```

The C# engine accepts `--mode equations | simulate | testset | closedloop`. The `closedloop` mode takes an optional `--suffix _Train` to write `CS_Predictions_ClosedLoop_Train.csv` instead of `CS_Predictions_ClosedLoop.csv`.

**Python tooling used:** standard library + `matplotlib`, `pandas`, `numpy` (for `Plot_CS_Predictions.py`). No `pip install` step is tracked; assume these are available.

## Architecture

### Data flow

```
kspicefiles/*.mdl + *.prm
    → [Phase 1: KSpiceParser.py]
    → data/extracted/KSpiceSystemMap.json

KSpiceSystemMap.json + data/raw/KspiceSim.csv
    → [Phase 2: KSpiceModelFactory.cs (C#)]
    → output/diagrams/TSA_Equations.json         (one entry per state per component)
    → output/diagrams/SignalMapping.json          (CSV column ↔ equation ID map)

TSA_Equations.json + KSpiceSystemMap.json
    → [Phase 3: EquationTopologyBuilder.py]
    → output/diagrams/TSA_Explicit_Topology.json  (directed graph: node = equation, edge = data dependency)

KspiceSim.csv + TSA_Equations.json + SignalMapping.json + TSA_Explicit_Topology.json
    → [Phase 4: DynamicPlantRunner.cs (C#)]
    → output/CS_Predictions.csv
    → output/CS_Identified_Parameters.json        (per-model fitted parameters)

CS_Identified_Parameters.json + held-out CSV
    → [Tests 1–3: OpenLoopTestRunner / ClosedLoopRunner (C#)]
    → output/CS_Predictions_{TestSet,ClosedLoop,ClosedLoop_Train}.csv
    → output/{TestSet,ClosedLoop,ClosedLoop_Train}_FitScores.json
```

### C# engine structure (`src/CSharp_Engine/KSpiceEngine/`)

**Orchestration:**
- `Program.cs` — CLI entry point; dispatches to modes based on `--mode` arg.
- `KSpiceModelFactory.cs` — Phase 2. Reads `KSpiceSystemMap.json`, emits one `JObject` per state per component. Component `KSpiceType` determines formula and equation role (`Separator/Tank` → pressure/level/temperature equations; `ControlValve/BlockValve/PipeFlow` → valve equations; `Compressor` → flow/pressure/temperature/speed; `Pump` → flow/pressure/temperature/speed; `pid`/`GenericASC`/`Controller` → control equations; everything else → `"Boundary ..."` formulas).

**System identification (Phase 4):**
- `DynamicPlantRunner.cs` — Main identification loop. Reads each equation from TSA_Equations.json, dispatches to a branch method based on `kspiceType` and `role`: `IdentifyPidModel`, `IdentifySeparatorLevel`, `IdentifyValveModel`, `IdentifyLinearModel`, `IdentifyGeneral`. Shared internal helpers (`NegateOutflows`, `IntegrateFlows`, `BuildUnitDataSet`, `AttachUnitParams`, `SolveLinearSystem`, `DetectTimeStep`) are used by the specialist identifier classes.
- `AscIdentifier.cs` — All Anti-Surge Controller (ASC) identification logic. Resolves P_in/P_out/MassFlow from topology, runs 9 OLS benchmark architectures, then fits a physics-based `KickBased` model via grid search + coordinate descent. KickBased always wins over OLS when its fit > 0% (OLS causes false kick-opens in closed-loop).
- `SeparatorPressureIdentifier.cs` — Benchmarks 6 strategies for gas separator pressure (integrated flows, curvature, net mass balance, surge-excluded, net signed flow, raw signed flows); prefers Strategy F (raw signed flows with leaky integrator) when fit ≥ 20%.

**Evaluation (Tests 1–3):**
- `IdentifiedModelEvaluator.cs` — Wraps an identified parameter `JObject` into a callable object. Reconstructs the appropriate runtime model (`UnitModel`, `PidController`, `AntiSurgePhysicalModel`, `ValvePhysicsModel`, etc.) and exposes `Iterate(inputs, dt)` and `WarmStart(y0)`.
- `OpenLoopTestRunner.cs` — Test 2. Applies frozen identified models to held-out CSV; each model's inputs come from the CSV directly (open-loop).
- `ClosedLoopRunner.cs` — Test 1/3. Each model's inputs come from *other models' predictions* (closed-loop). Key design: a `freeModelIds` set marks boundary signals that are always read from CSV truth. This set is derived in two steps: (1) seed from equations with "Boundary" formula; (2) BFS expansion: any equation whose P_in topology inputs are all free AND has no U(t) from a non-free controller is also free (catches inlet valves like `25ESV0001` that would otherwise create inlet-flow ↔ separator-pressure feedback). The topological sort handles algebraic loops by marking cycle-back edges and using the previous timestep's value.
- `SignalEquivalenceMap.cs` — Walks K-Spice transmitter/instrument wiring so controller measurement ports (e.g. `23LIC0001:Measurement`) resolve to the predicted model state rather than the raw CSV column.

**Runtime models (`CustomModels/`):**
- `AntiSurgePhysicalModel.cs` — Stateful ASC surrogate. Two architectures: `KickBased` (kick valve on surge entry, hold while in surge margin, ramp close once safe — used for closed-loop) and `LinearOLS` (static linear proxy, kept as fallback). Used by both `AscIdentifier` during fitting and `IdentifiedModelEvaluator` during closed-loop.
- `ValvePhysicsModel.cs` — `Q = K * Cv * U * sqrt(dP / rho)` with a fitted density tuning factor.

### Key conventions

- **KSpiceType drives everything.** `KSpiceModelFactory` assigns formulas and `DynamicPlantRunner` selects identification branches entirely from the `KSpiceType` field in `KSpiceSystemMap.json`. Adding support for a new component type means updating both files.
- **`CustomModels/` = runtime simulation only.** Identification/fitting logic lives in `*Identifier.cs` files at the engine root, not in `CustomModels/`. The custom models only need to implement `Iterate(double[], double)` and `WarmStart(double)`.
- **OLS benchmark is always run for ASC but always overridden by KickBased** (when KickBased fit > 0). The benchmark JSON is kept for diagnostic inspection.
- **`DetectTimeStep`** reads the `Time` column from the CSV (first positive difference). Falls back to 0.5 s if no Time column is found.
- **Fit scores** are written to sidecar `*_FitScores.json` files next to each predictions CSV, so the plotter can label plots with the actual evaluation-phase fit rather than the training fit.

### Important file locations

| File | Purpose |
|------|---------|
| `data/extracted/KSpiceSystemMap.json` | Parsed K-Spice model — source of truth for topology |
| `output/diagrams/TSA_Equations.json` | Equation descriptors (one entry = one modeled state) |
| `output/diagrams/SignalMapping.json` | CSV column ↔ model ID mapping |
| `output/diagrams/TSA_Explicit_Topology.json` | Directed dependency graph used by ClosedLoopRunner |
| `output/CS_Identified_Parameters.json` | All fitted model parameters (written by Phase 4, read by Tests 1–3) |
| `Plot_CS_Predictions.py` | Standalone plotter; reads predictions CSV + raw CSV → PNG per model |
