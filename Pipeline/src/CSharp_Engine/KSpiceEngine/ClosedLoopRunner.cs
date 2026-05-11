using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KSpiceEngine
{
    /// <summary>
    /// Test 1 — closed-loop simulation. Each identified model's inputs come from
    /// other models' predictions (current step where possible, previous step
    /// when there's an algebraic loop), with only a small set of "free" boundary
    /// signals coming from the CSV:
    ///
    ///   - FreeCsvColumns (see below): exit-valve outlet pressures + COLD0001 reservoir
    ///     + 25ESV0001 system inlet (mass flow and temperature)
    ///   - Equations marked "Boundary" (Feed1/Feed2/PSV0001/COLD0001 models)
    ///   - All controller setpoints (resolved via signalMap)
    ///
    /// Everything else uses the identified model predictions so errors compound over time,
    /// which is exactly what reveals whether the model system is internally consistent.
    /// </summary>
    public static class ClosedLoopRunner
    {
        // Raw CSV column names that bypass the model system — no model ID maps to these.
        // Includes exit-valve downstream pressures (outside the plant boundary), the cold
        // reservoir properties, and the system inlet flow/temperature from 25ESV0001
        // (external feed, no model is identified for it).
        // Plant-specific: update when the K-Spice topology changes.
        private static readonly HashSet<string> FreeCsvColumns = new(StringComparer.OrdinalIgnoreCase)
        {
            "23COLD0001:OutletStream.p",
            "23COLD0001:OutletStream.t",
            "23ESV0005_pf:OutletStream.p",
            "23LV0001_pf:OutletStream.p",
            "23LV0002_pf:OutletStream.p",
            "23TV0003_pf:OutletStream.p",
            "25ESV0001_pf:MassFlow",
            "25ESV0001_pf:OutletStream.t",
        };

        public static void Run(string csvPath, string systemMapPath, string equationsPath,
                               string mappingPath, string identifiedParamsPath,
                               string topologyPath, string outputCsvPath)
        {
            Console.WriteLine("\n[ClosedLoopRunner] Starting Test 1: closed-loop simulation...");
            Console.WriteLine($"  CSV : {csvPath}");

            var systemMap        = JObject.Parse(File.ReadAllText(systemMapPath));
            var models           = (JArray)systemMap["Models"];
            var signalMap        = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText(mappingPath));
            var equations        = JsonConvert.DeserializeObject<List<dynamic>>(File.ReadAllText(equationsPath));
            var identifiedParams = JObject.Parse(File.ReadAllText(identifiedParamsPath));
            var topology         = JObject.Parse(File.ReadAllText(topologyPath));
            var topoEdges        = (JArray)topology["edges"];

            var inputEdges = new Dictionary<string, List<(string fromNode, string label)>>();
            foreach (var edge in topoEdges)
            {
                string to    = (string)edge["to"];
                string from  = (string)edge["from"];
                string label = (string)edge["label"];
                if (!inputEdges.ContainsKey(to)) inputEdges[to] = new List<(string, string)>();
                inputEdges[to].Add((from, label));
            }
            var physicalNeighbors = DynamicPlantRunner.BuildPhysicalAdjacency(models);

            var dataset = DynamicPlantRunner.LoadCsvDataset(csvPath);
            int N = dataset.Values.First().Length;
            double dt = DynamicPlantRunner.DetectTimeStep(dataset);

            // Boundary model IDs: driven from CSV truth, never simulated.
            // Seed: equations explicitly marked "Boundary" in their formula.
            var freeModelIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var eq in equations)
            {
                string formula = (string)eq.Formula ?? "";
                if (formula.IndexOf("Boundary", StringComparison.OrdinalIgnoreCase) >= 0)
                    freeModelIds.Add((string)eq.ID);
            }

            // 25ESV0001 is the system inlet from outside the plant boundary.
            // Its flow and temperature are external disturbances, not controlled by
            // any identified model. Mark them free so closed-loop uses CSV truth.
            foreach (var inletId in new[] { "25ESV0001_MassFlow", "25ESV0001_Temperature" })
                freeModelIds.Add(inletId);

            Console.WriteLine($"[ClosedLoop] {freeModelIds.Count} free (boundary) model IDs: "
                            + string.Join(", ", freeModelIds.OrderBy(x => x)));

            // Collect the set of *predicted* model_ids first — these are the only
            // valid substitution targets. signalMap has extra convenience entries
            // (controller setpoints/measurements) whose keys are not predicted; we
            // must not let them short-circuit the equivalence trace.
            var validModelIds = new HashSet<string>(
                ((List<dynamic>)equations).Select(eq => (string)eq.ID),
                StringComparer.OrdinalIgnoreCase);

            // Inverse signal map: CSV column → predicted model_id only.
            var csvToModelId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in signalMap)
            {
                if (!validModelIds.Contains(kv.Key)) continue;
                if (!csvToModelId.ContainsKey(kv.Value))
                    csvToModelId[kv.Value] = kv.Key;
            }

            // Equivalence map walks K-spice wiring (transmitters, controller measurement
            // ports, instrument chains) so a CSV signal like "23LIC0001:Measurement"
            // resolves to "23VA0001_WaterLevel" — preventing the controller from
            // cheating with the alarm-system trace instead of the predicted level.
            var equiv = new SignalEquivalenceMap(models, signalMap, validModelIds);
            // Debug print: show what every controller's Measurement port resolves to
            // so it's easy to verify nothing is silently falling back to raw CSV.
            foreach (var eq in equations)
            {
                string role = (string)eq.Role; if (role != "Controller") continue;
                string c = (string)eq.Component;
                string measCol = signalMap.ContainsKey($"{c}_Measurement") ? signalMap[$"{c}_Measurement"] : null;
                if (measCol == null) continue;
                string traced = equiv.TryResolve(measCol);
                Console.WriteLine($"  [equiv] {c}.Measurement [{measCol}] -> {(traced ?? "(boundary CSV)")}");
            }

            // Build evaluators for every modelled state. Boundary/Fallback entries
            // get a placeholder evaluator so the runner can still copy CSV truth
            // into the predictions dict for plotting.
            var evaluators = new Dictionary<string, IdentifiedModelEvaluator>();
            var truthByModelId = new Dictionary<string, double[]>();
            var allModelIds = new List<string>();

            foreach (var eq in equations)
            {
                string id = (string)eq.ID;
                string comp = (string)eq.Component;
                string state = (string)eq.State;
                string mapKey = $"{comp}_{state}";

                allModelIds.Add(id);

                string truthCol = signalMap.ContainsKey(mapKey) ? signalMap[mapKey] : null;
                if (truthCol != null && dataset.ContainsKey(truthCol))
                    truthByModelId[id] = dataset[truthCol];

                var p = (JObject)identifiedParams[id];
                if (p == null)
                    continue;

                evaluators[id] = new IdentifiedModelEvaluator(id, p);
            }

            // Rewire ValvePhysicsModel inputs. For each of the three slots (P_in,
            // P_out, U) we prefer a topology edge whose source is an actual predicted
            // model (so predictions flow), then fall back to the trainer's SimInputs
            // CSV column for boundary signals. The model-id check matters: the
            // topology builder sometimes creates a synthetic "<comp>_Control" node for
            // valves with no dedicated PID (only an ASC), which would otherwise
            // resolve to NaN and collapse the whole gas chain at t=1.
            foreach (var kv in evaluators.ToList())
            {
                if (kv.Value.ModelType != "ValvePhysicsModel") continue;
                var p = (JObject)identifiedParams[kv.Key];
                var simInputs = (JArray)p?["SimInputs"];
                kv.Value.InputContract = HybridValveContract(kv.Key, inputEdges, simInputs, validModelIds);
            }

            // Compute evaluation order: stable topological sort over input edges.
            // Edges that would create a cycle are marked as "cycle-back" and use
            // the previous timestep's value of the source — exactly the trick
            // PlantSimulator uses internally (doDestBasedONYsimOfLastTimestep).
            var evalOrder = ComputeEvaluationOrder(allModelIds, inputEdges, out var cycleBackEdges);
            Console.WriteLine($"[ClosedLoop] {allModelIds.Count} models, evaluation depth {evalOrder.Count}, "
                            + $"{cycleBackEdges.Count} cycle-back edges (use prev-step values)");

            // Predictions[model_id] = double[N], filled in step-by-step.
            var predictions = new Dictionary<string, double[]>();
            foreach (var id in allModelIds)
                predictions[id] = new double[N];

            // Warm-start each evaluator from the CSV truth at t=0.
            foreach (var id in evaluators.Keys)
            {
                double y0 = truthByModelId.TryGetValue(id, out var arr) && arr.Length > 0 ? arr[0] : 0;
                evaluators[id].WarmStart(y0);
                predictions[id][0] = y0;
            }

            // Prime each model's internal state (LP filter, integrator, kick state…)
            // by iterating once at t=0 with truth-resolved inputs. Without this the
            // UnitModel's first call after construction returns the un-filtered
            // steady-state value, causing a step transient at t=1. Discard the
            // returned values — predictions[id][0] is fixed at the CSV truth.
            foreach (string id in evalOrder)
            {
                if (freeModelIds.Contains(id) || !evaluators.ContainsKey(id)) continue;
                var eval = evaluators[id];
                if (eval.ModelType == "Fallback" || eval.ModelType == "Boundary"
                    || eval.InputContract.Count == 0) continue;
                int n = eval.InputContract.Count;
                double[] step0 = new double[n];
                for (int k = 0; k < n; k++)
                {
                    var slot = eval.InputContract[k];
                    step0[k] = ResolveSlotValue(slot, id, 0, false, predictions, dataset,
                                                signalMap, csvToModelId, inputEdges, physicalNeighbors, equiv);
                }
                eval.Iterate(step0, dt); // priming call — its return value is unused
                eval.WarmStart(predictions[id][0]); // re-anchor "lastOutput" to truth so post-prime fallback returns are correct
            }

            // Step loop. At t=0 we already wrote warm-start values; t=1..N-1 evaluates.
            // Within each step, models are visited in evalOrder; each input is fetched
            // either from this-step predictions (default) or last-step (for cycle-back
            // edges, or any input whose source isn't yet computed at this step).
            //
            // The "not yet computed" case is real: training-time JSON InputNames can
            // reference models that aren't in the topology graph (HX_MassFlow trained
            // against ESV0005, but the topology routes through 23PV_0001 instead). The
            // topo sort wouldn't schedule ESV0005 before HX, so reading predictions[ESV][t]
            // on its default-initialised zero would silently corrupt HX. Tracking which
            // (id, t) pairs have been computed lets us fall back to t-1 cleanly.
            var computedThisStep = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            int progress = 0;
            for (int t = 1; t < N; t++)
            {
                computedThisStep.Clear();
                foreach (string id in evalOrder)
                {
                    if (freeModelIds.Contains(id))
                    {
                        predictions[id][t] = truthByModelId.TryGetValue(id, out var arr) && t < arr.Length ? arr[t] : 0;
                        computedThisStep.Add(id);
                        continue;
                    }
                    if (!evaluators.ContainsKey(id))
                    {
                        predictions[id][t] = truthByModelId.TryGetValue(id, out var arr) && t < arr.Length ? arr[t] : predictions[id][t - 1];
                        computedThisStep.Add(id);
                        continue;
                    }

                    var eval = evaluators[id];
                    if (eval.ModelType == "Fallback" || eval.ModelType == "Boundary"
                        || eval.InputContract.Count == 0)
                    {
                        predictions[id][t] = truthByModelId.TryGetValue(id, out var arr) && t < arr.Length ? arr[t] : predictions[id][t - 1];
                        computedThisStep.Add(id);
                        continue;
                    }
                    int nSlots = eval.InputContract.Count;
                    double[] step = new double[nSlots];
                    for (int k = 0; k < nSlots; k++)
                    {
                        var slot = eval.InputContract[k];
                        bool useLastStep = false;
                        // Cycle-back edges are recorded as (to=id, from=src). Force t-1.
                        if (cycleBackEdges.TryGetValue(id, out var cyc) && cyc.Contains(slot.SourceKey))
                            useLastStep = true;
                        // Source is a model_id that hasn't yet been evaluated this step?
                        // Use t-1 to avoid reading a default-zero array slot.
                        else if (predictions.ContainsKey(slot.SourceKey) && !computedThisStep.Contains(slot.SourceKey))
                            useLastStep = true;
                        step[k] = ResolveSlotValue(slot, id, t, useLastStep, predictions, dataset,
                                                   signalMap, csvToModelId, inputEdges, physicalNeighbors, equiv,
                                                   computedThisStep);
                    }
                    predictions[id][t] = eval.Iterate(step, dt);
                    computedThisStep.Add(id);
                }
                progress++;
                if (progress % 100 == 0) Console.WriteLine($"  Step {t}/{N}");
            }

            // Per-model fit vs CSV truth for a quick-look summary.
            int fitOk = 0;
            foreach (var id in allModelIds)
            {
                if (!truthByModelId.ContainsKey(id) || freeModelIds.Contains(id) || !evaluators.ContainsKey(id))
                    continue;
                double f = OpenLoopTestRunner.ComputeFit(truthByModelId[id], predictions[id]);
                Console.WriteLine($"[Model] {id} closed-loop fit = {f,6:F1}%");
                fitOk++;
            }

            // Write predictions CSV excluding free/boundary signals: those are just CSV
            // pass-through and would produce misleading 100% plots.
            var predictionsToWrite = predictions
                .Where(kv => !freeModelIds.Contains(kv.Key))
                .ToDictionary(kv => kv.Key, kv => kv.Value);
            DynamicPlantRunner.WriteValidationCsv(dataset, predictionsToWrite, signalMap, outputCsvPath, dt);

            // Persist closed-loop fits for non-free models only.
            var fits = new JObject();
            foreach (var id in allModelIds)
            {
                if (!truthByModelId.ContainsKey(id)) continue;
                if (freeModelIds.Contains(id) || !evaluators.ContainsKey(id)) continue;
                fits[id] = OpenLoopTestRunner.ComputeFit(truthByModelId[id], predictions[id]);
            }
            string predBase = Path.GetFileNameWithoutExtension(outputCsvPath); // e.g. CS_Predictions_ClosedLoop_Train
            string fitsName = predBase.StartsWith("CS_Predictions_")
                ? predBase.Substring("CS_Predictions_".Length) + "_FitScores.json"
                : predBase + "_FitScores.json";
            string fitsPath = Path.Combine(Path.GetDirectoryName(outputCsvPath), fitsName);
            File.WriteAllText(fitsPath, fits.ToString(Formatting.Indented));

            Console.WriteLine($"\n[SUCCESS] Closed-loop predictions for {fitOk} models written to:");
            Console.WriteLine($"          {outputCsvPath}");
            Console.WriteLine($"          Fits  : {fitsPath}");
        }

        // Resolve a single InputSlot's value at time t. Order of preference:
        //   1. Free CSV column (boundary) → CSV[t]
        //   2. Slot's source maps to a model_id we predict → predictions[id][t or t-1]
        //   3. Direct CSV column lookup → CSV[t]
        //   4. NaN (treated as bad data downstream)
        private static double ResolveSlotValue(IdentifiedModelEvaluator.InputSlot slot,
            string ownerId, int t, bool forceLastStep,
            Dictionary<string, double[]> predictions, Dictionary<string, double[]> dataset,
            Dictionary<string, string> signalMap, Dictionary<string, string> csvToModelId,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            Dictionary<string, HashSet<string>> physicalNeighbors,
            SignalEquivalenceMap equiv,
            HashSet<string> computedThisStep = null)
        {
            string key = slot.SourceKey;

            // Trainer sentinel for valves with no controller (BlockValves, hand-valves).
            // SimInputs[2] is the literal string "Assumed_100%" because no CSV column
            // exists for the U signal. Without this branch the runner returns NaN, the
            // valve produces 0 flow, and the whole gas chain collapses at t=1.
            if (string.Equals(key, "Assumed_100%", StringComparison.OrdinalIgnoreCase))
                return 100.0;

            // Helper for transitive predictions look-ups: if the resolved model_id
            // hasn't been evaluated this step, drop back to t-1 instead of the
            // default-zero array slot.
            int IndexFor(string modelId)
            {
                if (forceLastStep) return t - 1;
                if (computedThisStep != null && !computedThisStep.Contains(modelId)) return t - 1;
                return t;
            }

            // Local helper: given a CSV column name, return either the predicted value
            // of the model that physically produces it (preferred) or the CSV reading.
            // The equivalence map is what unblocks the controller-cheating case: it
            // walks K-spice transmitter wiring so e.g. "23LIC0001:Measurement" resolves
            // to "23VA0001_WaterLevel" instead of returning the alarm-system trace.
            double FromCsvCol(string csvCol)
            {
                if (csvCol == null) return double.NaN;
                // 1. Free boundary signal — must come from CSV regardless.
                if (FreeCsvColumns.Contains(csvCol))
                {
                    if (dataset.TryGetValue(csvCol, out var farr)) return SafeAt(farr, t);
                }
                // 2. Direct model output (signal-map inverse).
                if (csvToModelId.TryGetValue(csvCol, out string sid)
                    && predictions.TryGetValue(sid, out var parr1))
                    return SafeAt(parr1, IndexFor(sid));
                // 3. Equivalence (transmitter chain, etc.) → predicted model.
                string traced = equiv?.TryResolve(csvCol);
                if (traced != null && predictions.TryGetValue(traced, out var parr2))
                    return SafeAt(parr2, IndexFor(traced));
                // 4. Last resort — raw CSV reading. True boundary or unmodelled signal.
                if (dataset.TryGetValue(csvCol, out var arr)) return SafeAt(arr, t);
                return double.NaN;
            }

            // Special PID/ASC role tokens.
            if (key.StartsWith("@Setpoint:") || key.StartsWith("@Measurement:"))
            {
                string what = key.StartsWith("@Setpoint:") ? "Setpoint" : "Measurement";
                string c = key.Substring(key.IndexOf(':') + 1);
                string lookup = $"{c}_{what}";
                if (signalMap.TryGetValue(lookup, out string col))
                {
                    if (what == "Setpoint")
                    {
                        // 1. Cascade: a topology cascade_sp edge means another controller
                        //    predicts this setpoint (e.g. PIC0001 → SIC0001 setpoint).
                        if (inputEdges.TryGetValue(ownerId, out var spEdges))
                        {
                            var casc = spEdges.FirstOrDefault(e =>
                                string.Equals(e.label, "cascade_sp", StringComparison.OrdinalIgnoreCase));
                            if (casc.fromNode != null
                                && predictions.TryGetValue(casc.fromNode, out var cascPred))
                                return SafeAt(cascPred, IndexFor(casc.fromNode));
                        }
                        // 2. Follow / ratio: the setpoint CSV column traces (via transmitter chain)
                        //    to a modelled state being predicted — use the prediction.
                        //    Handles SIC1001/SIC3001 whose setpoint is the measured speed of KA0001/KA2001.
                        string traced2 = equiv?.TryResolve(col);
                        if (traced2 != null && predictions.TryGetValue(traced2, out var followPred))
                            return SafeAt(followPred, IndexFor(traced2));
                        // 3. Operator input from CSV (true external setpoint).
                        if (dataset.TryGetValue(col, out var arr)) return SafeAt(arr, t);
                        return double.NaN;
                    }
                    return FromCsvCol(col);
                }
                return double.NaN;
            }

            if (key.StartsWith("@P_in:") || key.StartsWith("@P_out:") || key.StartsWith("@Flow:"))
            {
                // ASC: re-resolve via topology each step is wasteful; cache outside if it
                // becomes a hot path. For now correctness > speed (N is small).
                var inputCols = DynamicPlantRunner.FindInputSignals(ownerId, inputEdges, signalMap, dataset, physicalNeighbors);
                return GetAscRoleValue(inputCols, key, t, predictions, csvToModelId, forceLastStep, equiv, computedThisStep);
            }

            if (key.StartsWith("@HX_Tin:") || key.StartsWith("@HX_Flow:") || key.StartsWith("@HX_CoolTemp:") || key.StartsWith("@HX_PartnerFlow:"))
            {
                var inputCols = DynamicPlantRunner.FindInputSignals(ownerId, inputEdges, signalMap, dataset, physicalNeighbors);
                return GetHxRoleValue(inputCols, key, t, predictions, csvToModelId, forceLastStep, equiv, computedThisStep);
            }

            // Source is a CSV column directly.
            if (dataset.ContainsKey(key) || FreeCsvColumns.Contains(key))
                return FromCsvCol(key);

            // Source is a model_id (signal-map key) — direct prediction read.
            if (predictions.TryGetValue(key, out var modelArr))
                return SafeAt(modelArr, IndexFor(key));

            // Source is a model-state key (e.g. "23VA0001_Pressure") — go through signal
            // map to its CSV and resolve via equivalence (handles UnitModel input slots).
            if (signalMap.TryGetValue(key, out string mapped))
                return FromCsvCol(mapped);

            return double.NaN;
        }

        private static double GetAscRoleValue(List<(string name, double[] data)> inputCols, string roleKey, int t,
            Dictionary<string, double[]> predictions, Dictionary<string, string> csvToModelId, bool forceLastStep,
            SignalEquivalenceMap equiv, HashSet<string> computedThisStep = null)
        {
            string role = roleKey.Substring(1, roleKey.IndexOf(':') - 1);
            // Same heuristic the trainer used to label Inlet/Outlet/Flow.
            (string name, double[] data) flow = default, p1 = default, p2 = default;
            foreach (var col in inputCols)
            {
                var n = col.name;
                bool looksFlow = n.IndexOf("Flow",     StringComparison.OrdinalIgnoreCase) >= 0
                              || n.IndexOf("MassFlow", StringComparison.OrdinalIgnoreCase) >= 0;
                if (looksFlow) { if (flow.data == null) flow = col; continue; }
                if (n.IndexOf("InletPressure", StringComparison.OrdinalIgnoreCase) >= 0)  { p1 = col; continue; }
                if (n.IndexOf("OutletPressure", StringComparison.OrdinalIgnoreCase) >= 0) { p2 = col; continue; }
                if (p1.data == null) p1 = col;
                else if (p2.data == null) p2 = col;
            }
            (string name, double[] data) pIn = p1, pOut = p2;
            if (p1.data != null && p2.data != null && p1.data.Length > 0 && p2.data.Length > 0
                && p1.data[0] > p2.data[0]) { pIn = p2; pOut = p1; }

            (string name, double[] data) chosen;
            switch (role)
            {
                case "P_in":  chosen = pIn;  break;
                case "P_out": chosen = pOut; break;
                case "Flow":  chosen = flow; break;
                default: return double.NaN;
            }
            if (chosen.data == null) return double.NaN;

            // Substitute predicted value if this column corresponds to a modelled state.
            // The name is "{key}({label})" — strip the parens to recover the key.
            // First try direct prediction (key is itself a model_id), then equivalence
            // tracing through the K-spice wiring as a fallback.
            // Use t-1 if the source model hasn't been evaluated yet this step (avoids
            // reading the default-zero slot when ASC runs before VA0001_Pressure in
            // the topological evaluation order).
            int paren = chosen.name.IndexOf('(');
            string key = paren > 0 ? chosen.name.Substring(0, paren).Trim() : chosen.name.Trim();
            if (predictions.TryGetValue(key, out var parr))
            {
                int idx = (forceLastStep || (computedThisStep != null && !computedThisStep.Contains(key))) ? t - 1 : t;
                return SafeAt(parr, idx);
            }
            string traced = equiv?.TryResolve(key);
            if (traced != null && predictions.TryGetValue(traced, out var parr2))
            {
                int idx = (forceLastStep || (computedThisStep != null && !computedThisStep.Contains(traced))) ? t - 1 : t;
                return SafeAt(parr2, idx);
            }
            return SafeAt(chosen.data, t);
        }

        private static double GetHxRoleValue(List<(string name, double[] data)> inputCols, string roleKey, int t,
            Dictionary<string, double[]> predictions, Dictionary<string, string> csvToModelId, bool forceLastStep,
            SignalEquivalenceMap equiv, HashSet<string> computedThisStep = null)
        {
            string role = roleKey.Substring(1, roleKey.IndexOf(':') - 1);
            (string name, double[] data) tin = default, flow = default, cool = default, partner = default;
            
            // Match HX inputs by topology edge labels (T_in, T_cool, m_partner, local_var for HX own flow).
            // Column names are formatted as "ModelKey(label)" by FindInputSignals.
            foreach (var col in inputCols)
            {
                string n = col.name;
                int pIdx = n.IndexOf('(');
                if (pIdx <= 0) continue;
                int closeParen = n.IndexOf(')', pIdx);
                if (closeParen <= pIdx) continue;
                string label = n.Substring(pIdx + 1, closeParen - pIdx - 1);
                
                if (string.Equals(label, "T_in", StringComparison.OrdinalIgnoreCase)) { tin = col; continue; }
                if (string.Equals(label, "T_cool", StringComparison.OrdinalIgnoreCase)) { cool = col; continue; }
                if (string.Equals(label, "m_partner", StringComparison.OrdinalIgnoreCase)) { partner = col; continue; }
                // HX's own mass flow is marked as "local_var" in the topology, but any "MassFlow" without a special label works.
                if (n.IndexOf("MassFlow", StringComparison.OrdinalIgnoreCase) >= 0 && flow.data == null) { flow = col; continue; }
            }

            (string name, double[] data) chosen = role switch
            {
                "HX_Tin" => tin,
                "HX_Flow" => flow,
                "HX_CoolTemp" => cool,
                "HX_PartnerFlow" => partner,
                _ => default
            };

            if (chosen.data == null)
                return double.NaN;

            int paren = chosen.name.IndexOf('(');
            string key = paren > 0 ? chosen.name.Substring(0, paren).Trim() : chosen.name.Trim();
            if (predictions.TryGetValue(key, out var parr))
            {
                int idx = (forceLastStep || (computedThisStep != null && !computedThisStep.Contains(key))) ? t - 1 : t;
                return SafeAt(parr, idx);
            }
            string traced = equiv?.TryResolve(key);
            if (traced != null && predictions.TryGetValue(traced, out var parr2))
            {
                int idx = (forceLastStep || (computedThisStep != null && !computedThisStep.Contains(traced))) ? t - 1 : t;
                return SafeAt(parr2, idx);
            }
            return SafeAt(chosen.data, t);
        }

        /// <summary>
        /// ValvePhysicsModel needs three positional inputs (P_in, P_out, U). Build
        /// each slot independently: topology edge if one exists, otherwise the
        /// trainer's SimInputs CSV column. SimInputs is the source of truth for
        /// boundary signals because it's what the trainer used for identification.
        /// </summary>
        private static List<IdentifiedModelEvaluator.InputSlot> HybridValveContract(
            string modelId,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            JArray simInputs,
            HashSet<string> validModelIds)
        {
            var slots = new List<IdentifiedModelEvaluator.InputSlot>();
            string[] labels = { "P_in", "P_out", "U(t)" };
            string[] simLabels = { "P_in", "P_out", "U" };
            inputEdges.TryGetValue(modelId, out var edges);
            for (int i = 0; i < 3; i++)
            {
                string preferred = null;  // edge whose source is a known predicted model
                string fallback = null;    // any edge with the right label (synthetic source)
                if (edges != null)
                {
                    foreach (var e in edges)
                    {
                        if (!string.Equals(e.label, labels[i], StringComparison.OrdinalIgnoreCase)) continue;
                        if (fallback == null) fallback = e.fromNode;
                        if (validModelIds != null && validModelIds.Contains(e.fromNode))
                        { preferred = e.fromNode; break; }
                    }
                }
                string source = preferred ?? fallback;
                if (source == null && simInputs != null && simInputs.Count > i)
                    source = (string)simInputs[i];
                if (source != null)
                    slots.Add(new IdentifiedModelEvaluator.InputSlot { SourceKey = source, Label = simLabels[i] });
            }
            return slots;
        }

        /// <summary>
        /// Build an input contract from labelled topology edges. Each requested label
        /// (e.g. "P_in", "P_out", "U(t)") is matched against an edge with that label
        /// pointing into <paramref name="modelId"/>; the edge's source node becomes the
        /// slot's SourceKey. Returns slots in the requested label order so the model's
        /// Iterate gets inputs in the right positional order. Missing labels yield no
        /// slot — caller decides whether to fall back to the original contract.
        /// </summary>
        private static List<IdentifiedModelEvaluator.InputSlot> ContractFromTopologyEdges(
            string modelId,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            string[] orderedLabels)
        {
            var slots = new List<IdentifiedModelEvaluator.InputSlot>();
            if (!inputEdges.TryGetValue(modelId, out var edges)) return slots;
            foreach (var label in orderedLabels)
            {
                // First match wins. Topology may legitimately have multiple edges with
                // the same label (e.g. 23UV0001_MassFlow has two U(t) edges, one from
                // its primary controller and one from the ASC). The trainer used the
                // first; we follow the same convention for consistency.
                var match = edges.FirstOrDefault(e => string.Equals(e.label, label, StringComparison.OrdinalIgnoreCase));
                if (match.fromNode == null) continue;
                slots.Add(new IdentifiedModelEvaluator.InputSlot
                {
                    SourceKey = match.fromNode,
                    Label = label,
                });
            }
            return slots;
        }

        private static double SafeAt(double[] arr, int t)
        {
            if (arr == null || arr.Length == 0) return double.NaN;
            if (t < 0) return arr[0];
            if (t >= arr.Length) return arr[arr.Length - 1];
            return arr[t];
        }

        // Stable topological sort. Returns a flattened evaluation order. Edges that
        // would close a cycle are pulled out into cycleBackEdges (keyed by the
        // *destination* model_id — i.e., for that model, those source keys must
        // be looked up at t-1). A simple DFS with a recursion stack is enough.
        private static List<string> ComputeEvaluationOrder(List<string> allIds,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            out Dictionary<string, HashSet<string>> cycleBackEdges)
        {
            var cycles  = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var onStack = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var order   = new List<string>();
            // Topology edges can point to nodes that aren't equation outputs (e.g. raw
            // CSV signals like "23PIC0001:Measurement"). Skip those during traversal —
            // they're handled at runtime by ResolveSlotValue, not by the simulator loop.
            var idSet = new HashSet<string>(allIds, StringComparer.OrdinalIgnoreCase);

            void Visit(string node)
            {
                if (visited.Contains(node)) return;
                if (onStack.Contains(node)) return; // cycle guard handled per-edge below
                if (!idSet.Contains(node)) return;
                onStack.Add(node);
                if (inputEdges.TryGetValue(node, out var edges))
                {
                    foreach (var (from, _) in edges)
                    {
                        if (!idSet.Contains(from)) continue; // CSV-signal upstream — not a node we evaluate
                        if (onStack.Contains(from))
                        {
                            // Edge from→node closes a cycle. Mark from as a cycle-back
                            // input for node so the runner uses prev-step value.
                            if (!cycles.TryGetValue(node, out var set))
                            {
                                set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                cycles[node] = set;
                            }
                            set.Add(from);
                            continue;
                        }
                        Visit(from);
                    }
                }
                onStack.Remove(node);
                visited.Add(node);
                order.Add(node);
            }

            foreach (var id in allIds) Visit(id);
            cycleBackEdges = cycles;
            return order;
        }
    }
}
