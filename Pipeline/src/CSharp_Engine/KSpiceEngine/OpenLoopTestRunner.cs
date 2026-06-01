using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine
{
    /// <summary>
    /// Test 2 — drive every identified model with CSV ground-truth inputs from a
    /// HELD-OUT dataset (KspiceSimTestdata.csv). No re-identification: uses the
    /// frozen parameters from CS_Identified_Parameters.json. Tells you whether
    /// the models track real plant behaviour or only fit the training trace.
    /// </summary>
    public static class OpenLoopTestRunner
    {
        public static void Run(string testCsvPath, string systemMapPath, string equationsPath,
                               string mappingPath, string identifiedParamsPath,
                               string topologyPath, string outputCsvPath)
        {
            Console.WriteLine("\n[OpenLoopTestRunner] Starting Test 2: frozen-model evaluation on held-out CSV...");
            Console.WriteLine($"  Test CSV : {testCsvPath}");

            var systemMap = JObject.Parse(File.ReadAllText(systemMapPath));
            var models = (JArray?)systemMap["Models"] ?? new JArray();
            var signalMap = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText(mappingPath))
                            ?? new Dictionary<string, string>();
            var equations = JsonConvert.DeserializeObject<List<dynamic>>(File.ReadAllText(equationsPath))
                            ?? new List<dynamic>();
            var identifiedParams = JObject.Parse(File.ReadAllText(identifiedParamsPath));
            var topology = JObject.Parse(File.ReadAllText(topologyPath));
            var topoEdges = (JArray?)topology["edges"] ?? new JArray();

            var inputEdges = new Dictionary<string, List<(string fromNode, string label)>>();
            foreach (var edge in topoEdges)
            {
                string to = (string?)edge["to"] ?? "";
                string from = (string?)edge["from"] ?? "";
                string label = (string?)edge["label"] ?? "";
                if (!inputEdges.ContainsKey(to)) inputEdges[to] = new List<(string, string)>();
                inputEdges[to].Add((from, label));
            }
            var physicalNeighbors = DynamicPlantRunner.BuildPhysicalAdjacency(models);

            var dataset = DynamicPlantRunner.LoadCsvDataset(testCsvPath);
            int numRows = dataset.Values.First().Length;
            double timeBase_s = DynamicPlantRunner.DetectTimeStep(dataset);

            var predictions = new Dictionary<string, double[]>();
            // Models that were genuinely run (not boundary/fallback/unresolved truth-copies).
            // Only these get a fit score written — copying truth into a skipped slot and then
            // scoring it against truth yields a meaningless 100%.
            var evaluated = new HashSet<string>();
            int ok = 0, skipped = 0, failed = 0;

            foreach (var eq in equations)
            {
                string id    = (string)eq.ID;
                string comp  = (string)eq.Component;
                string state = (string)eq.State;

                string mapKey = $"{comp}_{state}";
                if (!signalMap.ContainsKey(mapKey))
                {
                    skipped++;
                    continue;
                }
                string truthCol = signalMap[mapKey];
                if (!dataset.ContainsKey(truthCol))
                {
                    skipped++;
                    continue;
                }
                double[] Y_true = dataset[truthCol];

                var p = (JObject?)identifiedParams[id];
                if (p == null)
                {
                    skipped++;
                    continue;
                }
                string mt = (string?)p["ModelType"] ?? "";

                if (mt == "Boundary" || mt == "Fallback")
                {
                    // Boundary inputs are not predictions; copy truth so the plot
                    // stays aligned but skip in the success counter.
                    predictions[id] = (double[])Y_true.Clone();
                    skipped++;
                    continue;
                }

                try
                {
                    var evaluator = new IdentifiedModelEvaluator(id, p);
                    evaluator.WarmStart(Y_true[0]);

                    // Resolve each input slot to a CSV column on the test dataset.
                    var inputArrays = ResolveInputArrays(evaluator, comp, signalMap, dataset,
                                                         id, inputEdges, physicalNeighbors);
                    if (inputArrays == null)
                    {
                        Console.WriteLine($"[WARNING] {id}: could not resolve all inputs on test CSV — skipping");
                        predictions[id] = (double[])Y_true.Clone();
                        failed++;
                        continue;
                    }

                    double[] yPred = new double[numRows];
                    double[] step = new double[inputArrays.Count];
                    for (int t = 0; t < numRows; t++)
                    {
                        for (int k = 0; k < inputArrays.Count; k++)
                            step[k] = inputArrays[k]?[t] ?? double.NaN;
                        yPred[t] = evaluator.Iterate(step, timeBase_s);
                    }

                    double fit = ComputeFit(Y_true, yPred);
                    Console.WriteLine($"[Model] {id} [{mt}] test fit = {fit,6:F1}%");
                    predictions[id] = yPred;
                    evaluated.Add(id);
                    ok++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WARNING] {id}: evaluation failed: {ex.Message}");
                    predictions[id] = (double[])Y_true.Clone();
                    failed++;
                }
            }

            DynamicPlantRunner.WriteValidationCsv(dataset, predictions, signalMap, outputCsvPath);

            // Compute & dump per-model test-set fits to a small JSON next to the CSV.
            // Plotter reads it (if present) so plot titles show "test fit X%" instead
            // of the stale training fit. Without this the plots are misleading.
            var fits = new JObject();
            foreach (var kv in predictions)
            {
                if (!evaluated.Contains(kv.Key)) continue; // skip truth-copies (boundary/fallback/unresolved)
                if (!signalMap.ContainsKey(kv.Key)) continue;
                string col = signalMap[kv.Key];
                if (!dataset.ContainsKey(col)) continue;
                fits[kv.Key] = ComputeFit(dataset[col], kv.Value);
            }
            string fitsPath = Path.Combine(Path.GetDirectoryName(outputCsvPath) ?? "", "TestSet_FitScores.json");
            File.WriteAllText(fitsPath, fits.ToString(Formatting.Indented));

            Console.WriteLine($"\n[SUCCESS] Test-set predictions: {ok} OK, {skipped} skipped, {failed} failed");
            Console.WriteLine($"          Output: {outputCsvPath}");
            Console.WriteLine($"          Fits  : {fitsPath}");
        }

        // Resolves the model's InputContract into actual CSV time-series for the
        // test dataset. Reuses the same FindInputSignals / proxy-BFS path the
        // trainer used so signal names match exactly.
        private static List<double[]?>? ResolveInputArrays(IdentifiedModelEvaluator evaluator,
            string comp, Dictionary<string, string> signalMap,
            Dictionary<string, double[]> dataset, string modelId,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            Dictionary<string, HashSet<string>> physicalNeighbors)
        {
            var result = new List<double[]?>();

            foreach (var slot in evaluator.InputContract)
            {
                double[]? arr = null;
                string key = slot.SourceKey;

                if (key.StartsWith("@Setpoint:") || key.StartsWith("@Measurement:"))
                {
                    string what = key.StartsWith("@Setpoint:") ? "Setpoint" : "Measurement";
                    string c = key.Substring(key.IndexOf(':') + 1);
                    string lookup = $"{c}_{what}";
                    if (signalMap.TryGetValue(lookup, out var col) && dataset.ContainsKey(col))
                        arr = dataset[col];
                }
                else if (key.StartsWith("@P_in:") || key.StartsWith("@P_out:") || key.StartsWith("@Flow:"))
                {
                    // For ASC: re-use FindInputSignals on this node, then pick the right one
                    // by name heuristic (mirrors the trainer's logic).
                    var inputCols = DynamicPlantRunner.FindInputSignals(modelId, inputEdges, signalMap, dataset, physicalNeighbors);
                    arr = PickAscInput(inputCols, key);
                }
                else if (key.StartsWith("@HX_Tin:") || key.StartsWith("@HX_Flow:")
                      || key.StartsWith("@HX_CoolTemp:") || key.StartsWith("@HX_PartnerFlow:"))
                {
                    // HeatExchanger inputs: walk the topology for this node and pick the
                    // column by edge label, exactly as the trainer did. Open-loop → take the
                    // raw CSV series (no prediction substitution). Mirrors ClosedLoopRunner's
                    // GetHxRoleValue. Without this branch the model was silently skipped and
                    // the ground-truth signal copied through, producing a fake 100% fit.
                    var inputCols = DynamicPlantRunner.FindInputSignals(modelId, inputEdges, signalMap, dataset, physicalNeighbors);
                    arr = PickHxInput(inputCols, key);
                }
                else if (string.Equals(key, "Assumed_100%", StringComparison.OrdinalIgnoreCase))
                {
                    // Valve with no controller wired: U(t) is held fully open. Synthesize a
                    // constant 100% series (mirrors ClosedLoopRunner's sentinel). Without this
                    // the valve's MassFlow model was skipped and scored a fake 100%.
                    arr = Enumerable.Repeat(100.0, dataset.Values.First().Length).ToArray();
                }
                else if (signalMap.TryGetValue(key, out var col) && dataset.ContainsKey(col))
                {
                    // Direct lookup against signal map (UnitModel-based + IntegratedFlow).
                    arr = dataset[col];
                }
                else if (dataset.ContainsKey(key))
                {
                    // ValvePhysicsModel SimInputs are stored as raw CSV column names.
                    arr = dataset[key];
                }
                else
                {
                    // Last resort: try proxy resolution via physical adjacency.
                    int li = key.LastIndexOf('_');
                    if (li > 0)
                    {
                        string srcComp  = key.Substring(0, li);
                        string srcState = key.Substring(li + 1);
                        var inputCols = DynamicPlantRunner.FindInputSignals(modelId, inputEdges, signalMap, dataset, physicalNeighbors);
                        // Try to find by matching the slot key in inputCols
                        var match = inputCols.FirstOrDefault(c => c.name.StartsWith(key));
                        if (match.data != null) arr = match.data;
                    }
                }

                if (arr == null)
                {
                    // @HX_* slots are soft: the HeatExchanger model ignores/guards NaN for the
                    // inputs it doesn't use per subtype (GasSide ignores T_cool; WaterSide
                    // ignores the flows), so a missing one must not drop the whole model.
                    if (slot.Optional || key.StartsWith("@HX_"))
                    {
                        result.Add(null); // step loop fills NaN
                        continue;
                    }
                    Console.WriteLine($"  [MISS] {modelId} could not resolve input '{key}' in test CSV");
                    return null;
                }
                result.Add(arr);
            }
            return result;
        }

        // Resolve a HeatExchanger input role (@HX_Tin/@HX_Flow/@HX_CoolTemp/@HX_PartnerFlow)
        // to its raw CSV series by matching the topology edge label. Mirrors the column
        // selection in ClosedLoopRunner.GetHxRoleValue, but returns the truth series (open-loop)
        // rather than a predicted one. Returns null if the label isn't present — the caller
        // then feeds NaN, which the model guards (the slot is one this subtype ignores).
        private static double[]? PickHxInput(List<(string name, double[] data)> inputCols, string roleKey)
        {
            string role = roleKey.Substring(1, roleKey.IndexOf(':') - 1);
            (string name, double[] data) tin = default, flow = default, cool = default, partner = default;
            foreach (var col in inputCols)
            {
                string n = col.name;
                int pIdx = n.IndexOf('(');
                if (pIdx <= 0) continue;
                int closeParen = n.IndexOf(')', pIdx);
                if (closeParen <= pIdx) continue;
                string label = n.Substring(pIdx + 1, closeParen - pIdx - 1);

                if (string.Equals(label, "T_in", StringComparison.OrdinalIgnoreCase))      { tin = col;     continue; }
                if (string.Equals(label, "T_cool", StringComparison.OrdinalIgnoreCase))    { cool = col;    continue; }
                if (string.Equals(label, "m_partner", StringComparison.OrdinalIgnoreCase)) { partner = col; continue; }
                // The HX's own mass flow is labelled "local_var"; accept any MassFlow column.
                if (n.IndexOf("MassFlow", StringComparison.OrdinalIgnoreCase) >= 0 && flow.data == null) { flow = col; continue; }
            }

            return role switch
            {
                "HX_Tin"         => tin.data,
                "HX_Flow"        => flow.data,
                "HX_CoolTemp"    => cool.data,
                "HX_PartnerFlow" => partner.data,
                _                => null
            };
        }

        private static double[]? PickAscInput(List<(string name, double[] data)> inputCols, string roleKey)
        {
            string role = roleKey.Substring(1, roleKey.IndexOf(':') - 1);
            double[]? flow = null, press1 = null, press2 = null;
            foreach (var col in inputCols)
            {
                var n = col.name;
                bool looksFlow = n.IndexOf("Flow",     StringComparison.OrdinalIgnoreCase) >= 0
                              || n.IndexOf("MassFlow", StringComparison.OrdinalIgnoreCase) >= 0
                              || n.IndexOf("FlowDP",   StringComparison.OrdinalIgnoreCase) >= 0;
                if (looksFlow) { if (flow == null) flow = col.data; continue; }
                if (n.IndexOf("InletPressure", StringComparison.OrdinalIgnoreCase) >= 0) { press1 = col.data; continue; }
                if (n.IndexOf("OutletPressure", StringComparison.OrdinalIgnoreCase) >= 0) { press2 = col.data; continue; }
                if (press1 == null) press1 = col.data;
                else if (press2 == null) press2 = col.data;
            }

            double[]? pIn = press1, pOut = press2;
            if (press1 != null && press2 != null && press1.Length > 0 && press2.Length > 0
                && press1[0] > press2[0]) { pIn = press2; pOut = press1; }

            switch (role)
            {
                case "P_in":  return pIn;
                case "P_out": return pOut;
                case "Flow":  return flow;
            }
            return null;
        }

        internal static double ComputeFit(double[] yTrue, double[] yPred)
        {
            if (yTrue == null || yPred == null) return 0;
            int n = Math.Min(yTrue.Length, yPred.Length);
            if (n == 0) return 0;
            double mean = 0; int cnt = 0;
            for (int i = 0; i < n; i++) if (!double.IsNaN(yTrue[i])) { mean += yTrue[i]; cnt++; }
            if (cnt == 0) return 0;
            mean /= cnt;
            double sse = 0, tss = 0;
            for (int i = 0; i < n; i++)
            {
                if (double.IsNaN(yTrue[i]) || double.IsNaN(yPred[i])) continue;
                double r = yTrue[i] - yPred[i]; sse += r * r;
                double m = yTrue[i] - mean;     tss += m * m;
            }
            return tss > 0 ? Math.Max(-100, 100 * (1 - sse / tss)) : 0;
        }
    }
}
