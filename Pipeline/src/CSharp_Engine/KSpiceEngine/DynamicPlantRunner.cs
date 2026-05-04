using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using TimeSeriesAnalysis;
using TimeSeriesAnalysis.Dynamic;
using TimeSeriesAnalysis.Utility;

namespace KSpiceEngine
{
    public static class DynamicPlantRunner
    {
        public static void RunPlantSimulations(string csvPath, string systemMapPath, string equationsPath, string mappingPath, string outputCsvPath)
        {
            Console.WriteLine("\n[DynamicPlantRunner] Starting Phase 6: Training and Validation Pipeline...");
            
            // 1. Load System Map (for hardcoded physics parameters like PID Gains)
            var systemMap = JObject.Parse(File.ReadAllText(systemMapPath));
            var models = (JArray)systemMap["Models"];
            
            var compToType = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var model in models)
            {
                var name = (string)model["Name"];
                bool isPipeSuffix = name.EndsWith("_pf", StringComparison.OrdinalIgnoreCase);
                var rawName = name.Replace("_pf", "", StringComparison.OrdinalIgnoreCase);
                if (isPipeSuffix && compToType.ContainsKey(rawName))
                    continue;
                compToType[rawName] = (string)model["KSpiceType"];
            }
            
            // 2. Load Mapping Dictionary
            var signalMap = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText(mappingPath));
            
            // 3. Load Equations
            var equations = JsonConvert.DeserializeObject<List<dynamic>>(File.ReadAllText(equationsPath));
            
            // 4. Load Explicit Topology for edge wiring
            string topologyPath = Path.Combine(Path.GetDirectoryName(equationsPath), "TSA_Explicit_Topology.json");
            JObject topology = JObject.Parse(File.ReadAllText(topologyPath));
            var topoEdges = (JArray)topology["edges"];
            
            // Build adjacency: for each node, what are its input edges (from -> to)
            var inputEdges = new Dictionary<string, List<(string fromNode, string label)>>();
            foreach (var edge in topoEdges)
            {
                string to = (string)edge["to"];
                string from = (string)edge["from"];
                string label = (string)edge["label"];
                if (!inputEdges.ContainsKey(to))
                    inputEdges[to] = new List<(string, string)>();
                inputEdges[to].Add((from, label));
            }

            // Build physical KSpice adjacency for proxy signal resolution
            var physicalNeighbors = BuildPhysicalAdjacency(models);
            
            // 5. Load CSV Dataset (Raw KSpice true output)
            var dataset = LoadCsvDataset(csvPath);
            int numRows = dataset.Values.First().Length;
            double timeBase_s = 0.5; // K-Spice typical timestep
            
            // Output Storage Dictionary (ID -> Predicted Array)
            var predictions = new Dictionary<string, double[]>();
            var identifiedParams = new Dictionary<string, JObject>();
            
            // 6. Iterate through equations and simulate/identify
            foreach (var eq in equations)
            {
                string id = (string)eq.ID;
                string comp           = (string)eq.Component;
                string state          = (string)eq.State;
                string role           = (string)eq.Role;
                string formula        = (string)eq.Formula;
                string controllerType = (string)eq.ControllerType ?? "";  // "PID" | "ASC" | ""

                string kspiceType = compToType.ContainsKey(comp) ? compToType[comp] : "Unknown";
                
                string mapKey = $"{comp}_{state}";
                if (!signalMap.ContainsKey(mapKey))
                {
                    Console.WriteLine($"[WARNING] No actual mapping found for {id}. Skipping...");
                    continue;
                }
                
                string targetCsvHeader = signalMap[mapKey];
                if (!dataset.ContainsKey(targetCsvHeader)) continue;
                
                double[] Y_true = dataset[targetCsvHeader];

                // Skip boundary/source nodes (no inputs to model)
                if (formula.Contains("Boundary"))
                {
                    Console.WriteLine($"[Model] {id}: Boundary node (input-only), copying true signal.");
                    predictions[id] = (double[])Y_true.Clone();
                    identifiedParams[id] = new JObject { ["ModelType"] = "Boundary" };
                    continue;
                }

                // -----------------------------------------------------
                //   A. Controller Modeling (PID) — identify Kp/Ti from data
                //
                //   K-Spice's stored Gain/Action parameters don't translate
                //   directly to a usable Kp because of internal range scaling
                //   and unclear Action conventions (Action=0 actually behaves
                //   reverse-acting for our level/pressure controllers, which
                //   would require a negative effective Kp). Letting TSA's
                //   PidIdentifier learn Kp/Ti from the actual controller-
                //   output trace recovers the right magnitude AND sign in one
                //   step, the same approach the old greybox pipeline used.
                // -----------------------------------------------------
                if (role == "Controller" && controllerType == "PID")
                {
                    string measMapKey = $"{comp}_Measurement";
                    string spMapKey = $"{comp}_Setpoint";

                    if (!signalMap.ContainsKey(measMapKey) || !signalMap.ContainsKey(spMapKey))
                    {
                        Console.WriteLine($"[WARNING] {id}: No Measurement/Setpoint mapping for controller {comp}.");
                        predictions[id] = Enumerable.Repeat(0.0, numRows).ToArray();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "No Measurement/Setpoint signal mapping" };
                        continue;
                    }

                    var measSignal = signalMap[measMapKey];
                    var spSignal   = signalMap[spMapKey];

                    if (!dataset.ContainsKey(measSignal) || !dataset.ContainsKey(spSignal))
                    {
                        Console.WriteLine($"[WARNING] {id}: Measurement or Setpoint signal not in CSV.");
                        predictions[id] = Enumerable.Repeat(0.0, numRows).ToArray();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "Measurement or Setpoint not in CSV" };
                        continue;
                    }

                    double[] Y_meas = dataset[measSignal];
                    double[] Y_sp   = dataset[spSignal];

                    try
                    {
                        // Identify Kp, Ti from data via TSA's PidIdentifier:
                        //   Y_meas → process variable, Y_setpoint → setpoint,
                        //   U → actual ControllerOutput we want to reproduce.
                        var pidData = new UnitDataSet();
                        pidData.Y_meas     = Y_meas;
                        pidData.Y_setpoint = Y_sp;
                        pidData.U = new double[numRows, 1];
                        for (int i = 0; i < numRows; i++) pidData.U[i, 0] = Y_true[i];
                        pidData.CreateTimeStamps(timeBase_s);

                        var pidIdentifier = new PidIdentifier();
                        var pidParam = pidIdentifier.Identify(ref pidData);

                        double Kp = pidParam.Kp;
                        double Ti = pidParam.Ti_s;
                        double Td = pidParam.Td_s;

                        // Helper: simulate using TSA's PidController (same engine the
                        // identifier uses internally), warm-started at the true initial
                        // u so the first sample matches.
                        Func<double, double[]> SimulateWith = (kpUse) =>
                        {
                            var ctrl = new PidController(timeBase_s, kpUse, Ti, Td);
                            ctrl.WarmStart(Y_meas[0], Y_sp[0], Y_true[0]);
                            double[] u = ctrl.Iterate(Y_meas, Y_sp);
                            // u[0] from Iterate uses internal state; force exact match at start
                            u[0] = Y_true[0];
                            for (int i = 0; i < u.Length; i++)
                                u[i] = Math.Max(0, Math.Min(100, u[i]));
                            return u;
                        };

                        Func<double[], double> FitPrc = (pred) =>
                        {
                            double mean = Y_true.Average();
                            double sse = 0, tss = 0;
                            for (int i = 0; i < numRows; i++)
                            {
                                double r = Y_true[i] - pred[i];
                                sse += r * r;
                                double m = Y_true[i] - mean;
                                tss += m * m;
                            }
                            return (tss > 0) ? Math.Max(-100, 100 * (1 - sse / tss)) : 0;
                        };

                        // PidIdentifier's internal sign-flip check sometimes fails when
                        // process and controller-output correlation is weak, leaving Kp
                        // with the wrong sign. Try both signs and keep whichever gives
                        // the better external-simulation fit against the actual
                        // ControllerOutput trace.
                        double[] predPos = SimulateWith(Kp);
                        double[] predNeg = SimulateWith(-Kp);
                        double fitPos = FitPrc(predPos);
                        double fitNeg = FitPrc(predNeg);

                        double[] Y_pred;
                        double fitScore;
                        if (fitNeg > fitPos)
                        {
                            Kp = -Kp;
                            Y_pred = predNeg;
                            fitScore = fitNeg;
                            Console.WriteLine($"[Model] {id}: PID identified Kp={Kp:F4}, Ti={Ti:F2}s, Td={Td:F2}s (sign flipped from PidIdentifier — fit improved {fitPos:F1}%→{fitNeg:F1}%)");
                        }
                        else
                        {
                            Y_pred = predPos;
                            fitScore = fitPos;
                            Console.WriteLine($"[Model] {id}: PID identified Kp={Kp:F4}, Ti={Ti:F2}s, Td={Td:F2}s");
                        }
                        predictions[id] = Y_pred;
                        Console.WriteLine($"[SUCCESS] {id}: PID FitScore={fitScore:F1}%");

                        identifiedParams[id] = new JObject
                        {
                            ["ModelType"] = "PID",
                            ["Kp"]        = Kp,
                            ["Ti_s"]      = Ti,
                            ["Td_s"]      = Td,
                            ["FitScore"]  = fitScore,
                            ["Formula"]   = "Incremental PI (TSA PidController, data-identified): u(k) = u(k-1) + Kp*((e(k)-e(k-1)) + dt/Ti * e(k)),  e = SP - PV"
                        };
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[WARNING] {id}: PidIdentifier failed: {ex.Message}");
                        predictions[id] = (double[])Y_true.Clone();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = ex.Message };
                    }
                }
                
                // -----------------------------------------------------
                //   B. Anti-Surge Controller Modeling (ASC)
                // -----------------------------------------------------
                else if (role == "Controller" && controllerType == "ASC")
                {
                    Console.WriteLine($"[Model] {id}: ASC Generic Identification...");
                    
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    
                    if (inputCols.Count >= 3)
                    {
                        try
                        {
                            double[] flow = null, pIn = null, pOut = null;
                            string   flowName = null, pInName = null, pOutName = null;
                            // Collect ALL pressure candidates; pick suction (lowest mean) as
                            // pIn and discharge (highest mean) as pOut.  A simple press1/press2
                            // pair assignment would silently drop the third signal when there
                            // are three pressure inputs (e.g. VA0001+KA0001+HX0001), causing
                            // the two discharge-side pressures to be picked (tiny DP ≈ 0.2 bar)
                            // instead of the correct suction↔discharge pair (DP ≈ 24 bar).
                            var pressCands = new List<(double[] data, string name, double mean)>();
                            foreach (var col in inputCols)
                            {
                                var n = col.name;
                                if (n.IndexOf("FlowDP", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                    (n.IndexOf("KSpice:", StringComparison.OrdinalIgnoreCase) >= 0 &&
                                     (n.IndexOf("Flow", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                      n.IndexOf("MassFlow", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                      n.IndexOf("Performance", StringComparison.OrdinalIgnoreCase) >= 0)))
                                {
                                    if (flow == null || n.IndexOf("FlowDP", StringComparison.OrdinalIgnoreCase) >= 0)
                                    { flow = col.data; flowName = col.name; }
                                    continue;
                                }
                                if (n.IndexOf("InletPressure", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    // Explicit K-Spice inlet-pressure signal: insert at front
                                    double m = col.data.Average();
                                    pressCands.Insert(0, (col.data, col.name, m));
                                    continue;
                                }
                                if (n.IndexOf("OutletPressure", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    double m = col.data.Average();
                                    pressCands.Add((col.data, col.name, m));
                                    continue;
                                }
                                if (col.name.Contains("Flow") || col.name.Contains("Mass"))
                                {
                                    if (flow == null) { flow = col.data; flowName = col.name; }
                                }
                                else
                                {
                                    double m = col.data.Average();
                                    pressCands.Add((col.data, col.name, m));
                                }
                            }
                            if (pressCands.Count >= 2)
                            {
                                // pIn = lowest mean pressure (suction side), pOut = highest (discharge)
                                var sorted = pressCands.OrderBy(p => p.mean).ToList();
                                pIn    = sorted[0].data; pInName  = sorted[0].name;
                                pOut   = sorted[sorted.Count - 1].data; pOutName = sorted[sorted.Count - 1].name;
                            }
                            else if (pressCands.Count == 1)
                            {
                                pIn = pressCands[0].data; pInName = pressCands[0].name;
                            }

                            // ── Resolve predicted arrays (close the identification loop) ────────
                            // By the time the ASC is identified, upstream compressor/separator
                            // models are already fitted and their predictions are in the dict.
                            // Using those predicted signals instead of K-Spice truth means the
                            // surge proxy and all threshold parameters are calibrated in the
                            // same coordinate system the ASC will actually see in closed-loop.
                            // Falls back to K-Spice CSV for signals not yet predicted (boundaries).
                            double[] TryGetPred(string colName)
                            {
                                if (colName == null) return null;
                                int paren = colName.IndexOf('(');
                                string key = paren > 0 ? colName.Substring(0, paren).Trim() : colName.Trim();
                                return predictions.TryGetValue(key, out var pred) && pred != null ? pred : null;
                            }
                            double[] pIn_pred   = TryGetPred(pInName)   ?? pIn;
                            double[] pOut_pred  = TryGetPred(pOutName)  ?? pOut;
                            double[] flow_pred  = TryGetPred(flowName)  ?? flow;
                            bool usingPredicted = (pIn_pred != pIn || pOut_pred != pOut || flow_pred != flow);
                            Console.WriteLine($"[Model] {id}:   ASC inputs: pIn={pInName}, pOut={pOutName}, flow={flowName}  (predicted override: {usingPredicted})");
                            
                            if (pIn != null && pOut != null && flow != null)
                            {
                                // Pull OpenTime/CloseTime defaults from the K-Spice .prm where
                                // available. CloseTime there mirrors what we see in the trace
                                // (50→0% in ~30 s ⇒ 60 s full-stroke). The .prm OpenTime (20 s)
                                // is too slow vs the actual ~17 %/s rise — keep our 5 s default.
                                double closeTime_s = 60.0;
                                JObject kspiceModel = null;
                                foreach (var m in models)
                                {
                                    var rn = ((string)m["Name"]).Replace("_pf", "", StringComparison.OrdinalIgnoreCase);
                                    if (string.Equals(rn, comp, StringComparison.OrdinalIgnoreCase))
                                    { kspiceModel = (JObject)m; break; }
                                }
                                var prm = kspiceModel?["Parameters"] as JObject;
                                if (prm?["CloseTime"] != null) closeTime_s = (double)prm["CloseTime"];

                                // ── Multi-architecture benchmark ────────────────────────
                                // Each candidate is (label, feature vocabulary, LP τ). For
                                // each, we build the OLS feature matrix using the SAME
                                // EvaluateFeature function the runtime model uses, fit
                                // weights with a small ridge (avoids the P_out-noise
                                // overfit that gave the previous Weight_Pout = -564),
                                // then simulate with the AntiSurgePhysicalModel and score
                                // open-loop fit vs the K-Spice ControllerOutput trace.
                                // Candidate architectures. LP τ values are deliberately
                                // varied: small τ keeps transitions sharp, large τ kills
                                // plateau jitter. The asymmetric rate-limit handles the
                                // step-rise/ramp-down character even with small τ.
                                var linearFeats     = new[] {"P_out","MF","P_in","Const"};
                                var dpFeats         = new[] {"DP","MF","Const"};
                                var surgeFeats      = new[] {"DP","MF","MF2","DP_over_MF2","Const"};
                                var candidates = new List<(string label, string[] features, double lpTau)>
                                {
                                    ("LinearOLS",         linearFeats, 0.0),
                                    ("LinearOLS_LP1s",    linearFeats, 1.0),
                                    ("LinearOLS_LP2s",    linearFeats, 2.0),
                                    ("LinearOLS_LP3s",    linearFeats, 3.0),
                                    ("LinearOLS_LP5s",    linearFeats, 5.0),
                                    ("LinearDP_LP2s",     dpFeats,     2.0),
                                    ("LinearDP_LP3s",     dpFeats,     3.0),
                                    ("SurgeProxy_LP2s",   surgeFeats,  2.0),
                                    ("SurgeProxy_LP3s",   surgeFeats,  3.0),
                                };

                                int M = numRows;
                                var benchmark = new JArray();
                                double bestFit = double.NegativeInfinity;
                                double[] bestY = null;
                                JObject bestParams = null;

                                foreach (var cand in candidates)
                                {
                                    int K = cand.features.Length;

                                    // Ridge-OLS on raw features. λ=1e-3 in standardised scale
                                    // is enough to dampen the P_out-only fit without biasing
                                    // signals with a large natural range (MF, MF²).
                                    double[,] XtX = new double[K, K];
                                    double[]  Xty = new double[K];
                                    double[]  feat = new double[K];
                                    for (int i = 0; i < M; i++)
                                    {
                                        for (int k = 0; k < K; k++)
                                            feat[k] = KSpiceEngine.CustomModels.AntiSurgePhysicalModel
                                                          .EvaluateFeature(cand.features[k], pIn[i], pOut[i], flow[i]);
                                        for (int p = 0; p < K; p++)
                                        {
                                            Xty[p] += feat[p] * Y_true[i];
                                            for (int q = 0; q < K; q++) XtX[p, q] += feat[p] * feat[q];
                                        }
                                    }
                                    // Tiny ridge — only for numerical conditioning. We do NOT
                                    // want to penalise the OLS solution; aggressive ridge kills
                                    // the small-but-real P_out signal. Plateau-jitter is dealt
                                    // with by the LP filter stage, not by penalising weights.
                                    for (int p = 0; p < K; p++) XtX[p, p] += 1e-9;

                                    double[] w = SolveLinearSystem(XtX, Xty, K);
                                    if (w == null) continue;

                                    var ascModel = new KSpiceEngine.CustomModels.AntiSurgePhysicalModel(id,
                                        new string[]{"P_in", "P_out", "Flow"}, id);
                                    ascModel.modelParameters.Architecture    = cand.label;
                                    ascModel.modelParameters.FeatureNames    = cand.features;
                                    ascModel.modelParameters.FeatureWeights  = w;
                                    ascModel.modelParameters.LPFilter_Tau_s  = cand.lpTau;
                                    ascModel.modelParameters.CloseTime_s     = closeTime_s;
                                    ascModel.WarmStart(null, Y_true[0]);

                                    double[] y_sim = new double[M];
                                    double sse = 0, mean = Y_true.Average(), tss = 0;
                                    for (int i = 0; i < M; i++)
                                    {
                                        y_sim[i] = ascModel.Iterate(new double[]{ pIn[i], pOut[i], flow[i] }, timeBase_s)[0];
                                        double r = Y_true[i] - y_sim[i]; sse += r * r;
                                        double dm = Y_true[i] - mean;     tss += dm * dm;
                                    }
                                    double fit = (tss > 0) ? Math.Max(-100, 100 * (1 - sse / tss)) : 0;

                                    var weightsJson = new JObject();
                                    for (int k = 0; k < K; k++) weightsJson[cand.features[k]] = w[k];
                                    benchmark.Add(new JObject
                                    {
                                        ["Architecture"]   = cand.label,
                                        ["FeatureNames"]   = JArray.FromObject(cand.features),
                                        ["LPFilter_Tau_s"] = cand.lpTau,
                                        ["FitScore"]       = fit,
                                        ["Weights"]        = weightsJson
                                    });
                                    Console.WriteLine($"[Model] {id}:   {cand.label,-18} fit={fit,6:F2}%  τ_LP={cand.lpTau:F1}s  weights=[{string.Join(", ", cand.features.Zip(w, (n,v)=>$"{n}={v:G3}"))}]");

                                    if (fit > bestFit)
                                    {
                                        bestFit = fit;
                                        bestY = y_sim;
                                        bestParams = new JObject
                                        {
                                            ["ModelType"]      = "AntiSurgePhysicalModel",
                                            ["Architecture"]   = cand.label,
                                            ["FeatureNames"]   = JArray.FromObject(cand.features),
                                            ["FeatureWeights"] = JArray.FromObject(w),
                                            ["LPFilter_Tau_s"] = cand.lpTau,
                                            ["OpenTime_s"]     = ascModel.modelParameters.OpenTime_s,
                                            ["CloseTime_s"]    = ascModel.modelParameters.CloseTime_s,
                                            ["FitScore"]       = fit,
                                            ["Formula"]        = $"target = Σ w·feature(P_in,P_out,MF) over [{string.Join(", ", cand.features)}]; LP(τ={cand.lpTau:F1}s) → asymmetric rate-limit (open {100.0/ascModel.modelParameters.OpenTime_s:F1} %/s, close {100.0/closeTime_s:F2} %/s)."
                                        };
                                    }
                                }

                                // ── Kick-based candidate (TSA PidAntiSurgeParams style) ─────
                                // Mirrors the controller's *physics*, not just its trace:
                                //   1. surge_distance = MF + a·DP + b   (linear surge-line proxy)
                                //   2. in-surge ⇔ surge_distance < KickThreshold
                                //   3. while in-surge: u += KickRate·dt   (kick — fast open)
                                //   4. while safe & u>0: u -= RampDown/60·dt   (slow close)
                                // The intermittent in-surge ↔ safe oscillation during a sustained
                                // disturbance balances kick and ramp at a held-open plateau, just
                                // like the K-Spice trace.
                                {
                                    // Step 1 — derive ground-truth in_surge mask from K-Spice
                                    // internals if exposed (NormalizedFlow / NormalizedAsymmetricLimit
                                    // are GenericASC's own surge-margin signals — only used for
                                    // identification; runtime model uses raw P_in/P_out/MF only).
                                    // Fallback: derive from rising velocity of Y_true.
                                    bool[] inSurge = new bool[M];
                                    string nfKey   = $"{comp}:NormalizedFlow";
                                    string nasKey  = $"{comp}:NormalizedAsymmetricLimit";
                                    bool haveKspiceSurge = dataset.ContainsKey(nfKey) && dataset.ContainsKey(nasKey);
                                    if (haveKspiceSurge)
                                    {
                                        double[] nf  = dataset[nfKey];
                                        double[] nas = dataset[nasKey];
                                        for (int i = 0; i < M; i++) inSurge[i] = nf[i] < nas[i];
                                    }
                                    else
                                    {
                                        // Heuristic: in-surge whenever Y_true rises by > 1.5%/s
                                        // (matches the kick rate of a real ASC under disturbance).
                                        for (int i = 1; i < M; i++)
                                            inSurge[i] = (Y_true[i] - Y_true[i - 1]) / timeBase_s > 1.5;
                                    }

                                    // Step 2 — fit linear surge-margin from raw inputs.
                                    //
                                    // Best target available is the K-Spice ASC's own continuous
                                    // surge margin: (NormalizedFlow − NormalizedAsymmetricLimit).
                                    // This is a real-valued quantity (positive = safe, negative
                                    // = in surge), so we get a proper regression instead of a
                                    // bit-target classifier — the coefficients then carry physical
                                    // units (R² ≈ 0.93 on (MF, DP) for this dataset).
                                    //
                                    // Fallback when those signals aren't in the CSV: regress a
                                    // signed ±1 target on the in_surge mask (less informative,
                                    // class-imbalance handled with sample weights).
                                    int nSurge = 0, nSafe = 0;
                                    for (int i = 0; i < M; i++) { if (inSurge[i]) nSurge++; else nSafe++; }
                                    int nSurgeSamples = nSurge, nSafeSamples = nSafe;

                                    double[,] XtX = new double[3, 3];
                                    double[]  Xty = new double[3];

                                    if (haveKspiceSurge)
                                    {
                                        // Regress predicted features against K-Spice surge margin target.
                                        // Using pIn_pred/pOut_pred/flow_pred (predicted signals) as the
                                        // feature matrix means the fitted a,b are calibrated to the same
                                        // coordinate system the ASC will see at runtime in closed-loop.
                                        double[] nfArr  = dataset[nfKey];
                                        double[] nasArr = dataset[nasKey];
                                        for (int i = 0; i < M; i++)
                                        {
                                            double margin = nfArr[i] - nasArr[i];
                                            double DP_i = pOut_pred[i] - pIn_pred[i];
                                            double[] row = { flow_pred[i], DP_i, 1.0 };
                                            for (int p = 0; p < 3; p++)
                                            {
                                                Xty[p] += row[p] * margin;
                                                for (int q = 0; q < 3; q++) XtX[p, q] += row[p] * row[q];
                                            }
                                        }
                                    }
                                    else
                                    {
                                        double wSurge = (nSurge > 0 && nSafe > 0) ? 0.5 * M / Math.Max(1, nSurge) : 1.0;
                                        double wSafe  = (nSurge > 0 && nSafe > 0) ? 0.5 * M / Math.Max(1, nSafe)  : 1.0;
                                        for (int i = 0; i < M; i++)
                                        {
                                            double t = inSurge[i] ? -1.0 : +1.0;
                                            double w = inSurge[i] ? wSurge : wSafe;
                                            double DP_i = pOut_pred[i] - pIn_pred[i];
                                            double[] row = { flow_pred[i], DP_i, 1.0 };
                                            for (int p = 0; p < 3; p++)
                                            {
                                                Xty[p] += w * row[p] * t;
                                                for (int q = 0; q < 3; q++) XtX[p, q] += w * row[p] * row[q];
                                            }
                                        }
                                    }
                                    for (int p = 0; p < 3; p++) XtX[p, p] += 1e-9;
                                    double[] beta = SolveLinearSystem(XtX, Xty, 3);

                                    double surgeProxy_mf = 1.0, surgeProxy_a = 0.0, surgeProxy_b = -35.0, kickThreshold = 0.0;
                                    if (beta != null && nSurgeSamples > 5 && nSafeSamples > 5)
                                    {
                                        // Store all three regression coefficients directly; normalise
                                        // by the std-dev of the raw surge distances so the model
                                        // output is always O(1) regardless of which physical variable
                                        // dominates the surge margin.
                                        //
                                        // The regression target (NF-NAS or binary ±1) is negative for
                                        // in-surge and positive for safe, so the raw regression output
                                        // already has the correct sign convention: SD < 0 → in-surge.
                                        // No sign flip needed (unlike the old /beta[0] normalisation).
                                        double sumSd = 0, sumSd2 = 0;
                                        for (int i = 0; i < M; i++)
                                        {
                                            double DP_i = pOut_pred[i] - pIn_pred[i];
                                            double sd = beta[0] * flow_pred[i] + beta[1] * DP_i + beta[2];
                                            sumSd += sd; sumSd2 += sd * sd;
                                        }
                                        double meanSd = sumSd / M;
                                        double stdSd  = Math.Sqrt(Math.Max(1e-12, sumSd2 / M - meanSd * meanSd));
                                        double scale  = Math.Max(1e-6, stdSd);
                                        surgeProxy_mf = beta[0] / scale;
                                        surgeProxy_a  = beta[1] / scale;
                                        surgeProxy_b  = beta[2] / scale;
                                        kickThreshold = 0.0;
                                    }

                                    // Step 3 — kick rate from typical in-surge rise (75th pct of
                                    // dy>0 during in-surge samples — robust to a single outlier
                                    // and not biased low by saturated samples where dy=0).
                                    // Step 4 — ramp-down rate from genuine ramp-down periods only
                                    //          (75th pct of falls > 0.2 %/s), so idle samples
                                    //          where ASC≈0 and dy/dt≈0 don't drag the estimate.
                                    var rises = new List<double>();
                                    var falls = new List<double>();
                                    for (int i = 1; i < M; i++)
                                    {
                                        double dy = (Y_true[i] - Y_true[i - 1]) / timeBase_s;
                                        if (inSurge[i] && dy > 0.2 && Y_true[i] < 99.5) rises.Add(dy);
                                        if (!inSurge[i] && dy < -0.2 && Y_true[i - 1] > 5.0) falls.Add(-dy);
                                    }
                                    // Initial guesses — kick rate from the 90th-pct in-surge rise
                                    // (max would over-fit a single noisy spike; 90th-pct anchors
                                    // to the actual fast-kick episodes). Ramp from 75th-pct fall.
                                    double Pct(List<double> xs, double q)
                                    {
                                        if (xs.Count == 0) return double.NaN;
                                        xs.Sort();
                                        int idx = (int)Math.Floor(q * (xs.Count - 1));
                                        return xs[Math.Max(0, Math.Min(xs.Count - 1, idx))];
                                    }
                                    double kickRate = double.IsNaN(Pct(rises, 0.9)) ? 8.0  : Pct(rises, 0.9);
                                    double rampDown = double.IsNaN(Pct(falls, 0.75)) ? 60.0 : Pct(falls, 0.75) * 60.0; // %/min

                                    // Step 5 — joint grid search over (KickGain, RampDown, Threshold).
                                    // KickGain (proportional kick) is the analogue of K-Spice
                                    // GenericASC's FastProportionalGain (.prm value here = 2):
                                    // kick rate = KickGain · max(0, threshold - surge_distance).
                                    // This makes marginal-surge moments produce gentle kicks
                                    // (the held-open plateau) and deep-surge moments produce
                                    // hard fast kicks (the t=5s step-open). Pure constant kick
                                    // can't do both at once; proportional kick can.
                                    //
                                    // Initial KickGain estimate: peak rise rate / typical surge depth.
                                    double meanSurgeDepth = 0.0;
                                    int nDepthSamples = 0;
                                    for (int i = 0; i < M; i++)
                                    {
                                        if (inSurge[i])
                                        {
                                            double sd = surgeProxy_mf * flow_pred[i] + surgeProxy_a * (pOut_pred[i] - pIn_pred[i]) + surgeProxy_b;
                                            if (sd < kickThreshold) { meanSurgeDepth += (kickThreshold - sd); nDepthSamples++; }
                                        }
                                    }
                                    if (nDepthSamples > 0) meanSurgeDepth /= nDepthSamples; else meanSurgeDepth = 5.0;
                                    double kickGain0 = (meanSurgeDepth > 0.5) ? kickRate / meanSurgeDepth : 1.0;

                                    double bestKickFit  = double.NegativeInfinity;
                                    double bestKickRate = 0.0;
                                    double bestKickGain = 0.0;
                                    double bestRamp     = rampDown;
                                    double bestRampTau  = 0.0;
                                    double bestMargLP   = 0.0;
                                    double bestThr      = kickThreshold;
                                    double bestHoldThr  = kickThreshold;
                                    double[] kickRateGrid = { 0.0, 2.0, 4.0, 6.0, 8.0, 10.0 };
                                    double[] gainGrid     = { 0.0, 0.5, 1.0, 1.5, 2.0 };
                                    double[] rampGrid     = { 0.0, 15.0, 30.0, 60.0 };
                                    // RampDecay_Tau_s: 0 = pure linear (current); >0 = combined
                                    // linear+exponential. Smaller tau = faster initial decay,
                                    // longer tail. Range covers strong → mild exponential.
                                    double[] tauRampGrid  = { 0.0, 30.0, 60.0, 100.0, 200.0 };
                                    // Surge distances are normalised to unit std-dev, so
                                    // typical in-surge values are O(0.5-2) below threshold.
                                    double[] thrGrid      = { -2.0, -1.0, -0.5, 0.0 };
                                    double[] holdDeltas   = { 0.5, 1.0, 1.5, 2.0, 3.0, 5.0 };
                                    // SurgeMargin_LP_Tau_s: 0 = use raw margin; >0 adds memory so
                                    // brief excursions out of surge don't end the hold prematurely.
                                    // Wider tau ⇒ longer effective hold.
                                    double[] tauMargGrid  = { 0.0, 5.0, 15.0, 30.0 };

                                    // Composite objective: MSE-fit minus peak-mismatch penalty.
                                    // The peak penalty steers the optimum toward trajectories that
                                    // PEAK at the right value (matches K-Spice plateau height) —
                                    // pure MSE alone can be tied between "overshoot+fast-decay" and
                                    // "correct-plateau", and the user cares about behavioural shape.
                                    double peakRef = 0.0;
                                    for (int i = 0; i < M; i++) if (Y_true[i] > peakRef) peakRef = Y_true[i];

                                    // Helper: simulate KickBased with a parameter vector and score.
                                    Func<double, double, double, double, double, double, double, double, (double fit, double composite, double[] y)>
                                    Simulate = (kr, kg, rd, tauR, tauM, kThr, hThr, peakP) =>
                                    {
                                        var m2 = new KSpiceEngine.CustomModels.AntiSurgePhysicalModel(id,
                                            new string[]{"P_in", "P_out", "Flow"}, id);
                                        m2.modelParameters.Architecture              = "KickBased";
                                        m2.modelParameters.SurgeProxy_MF_Coeff       = surgeProxy_mf;
                                        m2.modelParameters.SurgeProxy_a              = surgeProxy_a;
                                        m2.modelParameters.SurgeProxy_b              = surgeProxy_b;
                                        m2.modelParameters.KickThreshold             = kThr;
                                        m2.modelParameters.HoldThreshold             = hThr;
                                        m2.modelParameters.KickRate_PrcPerSec        = kr;
                                        m2.modelParameters.KickGain_PrcPerSecPerUnit = kg;
                                        m2.modelParameters.RampDown_PrcPerMin        = rd;
                                        m2.modelParameters.RampDecay_Tau_s           = tauR;
                                        m2.modelParameters.SurgeMargin_LP_Tau_s      = tauM;
                                        m2.WarmStart(null, Y_true[0]);
                                        double sse2 = 0, mean2 = Y_true.Average(), tss2 = 0, peakSim = 0;
                                        double[] ySim = new double[M];
                                        for (int i = 0; i < M; i++)
                                        {
                                            double y = m2.Iterate(new double[]{ pIn_pred[i], pOut_pred[i], flow_pred[i] }, timeBase_s)[0];
                                            ySim[i] = y;
                                            if (y > peakSim) peakSim = y;
                                            double r = Y_true[i] - y; sse2 += r * r;
                                            double dm = Y_true[i] - mean2; tss2 += dm * dm;
                                        }
                                        double f = (tss2 > 0) ? Math.Max(-100, 100 * (1 - sse2 / tss2)) : 0;
                                        double composite = f - peakP * Math.Abs(peakSim - peakRef);
                                        return (f, composite, ySim);
                                    };

                                    double peakPenalty = 0.4;
                                    double bestComposite = double.NegativeInfinity;
                                    foreach (double kr in kickRateGrid)
                                    foreach (double kg in gainGrid)
                                    foreach (double rd in rampGrid)
                                    foreach (double tauR in tauRampGrid)
                                    foreach (double tauM in tauMargGrid)
                                    foreach (double thr in thrGrid)
                                    foreach (double hDelta in holdDeltas)
                                    {
                                        if (kr == 0.0 && kg == 0.0) continue;
                                        if (rd == 0.0 && tauR == 0.0) continue; // no decay → never closes
                                        var (f, score, _) = Simulate(kr, kg, rd, tauR, tauM, thr, thr + hDelta, peakPenalty);
                                        if (score > bestComposite)
                                        {
                                            bestComposite = score;
                                            bestKickFit  = f;
                                            bestKickRate = kr;
                                            bestKickGain = kg;
                                            bestRamp     = rd;
                                            bestRampTau  = tauR;
                                            bestMargLP   = tauM;
                                            bestThr      = thr;
                                            bestHoldThr  = thr + hDelta;
                                        }
                                    }
                                    double gridFit = bestKickFit;

                                    // ── Coordinate-descent refinement ───────────────────────────
                                    // Grid is coarse; refine each parameter locally by trying small
                                    // ±step moves. Each accepted move improves the composite score;
                                    // when no move helps, halve all step sizes and try again. This
                                    // gives meaningful per-parameter tuning — every accepted change
                                    // is a measured improvement on the actual K-Spice trace.
                                    double[] paramVec   = { bestKickRate, bestKickGain, bestRamp, bestRampTau, bestMargLP, bestThr, bestHoldThr };
                                    double[] paramSteps = {  1.0,         0.25,         5.0,     20.0,        5.0,        1.0,     2.0 };
                                    double[] paramMin   = {  0.0,         0.0,          0.0,     0.0,         0.0,      -20.0,   -20.0 };
                                    double[] paramMax   = { 25.0,         5.0,        300.0,   500.0,        60.0,       10.0,    50.0 };
                                    string[] paramNames = { "KickRate", "KickGain", "RampDown", "RampDecay_Tau", "SurgeMarg_LP_Tau", "KickThreshold", "HoldThreshold" };
                                    int nMoves = 0;
                                    for (int iter = 0; iter < 60; iter++)
                                    {
                                        bool improved = false;
                                        for (int p = 0; p < paramVec.Length; p++)
                                        {
                                            foreach (double dir in new[] { -1.0, +1.0 })
                                            {
                                                double trial = paramVec[p] + dir * paramSteps[p];
                                                if (trial < paramMin[p] || trial > paramMax[p]) continue;
                                                double tmp = paramVec[p];
                                                paramVec[p] = trial;
                                                if (paramVec[6] < paramVec[5]) { paramVec[p] = tmp; continue; } // hThr ≥ kThr
                                                if (paramVec[0] == 0 && paramVec[1] == 0) { paramVec[p] = tmp; continue; }
                                                if (paramVec[2] == 0 && paramVec[3] == 0) { paramVec[p] = tmp; continue; }
                                                var (f, sc, _) = Simulate(paramVec[0], paramVec[1], paramVec[2], paramVec[3], paramVec[4], paramVec[5], paramVec[6], peakPenalty);
                                                if (sc > bestComposite + 1e-6)
                                                {
                                                    bestComposite = sc;
                                                    bestKickFit   = f;
                                                    improved      = true;
                                                    nMoves++;
                                                }
                                                else
                                                {
                                                    paramVec[p] = tmp;
                                                }
                                            }
                                        }
                                        if (!improved)
                                        {
                                            bool anyAlive = false;
                                            for (int p = 0; p < paramSteps.Length; p++)
                                            {
                                                paramSteps[p] *= 0.5;
                                                if (paramSteps[p] > 1e-3) anyAlive = true;
                                            }
                                            if (!anyAlive) break;
                                        }
                                    }
                                    bestKickRate = paramVec[0];
                                    bestKickGain = paramVec[1];
                                    bestRamp     = paramVec[2];
                                    bestRampTau  = paramVec[3];
                                    bestMargLP   = paramVec[4];
                                    bestThr      = paramVec[5];
                                    bestHoldThr  = paramVec[6];
                                    Console.WriteLine($"[Model] {id}:   KickBased grid={gridFit:F2}% → after {nMoves} coord-descent moves: fit={bestKickFit:F2}%");

                                    // Final simulation with refined kick params, for the trace
                                    var (_, _, yKick) = Simulate(bestKickRate, bestKickGain, bestRamp, bestRampTau,
                                                                  bestMargLP, bestThr, bestHoldThr, peakPenalty);

                                    string surgeSrc = haveKspiceSurge ? "K-Spice NormalizedFlow vs NormalizedAsymmetricLimit"
                                                                       : "Y_true rising-velocity heuristic";
                                    Console.WriteLine($"[Model] {id}:   KickBased final    fit={bestKickFit,6:F2}%  surge_dist={surgeProxy_mf:F4}·MF+{surgeProxy_a:F4}·DP+{surgeProxy_b:F4}, kThr={bestThr:F2}, holdThr={bestHoldThr:F2}, kr={bestKickRate:F2}%/s, kg={bestKickGain:F3}/unit, ramp={bestRamp:F1}%/min, τ_decay={bestRampTau:F1}s, τ_marg={bestMargLP:F1}s");

                                    benchmark.Add(new JObject
                                    {
                                        ["Architecture"]               = "KickBased",
                                        ["SurgeProxy_MF_Coeff"]        = surgeProxy_mf,
                                        ["SurgeProxy_a"]               = surgeProxy_a,
                                        ["SurgeProxy_b"]               = surgeProxy_b,
                                        ["KickThreshold"]              = bestThr,
                                        ["HoldThreshold"]              = bestHoldThr,
                                        ["KickRate_PrcPerSec"]         = bestKickRate,
                                        ["KickGain_PrcPerSecPerUnit"]  = bestKickGain,
                                        ["RampDown_PrcPerMin"]         = bestRamp,
                                        ["RampDecay_Tau_s"]            = bestRampTau,
                                        ["SurgeMargin_LP_Tau_s"]       = bestMargLP,
                                        ["FitScore"]                   = bestKickFit,
                                        ["SurgeTruthSource"]           = surgeSrc
                                    });

                                    // Prefer KickBased over the OLS curve-fits whenever it achieves
                                    // a positive fit (> 0%). Physics-based surge detection is
                                    // essential for correct closed-loop behavior: OLS curve-fits
                                    // output non-zero even in safe conditions (they regress the
                                    // steady-state trace, not just surge events), causing false
                                    // kicks and eventual closed-loop divergence.
                                    if (bestKickFit > 0.0)
                                    {
                                        bestFit = bestKickFit;
                                        bestY   = yKick;
                                        bestParams = new JObject
                                        {
                                            ["ModelType"]                  = "AntiSurgePhysicalModel",
                                            ["Architecture"]               = "KickBased",
                                            ["SurgeProxy_MF_Coeff"]        = surgeProxy_mf,
                                            ["SurgeProxy_a"]               = surgeProxy_a,
                                            ["SurgeProxy_b"]               = surgeProxy_b,
                                            ["KickThreshold"]              = bestThr,
                                            ["HoldThreshold"]              = bestHoldThr,
                                            ["KickRate_PrcPerSec"]         = bestKickRate,
                                            ["KickGain_PrcPerSecPerUnit"]  = bestKickGain,
                                            ["RampDown_PrcPerMin"]         = bestRamp,
                                            ["RampDecay_Tau_s"]            = bestRampTau,
                                            ["SurgeMargin_LP_Tau_s"]       = bestMargLP,
                                            ["FitScore"]                   = bestKickFit,
                                            ["SurgeTruthSource"]           = surgeSrc,
                                            ["Formula"]                    = $"surge_distance = {surgeProxy_mf:F4}·MF + {surgeProxy_a:F4}·DP + {surgeProxy_b:F4}" + (bestMargLP > 0 ? $", LP-filtered (τ={bestMargLP:F1}s)" : "") + $";  KICK if <{bestThr:F2}: u += ({bestKickRate:F2} + {bestKickGain:F3}·max(0,{bestThr:F2}−surge_distance)) %/s·dt;  HOLD if [{bestThr:F2},{bestHoldThr:F2}]: u unchanged;  RAMP if >{bestHoldThr:F2}: u -= ({bestRamp:F1}/60" + (bestRampTau > 0 ? $" + u/{bestRampTau:F1}" : "") + ") %/s·dt"
                                        };
                                    }
                                }

                                if (bestY != null && bestParams != null)
                                {
                                    predictions[id] = bestY;
                                    bestParams["Benchmark"] = benchmark;
                                    identifiedParams[id]   = bestParams;
                                    Console.WriteLine($"[SUCCESS] {id}: best architecture = {bestParams["Architecture"]}, fit={bestFit:F2}%");
                                }
                                else
                                {
                                    Console.WriteLine($"[WARNING] {id}: All ASC candidates failed.");
                                    predictions[id] = (double[])Y_true.Clone();
                                    identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "All ASC candidates failed" };
                                }
                            }
                            else
                            {
                                Console.WriteLine($"[WARNING] {id}: ASC missing P_in, P_out or Flow inputs.");
                                predictions[id] = (double[])Y_true.Clone();
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[WARNING] {id}: ASC identification exception: {ex.Message}");
                            predictions[id] = (double[])Y_true.Clone();
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[WARNING] {id}: No input signals found for ASC.");
                        predictions[id] = Enumerable.Repeat(0.0, numRows).ToArray();
                    }
                }
                

                // -----------------------------------------------------
                //   C1. Separator/Tank Pressure — integrated mass balance
                //
                //   Gas pressure obeys dP/dt = k*(m_in - m_out). Three strategies
                //   are tried and the one with the highest training fit is chosen:
                //
                //   A) IdentifyLinear on per-stream integrated flows (linear gains)
                //   B) Identify (curvature) on per-stream integrated flows (captures
                //      nonlinear compressibility: pressure drops faster per kg as
                //      total gas content decreases)
                //   C) NetMassBalance: single gain on the integrated net flow
                //      (sum m_in – sum m_out). Fewer parameters, more robust when
                //      individual flow signals are noisy or correlated.
                // -----------------------------------------------------
                else if (state == "Pressure" && (
                    kspiceType.IndexOf("Separator", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    kspiceType.IndexOf("Tank",      StringComparison.OrdinalIgnoreCase) >= 0))
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    
                    // QUICK FIX: Gas pressure is dominated by gas flow. Liquid mass flows (which are huge due to high density)
                    // create massive multicollinearity and closed-loop oscillation if included in the gas pressure integrator.
                    if (id == "23VA0001_Pressure")
                    {
                        inputCols = inputCols.Where(c => !c.Item1.Contains("23PA0001_MassFlow") && !c.Item1.Contains("23LV0001_MassFlow") && !c.Item1.Contains("23LV0002_MassFlow")).ToList();
                        Console.WriteLine($"  [FILTER] Excluding liquid outflows from {id} to prevent regression oscillation.");
                    }

                    if (inputCols.Count > 0)
                    {
                        // Negate outflow signals so all identified gains are positive.
                        var signedInputs = inputCols.Select(x =>
                        {
                            bool isOutflow = x.Item1.Contains("(m_out)") || x.Item1.Contains("(m_sum)");
                            if (isOutflow)
                            {
                                double[] neg = x.Item2.Select(v => -v).ToArray();
                                return ($"{x.Item1}_neg", neg);
                            }
                            return x;
                        }).ToList();

                        // Integrate each signed flow: accum_i(t) = ∫ F_signed_i dt
                        var integratedInputs = signedInputs.Select(x =>
                        {
                            double[] integ = new double[x.Item2.Length];
                            integ[0] = 0.0;
                            for (int j = 1; j < x.Item2.Length; j++)
                                integ[j] = integ[j - 1] + x.Item2[j] * timeBase_s;
                            return ($"Int_{x.Item1}", integ);
                        }).ToList();

                        int nOut = inputCols.Count(x => x.Item1.Contains("(m_out)") || x.Item1.Contains("(m_sum)"));
                        Console.WriteLine($"[Model] {id}: Trying 3 pressure strategies, {integratedInputs.Count} inputs ({nOut} outflows negated)");

                        var strategyResults = new List<(double[] ySim, double fitScore, string name, JObject mParams)>();

                        try
                        {
                            // ── A) IdentifyLinear — linear per-stream gains ──────────────
                            {
                                var ds = BuildUnitDataSet(integratedInputs, Y_true, timeBase_s);
                                var m  = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                                if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null)
                                {
                                    double fit = m.modelParameters.Fitting.FitScorePrc;
                                    var p = new JObject
                                    {
                                        ["ModelType"]      = "IdentifyLinear_IntegratedFlow",
                                        ["FitScore"]       = fit,
                                        ["TimeConstant_s"] = m.modelParameters.TimeConstant_s,
                                        ["Formula"]        = "Pressure(t) = Bias + sum(gain_i * integral(input_i * dt))  [outflows negated]"
                                    };
                                    if (m.modelParameters.LinearGains != null)
                                    {
                                        p["LinearGains"] = JArray.FromObject(m.modelParameters.LinearGains);
                                        p["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray());
                                    }
                                    AttachUnitParams(p, m.modelParameters);
                                    Console.WriteLine($"[Model] {id}:   A) IdentifyLinear          fit={fit:F1}%");
                                    strategyResults.Add((ds.Y_sim, fit, "IdentifyLinear", p));
                                }
                            }

                            // ── B) Identify with curvature — nonlinear compressibility ───
                            {
                                var ds = BuildUnitDataSet(integratedInputs, Y_true, timeBase_s);
                                var m  = UnitIdentifier.Identify(ref ds, null, false);
                                if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null)
                                {
                                    double fit = m.modelParameters.Fitting.FitScorePrc;
                                    var p = new JObject
                                    {
                                        ["ModelType"]      = "IdentifyLinear_IntegratedFlow",
                                        ["FitScore"]       = fit,
                                        ["TimeConstant_s"] = m.modelParameters.TimeConstant_s,
                                        ["Formula"]        = "Pressure(t) = Bias + sum(gain_i * integral(input_i*dt) + curv_i*(integral-U0_i)^2/UNorm_i)  [outflows negated]"
                                    };
                                    if (m.modelParameters.LinearGains != null)
                                    {
                                        p["LinearGains"] = JArray.FromObject(m.modelParameters.LinearGains);
                                        p["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray());
                                    }
                                    AttachUnitParams(p, m.modelParameters);
                                    Console.WriteLine($"[Model] {id}:   B) Identify(curvature)     fit={fit:F1}%");
                                    strategyResults.Add((ds.Y_sim, fit, "Identify+curvature", p));
                                }
                            }

                            // ── C) NetMassBalance — single gain on net integrated flow ───
                            {
                                int N = signedInputs[0].Item2.Length;
                                double[] netFlow = new double[N];
                                for (int j = 0; j < N; j++)
                                    foreach (var s in signedInputs) netFlow[j] += s.Item2[j];
                                double[] netInteg = new double[N];
                                for (int j = 1; j < N; j++)
                                    netInteg[j] = netInteg[j - 1] + netFlow[j] * timeBase_s;
                                var netInputs = new List<(string, double[])> { ("NetFlow(m_net)", netInteg) };
                                var ds = BuildUnitDataSet(netInputs, Y_true, timeBase_s);
                                var m  = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                                if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null &&
                                    m.modelParameters.LinearGains?.Length > 0)
                                {
                                    double fit  = m.modelParameters.Fitting.FitScorePrc;
                                    double gain = m.modelParameters.LinearGains[0];
                                    var p = new JObject
                                    {
                                        ["ModelType"] = "NetMassBalance",
                                        ["FitScore"]  = fit,
                                        ["Gain"]      = gain,
                                        ["InputNames"] = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray()),
                                        ["Formula"]   = "Pressure(t) = y(0) + Gain * integral(net_flow * dt)  [net = sum m_in - sum m_out]"
                                    };
                                    AttachUnitParams(p, m.modelParameters);
                                    Console.WriteLine($"[Model] {id}:   C) NetMassBalance           fit={fit:F1}%  gain={gain:G4}");
                                    strategyResults.Add((ds.Y_sim, fit, "NetMassBalance", p));
                                }
                            }

                            // ── D) IdentifyLinear, excluding high-error (surge) indices ──
                            // Detects time steps where the pressure changes unusually fast
                            // (5× the median rate) and excludes them from the OLS so that
                            // the regression is not biased by the surge excursion.
                            {
                                int N = Y_true.Length;
                                // Compute absolute rate of pressure change
                                var rates = new double[N];
                                for (int j = 1; j < N; j++)
                                    rates[j] = Math.Abs(Y_true[j] - Y_true[j - 1]);
                                var sortedRates = rates.OrderBy(r => r).ToArray();
                                double median = sortedRates[N / 2];
                                double surgeThreshold = median * 5.0;

                                var excl = new List<int>();
                                for (int j = 0; j < N; j++)
                                    if (rates[j] > surgeThreshold) excl.Add(j);

                                if (excl.Count > 0 && excl.Count < N * 0.4)
                                {
                                    var ds = BuildUnitDataSet(integratedInputs, Y_true, timeBase_s);
                                    ds.IndicesToIgnore = excl;
                                    var m = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                                    if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null)
                                    {
                                        double fit = m.modelParameters.Fitting.FitScorePrc;
                                        var p = new JObject
                                        {
                                            ["ModelType"]      = "IdentifyLinear_IntegratedFlow",
                                            ["FitScore"]       = fit,
                                            ["TimeConstant_s"] = m.modelParameters.TimeConstant_s,
                                            ["Formula"]        = "Pressure(t) = Bias + sum(gain_i * integral(input_i * dt))  [surge indices excluded from fit]"
                                        };
                                        if (m.modelParameters.LinearGains != null)
                                        {
                                            p["LinearGains"] = JArray.FromObject(m.modelParameters.LinearGains);
                                            p["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray());
                                        }
                                        AttachUnitParams(p, m.modelParameters);
                                        Console.WriteLine($"[Model] {id}:   D) IdentifyLinear(excl {excl.Count} surge pts) fit={fit:F1}%");
                                        strategyResults.Add((ds.Y_sim, fit, "IdentifyLinear-surgeFree", p));
                                    }
                                }
                            }

                            // ── E) UnitIdentifier on net signed flow (single stable input) ─
                            // Sum all signed inputs (outflows pre-negated) into one F_net.
                            // Fit P(t) = LP(Bias + gain * F_net, Tc) on this single variable.
                            // Avoids the ±K / ∓K multicollinearity that per-stream OLS produces
                            // when two flows (e.g. compressor + bypass valve) are correlated —
                            // large opposing gains amplify prediction errors in closed-loop.
                            // No integration → no drift accumulation.  Tc naturally found by
                            // UnitIdentifier; should recover the old ~38 s time constant.
                            {
                                int N2 = signedInputs[0].Item2.Length;
                                double[] netRaw = new double[N2];
                                for (int j = 0; j < N2; j++)
                                    foreach (var s in signedInputs) netRaw[j] += s.Item2[j];
                                var netInput = new List<(string, double[])> { ("NetFlow_signed", netRaw) };
                                var ds = BuildUnitDataSet(netInput, Y_true, timeBase_s);
                                var m  = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                                if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null)
                                {
                                    double fit        = m.modelParameters.Fitting.FitScorePrc;
                                    double singleGain = m.modelParameters.LinearGains?.Length > 0
                                                        ? m.modelParameters.LinearGains[0] : 0.0;
                                    var p = new JObject
                                    {
                                        ["ModelType"]      = "UnitIdentifier_NetSignedFlow",
                                        ["FitScore"]       = fit,
                                        ["TimeConstant_s"] = m.modelParameters.TimeConstant_s,
                                        ["Formula"]        = $"Pressure(t) = LP(Bias + gain*F_net, Tc={m.modelParameters.TimeConstant_s:F1}s)  [F_net = Σm_in − Σm_out, single gain]"
                                    };
                                    p["LinearGains"] = JArray.FromObject(new[] { singleGain });
                                    p["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray());
                                    AttachUnitParams(p, m.modelParameters);
                                    Console.WriteLine($"[Model] {id}:   E) UnitIdent(net signed flow)   fit={fit:F1}%  Tc={m.modelParameters.TimeConstant_s:F1}s  gain={singleGain:G4}");
                                    strategyResults.Add((ds.Y_sim, fit, "IdentifyLinear-rawFlows", p));
                                }
                            }

                            // ── F) UnitIdentifier on raw signed flows (per-stream gains, leaky integrator)
                            // This is the classic "GreyBox" approach the user requested.
                            // Evaluator maps this to "UnitIdentifier_SignedFlows" so outflows are negated at eval,
                            // but each stream gets its own proportional weight.
                            {
                                var ds = BuildUnitDataSet(signedInputs, Y_true, timeBase_s);
                                // Identify non-integrated raw flows: dP/dt = LP( w_i*flow_i, Tc )
                                var m = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                                if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null)
                                {
                                    double fit = m.modelParameters.Fitting.FitScorePrc;
                                    var p = new JObject
                                    {
                                        ["ModelType"]      = "UnitIdentifier_SignedFlows",
                                        ["FitScore"]       = fit,
                                        ["TimeConstant_s"] = m.modelParameters.TimeConstant_s,
                                        ["Formula"]        = "Pressure(t) = LP(Bias + sum(gain_i * F_i), Tc)  [outflows pre-negated in evaluation]"
                                    };
                                    if (m.modelParameters.LinearGains != null)
                                    {
                                        p["LinearGains"] = JArray.FromObject(m.modelParameters.LinearGains);
                                        p["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray());
                                    }
                                    AttachUnitParams(p, m.modelParameters);
                                    Console.WriteLine($"[Model] {id}:   F) UnitIdent(raw signed flows) fit={fit:F1}% Tc={m.modelParameters.TimeConstant_s:F1}s");
                                    strategyResults.Add((ds.Y_sim, fit, "UnitIdent-SignedFlows", p));
                                }
                            }

                            if (strategyResults.Count == 0)
                            {
                                Console.WriteLine($"[WARNING] {id}: All pressure strategies failed.");
                                predictions[id] = (double[])Y_true.Clone();
                                identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "All pressure strategies failed" };
                                continue;
                            }

                            // The user explicitly prefers the UnitIdentifier on raw signed flows (F) for 23VA0001
                            var stratF = strategyResults.FirstOrDefault(r => r.name == "UnitIdent-SignedFlows");
                            (double[] ySim, double fitScore, string name, JObject mParams) best;
                            
                            if (id == "23VA0001_Pressure" && stratF.name != null && stratF.fitScore >= 20.0)
                            {
                                best = stratF;
                                Console.WriteLine($"[Model] {id}: Preferring Strategy F (UnitIdent-SignedFlows) fit={stratF.fitScore:F1}% as requested by user.");
                            }
                            else
                            {
                                best = strategyResults.OrderByDescending(r => r.fitScore).First();
                            }
                            Console.WriteLine($"[SUCCESS] {id}: Best={best.name}, FitScore={best.fitScore:F1}%");
                            predictions[id]     = best.ySim;
                            identifiedParams[id] = best.mParams;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[WARNING] {id}: Pressure identification exception: {ex.Message}");
                            predictions[id] = (double[])Y_true.Clone();
                            identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = ex.Message };
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[WARNING] {id}: No inputs found for separator pressure.");
                        predictions[id] = Enumerable.Repeat(0.0, numRows).ToArray();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "No input signals found in topology" };
                    }
                }

                // -----------------------------------------------------
                //   C2. Separator Level — pure integrator (mass balance)
                // -----------------------------------------------------
                else if (state.EndsWith("Level") && kspiceType.IndexOf("Separator", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);

                    // Liquid level models must only include liquid-phase flows as outflows.
                    // Compressors (and similar gas-only equipment) remove gas, not liquid;
                    // including their MassFlow as a level outflow causes multicollinearity
                    // and inverted gains in the integrator identification.
                    inputCols = inputCols.Where(x => {
                        if (!x.Item1.Contains("(m_out)") && !x.Item1.Contains("(m_sum)")) return true;
                        int paren = x.Item1.IndexOf('(');
                        string sigKey = paren > 0 ? x.Item1.Substring(0, paren) : x.Item1;
                        int lastUs = sigKey.LastIndexOf('_');
                        string srcComp = lastUs > 0 ? sigKey.Substring(0, lastUs) : sigKey;
                        string ktype2 = compToType.ContainsKey(srcComp) ? compToType[srcComp] : "";
                        bool isGas = ktype2.IndexOf("Compressor", StringComparison.OrdinalIgnoreCase) >= 0;
                        if (isGas) Console.WriteLine($"  [FILTER] Excluding gas outflow {sigKey} from liquid level {id}");
                        return !isGas;
                    }).ToList();

                    if (inputCols.Count > 0)
                    {
                        // Negate outflow data so all identified gains are positive:
                        //   dL/dt = gain_in * F_in + gain_out * (-F_out)
                        var signedInputs = inputCols.Select(x =>
                        {
                            bool isOutflow = x.Item1.Contains("(m_out)") || x.Item1.Contains("(m_sum)") || x.Item1.Contains("(mass_out");
                            if (isOutflow)
                            {
                                double[] neg = x.Item2.Select(v => -v).ToArray();
                                return ($"{x.Item1}_neg", neg);
                            }
                            return x;
                        }).ToList();

                        // Integrated-flow regression: fit Level(t) = Bias + Σ c_i × ∫F_signed_i dt
                        // Mathematically equivalent to dL/dt = Σ c_i × F_i (pure integrator),
                        // but far more numerically stable — integrating noisy flows smooths them,
                        // whereas differentiating noisy Level amplifies noise. This is the same
                        // technique used in the older greybox TankLevelIdentifier.
                        var integratedInputs = signedInputs.Select(x =>
                        {
                            double[] integ = new double[x.Item2.Length];
                            integ[0] = 0.0;
                            for (int j = 1; j < x.Item2.Length; j++)
                                integ[j] = integ[j - 1] + x.Item2[j] * timeBase_s;
                            return ($"Int_{x.Item1}", integ);
                        }).ToList();

                        int nOut = inputCols.Count(x => x.Item1.Contains("(m_out)") || x.Item1.Contains("(m_sum)") || x.Item1.Contains("(mass_out"));
                        Console.WriteLine($"[Model] {id}: IdentifyLinear on integrated flows with {integratedInputs.Count} inputs ({nOut} outflows negated)");
                        try
                        {
                            var unitDataSet = BuildUnitDataSet(integratedInputs, Y_true, timeBase_s);
                            var model = UnitIdentifier.IdentifyLinear(ref unitDataSet, null, false);

                            if (!(model.modelParameters.Fitting.WasAbleToIdentify && unitDataSet.Y_sim != null))
                            {
                                Console.WriteLine($"[WARNING] {id}: Integrated-flow regression failed.");
                                predictions[id] = (double[])Y_true.Clone();
                                identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "IdentifyLinear on integrated flows failed" };
                                continue;
                            }

                            predictions[id] = unitDataSet.Y_sim;
                            Console.WriteLine($"[SUCCESS] {id}: Integrated-flow regression. FitScore={model.modelParameters.Fitting.FitScorePrc:F1}%");

                            var mParams = new JObject
                            {
                                ["ModelType"]      = "IdentifyLinear_IntegratedFlow",
                                ["FitScore"]       = model.modelParameters.Fitting.FitScorePrc,
                                ["TimeConstant_s"] = model.modelParameters.TimeConstant_s,
                                ["Formula"]        = "Level(t) = Bias + sum(gain_i * integral(input_i * dt))  [outflows negated]"
                            };
                            if (model.modelParameters.LinearGains != null)
                            {
                                mParams["LinearGains"] = JArray.FromObject(model.modelParameters.LinearGains);
                                // Store original (un-negated, un-integrated) names so the plot matches CSV columns
                                mParams["InputNames"] = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray());
                            }
                            AttachUnitParams(mParams, model.modelParameters);
                            identifiedParams[id] = mParams;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[WARNING] {id}: Integrator exception: {ex.Message}");
                            predictions[id] = (double[])Y_true.Clone();
                            identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = ex.Message };
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[WARNING] {id}: No inputs found for separator level.");
                        predictions[id] = Enumerable.Repeat(0.0, numRows).ToArray();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "No input signals found in topology" };
                    }
                }
                
                // -----------------------------------------------------
                //   D. Custom Outlet Valve Model (MassFlow)
                // -----------------------------------------------------
                else if (state == "MassFlow" && kspiceType.IndexOf("Valve", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    Console.WriteLine($"[Model] {id}: Outlet Valve Physics Model identification");

                    string pfComp = comp.EndsWith("_pf", StringComparison.OrdinalIgnoreCase) ? comp : comp + "_pf";
                    string compBase = comp.EndsWith("_pf", StringComparison.OrdinalIgnoreCase) ? comp.Substring(0, comp.Length - 3) : comp;

                    // 1. Get Cv 
                    double cv = 100.0;
                    var valveModelInfo = models.FirstOrDefault(m => ((string)m["Name"]).Equals(compBase, StringComparison.OrdinalIgnoreCase));
                    if (valveModelInfo != null && valveModelInfo["Parameters"] != null)
                    {
                        if (valveModelInfo["Parameters"]["CvFullyOpen"] != null)
                             cv = (double)valveModelInfo["Parameters"]["CvFullyOpen"];
                        else if (valveModelInfo["Parameters"]["CvCheckValveFullyOpen"] != null)
                             cv = (double)valveModelInfo["Parameters"]["CvCheckValveFullyOpen"];
                    }

                    // 2. Map Signals
                    double[] pIn = null;
                    double[] pOut = null;
                    double[] uData = null;

                    string[] pInCands = { $"{pfComp}:InletStream.p", $"{pfComp}:InletPressure", signalMap.ContainsKey($"{compBase}_UpstreamPressure") ? signalMap[$"{compBase}_UpstreamPressure"] : null };
                    string[] pOutCands = { $"{pfComp}:OutletStream.p", $"{pfComp}:OutletPressure", signalMap.ContainsKey($"{compBase}_DownstreamPressure") ? signalMap[$"{compBase}_DownstreamPressure"] : null };
                    // Prefer the analog modulating signal; Opening is a discrete state flag in K-Spice
                    // (true only when position > RunningLightOpenPosition), not a 0-100% control input.
                    string[] uCands = { 
                        signalMap.ContainsKey($"{compBase}_ControlSignal") ? signalMap[$"{compBase}_ControlSignal"] : null,
                        $"{compBase}:LocalControlSignalIn", 
                        $"{compBase}:TargetPosition", 
                        $"{compBase}:Opening" 
                    };

                    string pInCol = null, pOutCol = null, uCol = null;

                    foreach (var cand in pInCands) {
                        if (cand != null && dataset.ContainsKey(cand)) { pIn = dataset[cand]; pInCol = cand; break; }
                    }
                    foreach (var cand in pOutCands) {
                        if (cand != null && dataset.ContainsKey(cand)) { pOut = dataset[cand]; pOutCol = cand; break; }
                    }
                    foreach (var cand in uCands) {
                        if (cand != null && dataset.ContainsKey(cand)) { uData = dataset[cand]; uCol = cand; break; }
                    }

                    if (uData == null) {
                        uData = Enumerable.Repeat(100.0, numRows).ToArray();
                        uCol = "Assumed_100%";
                    }

                    if (pIn != null && pOut != null)
                    {
                        // 3. Compute Features and Least Squares Fit DensityTuningFactor
                        int safeRows = Math.Min(numRows, Math.Min(Y_true.Length, Math.Min(pIn.Length, Math.Min(pOut.Length, uData.Length))));
                        
                        double sumYF = 0, sumFF = 0;
                        int validSamples = 0;
                        double[] feature = new double[safeRows];
                        for (int i = 0; i < safeRows; i++)
                        {
                            if (double.IsNaN(Y_true[i]) || double.IsNaN(pIn[i]) || double.IsNaN(pOut[i]) || double.IsNaN(uData[i]))
                                continue;

                            // Always treat U as a percent — see ValvePhysicsModel.Iterate for rationale.
                            double uVal = uData[i] / 100.0;
                            if (uVal < 0) uVal = 0;
                            if (uVal > 1.0) uVal = 1.0;
                            double dP = pIn[i] - pOut[i];
                            if (dP < 0) dP = 0;

                            feature[i] = cv * uVal * Math.Sqrt(dP);
                            sumYF += Y_true[i] * feature[i];
                            sumFF += feature[i] * feature[i];
                            validSamples++;
                        }

                        if (validSamples == 0)
                        {
                            Console.WriteLine($"[WARNING] {id}: No valid samples for regression (all NaN). Falling back.");
                            predictions[id] = (double[])Y_true.Clone();
                            identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "All-NaN inputs for valve regression" };
                            continue;
                        }

                        // Fit scaling factor K
                        double tuningFactor = sumFF > 1e-9 ? sumYF / sumFF : 1.0;

                        // 4. Simulate the result
                        var valveModel = new KSpiceEngine.CustomModels.ValvePhysicsModel(id, new string[]{ pInCol, pOutCol, uCol }, id);
                        valveModel.modelParameters.Cv = cv;
                        valveModel.modelParameters.DensityTuningFactor = tuningFactor;

                        double[] Y_pred = new double[numRows];
                        double sumSqErr = 0;
                        double sumTotSq = 0;
                        // Mean of valid (non-NaN) Y_true samples — robust to ingest gaps.
                        double sumTrue = 0; int meanCount = 0;
                        for (int i = 0; i < Y_true.Length; i++)
                        {
                            if (!double.IsNaN(Y_true[i])) { sumTrue += Y_true[i]; meanCount++; }
                        }
                        double meanTrue = meanCount > 0 ? sumTrue / meanCount : 0;

                        int scoredSamples = 0;
                        for (int i = 0; i < safeRows; i++)
                        {
                            if (double.IsNaN(pIn[i]) || double.IsNaN(pOut[i]) || double.IsNaN(uData[i]))
                            {
                                Y_pred[i] = double.NaN;
                                continue;
                            }
                            Y_pred[i] = valveModel.Iterate(new double[]{ pIn[i], pOut[i], uData[i] }, timeBase_s)[0];
                            if (double.IsNaN(Y_true[i])) continue;
                            double res = Y_true[i] - Y_pred[i];
                            sumSqErr += res * res;
                            scoredSamples++;
                        }
                        for (int i = 0; i < Y_true.Length; i++)
                        {
                            if (double.IsNaN(Y_true[i])) continue;
                            double dm = Y_true[i] - meanTrue;
                            sumTotSq += dm * dm;
                        }
    
                        double fitScore = sumTotSq > 0 ? Math.Max(-100, 100.0 * (1.0 - sumSqErr / sumTotSq)) : 0;
                        
                        predictions[id] = Y_pred;
                        Console.WriteLine($"[SUCCESS] {id}: ValvePhysicsModel identified! FitScore={fitScore:F1}% (Cv={cv}, TuningFactor={tuningFactor:F6})");
                        
                        // The plotter receives the topology-resolved P_in (upstream component
                        // pressure) and U(t) (controller output) automatically via topology edges,
                        // so InputNames only carries the local pipe outlet pressure — the one
                        // signal the topology cannot supply (no downstream-component edge).
                        var modelParams = new JObject
                        {
                            ["ModelType"]      = "ValvePhysicsModel",
                            ["FitScore"]       = fitScore,
                            ["Cv"]             = cv,
                            ["DensityTuningFactor"] = tuningFactor,
                            ["Formula"]        = $"Q = {tuningFactor:F4} * {cv} * U * sqrt(max(0, P_in - P_out))",
                            ["InputNames"]     = new JArray { pOutCol },
                            ["LinearGains"]    = new JArray { -1.0 },
                            ["SimInputs"]      = new JArray { pInCol, pOutCol, uCol }
                        };

                        identifiedParams[id] = modelParams;
                    }
                    else
                    {
                        Console.WriteLine($"[WARNING] {id}: Missing Pressure data. P_in={pInCol ?? "none"}, P_out={pOutCol ?? "none"}. Falling back to default.");
                        predictions[id] = (double[])Y_true.Clone();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "Missing Pressure data for custom valve model" };
                    }
                }
                
                // -----------------------------------------------------
                //   E. Separator Temperature: data-driven linear fit
                // -----------------------------------------------------
                else if (state == "Temperature" && kspiceType.IndexOf("Separator", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    Console.WriteLine($"[Model] {id}: IdentifyLinear (separator temperature, {inputCols.Count} inputs)");

                    if (inputCols.Count > 0)
                    {
                        try
                        {
                            var unitDataSet = BuildUnitDataSet(inputCols, Y_true, timeBase_s);
                            var model = UnitIdentifier.IdentifyLinear(ref unitDataSet, null, false);
                            if (model.modelParameters.Fitting.WasAbleToIdentify && unitDataSet.Y_sim != null)
                            {
                                predictions[id] = unitDataSet.Y_sim;
                                double fitScore = model.modelParameters.Fitting.FitScorePrc;
                                Console.WriteLine($"[SUCCESS] {id}: FitScore={fitScore:F1}%");
                                var dParams = new JObject
                                {
                                    ["ModelType"]      = "IdentifyLinear",
                                    ["FitScore"]       = fitScore,
                                    ["TimeConstant_s"] = model.modelParameters.TimeConstant_s,
                                    ["Formula"]        = "First-order linear MISO with time constant"
                                };
                                if (model.modelParameters.LinearGains != null)
                                {
                                    dParams["LinearGains"] = JArray.FromObject(model.modelParameters.LinearGains);
                                    dParams["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.Item1).ToArray());
                                }
                                AttachUnitParams(dParams, model.modelParameters);
                                identifiedParams[id] = dParams;
                            }
                            else
                            {
                                Console.WriteLine($"[WARNING] {id}: IdentifyLinear could not fit.");
                                predictions[id] = (double[])Y_true.Clone();
                                identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "IdentifyLinear could not fit" };
                            }
                        }
                        catch (Exception ex)
                        {
                            predictions[id] = (double[])Y_true.Clone();
                            identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = ex.Message };
                        }
                    }
                    else
                    {
                        predictions[id] = (double[])Y_true.Clone();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "No input signals in topology" };
                    }
                }

                // -----------------------------------------------------
                //   F. All other models: Use TSA UnitIdentifier
                // -----------------------------------------------------
                else
                {
                    // Find all input signals from the topology edges
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    var idInputs = inputCols;

                    if (idInputs.Count == 0)
                    {
                        Console.WriteLine($"[WARNING] {id}: No input signals found in topology. Copying true signal.");
                        predictions[id] = (double[])Y_true.Clone();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "No input signals in topology" };
                        continue;
                    }
                    
                    Console.WriteLine($"[Model] {id}: UnitIdentifier with {idInputs.Count} inputs [{string.Join(", ", idInputs.Select(x => x.Item1))}]");
                    
                    try
                    {
                        var unitDataSet = BuildUnitDataSet(idInputs, Y_true, timeBase_s);
                        
                        // Use dynamic identification (with time constant) for flow/pressure, static for temperature
                        UnitModel model;
                        if (state == "Temperature")
                        {
                            model = UnitIdentifier.IdentifyLinear(ref unitDataSet, null, false);
                        }
                        else
                        {
                            model = UnitIdentifier.Identify(ref unitDataSet, null, false);
                        }
                        
                        if (model.modelParameters.Fitting.WasAbleToIdentify && unitDataSet.Y_sim != null)
                        {
                            predictions[id] = unitDataSet.Y_sim;
                            double fitScore = model.modelParameters.Fitting.FitScorePrc;
                            Console.WriteLine($"[SUCCESS] {id}: Identified. FitScore={fitScore:F1}%, Tc={model.modelParameters.TimeConstant_s:F2}s");

                            var modelParams = new JObject
                            {
                                ["ModelType"]      = "UnitIdentifier",
                                ["FitScore"]       = fitScore,
                                ["TimeConstant_s"] = model.modelParameters.TimeConstant_s,
                                ["Formula"]        = $"Dynamic MISO: {idInputs.Count} inputs, Tc={model.modelParameters.TimeConstant_s:F2}s"
                            };
                            if (model.modelParameters.LinearGains != null)
                            {
                                modelParams["LinearGains"] = JArray.FromObject(model.modelParameters.LinearGains);
                                modelParams["InputNames"]  = JArray.FromObject(idInputs.Select(x => x.Item1).ToArray());
                            }
                            AttachUnitParams(modelParams, model.modelParameters);
                            identifiedParams[id] = modelParams;
                        }
                        else
                        {
                            Console.WriteLine($"[WARNING] {id}: Identification failed, copying true signal.");
                            predictions[id] = (double[])Y_true.Clone();
                            identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "UnitIdentifier could not identify" };
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[WARNING] {id}: Identification exception: {ex.Message}");
                        predictions[id] = (double[])Y_true.Clone();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = ex.Message };
                    }
                }
            }
            
            // 7. Write Validation Dataset (True + Predicted)
            WriteValidationCsv(dataset, predictions, signalMap, outputCsvPath);
            Console.WriteLine($"\n[SUCCESS] Validation Data Written to {outputCsvPath}");
            
            // 8. Write Identified Parameters JSON
            string paramsOutPath = Path.Combine(Path.GetDirectoryName(outputCsvPath), "CS_Identified_Parameters.json");
            File.WriteAllText(paramsOutPath, JsonConvert.SerializeObject(identifiedParams, Formatting.Indented));
            Console.WriteLine($"[SUCCESS] Identified Parameters Written to {paramsOutPath}");
        }
        
        /// <summary>
        /// Save the bias / U0 / UNorm / curvature numbers from a TSA UnitParameters
        /// alongside the LinearGains we already serialise. These are needed when the
        /// model has to be re-applied to a different dataset (Tests 1 & 2) — gains
        /// alone don't carry the operating-point offset.
        /// </summary>
        private static void AttachUnitParams(JObject target, TimeSeriesAnalysis.Dynamic.UnitParameters p)
        {
            if (p == null) return;
            target["Bias"] = p.Bias;
            if (p.U0 != null)         target["U0"]         = JArray.FromObject(p.U0);
            if (p.UNorm != null)      target["UNorm"]      = JArray.FromObject(p.UNorm);
            if (p.Curvatures != null) target["Curvatures"] = JArray.FromObject(p.Curvatures);
        }

        /// <summary>
        /// Solve A·x = b for x by Gaussian elimination with partial pivoting.
        /// Returns null if A is singular. Used by the ASC linear-proxy fit.
        /// </summary>
        private static double[] SolveLinearSystem(double[,] A, double[] b, int n)
        {
            double[,] m = new double[n, n];
            double[]  r = new double[n];
            for (int i = 0; i < n; i++) { r[i] = b[i]; for (int j = 0; j < n; j++) m[i, j] = A[i, j]; }

            for (int i = 0; i < n; i++)
            {
                // Pivot
                int maxRow = i;
                double maxVal = Math.Abs(m[i, i]);
                for (int k = i + 1; k < n; k++)
                {
                    double v = Math.Abs(m[k, i]);
                    if (v > maxVal) { maxVal = v; maxRow = k; }
                }
                if (maxVal < 1e-15) return null;

                if (maxRow != i)
                {
                    for (int k = 0; k < n; k++) { double t = m[i, k]; m[i, k] = m[maxRow, k]; m[maxRow, k] = t; }
                    double tb = r[i]; r[i] = r[maxRow]; r[maxRow] = tb;
                }

                // Eliminate
                for (int k = i + 1; k < n; k++)
                {
                    double factor = m[k, i] / m[i, i];
                    for (int j = i; j < n; j++) m[k, j] -= factor * m[i, j];
                    r[k] -= factor * r[i];
                }
            }

            // Back-substitute
            double[] x = new double[n];
            for (int i = n - 1; i >= 0; i--)
            {
                double s = r[i];
                for (int j = i + 1; j < n; j++) s -= m[i, j] * x[j];
                x[i] = s / m[i, i];
            }
            return x;
        }

        /// <summary>
        /// Find all input CSV columns for a given topology node by looking at edges pointing TO this node,
        /// then resolving each source node's signal mapping.
        /// When a source has no direct CSV signal, performs a directional BFS through physical KSpice
        /// adjacency to find the nearest series-connected component with the same state that IS mapped.
        /// </summary>
        internal static List<(string name, double[] data)> FindInputSignals(
            string nodeId,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            Dictionary<string, string> signalMap,
            Dictionary<string, double[]> dataset,
            Dictionary<string, HashSet<string>> physicalNeighbors)
        {
            var result = new List<(string, double[])>();
            var usedCols = new HashSet<string>();

            if (!inputEdges.ContainsKey(nodeId))
                return result;

            // Extract parent component (e.g. "23VA0001" from "23VA0001_Pressure")
            // so proxy BFS won't walk back upstream into the node we're wiring.
            int parentSplit = nodeId.LastIndexOf('_');
            string parentComp = parentSplit > 0 ? nodeId.Substring(0, parentSplit) : null;

            foreach (var (fromNode, label) in inputEdges[nodeId])
            {
                string resolvedKey = null;

                // 1. Direct lookup in signal map
                if (signalMap.ContainsKey(fromNode))
                {
                    resolvedKey = fromNode;
                }
                else
                {
                    int li = fromNode.LastIndexOf('_');
                    if (li > 0)
                    {
                        string fromComp  = fromNode.Substring(0, li);
                        string fromState = fromNode.Substring(li + 1);
                        string altKey    = $"{fromComp}_{fromState}";

                        // 2. Rebuilt key (handles minor formatting differences)
                        if (signalMap.ContainsKey(altKey))
                        {
                            resolvedKey = altKey;
                        }
                        else
                        {
                            // 3. Proxy: BFS downstream through physical KSpice wiring,
                            //    excluding the upstream parent to enforce directionality.
                            string proxyKey = FindProxySignal(fromComp, fromState, parentComp,
                                                              physicalNeighbors, signalMap, dataset);
                            if (proxyKey != null)
                            {
                                Console.WriteLine($"  [PROXY] {fromNode} has no CSV signal — using {proxyKey} as proxy (series path)");
                                resolvedKey = proxyKey;
                            }
                            else
                            {
                                Console.WriteLine($"  [SKIP]  {fromNode} has no CSV signal and no proxy found");
                            }
                        }
                    }
                }

                if (resolvedKey == null) continue;

                string csvCol = signalMap[resolvedKey];
                if (dataset.ContainsKey(csvCol) && !usedCols.Contains(csvCol))
                {
                    result.Add(($"{resolvedKey}({label})", dataset[csvCol]));
                    usedCols.Add(csvCol);
                }
            }

            return result;
        }

        /// <summary>
        /// BFS through physical KSpice adjacency starting from <paramref name="missingComp"/>,
        /// excluding <paramref name="parentComp"/> to stay directional (downstream only).
        /// Returns the signal map key of the nearest neighbour that has state <paramref name="stateSuffix"/>
        /// and a CSV column, or null if none found within <paramref name="maxHops"/> hops.
        /// </summary>
        private static string FindProxySignal(
            string missingComp,
            string stateSuffix,
            string parentComp,
            Dictionary<string, HashSet<string>> physicalNeighbors,
            Dictionary<string, string> signalMap,
            Dictionary<string, double[]> dataset,
            int maxHops = 8)
        {
            if (!physicalNeighbors.ContainsKey(missingComp)) return null;

            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { missingComp };
            if (parentComp != null) visited.Add(parentComp); // block upstream direction

            var queue = new Queue<(string comp, int depth)>();
            queue.Enqueue((missingComp, 0));

            while (queue.Count > 0)
            {
                var (comp, depth) = queue.Dequeue();
                if (depth >= maxHops) continue;

                if (!physicalNeighbors.ContainsKey(comp)) continue;
                foreach (string neighbor in physicalNeighbors[comp])
                {
                    if (visited.Contains(neighbor)) continue;
                    visited.Add(neighbor);

                    string candidateKey = $"{neighbor}_{stateSuffix}";
                    if (signalMap.ContainsKey(candidateKey) && dataset.ContainsKey(signalMap[candidateKey]))
                        return candidateKey;

                    queue.Enqueue((neighbor, depth + 1));
                }
            }
            return null;
        }

        /// <summary>
        /// Builds a bidirectional physical adjacency map from K-Spice stream connections.
        /// Each node is a component name (normalised: _pf and _m suffixes stripped).
        /// Only stream ports (Destination contains "Stream") are considered — control
        /// signal wires are ignored.
        /// </summary>
        internal static Dictionary<string, HashSet<string>> BuildPhysicalAdjacency(JArray models)
        {
            var adj = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            string Normalize(string name)
            {
                if (name.EndsWith("_pf", StringComparison.OrdinalIgnoreCase))
                    return name.Substring(0, name.Length - 3);
                if (name.EndsWith("_m", StringComparison.OrdinalIgnoreCase))
                    return name.Substring(0, name.Length - 2);
                return name;
            }

            foreach (var model in models)
            {
                string rawName  = (string)model["Name"] ?? "";
                string compName = Normalize(rawName);

                var inputs = (JArray)model["Inputs"];
                if (inputs == null || inputs.Count == 0) continue;

                foreach (var inp in inputs)
                {
                    string dst = (string)inp["Destination"] ?? "";
                    if (dst.IndexOf("Stream", StringComparison.OrdinalIgnoreCase) < 0) continue;

                    string src = (string)inp["Source"] ?? "";
                    int colonIdx = src.IndexOf(':');
                    if (colonIdx <= 0) continue;

                    string srcComp = Normalize(src.Substring(0, colonIdx));
                    if (string.Equals(compName, srcComp, StringComparison.OrdinalIgnoreCase)) continue;

                    if (!adj.ContainsKey(compName)) adj[compName] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    if (!adj.ContainsKey(srcComp))  adj[srcComp]  = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    adj[compName].Add(srcComp);
                    adj[srcComp].Add(compName);
                }
            }

            return adj;
        }
        
        /// <summary>
        /// Build a UnitDataSet for TSA UnitIdentifier from the list of input columns and the output.
        /// </summary>
        private static UnitDataSet BuildUnitDataSet(List<(string name, double[] data)> inputCols, double[] Y_true, double timeBase_s)
        {
            int N = Y_true.Length;
            var unitDataSet = new UnitDataSet($"Model");
            unitDataSet.Y_meas = (double[])Y_true.Clone();
            unitDataSet.CreateTimeStamps(timeBase_s);
            
            // Build U matrix: each input is a column
            double[,] U = new double[N, inputCols.Count];
            for (int col = 0; col < inputCols.Count; col++)
            {
                var data = inputCols[col].data;
                int len = Math.Min(N, data.Length);
                for (int row = 0; row < len; row++)
                {
                    U[row, col] = data[row];
                }
            }
            unitDataSet.U = U;
            
            return unitDataSet;
        }

        internal static Dictionary<string, double[]> LoadCsvDataset(string path)
        {
            var data = new Dictionary<string, List<double>>();
            var lines = File.ReadAllLines(path).Where(l => !string.IsNullOrWhiteSpace(l)).ToArray();
            var headers = lines[0].Split(',').Select(h => h.Trim()).ToList();

            foreach (var h in headers) data[h] = new List<double>();

            for (int i = 1; i < lines.Length; i++)
            {
                var vals = lines[i].Split(',');
                for (int j = 0; j < headers.Count; j++)
                {
                    double v = double.NaN;
                    if (j < vals.Length)
                    {
                        var cell = vals[j].Trim();
                        if (cell.Equals("true", StringComparison.OrdinalIgnoreCase)) v = 1.0;
                        else if (cell.Equals("false", StringComparison.OrdinalIgnoreCase)) v = 0.0;
                        else double.TryParse(cell, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out v);
                    }
                    // Always append so all columns stay aligned (length == row count).
                    data[headers[j]].Add(v);
                }
            }

            return data.ToDictionary(kv => kv.Key, kv => kv.Value.ToArray());
        }

        internal static void WriteValidationCsv(Dictionary<string, double[]> dataset, Dictionary<string, double[]> predictions, Dictionary<string, string> signalMap, string path)
        {
            var outputLines = new List<string>();
            int rows = dataset.First().Value.Length;
            
            var newHeaders = new List<string>();
            var trueVectors = new List<double[]>();
            var predVectors = new List<double[]>();
            
            foreach (var kvp in predictions)
            {
                string id = kvp.Key;
                string trueHeader = signalMap.ContainsKey(id) ? signalMap[id] : null;
                
                if (trueHeader != null && dataset.ContainsKey(trueHeader))
                {
                    newHeaders.Add($"{id}_True");
                    newHeaders.Add($"{id}_Predicted");
                    
                    trueVectors.Add(dataset[trueHeader]);
                    predVectors.Add(kvp.Value);
                }
            }
            
            outputLines.Add("Time," + string.Join(",", newHeaders));
            
            for (int r = 0; r < rows; r++)
            {
                var rowData = new List<string> { (r * 0.5).ToString("F2", System.Globalization.CultureInfo.InvariantCulture) };
                for (int c = 0; c < trueVectors.Count; c++)
                {
                    double trueVal = r < trueVectors[c].Length ? trueVectors[c][r] : 0;
                    double predVal = r < predVectors[c].Length ? predVectors[c][r] : 0;
                    rowData.Add(trueVal.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    rowData.Add(predVal.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
                outputLines.Add(string.Join(",", rowData));
            }
            
            File.WriteAllLines(path, outputLines);
        }

    }
}
