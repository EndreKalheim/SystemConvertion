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
        public static void RunPlantSimulations(
            string csvPath, string systemMapPath, string equationsPath,
            string mappingPath, string outputCsvPath, string? selectedKspiceModelPath = null)
        {
            Console.WriteLine("\n[DynamicPlantRunner] Starting Phase 6: Training and Validation Pipeline...");

            var systemMap = JObject.Parse(File.ReadAllText(systemMapPath));
            var models    = (JArray)systemMap["Models"]!;

            var compToType = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var model in models)
            {
                string name      = (string)model["Name"]! ?? "";
                string rawName   = name.Replace("_pf", "", StringComparison.OrdinalIgnoreCase);
                bool   isPipeSfx = name.EndsWith("_pf", StringComparison.OrdinalIgnoreCase);
                if (isPipeSfx && compToType.ContainsKey(rawName)) continue;
                compToType[rawName] = (string)model["KSpiceType"]! ?? "";
            }

            var signalMap  = JsonConvert.DeserializeObject<Dictionary<string, string>>(File.ReadAllText(mappingPath))
                             ?? new Dictionary<string, string>();
            var equations  = JsonConvert.DeserializeObject<List<dynamic>>(File.ReadAllText(equationsPath))
                             ?? new List<dynamic>();

            string topologyPath = Path.Combine(Path.GetDirectoryName(equationsPath)!, "TSA_Explicit_Topology.json");
            var topoEdges = (JArray)JObject.Parse(File.ReadAllText(topologyPath))["edges"]!;

            var inputEdges = new Dictionary<string, List<(string fromNode, string label)>>();
            foreach (var edge in topoEdges)
            {
                string to = (string)edge["to"]! ?? "", from = (string)edge["from"]! ?? "", label = (string)edge["label"]! ?? "";
                if (!inputEdges.ContainsKey(to)) inputEdges[to] = new List<(string, string)>();
                inputEdges[to].Add((from, label));
            }

            var physicalNeighbors = BuildPhysicalAdjacency(models);
            var dataset           = LoadCsvDataset(csvPath);
            int numRows           = dataset.Values.First().Length;
            double timeBase_s     = DetectTimeStep(dataset);

            var predictions      = new Dictionary<string, double[]>();
            var identifiedParams = new Dictionary<string, JObject>();
            var allEquationIds   = new HashSet<string>(equations.Select(eq => (string)eq.ID));

            foreach (var eq in equations)
            {
                string id             = (string)eq.ID;
                string comp           = (string)eq.Component;
                string state          = (string)eq.State;
                string role           = (string)eq.Role;
                string controllerType = (string)eq.ControllerType ?? "";
                // Boundary / input-only states are the ones KSpiceModelFactory emits with no
                // topology inputs (unrecognised component types and FlowElements). The factory
                // no longer writes a "Formula" string, so detect them via an empty Inputs list
                // rather than a formula substring match.
                int inputCount        = (((JObject)eq)["Inputs"] as JArray)?.Count ?? 0;
                string kspiceType     = compToType.ContainsKey(comp) ? compToType[comp] : "Unknown";

                string mapKey = $"{comp}_{state}";
                string? targetCsvHeader = signalMap.ContainsKey(mapKey) ? signalMap[mapKey] : null;
                bool hasCsvTruth = targetCsvHeader != null && dataset.ContainsKey(targetCsvHeader);

                if (!hasCsvTruth)
                {
                    // Truth column absent (either no signalMap entry, or column not in CSV).
                    // For MassFlow equations: try synthesizing Y_true from m_in/m_out
                    // topology edges. This handles components like PV_0001 whose CSV
                    // columns are missing in partial-coverage datasets (LP-only training).
                    if (state == "MassFlow")
                    {
                        // Use direct topology resolution (no proxy) to avoid physically
                        // spurious signals (e.g. HX1001 proxied to ESV0005) corrupting
                        // the synthesized mass balance. Only include edges whose fromNode
                        // maps to an actual CSV column in the dataset.
                        var mInCols  = new List<(string name, double[] data)>();
                        var mOutCols = new List<(string name, double[] data)>();
                        if (inputEdges.ContainsKey(id))
                        {
                            var usedCols = new HashSet<string>();
                            foreach (var (fromNode, label) in inputEdges[id])
                            {
                                if (label != "m_in" && label != "m_out") continue;
                                string? resolvedKey = signalMap.ContainsKey(fromNode) ? fromNode : null;
                                if (resolvedKey == null)
                                {
                                    int li = fromNode.LastIndexOf('_');
                                    if (li > 0)
                                    {
                                        string altKey = $"{fromNode.Substring(0, li)}_{fromNode.Substring(li + 1)}";
                                        if (signalMap.ContainsKey(altKey)) resolvedKey = altKey;
                                    }
                                }
                                if (resolvedKey == null) continue;
                                string csvCol = signalMap[resolvedKey];
                                if (!dataset.ContainsKey(csvCol) || usedCols.Contains(csvCol)) continue;
                                usedCols.Add(csvCol);
                                var entry = ($"{resolvedKey}({label})", dataset[csvCol]);
                                if (label == "m_in")  mInCols .Add(entry);
                                else                  mOutCols.Add(entry);
                            }
                        }
                        if (mInCols.Count > 0)
                        {
                            Console.WriteLine($"[Model] {id}: No CSV truth — synthesizing Y_true from {mInCols.Count} m_in, {mOutCols.Count} m_out topology edges.");
                            double[] Y_synth = new double[numRows];
                            for (int si = 0; si < numRows; si++)
                            {
                                double mIn  = mInCols .Sum(c => si < c.data.Length && !double.IsNaN(c.data[si]) ? c.data[si] : 0.0);
                                double mOut = mOutCols.Sum(c => si < c.data.Length && !double.IsNaN(c.data[si]) ? c.data[si] : 0.0);
                                Y_synth[si] = mIn - mOut;
                            }
                            var synthResult = IdentifyMassBalanceValve(id, Y_synth, numRows, mInCols, mOutCols);
                            predictions[id]      = synthResult.pred;
                            identifiedParams[id] = synthResult.pars;
                        }
                        else
                        {
                            Console.WriteLine($"[WARNING] {id}: No CSV truth and no m_in topology edges — skipping.");
                        }
                    }
                    else if (targetCsvHeader == null)
                    {
                        Console.WriteLine($"[WARNING] No actual mapping found for {id}. Skipping...");
                    }
                    else
                    {
                        Console.WriteLine($"[WARNING] {id}: No CSV data for state={state} — skipping.");
                    }
                    continue;
                }
                double[] Y_true = dataset[targetCsvHeader!];

                if (inputCount == 0)
                {
                    Console.WriteLine($"[Model] {id}: Boundary node (input-only), copying true signal.");
                    predictions[id]      = (double[])Y_true.Clone();
                    identifiedParams[id] = new JObject { ["ModelType"] = "Boundary" };
                    continue;
                }

                (double[] pred, JObject pars) result;

                if (role == "Controller" && controllerType == "PID")
                {
                    result = IdentifyPidModel(id, comp, Y_true, numRows, timeBase_s, signalMap, dataset, (JObject)eq);
                }
                else if (role == "Controller" && controllerType == "ASC")
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    if (inputCols.Count < 2)
                    {
                        Console.WriteLine($"[WARNING] {id}: No input signals found for ASC.");
                        result = (Enumerable.Repeat(0.0, numRows).ToArray(), Fallback("No input signals found for ASC"));
                    }
                    else
                    {
                        result = AscIdentifier.Identify(id, comp, Y_true, timeBase_s, inputCols, predictions, models, selectedKspiceModelPath ?? "", dataset);
                    }
                }
                else if (state == "Pressure" && IsContainerType(kspiceType))
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    inputCols = FilterLiquidOutflows(inputCols, compToType, id);
                    if (inputCols.Count == 0)
                    {
                        Console.WriteLine($"[WARNING] {id}: No inputs found for separator pressure.");
                        result = (Enumerable.Repeat(0.0, numRows).ToArray(), Fallback("No input signals found in topology"));
                    }
                    else
                    {
                        result = SeparatorPressureIdentifier.Identify(id, Y_true, inputCols, timeBase_s);
                    }
                }
                else if (state == "Pressure"
                    && (kspiceType.IndexOf("ControlValve", StringComparison.OrdinalIgnoreCase) >= 0
                        || kspiceType.IndexOf("PipeFlow",    StringComparison.OrdinalIgnoreCase) >= 0
                        || kspiceType.IndexOf("BlockValve",  StringComparison.OrdinalIgnoreCase) >= 0))
                {
                    // Inter-stage pipe junction: HP suction header modeled as a mass-balance vessel.
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    Console.WriteLine($"[Model] {id}: Inter-stage junction vessel ({inputCols.Count} flow inputs)");
                    result = inputCols.Count == 0
                        ? (Enumerable.Repeat(0.0, numRows).ToArray(), Fallback("No flow inputs for inter-stage junction"))
                        : SeparatorPressureIdentifier.Identify(id, Y_true, inputCols, timeBase_s, isInterstageJunction: true);
                }
                else if (state.EndsWith("Level") && IsContainerType(kspiceType))
                {
                    result = IdentifySeparatorLevel(id, Y_true, numRows, timeBase_s,
                                                    inputEdges, signalMap, dataset, physicalNeighbors, compToType);
                }
                else if (state == "MassFlow" && kspiceType.IndexOf("Valve", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    result = IdentifyValveModel(id, comp, Y_true, numRows, timeBase_s, models, signalMap, dataset, inputCols, physicalNeighbors);
                }
                else if (state == "Temperature" && kspiceType.IndexOf("HeatExchanger", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);
                    Console.WriteLine($"[Model] {id}: HeatExchangerIdentifier (HX component, {inputCols.Count} inputs)");
                    result = HeatExchangerIdentifier.Identify(id, Y_true, inputCols, timeBase_s);
                }
                else
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);

                    result = IdentifyGeneral(id, state, Y_true, numRows, inputCols, timeBase_s);

                    // For all compressors: if OLS gives a negative P_in gain it is a spurious
                    // correlation (physical: higher inlet P → more flow → positive gain).
                    // A negative P_in gain creates positive feedback with the upstream separator
                    // NetMassBalance. Re-identify without P_in to prevent instability.
                    if (state == "MassFlow"
                        && kspiceType.IndexOf("Compressor", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        var inames = result.pars["InputNames"] as JArray;
                        var gains  = result.pars["LinearGains"] as JArray;
                        int pinIdx = -1;
                        for (int k = 0; k < (inames?.Count ?? 0); k++)
                            if (((string)inames![k]! ?? "").EndsWith("(P_in)")) { pinIdx = k; break; }
                        if (pinIdx >= 0 && gains != null && (double)gains[pinIdx]! < 0)
                        {
                            inputCols = inputCols.Where(c => !c.name.EndsWith("(P_in)")).ToList();
                            Console.WriteLine($"[Model] {id}: P_in gain < 0 (non-physical OLS artifact) → excluded; re-identifying");
                            result = IdentifyGeneral(id, state, Y_true, numRows, inputCols, timeBase_s);
                        }
                    }

                }

                predictions[id]      = result.pred;
                identifiedParams[id] = result.pars;
            }

            WriteValidationCsv(dataset, predictions, signalMap, outputCsvPath, timeBase_s);
            Console.WriteLine($"\n[SUCCESS] Validation Data Written to {outputCsvPath}");

            string paramsOutPath = Path.Combine(Path.GetDirectoryName(outputCsvPath)!, "CS_Identified_Parameters.json");
            // (Path.GetDirectoryName above already null-asserted)
            File.WriteAllText(paramsOutPath, JsonConvert.SerializeObject(identifiedParams, Formatting.Indented));
            Console.WriteLine($"[SUCCESS] Identified Parameters Written to {paramsOutPath}");
        }

        // ── Model identification branches ────────────────────────────────────

        private static (double[] pred, JObject pars) IdentifyPidModel(
            string id, string comp, double[] Y_true, int numRows, double timeBase_s,
            Dictionary<string, string> signalMap, Dictionary<string, double[]> dataset,
            JObject? eqDesc = null)
        {
            string measMapKey = $"{comp}_Measurement";
            string spMapKey   = $"{comp}_Setpoint";
            if (!signalMap.ContainsKey(measMapKey) || !signalMap.ContainsKey(spMapKey))
            {
                Console.WriteLine($"[WARNING] {id}: No Measurement/Setpoint mapping for controller {comp}.");
                return (Enumerable.Repeat(0.0, numRows).ToArray(), Fallback("No Measurement/Setpoint signal mapping"));
            }

            string measSignal = signalMap[measMapKey];
            string spSignal   = signalMap[spMapKey];
            if (!dataset.ContainsKey(measSignal))
            {
                Console.WriteLine($"[WARNING] {id}: Measurement signal not in CSV.");
                return (Enumerable.Repeat(0.0, numRows).ToArray(), Fallback("Measurement not in CSV"));
            }

            double[] Y_meas = dataset[measSignal];
            bool spFromCsv  = dataset.ContainsKey(spSignal);
            double[] Y_sp;
            double   spMean = double.NaN;
            if (spFromCsv)
            {
                Y_sp = dataset[spSignal];
            }
            else
            {
                // Setpoint not in CSV (e.g. PIC2001, SIC2001/3001 in single-train-CSV datasets).
                // Use mean(measurement) as a constant setpoint so K-Spice params can be identified
                // and stored, and so CL evaluation has a valid fallback setpoint to regulate toward.
                var validMeas = Y_meas.Where(v => !double.IsNaN(v)).ToArray();
                spMean = validMeas.Length > 0 ? validMeas.Average() : 0.0;
                Y_sp   = Enumerable.Repeat(spMean, numRows).ToArray();
                Console.WriteLine($"[Model] {id}: Setpoint not in CSV — using mean(measurement)={spMean:F4} as constant SP fallback.");
            }
            try
            {
                // PidController.Iterate clips its output to [0, 100]. When the
                // controller output signal is in physical units (e.g. RPM ~750-984
                // for a speed cascade), identification and simulation both fail
                // because every sample is immediately pegged to 0 or 100.
                // Normalise Y_true to [0, 100] before identification; denormalise
                // the simulated prediction before returning so callers and fit
                // scores work in the original engineering units.
                double yMin = Y_true.Min(), yMax = Y_true.Max();
                bool needsNorm = yMin < -1e-6 || yMax > 100 + 1e-6;
                // If K-Spice output range is available and wider than 100, prefer
                // it over the data range — more stable when data excitation is low.
                if (eqDesc?["KSpice_OutRangeLow"] != null && eqDesc["KSpice_OutRangeHigh"] != null)
                {
                    double olo = (double)eqDesc["KSpice_OutRangeLow"]!;
                    double ohi = (double)eqDesc["KSpice_OutRangeHigh"]!;
                    double slack = Math.Max(1.0, (ohi - olo) * 0.1);
                    bool dataFitsKSpice = Y_true.Min() >= olo - slack && Y_true.Max() <= ohi + slack;
                    if (ohi - olo > 100 + 1e-6 && dataFitsKSpice)
                    { needsNorm = true; yMin = olo; yMax = ohi; }
                }
                double yRange = Math.Abs(yMax - yMin);
                double[] Y_sim = (needsNorm && yRange > 1e-9)
                    ? Y_true.Select(v => (v - yMin) / yRange * 100.0).ToArray()
                    : Y_true;
                if (needsNorm)
                    Console.WriteLine($"[Model] {id}: output range [{yMin:F1}, {yMax:F1}] — normalising to [0,100] for PID sim");

                var pidData = new UnitDataSet();
                pidData.Y_meas     = Y_meas;
                pidData.Y_setpoint = Y_sp;
                pidData.U = new double[numRows, 1];
                for (int i = 0; i < numRows; i++) pidData.U[i, 0] = Y_sim[i];
                pidData.CreateTimeStamps(timeBase_s);

                var pidParam = new PidIdentifier().Identify(ref pidData);
                double Kp = pidParam.Kp, Ti = pidParam.Ti_s, Td = pidParam.Td_s;

                double[] SimulateWith(double kpUse, double tiUse, double tdUse)
                {
                    double[] u = new double[numRows];
                    u[0] = Y_sim[0];
                    double uprev = Y_sim[0];
                    double eprev = Y_sp[0] - Y_meas[0];
                    double dtOverTi = tiUse > 1e-9 ? timeBase_s / tiUse : 0.0;
                    for (int i = 1; i < numRows; i++)
                    {
                        double e = Y_sp[i] - Y_meas[i];
                        double du = kpUse * ((e - eprev) + dtOverTi * e);
                        uprev = Math.Max(0, Math.Min(100, uprev + du));
                        u[i] = uprev;
                        eprev = e;
                    }
                    if (needsNorm && yRange > 1e-9)
                        for (int i = 0; i < u.Length; i++) u[i] = u[i] / 100.0 * yRange + yMin;
                    return u;
                }

                double FitPrc(double[] pred)
                {
                    double mean = Y_true.Average(), sse = 0, tss = 0;
                    for (int i = 0; i < numRows; i++)
                    {
                        double r = Y_true[i] - pred[i]; sse += r * r;
                        double m = Y_true[i] - mean;    tss += m * m;
                    }
                    return tss > 0 ? Math.Max(-100, 100 * (1 - sse / tss)) : 0;
                }

                double[] predPos = SimulateWith(Kp, Ti, Td), predNeg = SimulateWith(-Kp, Ti, Td);
                double fitPos = FitPrc(predPos), fitNeg = FitPrc(predNeg);

                double[] Y_pred;
                double fitScore;
                string source = "data-identified";
                if (fitNeg > fitPos)
                {
                    Kp = -Kp; Y_pred = predNeg; fitScore = fitNeg;
                    Console.WriteLine($"[Model] {id}: PID identified Kp={Kp:F4}, Ti={Ti:F2}s, Td={Td:F2}s (sign flipped — fit improved {fitPos:F1}%→{fitNeg:F1}%)");
                }
                else
                {
                    Y_pred = predPos; fitScore = fitPos;
                    Console.WriteLine($"[Model] {id}: PID identified Kp={Kp:F4}, Ti={Ti:F2}s, Td={Td:F2}s");
                }

                // Try K-Spice parameters as a fallback when identification is poor.
                // K-Spice normalises error by MeasRange; convert to TSA per-unit:
                //   Kp_tsa = Gain * 100 / MeasRange  (output is [0,100] normalised)
                if (eqDesc?["KSpice_Gain"] != null && eqDesc["KSpice_MeasRange"] != null)
                {
                    double kGain  = (double)eqDesc["KSpice_Gain"]!;
                    double kRange = (double)eqDesc["KSpice_MeasRange"]!;
                    double kTi    = eqDesc["KSpice_Ti_s"] != null ? (double)eqDesc["KSpice_Ti_s"]! : Ti;
                    double kTd    = eqDesc["KSpice_Td_s"] != null ? (double)eqDesc["KSpice_Td_s"]! : 0.0;
                    double kKp    = kGain * 100.0 / kRange;
                    // Detect unit mismatch: K-Spice can store MeasRange in SI base units
                    // (Pa) while the CSV uses derived units (bar). If the declared range
                    // is >1000× the actual data range, scale by 1e-5 (Pa → bar).
                    double measDataRange = Y_meas.Max() - Y_meas.Min();
                    if (measDataRange > 1e-6 && kRange > measDataRange * 1000)
                        kKp = kGain * 100.0 / (kRange / 1e5);
                    double[] ksPos = SimulateWith(kKp,  kTi, kTd);
                    double[] ksNeg = SimulateWith(-kKp, kTi, kTd);
                    double fitKsPos = FitPrc(ksPos), fitKsNeg = FitPrc(ksNeg);
                    double bestKsFit = fitKsPos >= fitKsNeg ? fitKsPos : fitKsNeg;
                    double bestKsKp  = fitKsPos >= fitKsNeg ? kKp : -kKp;
                    double[] bestKsPred = fitKsPos >= fitKsNeg ? ksPos : ksNeg;
                    if (bestKsFit > fitScore)
                    {
                        Kp = bestKsKp; Ti = kTi; Td = kTd;
                        Y_pred = bestKsPred; fitScore = bestKsFit;
                        source = "K-Spice";
                        Console.WriteLine($"[Model] {id}: K-Spice params better — Kp={Kp:G4}, Ti={Ti:F2}s (fit {fitScore:F1}%)");
                    }
                }

                // Gain-scale search: PidIdentifier can underestimate Kp when long flat
                // periods dominate its objective. Try scaled-up gains with the anti-windup
                // simulation to recover the true proportional response.
                if (fitScore < 99.0)
                {
                    double baseKp = Kp;
                    foreach (double scale in new[] { 1.5, 2.0, 3.0, 4.0, 5.0, 7.0, 10.0, 15.0 })
                    {
                        foreach (double sign in new[] { 1.0, -1.0 })
                        {
                            double scaledKp = sign * Math.Abs(baseKp) * scale;
                            double[] cand = SimulateWith(scaledKp, Ti, Td);
                            double cf = FitPrc(cand);
                            if (cf > fitScore)
                            {
                                Kp = scaledKp; Y_pred = cand; fitScore = cf;
                                source = $"data-identified (x{scale:G3})";
                            }
                        }
                    }
                    if (source.Contains("x"))
                        Console.WriteLine($"[Model] {id}: gain scale improved fit → Kp={Kp:G4}, Ti={Ti:F2}s (fit {fitScore:F1}%)");
                }

                Console.WriteLine($"[SUCCESS] {id}: PID FitScore={fitScore:F1}% ({source})");

                var pidPars = new JObject
                {
                    ["ModelType"]   = "PID",
                    ["Kp"]          = Kp,
                    ["Ti_s"]        = Ti,
                    ["Td_s"]        = Td,
                    ["FitScore"]    = fitScore,
                    ["ParamSource"] = source,
                    ["YMin"]        = needsNorm ? (double?)yMin : null,
                    ["YMax"]        = needsNorm ? (double?)yMax : null,
                    ["Formula"]     = "Incremental PI (TSA PidController): u(k) = u(k-1) + Kp*((e(k)-e(k-1)) + dt/Ti * e(k)),  e = SP - PV"
                };
                if (!spFromCsv && !double.IsNaN(spMean))
                    pidPars["SetpointMean"] = spMean;
                return (Y_pred, pidPars);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARNING] {id}: PidIdentifier failed: {ex.Message}");
                return ((double[])Y_true.Clone(), Fallback(ex.Message));
            }
        }

        private static (double[] pred, JObject pars) IdentifySeparatorLevel(
            string id, double[] Y_true, int numRows, double timeBase_s,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            Dictionary<string, string> signalMap,
            Dictionary<string, double[]> dataset,
            Dictionary<string, HashSet<string>> physicalNeighbors,
            Dictionary<string, string> compToType)
        {
            var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset, physicalNeighbors);

            // Liquid level: exclude gas-phase (compressor) outflows — they move gas, not liquid
            inputCols = inputCols.Where(x => {
                if (!x.name.Contains("(m_out)") && !x.name.Contains("(m_sum)")) return true;
                int paren = x.name.IndexOf('(');
                string sigKey  = paren > 0 ? x.name.Substring(0, paren) : x.name;
                int lastUs     = sigKey.LastIndexOf('_');
                string srcComp = lastUs > 0 ? sigKey.Substring(0, lastUs) : sigKey;
                string ktype   = compToType.ContainsKey(srcComp) ? compToType[srcComp] : "";
                bool isGas     = ktype.IndexOf("Compressor", StringComparison.OrdinalIgnoreCase) >= 0;
                if (isGas) Console.WriteLine($"  [FILTER] Excluding gas outflow {sigKey} from liquid level {id}");
                return !isGas;
            }).ToList();

            if (inputCols.Count == 0)
            {
                Console.WriteLine($"[WARNING] {id}: No inputs for level model after filtering.");
                return ((double[])Y_true.Clone(), Fallback("No inputs for level model after filtering"));
            }

            var signedFlows    = NegateOutflows(inputCols);
            var integratedCols = IntegrateFlows(signedFlows, timeBase_s);

            try
            {
                var unitDataSet = BuildUnitDataSet(integratedCols, Y_true, timeBase_s);
                var model = UnitIdentifier.IdentifyLinear(ref unitDataSet, null, false);
                if (!model.modelParameters.Fitting.WasAbleToIdentify || unitDataSet.Y_sim == null)
                {
                    Console.WriteLine($"[WARNING] {id}: IdentifyLinear_IntegratedFlow could not fit.");
                    return ((double[])Y_true.Clone(), Fallback("IdentifyLinear_IntegratedFlow could not fit"));
                }
                double fitScore = model.modelParameters.Fitting.FitScorePrc;
                Console.WriteLine($"[SUCCESS] {id}: Level IdentifyLinear_IntegratedFlow FitScore={fitScore:F1}%");
                var dParams = new JObject
                {
                    ["ModelType"]  = "IdentifyLinear_IntegratedFlow",
                    ["FitScore"]   = fitScore,
                    ["InputNames"] = JArray.FromObject(inputCols.Select(x => x.name).ToArray()),
                    ["Formula"]    = "Level(t) ~ y(0) + sum_i( gain_i * integral(signed_flow_i * dt) )"
                };
                if (model.modelParameters.LinearGains != null)
                    dParams["LinearGains"] = JArray.FromObject(model.modelParameters.LinearGains);
                AttachUnitParams(dParams, model.modelParameters);
                return (unitDataSet.Y_sim, dParams);
            }
            catch (Exception ex)
            {
                return ((double[])Y_true.Clone(), Fallback(ex.Message));
            }
        }

        private static (double[] pred, JObject pars) IdentifyValveModel(
            string id, string comp, double[] Y_true, int numRows, double timeBase_s,
            JArray models, Dictionary<string, string> signalMap, Dictionary<string, double[]> dataset,
            List<(string name, double[] data)> inputCols,
            Dictionary<string, HashSet<string>>? physicalNeighbors = null)
        {
            Console.WriteLine($"[Model] {id}: Outlet Valve Physics Model identification");
            string compBase = comp.EndsWith("_pf", StringComparison.OrdinalIgnoreCase)
                              ? comp.Substring(0, comp.Length - 3) : comp;
            string pfComp   = compBase + "_pf";

            double cv = 100.0;
            var valveInfo = models.FirstOrDefault(m => ((string)m["Name"]! ?? "").Equals(compBase, StringComparison.OrdinalIgnoreCase));
            if (valveInfo?["Parameters"] != null)
            {
                if (valveInfo["Parameters"]!["CvFullyOpen"] != null)
                    cv = (double)valveInfo["Parameters"]!["CvFullyOpen"]!;
                else if (valveInfo["Parameters"]!["CvCheckValveFullyOpen"] != null)
                    cv = (double)valveInfo["Parameters"]!["CvCheckValveFullyOpen"]!;
            }

            double[]? pIn = null, pOut = null, uData = null;
            string? pInCol = null, pOutCol = null, uCol = null;

            foreach (var col in inputCols)
            {
                if (col.name.EndsWith("(P_in)"))  { pIn = col.data; pInCol = col.name; }
                if (col.name.EndsWith("(P_out)")) { pOut = col.data; pOutCol = col.name; }
                if (col.name.EndsWith("(U(t))") || col.name.EndsWith("(local_var)")) { uData = col.data; uCol = col.name; }
            }

            if (pIn == null)
            {
                string?[] pInCands = { $"{pfComp}:InletStream.p", $"{pfComp}:InletPressure", signalMap.ContainsKey($"{compBase}_UpstreamPressure") ? signalMap[$"{compBase}_UpstreamPressure"] : null };
                foreach (var cand in pInCands) { if (cand != null && dataset.ContainsKey(cand)) { pIn = dataset[cand]; pInCol = cand; break; } }
            }

            if (pOut == null)
            {
                string?[] pOutCands = { $"{pfComp}:OutletStream.p", $"{pfComp}:OutletPressure", signalMap.ContainsKey($"{compBase}_DownstreamPressure") ? signalMap[$"{compBase}_DownstreamPressure"] : null };
                foreach (var cand in pOutCands) { if (cand != null && dataset.ContainsKey(cand)) { pOut = dataset[cand]; pOutCol = cand; break; } }
            }

            // If pOut was resolved to a raw K-Spice stream name (contains ":"), it won't exist
            // as an identified model in CL → NaN → flat flow. Replace it with the downstream
            // neighbour's identified Pressure model ID using the physical adjacency graph.
            // Example (TutorialHN): "23UV0001_pf:OutletStream.p" → "23VA0001_Pressure"
            // Triggers only when the topology builder failed to create a P_out edge (e.g. the
            // UV outlet connects directly to a separator, not via a compressor inlet).
            // Rev4 UV valves already have a topology P_out edge (equation ID, no ":") → no change.
            if (pOutCol != null && pOutCol.Contains(":") && physicalNeighbors != null)
            {
                string? pInBase = pInCol?.Split('(')[0].Trim();  // strip "(P_in)" label if present
                if (physicalNeighbors.TryGetValue(compBase, out var neighbors))
                {
                    foreach (string nb in neighbors)
                    {
                        string candidateId = $"{nb}_Pressure";
                        if (candidateId.Contains(":")) continue;
                        if (string.Equals(candidateId, pInBase, StringComparison.OrdinalIgnoreCase)) continue;
                        // Equation IDs are never raw CSV column names — resolve via signalMap.
                        // Previous attempt checked dataset.ContainsKey(candidateId) which always
                        // fails because dataset keys are raw K-Spice stream names, not equation IDs.
                        string mappedCol = signalMap.TryGetValue(candidateId, out var c) ? c : candidateId;
                        if (!dataset.ContainsKey(mappedCol)) continue;
                        Console.WriteLine($"[ValveIdentify] {id}: Replacing raw K-Spice P_out '{pOutCol}' → equation ID '{candidateId}' (CSV: {mappedCol})");
                        pOut    = dataset[mappedCol];
                        pOutCol = candidateId;   // store equation ID so CL resolves via predictions
                        break;
                    }
                }
            }

            if (uData == null) { uData = Enumerable.Repeat(100.0, numRows).ToArray(); uCol = "Assumed_100%"; }

            // Mass balance mode: topology gave m_in/m_out flow edges instead of P_out.
            // This happens for discharge-side internal valves (e.g. inter-stage ESV) whose
            // downstream pressure BFS overshot to the next compressor's discharge pressure.
            var mInCols  = inputCols.Where(c => c.name.EndsWith("(m_in)")).ToList();
            var mOutCols = inputCols.Where(c => c.name.EndsWith("(m_out)")).ToList();
            if (pOut == null && mInCols.Count > 0)
                return IdentifyMassBalanceValve(id, Y_true, numRows, mInCols, mOutCols);

            if (pIn == null || pOut == null)
            {
                Console.WriteLine($"[WARNING] {id}: Missing Pressure data. P_in={pInCol ?? "none"}, P_out={pOutCol ?? "none"}. Falling back.");
                return ((double[])Y_true.Clone(), Fallback("Missing Pressure data for custom valve model"));
            }

            int safeRows = Math.Min(numRows, Math.Min(Y_true.Length, Math.Min(pIn.Length, Math.Min(pOut.Length, uData.Length))));
            double sumYF = 0, sumFF = 0;
            int validSamples = 0;
            for (int i = 0; i < safeRows; i++)
            {
                if (double.IsNaN(Y_true[i]) || double.IsNaN(pIn[i]) || double.IsNaN(pOut[i]) || double.IsNaN(uData[i])) continue;
                double uVal = Math.Max(0, Math.Min(1.0, uData[i] / 100.0));
                double dP   = Math.Max(0, pIn[i] - pOut[i]);
                double f    = cv * uVal * Math.Sqrt(dP);
                sumYF += Y_true[i] * f; sumFF += f * f; validSamples++;
            }
            if (validSamples == 0)
            {
                Console.WriteLine($"[WARNING] {id}: No valid samples for valve regression (all NaN). Falling back.");
                return ((double[])Y_true.Clone(), Fallback("All-NaN inputs for valve regression"));
            }

            double tuningFactor = sumFF > 1e-9 ? sumYF / sumFF : 1.0;
            var valveModel = new CustomModels.ValvePhysicsModel(id, new[] { pInCol ?? "", pOutCol ?? "", uCol ?? "" }, id);
            valveModel.modelParameters.Cv                 = cv;
            valveModel.modelParameters.DensityTuningFactor = tuningFactor;

            double[] Y_pred = new double[numRows];
            double sumSqErr = 0, sumTotSq = 0;
            double sumTrue = 0; int meanCount = 0;
            for (int i = 0; i < Y_true.Length; i++) { if (!double.IsNaN(Y_true[i])) { sumTrue += Y_true[i]; meanCount++; } }
            double meanTrue = meanCount > 0 ? sumTrue / meanCount : 0;

            for (int i = 0; i < safeRows; i++)
            {
                if (double.IsNaN(pIn[i]) || double.IsNaN(pOut[i]) || double.IsNaN(uData[i])) { Y_pred[i] = double.NaN; continue; }
                Y_pred[i] = valveModel.Iterate(new[] { pIn[i], pOut[i], uData[i] }, timeBase_s)[0];
                if (!double.IsNaN(Y_true[i])) { double res = Y_true[i] - Y_pred[i]; sumSqErr += res * res; }
            }
            for (int i = 0; i < Y_true.Length; i++) { if (!double.IsNaN(Y_true[i])) { double dm = Y_true[i] - meanTrue; sumTotSq += dm * dm; } }
            double fitScore = sumTotSq > 0 ? Math.Max(-100, 100.0 * (1.0 - sumSqErr / sumTotSq)) : 0;

            Console.WriteLine($"[SUCCESS] {id}: ValvePhysicsModel identified! FitScore={fitScore:F1}% (Cv={cv}, TuningFactor={tuningFactor:F6})");
            return (Y_pred, new JObject
            {
                ["ModelType"]          = "ValvePhysicsModel",
                ["FitScore"]           = fitScore,
                ["Cv"]                 = cv,
                ["DensityTuningFactor"] = tuningFactor,
                ["Formula"]            = $"Q = {tuningFactor:F4} * {cv} * U * sqrt(max(0, P_in - P_out))",
                ["InputNames"]         = new JArray { pOutCol },
                ["LinearGains"]        = new JArray { -1.0 },
                ["SimInputs"]          = new JArray { pInCol, pOutCol, uCol }
            });
        }

        private static (double[] pred, JObject pars) IdentifyMassBalanceValve(
            string id, double[] Y_true, int numRows,
            List<(string name, double[] data)> mInCols,
            List<(string name, double[] data)> mOutCols)
        {
            Console.WriteLine($"[Model] {id}: MassBalance valve identification (F = Gain*(m_in - sum(m_out)))");

            // OLS: Gain = Σ(Y_true · X) / Σ(X²)  where X = sum(m_in) - sum(m_out)
            double sumYX = 0.0, sumXX = 0.0;
            int validSamples = 0;
            int safeRows = Math.Min(numRows, Y_true.Length);

            for (int i = 0; i < safeRows; i++)
            {
                if (double.IsNaN(Y_true[i])) continue;
                double mIn  = mInCols .Sum(c => i < c.data.Length && !double.IsNaN(c.data[i]) ? c.data[i] : 0.0);
                double mOut = mOutCols.Sum(c => i < c.data.Length && !double.IsNaN(c.data[i]) ? c.data[i] : 0.0);
                double x = mIn - mOut;
                if (double.IsNaN(x)) continue;
                sumYX += Y_true[i] * x;
                sumXX += x * x;
                validSamples++;
            }

            if (validSamples == 0)
            {
                Console.WriteLine($"[WARNING] {id}: MassBalance — no valid samples. Falling back.");
                return ((double[])Y_true.Clone(), Fallback("MassBalance: all-NaN inputs"));
            }

            double gain = sumXX > 1e-9 ? sumYX / sumXX : 1.0;

            // Compute predictions and fit score
            double[] Y_pred = new double[safeRows];
            double sumSqErr = 0.0, sumTotSq = 0.0;
            double meanTrue = 0.0; int meanCnt = 0;
            for (int i = 0; i < safeRows; i++) { if (!double.IsNaN(Y_true[i])) { meanTrue += Y_true[i]; meanCnt++; } }
            if (meanCnt > 0) meanTrue /= meanCnt;

            for (int i = 0; i < safeRows; i++)
            {
                double mIn  = mInCols .Sum(c => i < c.data.Length && !double.IsNaN(c.data[i]) ? c.data[i] : 0.0);
                double mOut = mOutCols.Sum(c => i < c.data.Length && !double.IsNaN(c.data[i]) ? c.data[i] : 0.0);
                Y_pred[i] = gain * (mIn - mOut);
                if (!double.IsNaN(Y_true[i]))
                {
                    double res = Y_true[i] - Y_pred[i]; sumSqErr += res * res;
                    double dm  = Y_true[i] - meanTrue;  sumTotSq += dm  * dm;
                }
            }
            double fitScore = sumTotSq > 0 ? Math.Max(-100.0, 100.0 * (1.0 - sumSqErr / sumTotSq)) : 0.0;
            Console.WriteLine($"[SUCCESS] {id}: MassBalance identified. FitScore={fitScore:F1}% (Gain={gain:F4}, samples={validSamples})");

            // Build InputNames list: "key(m_in)" and "key(m_out)" — the evaluator uses
            // UnitIdentifier_SignedFlows which negates m_out inputs automatically,
            // so the same positive gain applies to all entries.
            var inputNames = new JArray();
            var linearGains = new JArray();
            foreach (var col in mInCols)  { inputNames.Add(col.name); linearGains.Add(gain); }
            foreach (var col in mOutCols) { inputNames.Add(col.name); linearGains.Add(gain); }

            return (Y_pred, new JObject
            {
                ["ModelType"]    = "UnitIdentifier_SignedFlows",
                ["FitScore"]     = fitScore,
                ["Formula"]      = $"F = {gain:F4} * (sum(m_in) - sum(m_out))",
                ["InputNames"]   = inputNames,
                ["LinearGains"]  = linearGains,
                ["Bias"]         = 0.0
            });
        }

        private static (double[] pred, JObject pars) IdentifyLinearModel(
            string id, double[] Y_true,
            List<(string name, double[] data)> inputCols, double timeBase_s)
        {
            if (inputCols.Count == 0)
                return ((double[])Y_true.Clone(), Fallback("No input signals in topology"));
            try
            {
                var unitDataSet = BuildUnitDataSet(inputCols, Y_true, timeBase_s);
                var model = UnitIdentifier.IdentifyLinear(ref unitDataSet, null, false);
                if (!model.modelParameters.Fitting.WasAbleToIdentify || unitDataSet.Y_sim == null)
                {
                    Console.WriteLine($"[WARNING] {id}: IdentifyLinear could not fit.");
                    return ((double[])Y_true.Clone(), Fallback("IdentifyLinear could not fit"));
                }
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
                    dParams["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.name).ToArray());
                }
                AttachUnitParams(dParams, model.modelParameters);
                return (unitDataSet.Y_sim, dParams);
            }
            catch (Exception ex)
            {
                return ((double[])Y_true.Clone(), Fallback(ex.Message));
            }
        }

        private static (double[] pred, JObject pars) IdentifyGeneral(
            string id, string state, double[] Y_true, int numRows,
            List<(string name, double[] data)> inputCols, double timeBase_s)
        {
            if (inputCols.Count == 0)
            {
                Console.WriteLine($"[WARNING] {id}: No input signals found in topology. Copying true signal.");
                return ((double[])Y_true.Clone(), Fallback("No input signals in topology"));
            }
            Console.WriteLine($"[Model] {id}: UnitIdentifier with {inputCols.Count} inputs [{string.Join(", ", inputCols.Select(x => x.name))}]");
            try
            {
                var unitDataSet = BuildUnitDataSet(inputCols, Y_true, timeBase_s);
                UnitModel model = state == "Temperature"
                    ? UnitIdentifier.IdentifyLinear(ref unitDataSet, null, false)
                    : UnitIdentifier.Identify(ref unitDataSet, null, false);

                if (!model.modelParameters.Fitting.WasAbleToIdentify || unitDataSet.Y_sim == null)
                {
                    Console.WriteLine($"[WARNING] {id}: Identification failed, copying true signal.");
                    return ((double[])Y_true.Clone(), Fallback("UnitIdentifier could not identify"));
                }

                double fitScore = model.modelParameters.Fitting.FitScorePrc;
                // Speed models feed back into their own controller's measurement port.
                // A zero-TC (static gain) speed model creates an algebraic loop that
                // oscillates in closed-loop simulation. Clamp to min 2×dt so the PID
                // integral action sees a properly lagged speed signal.
                double tc = model.modelParameters.TimeConstant_s;
                if (state == "Speed" && tc < 2.0 * timeBase_s)
                {
                    tc = 2.0 * timeBase_s;
                    Console.WriteLine($"[Model] {id}: Speed TC enforced to {tc:F2}s (min 2×dt for CL stability)");
                }
                Console.WriteLine($"[SUCCESS] {id}: Identified. FitScore={fitScore:F1}%, Tc={tc:F2}s");
                var modelParams = new JObject
                {
                    ["ModelType"]      = "UnitIdentifier",
                    ["FitScore"]       = fitScore,
                    ["TimeConstant_s"] = tc,
                    ["Formula"]        = $"Dynamic MISO: {inputCols.Count} inputs, Tc={tc:F2}s"
                };
                if (model.modelParameters.LinearGains != null)
                {
                    modelParams["LinearGains"] = JArray.FromObject(model.modelParameters.LinearGains);
                    modelParams["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.name).ToArray());
                }
                AttachUnitParams(modelParams, model.modelParameters);
                return (unitDataSet.Y_sim, modelParams);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARNING] {id}: Identification exception: {ex.Message}");
                return ((double[])Y_true.Clone(), Fallback(ex.Message));
            }
        }

        private static (double[] pred, JObject pars) IdentifyCompressionRatioPressure(
            string id, double[] Y_true,
            List<(string name, double[] data)> inputCols)
        {
            // Physics-based model: P_out = CR × P_in, where CR = mean(P_out) / mean(P_in).
            // This ensures P_out/P_in = CR = constant in CL, so ASC never triggers spuriously
            // from separator-pressure drift → UV stays near zero → PV_0001 stable.
            //
            // UnitIdentifier evaluates: y = Bias + gain × (u - U0)
            // With Bias=meanY, gain=CR, U0=meanPin:
            //   y = meanY + CR × (P_in - meanPin) = CR × P_in  [since meanY = CR×meanPin → offset=0]
            //
            // Side effect: P_out = CR × P_in → ΔP for PV_1002 (Cv=3000) becomes
            // CR × PV_0001 − export_P = mean(KA_outlet) − export_P ≈ 0.12 bar at training mean,
            // fixing the −100% CL fit caused by BoundaryAnchorPressure setting ΔP = 0.
            double meanY = Y_true.Where(v => !double.IsNaN(v) && v > 0).DefaultIfEmpty(0).Average();

            // Find the P_in input from topology-derived inputCols (edge labelled "(P_in)")
            var pinCol = inputCols.FirstOrDefault(c => c.name.EndsWith("(P_in)"));
            if (pinCol.data == null)
            {
                Console.WriteLine($"[WARNING] {id}: No P_in topology edge found for CompressionRatioPressure — using constant.");
                double[] constPred = meanY > 0
                    ? Y_true.Select(_ => meanY).ToArray()
                    : (double[])Y_true.Clone();
                double constFit = OpenLoopTestRunner.ComputeFit(Y_true, constPred);
                return (constPred, new JObject
                {
                    ["ModelType"] = "ConstantUMin",
                    ["FitScore"]  = constFit,
                    ["Value"]     = meanY,
                    ["Formula"]   = $"P_out = {meanY:F3} bar  [constant; no P_in edge found]"
                });
            }

            double meanPin = pinCol.data.Where(v => !double.IsNaN(v) && v > 0).DefaultIfEmpty(0).Average();
            if (meanPin <= 0 || meanY <= 0)
            {
                Console.WriteLine($"[WARNING] {id}: Invalid mean values (P_in={meanPin:F2}, P_out={meanY:F2}) — using constant.");
                double[] constPred = meanY > 0
                    ? Y_true.Select(_ => meanY).ToArray()
                    : (double[])Y_true.Clone();
                double constFit = OpenLoopTestRunner.ComputeFit(Y_true, constPred);
                return (constPred, new JObject
                {
                    ["ModelType"] = "ConstantUMin",
                    ["FitScore"]  = constFit,
                    ["Value"]     = meanY,
                    ["Formula"]   = $"P_out = {meanY:F3} bar  [constant; invalid mean pressures]"
                });
            }

            double CR     = meanY / meanPin;
            double[] pred = pinCol.data.Select(v => double.IsNaN(v) ? double.NaN : CR * v).ToArray();
            double fit    = OpenLoopTestRunner.ComputeFit(Y_true, pred);

            // Extract the source signal ID: strip the "(P_in)" suffix from the name
            string pinId = pinCol.name.Substring(0, pinCol.name.LastIndexOf('('));

            Console.WriteLine($"[Model] {id}: CompressionRatioPressure  P_in={pinId}  CR={CR:F4}  mean(P_in)={meanPin:F2} bar  OL fit={fit:F1}%");

            var p = new JObject
            {
                ["ModelType"]      = "UnitIdentifier",
                ["FitScore"]       = fit,
                ["LinearGains"]    = JArray.FromObject(new double[] { CR }),
                ["Bias"]           = meanY,
                ["U0"]             = JArray.FromObject(new double[] { meanPin }),
                ["TimeConstant_s"] = 0.0,
                ["TimeDelay_s"]    = 0.0,
                ["InputNames"]     = JArray.FromObject(new string[] { $"{pinId}(P_in_compression_ratio)" }),
                ["Formula"]        = $"P_out = {CR:F4} × P_in  [compression ratio; P_in = {pinId}]"
            };
            return (pred, p);
        }

        // ── Routing helpers ──────────────────────────────────────────────────

        private static bool IsContainerType(string kspiceType) =>
            kspiceType.IndexOf("Separator", StringComparison.OrdinalIgnoreCase) >= 0 ||
            kspiceType.IndexOf("Tank",      StringComparison.OrdinalIgnoreCase) >= 0;

        /// <summary>
        /// For separator gas pressure, exclude liquid-phase outflows (from Pump-type components).
        /// Liquid mass flows are large due to high density and cause multicollinearity in the
        /// gas pressure integrator — mirrors the Compressor exclusion done for liquid level.
        /// </summary>
        private static List<(string name, double[] data)> FilterLiquidOutflows(
            List<(string name, double[] data)> inputCols,
            Dictionary<string, string> compToType,
            string nodeId)
        {
            // Topology already correctly identifies which m_out signals belong to the gas
            // pressure model — no additional type-based filtering needed here.
            return inputCols;
        }

        private static JObject Fallback(string reason) =>
            new JObject { ["ModelType"] = "Fallback", ["Reason"] = reason };

        internal static double DetectTimeStep(Dictionary<string, double[]> dataset, double fallback = 0.5)
        {
            foreach (var key in dataset.Keys)
            {
                if (!key.Equals("Time",      StringComparison.OrdinalIgnoreCase) &&
                    !key.Equals("t",         StringComparison.OrdinalIgnoreCase) &&
                    !key.Equals("ModelTime", StringComparison.OrdinalIgnoreCase)) continue;
                var col = dataset[key];
                if (col.Length >= 2) return col[1] - col[0];
            }
            return fallback;
        }

        // ── Shared data helpers (internal: used by AscIdentifier and SeparatorPressureIdentifier) ──

        internal static List<(string name, double[] data)> NegateOutflows(List<(string name, double[] data)> inputCols)
        {
            return inputCols.Select(x => {
                bool isOut = x.name.Contains("(m_out)") || x.name.Contains("(m_sum)") || x.name.Contains("(mass_out");
                return isOut ? ($"{x.name}_neg", x.data.Select(v => -v).ToArray()) : x;
            }).ToList();
        }

        internal static List<(string name, double[] data)> IntegrateFlows(
            List<(string name, double[] data)> signedInputs, double timeBase_s)
        {
            return signedInputs.Select(x => {
                double[] integ = new double[x.data.Length];
                for (int j = 1; j < x.data.Length; j++)
                    integ[j] = integ[j - 1] + x.data[j] * timeBase_s;
                return ($"Int_{x.name}", integ);
            }).ToList();
        }

        internal static void AttachUnitParams(JObject target, TimeSeriesAnalysis.Dynamic.UnitParameters p)
        {
            if (p == null) return;
            target["Bias"] = p.Bias;
            if (p.U0        != null) target["U0"]         = JArray.FromObject(p.U0);
            if (p.UNorm     != null) target["UNorm"]      = JArray.FromObject(p.UNorm);
            if (p.Curvatures != null) target["Curvatures"] = JArray.FromObject(p.Curvatures);
        }

        internal static double[]? SolveLinearSystem(double[,] A, double[] b, int n)
        {
            double[,] m = new double[n, n];
            double[]  r = new double[n];
            for (int i = 0; i < n; i++) { r[i] = b[i]; for (int j = 0; j < n; j++) m[i, j] = A[i, j]; }

            for (int i = 0; i < n; i++)
            {
                int maxRow = i; double maxVal = Math.Abs(m[i, i]);
                for (int k = i + 1; k < n; k++) { double v = Math.Abs(m[k, i]); if (v > maxVal) { maxVal = v; maxRow = k; } }
                if (maxVal < 1e-15) return null;
                if (maxRow != i)
                {
                    for (int k = 0; k < n; k++) { double t = m[i, k]; m[i, k] = m[maxRow, k]; m[maxRow, k] = t; }
                    double tb = r[i]; r[i] = r[maxRow]; r[maxRow] = tb;
                }
                for (int k = i + 1; k < n; k++)
                {
                    double factor = m[k, i] / m[i, i];
                    for (int j = i; j < n; j++) m[k, j] -= factor * m[i, j];
                    r[k] -= factor * r[i];
                }
            }
            double[] x = new double[n];
            for (int i = n - 1; i >= 0; i--)
            {
                double s = r[i];
                for (int j = i + 1; j < n; j++) s -= m[i, j] * x[j];
                x[i] = s / m[i, i];
            }
            return x;
        }

        internal static UnitDataSet BuildUnitDataSet(
            List<(string name, double[] data)> inputCols, double[] Y_true, double timeBase_s)
        {
            int N = Y_true.Length;
            var ds = new UnitDataSet("Model");
            ds.Y_meas = (double[])Y_true.Clone();
            ds.CreateTimeStamps(timeBase_s);
            double[,] U = new double[N, inputCols.Count];
            for (int col = 0; col < inputCols.Count; col++)
            {
                int len = Math.Min(N, inputCols[col].data.Length);
                for (int row = 0; row < len; row++) U[row, col] = inputCols[col].data[row];
            }
            ds.U = U;
            return ds;
        }

        // ── Topology helpers ─────────────────────────────────────────────────

        /// <summary>
        /// Find all input CSV columns for a topology node via edge wiring, with BFS
        /// proxy fallback for components that have no direct CSV signal.
        /// </summary>
        internal static List<(string name, double[] data)> FindInputSignals(
            string nodeId,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            Dictionary<string, string> signalMap,
            Dictionary<string, double[]> dataset,
            Dictionary<string, HashSet<string>> physicalNeighbors)
        {
            var result   = new List<(string, double[])>();
            var usedCols = new HashSet<string>();
            if (!inputEdges.ContainsKey(nodeId)) return result;

            int parentSplit = nodeId.LastIndexOf('_');
            string? parentComp = parentSplit > 0 ? nodeId.Substring(0, parentSplit) : null;

            foreach (var (fromNode, label) in inputEdges[nodeId])
            {
                string? resolvedKey = null;
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
                        if (signalMap.ContainsKey(altKey))
                        {
                            resolvedKey = altKey;
                        }
                        else
                        {
                            string? proxyKey = FindProxySignal(fromComp, fromState, parentComp,
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

        private static string? FindProxySignal(
            string missingComp, string stateSuffix, string? parentComp,
            Dictionary<string, HashSet<string>> physicalNeighbors,
            Dictionary<string, string> signalMap,
            Dictionary<string, double[]> dataset,
            int maxHops = 8)
        {
            if (!physicalNeighbors.ContainsKey(missingComp)) return null;
            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { missingComp };
            if (parentComp != null) visited.Add(parentComp);
            var queue = new Queue<(string comp, int depth)>();
            queue.Enqueue((missingComp, 0));
            while (queue.Count > 0)
            {
                var (comp, depth) = queue.Dequeue();
                if (depth >= maxHops || !physicalNeighbors.ContainsKey(comp)) continue;
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

        internal static Dictionary<string, HashSet<string>> BuildPhysicalAdjacency(JArray models)
        {
            var adj = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            string Normalize(string name)
            {
                if (name.EndsWith("_pf", StringComparison.OrdinalIgnoreCase)) return name.Substring(0, name.Length - 3);
                if (name.EndsWith("_m",  StringComparison.OrdinalIgnoreCase)) return name.Substring(0, name.Length - 2);
                return name;
            }

            foreach (var model in models)
            {
                string compName = Normalize((string?)model["Name"] ?? "");
                var inputs = (JArray?)model["Inputs"];
                if (inputs == null) continue;
                foreach (var inp in inputs)
                {
                    string dst = (string?)inp["Destination"] ?? "";
                    if (dst.IndexOf("Stream", StringComparison.OrdinalIgnoreCase) < 0) continue;
                    string src = (string?)inp["Source"] ?? "";
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

        // ── CSV I/O ──────────────────────────────────────────────────────────

        internal static Dictionary<string, double[]> LoadCsvDataset(string path)
        {
            var data    = new Dictionary<string, List<double>>();
            var lines   = File.ReadAllLines(path).Where(l => !string.IsNullOrWhiteSpace(l)).ToArray();
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
                        string cell = vals[j].Trim();
                        if      (cell.Equals("true",  StringComparison.OrdinalIgnoreCase)) v = 1.0;
                        else if (cell.Equals("false", StringComparison.OrdinalIgnoreCase)) v = 0.0;
                        else double.TryParse(cell, System.Globalization.NumberStyles.Any,
                                             System.Globalization.CultureInfo.InvariantCulture, out v);
                    }
                    data[headers[j]].Add(v);
                }
            }
            return data.ToDictionary(kv => kv.Key, kv => kv.Value.ToArray());
        }

        internal static void WriteValidationCsv(
            Dictionary<string, double[]> dataset,
            Dictionary<string, double[]> predictions,
            Dictionary<string, string> signalMap,
            string path,
            double timeBase_s = 0.5)
        {
            int rows = dataset.First().Value.Length;
            var newHeaders  = new List<string>();
            var trueVectors = new List<double[]>();
            var predVectors = new List<double[]>();

            foreach (var kvp in predictions)
            {
                string? trueHeader = signalMap.ContainsKey(kvp.Key) ? signalMap[kvp.Key] : null;
                if (trueHeader != null && dataset.ContainsKey(trueHeader))
                {
                    newHeaders.Add($"{kvp.Key}_True");
                    newHeaders.Add($"{kvp.Key}_Predicted");
                    trueVectors.Add(dataset[trueHeader]);
                    predVectors.Add(kvp.Value);
                }
            }

            var outputLines = new List<string> { "Time," + string.Join(",", newHeaders) };
            for (int r = 0; r < rows; r++)
            {
                var rowData = new List<string> { (r * timeBase_s).ToString("F2", System.Globalization.CultureInfo.InvariantCulture) };
                for (int c = 0; c < trueVectors.Count; c++)
                {
                    double tv = r < trueVectors[c].Length ? trueVectors[c][r] : 0;
                    double pv = r < predVectors[c].Length ? predVectors[c][r] : 0;
                    rowData.Add(tv.ToString(System.Globalization.CultureInfo.InvariantCulture));
                    rowData.Add(pv.ToString(System.Globalization.CultureInfo.InvariantCulture));
                }
                outputLines.Add(string.Join(",", rowData));
            }
            File.WriteAllLines(path, outputLines);
        }
    }
}
