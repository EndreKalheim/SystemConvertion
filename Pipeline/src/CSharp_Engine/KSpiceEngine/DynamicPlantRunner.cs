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
                var rawName = ((string)model["Name"]).Replace("_pf", "", StringComparison.OrdinalIgnoreCase);
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
                string comp = (string)eq.Component;
                string state = (string)eq.State;
                string role = (string)eq.Role;
                string formula = (string)eq.Formula;
                
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
                //   A. Controller Modeling (PID) - Incremental PI
                // -----------------------------------------------------
                if (role == "Controller" && !comp.StartsWith("23ASC"))
                {
                    string measMapKey = $"{comp}_Measurement";
                    string spMapKey = $"{comp}_Setpoint";
                    
                    if (signalMap.ContainsKey(measMapKey) && signalMap.ContainsKey(spMapKey))
                    {
                        var measSignal = signalMap[measMapKey];
                        var spSignal = signalMap[spMapKey];
                        
                        if (dataset.ContainsKey(measSignal) && dataset.ContainsKey(spSignal))
                        {
                            double[] Y_meas = dataset[measSignal];
                            double[] Y_sp = dataset[spSignal];
                            
                            // Get PID params from K-Spice model
                            var kspiceModel = models.FirstOrDefault(m => 
                                string.Equals((string)m["Name"], comp, StringComparison.OrdinalIgnoreCase) || 
                                string.Equals((string)m["Name"], comp.Replace("000", "00"), StringComparison.OrdinalIgnoreCase));
                                              
                            if (kspiceModel != null)
                            {
                                var parameters = kspiceModel["Parameters"];
                                double kp = (double?)parameters["Gain"] ?? 1.0;
                                double ti = (double?)parameters["IntegralTime"] ?? 100.0;
                                double td = (double?)parameters["DerivativeTime"] ?? 0.0;
                                
                                // Check for reverse action
                                bool reverseAction = false;
                                var actionParam = parameters["ControllerAction"];
                                if (actionParam != null)
                                {
                                    string actionStr = actionParam.ToString().ToLower();
                                    reverseAction = actionStr.Contains("reverse") || actionStr == "1";
                                }
                                
                                double actionSign = reverseAction ? -1.0 : 1.0;
                                
                                Console.WriteLine($"[Model] {id}: Incremental PI. Kp={kp}, Ti={ti}, Td={td}, Reverse={reverseAction}");
                                
                                // Incremental/Velocity PI: u(k) = u(k-1) + Kp * [(e(k) - e(k-1)) + dt/Ti * e(k)]
                                double[] Y_pred = new double[numRows];
                                double dt = timeBase_s;
                                
                                // Initialize with the true value at t=0
                                Y_pred[0] = Y_true[0];
                                double error_prev = actionSign * (Y_sp[0] - Y_meas[0]);
                                
                                for (int i = 1; i < numRows; i++)
                                {
                                    double error = actionSign * (Y_sp[i] - Y_meas[i]);
                                    
                                    double du = kp * ((error - error_prev) + (ti > 0 ? (dt / ti) * error : 0));
                                    
                                    Y_pred[i] = Y_pred[i - 1] + du;
                                    
                                    // Clamp to [0, 100] typical for K-Spice controllers
                                    Y_pred[i] = Math.Max(0, Math.Min(100, Y_pred[i]));
                                    
                                    error_prev = error;
                                }
                                predictions[id] = Y_pred;
                                
                                var pidParams = new JObject();
                                pidParams["ModelType"] = "PID";
                                pidParams["Kp"] = kp;
                                pidParams["Ti"] = ti;
                                pidParams["Td"] = td;
                                pidParams["Action"] = reverseAction ? 1 : 0;
                                pidParams["Formula"] = "Incremental PI: u(k) = u(k-1) + Kp * (e(k) - e(k-1) + dt/Ti * e(k))";
                                identifiedParams[id] = pidParams;
                            }
                            else
                            {
                                Console.WriteLine($"[WARNING] {id}: Could not find K-Spice model for PID params.");
                                predictions[id] = (double[])Y_true.Clone();
                                identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "KSpice model not found for PID params" };
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[WARNING] {id}: Measurement or Setpoint signal not in CSV.");
                            predictions[id] = Enumerable.Repeat(0.0, numRows).ToArray();
                            identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "Measurement or Setpoint not in CSV" };
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[WARNING] {id}: No Measurement/Setpoint mapping for controller {comp}.");
                        predictions[id] = Enumerable.Repeat(0.0, numRows).ToArray();
                        identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "No Measurement/Setpoint signal mapping" };
                    }
                }
                
                // -----------------------------------------------------
                //   B. Anti-Surge Controller Modeling (ASC)
                // -----------------------------------------------------
                else if (role == "Controller" && kspiceType == "GenericASC")
                {
                    Console.WriteLine($"[Model] {id}: ASC Generic Identification...");
                    
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset);
                    
                    if (inputCols.Count >= 3)
                    {
                        try
                        {
                            double[] flow = null, press1 = null, press2 = null, pIn = null, pOut = null;
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
                                        flow = col.data;
                                    continue;
                                }
                                if (n.IndexOf("InletPressure", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    press1 = col.data;
                                    continue;
                                }
                                if (n.IndexOf("OutletPressure", StringComparison.OrdinalIgnoreCase) >= 0)
                                {
                                    press2 = col.data;
                                    continue;
                                }
                                if (col.name.Contains("Flow") || col.name.Contains("Mass"))
                                {
                                    if (flow == null) flow = col.data;
                                }
                                else if (press1 == null) press1 = col.data;
                                else press2 = col.data;
                            }
                            if (press1 != null && press2 != null) {
                                if (press1[0] < press2[0]) { pIn = press1; pOut = press2; }
                                else { pIn = press2; pOut = press1; }
                            }
                            
                            if (pIn != null && pOut != null && flow != null)
                            {
                                var ascModel = new KSpiceEngine.CustomModels.AntiSurgePhysicalModel(id, new string[]{"P_in", "P_out", "Flow"}, id);
                                ascModel.WarmStart(null, Y_true[0]);
                                
                                double[] y_sim = new double[numRows];
                                for (int i = 0; i < numRows; i++) {
                                    y_sim[i] = ascModel.Iterate(new double[]{ pIn[i], pOut[i], flow[i] }, timeBase_s)[0];
                                }
                                
                                predictions[id] = y_sim;
                                Console.WriteLine($"[Model] {id}: AntiSurgePhysicalModel simulated successfully.");
                                
                                var ascParams = new JObject();
                                ascParams["ModelType"] = "AntiSurgePhysicalModel";
                                ascParams["Formula"] = "Surge Distance PI";
                                identifiedParams[id] = ascParams;
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
                //   C. Separator Volume Modeling (Integrators for Mass/Pressure/Level)
                // -----------------------------------------------------
                else if ((state.EndsWith("Level") || state == "Pressure") && kspiceType.IndexOf("Separator", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset);
                    if (inputCols.Count > 0)
                    {
                        // Separate inflows (m_in, mass_in) from outflows (m_out, mass_out_drain).
                        // Negate outflow data so IdentifyLinearDiff can use all-positive gains:
                        //   dL/dt = gain_in * F_in + gain * (-F_out)  →  correct mass-balance sign
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

                        Console.WriteLine($"[Model] {id}: UnitIdentifier.IdentifyLinearDiff (Integrator) with {signedInputs.Count} inputs ({inputCols.Count(x => x.Item1.Contains("(m_out)") || x.Item1.Contains("(m_sum)") || x.Item1.Contains("(mass_out")) } outflows negated)");
                        try
                        {
                            var unitDataSet = BuildUnitDataSet(signedInputs, Y_true, timeBase_s);
                            var model = UnitIdentifier.IdentifyLinearDiff(ref unitDataSet, null, false);

                            if (!(model.modelParameters.Fitting.WasAbleToIdentify && unitDataSet.Y_sim != null))
                            {
                                Console.WriteLine($"[WARNING] {id}: Integrator identification failed, copying actuals.");
                                predictions[id] = (double[])Y_true.Clone();
                                identifiedParams[id] = new JObject { ["ModelType"] = "Fallback", ["Reason"] = "IdentifyLinearDiff failed" };
                                continue;
                            }

                            // Anchor initial condition to measured value
                            double offset = Y_true[0] - unitDataSet.Y_sim[0];
                            double[] adjustedSim = new double[numRows];
                            for (int j = 0; j < numRows; j++)
                                adjustedSim[j] = unitDataSet.Y_sim[j] + offset;

                            predictions[id] = adjustedSim;
                            Console.WriteLine($"[SUCCESS] {id}: Integrator identified. FitScore={model.modelParameters.Fitting.FitScorePrc:F1}%");

                            var mParams = new JObject();
                            mParams["ModelType"] = "IdentifyLinearDiff";
                            mParams["FitScore"] = model.modelParameters.Fitting.FitScorePrc;
                            mParams["TimeConstant_s"] = model.modelParameters.TimeConstant_s;
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
                //   D. Separator Temperature: data-driven linear fit
                // -----------------------------------------------------
                else if (state == "Temperature" && kspiceType.IndexOf("Separator", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset);
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
                                identifiedParams[id] = new JObject {
                                    ["ModelType"]      = "IdentifyLinear",
                                    ["FitScore"]       = fitScore,
                                    ["TimeConstant_s"] = model.modelParameters.TimeConstant_s
                                };
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
                //   E. All other models: Use TSA UnitIdentifier
                // -----------------------------------------------------
                else
                {
                    // Find all input signals from the topology edges
                    var inputCols = FindInputSignals(id, inputEdges, signalMap, dataset);
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

                            var modelParams = new JObject();
                            modelParams["ModelType"] = "UnitIdentifier";
                            modelParams["FitScore"] = fitScore;
                            modelParams["TimeConstant_s"] = model.modelParameters.TimeConstant_s;
                            if (model.modelParameters.LinearGains != null)
                                modelParams["LinearGains"] = JArray.FromObject(model.modelParameters.LinearGains);
                            modelParams["Formula"] = $"Dynamic MISO: {idInputs.Count} inputs, Tc={model.modelParameters.TimeConstant_s:F2}s";
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
        /// Find all input CSV columns for a given topology node by looking at edges pointing TO this node,
        /// then resolving each source node's signal mapping.
        /// </summary>
        private static List<(string name, double[] data)> FindInputSignals(
            string nodeId,
            Dictionary<string, List<(string fromNode, string label)>> inputEdges,
            Dictionary<string, string> signalMap,
            Dictionary<string, double[]> dataset)
        {
            var result = new List<(string, double[])>();
            var usedCols = new HashSet<string>();
            
            if (!inputEdges.ContainsKey(nodeId))
                return result;
            
            foreach (var (fromNode, label) in inputEdges[nodeId])
            {
                // The fromNode is a topology state like "23VA001_Pressure" — look for its signal mapping
                if (signalMap.ContainsKey(fromNode))
                {
                    string csvCol = signalMap[fromNode];
                    if (dataset.ContainsKey(csvCol) && !usedCols.Contains(csvCol))
                    {
                        result.Add(($"{fromNode}({label})", dataset[csvCol]));
                        usedCols.Add(csvCol);
                    }
                }
                else
                {
                    // Try to resolve the signal name directly from the fromNode components (IDs like 23VA001_TotalLevel)
                    int li = fromNode.LastIndexOf('_');
                    if (li <= 0) continue;
                    string fromComp = fromNode.Substring(0, li);
                    string fromState = fromNode.Substring(li + 1);
                    string altKey = $"{fromComp}_{fromState}";
                    
                    if (signalMap.ContainsKey(altKey))
                    {
                        string csvCol = signalMap[altKey];
                        if (dataset.ContainsKey(csvCol) && !usedCols.Contains(csvCol))
                        {
                            result.Add(($"{altKey}({label})", dataset[csvCol]));
                            usedCols.Add(csvCol);
                        }
                    }
                }
            }
            
            return result;
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

        private static Dictionary<string, double[]> LoadCsvDataset(string path)
        {
            var data = new Dictionary<string, List<double>>();
            var lines = File.ReadAllLines(path);
            var headers = lines[0].Split(',').Select(h => h.Trim()).ToList();
            
            foreach (var h in headers) data[h] = new List<double>();
            
            for (int i = 1; i < lines.Length; i++)
            {
                var vals = lines[i].Split(',');
                for (int j = 0; j < vals.Length; j++)
                {
                    if (j < headers.Count)
                    {
                        if (double.TryParse(vals[j], System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out double d))
                            data[headers[j]].Add(d);
                    }
                }
            }
            
            return data.ToDictionary(kv => kv.Key, kv => kv.Value.ToArray());
        }

        private static void WriteValidationCsv(Dictionary<string, double[]> dataset, Dictionary<string, double[]> predictions, Dictionary<string, string> signalMap, string path)
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