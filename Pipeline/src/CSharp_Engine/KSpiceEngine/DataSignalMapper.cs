using System;
using System.Collections.Generic;
using System.Linq;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IO;

namespace KSpiceEngine
{
    public static class DataSignalMapper
    {
        public static Dictionary<string, string> GenerateMapping(List<string> csvHeaders, List<dynamic> tsaEquations, string systemMapPath)
        {
            var systemMap = JObject.Parse(File.ReadAllText(systemMapPath));
            var mapping = new Dictionary<string, string>();
            foreach (var eq in tsaEquations)
            {
                string id = eq.ID;
                string comp = eq.Component;
                string stateType = eq.State;
                string role = eq.Role;

                string matchedHeader = FindBestMatch(csvHeaders, comp, stateType, systemMap);
                if (matchedHeader != null) {
                    mapping[id] = matchedHeader;
                }

                // --- Automatic Missing Signal/Boundary Binding ---
                // PID controllers get Setpoint + Measurement mappings used for PID identification.
                // ASC controllers are excluded — they have no single PV/SP in the PID sense.
                string controllerType = (string)eq.ControllerType ?? "";
                if (role == "Controller" && stateType == "Control" && controllerType == "PID") {
                    string spMatch = FindBestMatch(csvHeaders, comp, "Setpoint", systemMap);
                    if (spMatch != null) mapping[$"{comp}_Setpoint"] = spMatch;

                    string measMatch = FindBestMatch(csvHeaders, comp, "Measurement", systemMap);
                    if (measMatch != null) mapping[$"{comp}_Measurement"] = measMatch;
                }

                // If it's FlowEquipment, binding explicit upstream/downstream pressure for boundaries
                if (role == "FlowEquipment") {
                    string outPressMatch = FindBestMatch(csvHeaders, comp, "DownstreamPressure", systemMap);
                    if (outPressMatch != null) mapping[$"{comp}_DownstreamPressure"] = outPressMatch;

                    string inPressMatch = FindBestMatch(csvHeaders, comp, "UpstreamPressure", systemMap);
                    if (inPressMatch != null) mapping[$"{comp}_UpstreamPressure"] = inPressMatch;

                    // Map control signal for valves (Opening / LocalControlSignalIn)
                    string ctrlMatch = FindBestMatch(csvHeaders, comp, "ControlSignal", systemMap);
                    if (ctrlMatch != null) mapping[$"{comp}_ControlSignal"] = ctrlMatch;
                }
            }

            // Second pass: controllers whose _Control mapping is missing because their
            // ControllerOutput signal isn't logged directly in the CSV.
            //
            // Strategy A (cascade master): if the inner controller receives its setpoint
            // (ExternalSetpoint) from an outer controller whose output IS in the CSV,
            // use that outer controller's output as the proxy.  Rationale: tight inner
            // loops (e.g. speed) track their setpoint closely, so the master output
            // is physically equivalent and — crucially — will be PREDICTED from the
            // relevant plant state in closed-loop (creating self-correcting feedback).
            //
            // Strategy B (output receiver): if no cascade master found, find the
            // component that physically receives this controller's output and map to
            // whatever CSV column name that input port appears under.
            var allModels = (JArray)systemMap["Models"];
            foreach (var eq in tsaEquations)
            {
                if ((string)eq.Role != "Controller") continue;
                string comp    = (string)eq.Component;
                string ctrlKey = $"{comp}_Control";
                if (mapping.ContainsKey(ctrlKey)) continue;

                // Locate this controller in the system map.
                JObject ctrlModel = null;
                foreach (var m in allModels)
                    if (string.Equals((string)m["Name"], comp, StringComparison.OrdinalIgnoreCase))
                        { ctrlModel = (JObject)m; break; }
                if (ctrlModel == null) continue;

                // Strategy A: ExternalSetpoint from a cascade master controller.
                var ctrlInputs = (JArray)ctrlModel["Inputs"];
                if (ctrlInputs != null)
                {
                    foreach (var inp in ctrlInputs)
                    {
                        string dst = (string)inp["Destination"] ?? "";
                        if (!dst.Equals("ExternalSetpoint", StringComparison.OrdinalIgnoreCase)) continue;
                        string src   = (string)inp["Source"] ?? "";
                        var   match  = csvHeaders.FirstOrDefault(h =>
                            h.Equals(src, StringComparison.OrdinalIgnoreCase));
                        if (match != null)
                        {
                            mapping[ctrlKey] = match;
                            Console.WriteLine($"[SignalMapper] {ctrlKey} -> {match} (cascade ExternalSetpoint trace)");
                            break;
                        }
                    }
                }
                if (mapping.ContainsKey(ctrlKey)) continue;

                // Strategy B: component that receives this controller's output.
                string outputSignal = $"{comp}:ControllerOutput";
                foreach (var model in allModels)
                {
                    var inputs = (JArray)model["Inputs"];
                    if (inputs == null) continue;
                    foreach (var inp in inputs)
                    {
                        string src = (string)inp["Source"] ?? "";
                        if (!src.Equals(outputSignal, StringComparison.OrdinalIgnoreCase)) continue;
                        string dst            = (string)inp["Destination"] ?? "";
                        string receiverName   = (string)model["Name"] ?? "";
                        string candidateSignal = $"{receiverName}:{dst}";
                        var match = csvHeaders.FirstOrDefault(h =>
                            h.Equals(candidateSignal, StringComparison.OrdinalIgnoreCase));
                        if (match != null)
                        {
                            mapping[ctrlKey] = match;
                            Console.WriteLine($"[SignalMapper] {ctrlKey} -> {match} (cascade output trace)");
                            break;
                        }
                    }
                    if (mapping.ContainsKey(ctrlKey)) break;
                }
            }

            // Auto-detect speed signals for rotating equipment (pumps, compressors).
            // Any CSV column ending in ":Speed" is mapped as {comp}_Speed → {col}
            // so the topology can wire it as a PUMP_SPEED boundary input in CL.
            foreach (var header in csvHeaders)
            {
                if (header.EndsWith(":Speed", StringComparison.OrdinalIgnoreCase))
                {
                    string compName = header.Split(':')[0];
                    string mapKey   = $"{compName}_Speed";
                    if (!mapping.ContainsKey(mapKey))
                        mapping[mapKey] = header;
                }
            }

            return mapping;
        }

        private static string TracePhysicalMeasurement(JObject systemMap, string controllerName)
        {
            var models = (JArray)systemMap["Models"];
            
            // Map the cleaned CSV name back to the KSpiceSystemMap name for tracing
            string mapLookupName = controllerName;
            if (mapLookupName == "23LIC0002") mapLookupName = "23LIC002";

            var controller = models.FirstOrDefault(m => string.Equals((string)m["Name"], mapLookupName, StringComparison.OrdinalIgnoreCase));
            if (controller == null) return null;

            var inputs = (JArray)controller["Inputs"];
            var measInput = inputs?.FirstOrDefault(i => ((string)i["Destination"]) == "Measurement");
            if (measInput == null) return null;

            string source = (string)measInput["Source"];
            string transmitterName = source.Split(':')[0];

            // Trace transmitter to real physical volume/flow element
            var transmitter = models.FirstOrDefault(m => string.Equals((string)m["Name"], transmitterName, StringComparison.OrdinalIgnoreCase));
            if (transmitter != null)
            {
                var tInputs = (JArray)transmitter["Inputs"];
                if (tInputs != null && tInputs.Count > 0)
                {
                    string realSource = (string)tInputs[0]["Source"];
                    // Attempt alias fixing (23VA001 -> 23VA0001)
                    if (realSource.StartsWith("23VA001:")) realSource = realSource.Replace("23VA001:", "23VA0001:");

                    // Step backwards through pipe volumes if needed (e.g. pv_23L0005:Temperature -> 23HX0001:Temperature)
                    if (realSource.StartsWith("pv_"))
                    {
                        string pvName = realSource.Split(':')[0];
                        var pv = models.FirstOrDefault(m => string.Equals((string)m["Name"], pvName, StringComparison.OrdinalIgnoreCase));
                        if (pv != null)
                        {
                            var pvInputs = (JArray)pv["Inputs"];
                            if (pvInputs != null && pvInputs.Count > 0)
                            {
                                string pvSource = (string)pvInputs[0]["Source"];
                                string rootComp = pvSource.Split(':')[0];
                                string signalType = realSource.Split(':')[1];

                                if (rootComp.StartsWith("23HX") && signalType == "Temperature")
                                {
                                    realSource = $"{rootComp}:OutletStream.t";
                                }
                                else
                                {
                                    realSource = $"{rootComp}:{signalType}";
                                }
                            }
                        }
                    }

                    return realSource;
                }
            }
            
            return source;
        }

        private static string TraceValveControlSignal(JObject systemMap, string valveName)
        {
            var models = (JArray)systemMap["Models"];
            var valve = models.FirstOrDefault(m => string.Equals((string)m["Name"], valveName, StringComparison.OrdinalIgnoreCase));
            if (valve == null) return null;

            var inputs = (JArray)valve["Inputs"];
            if (inputs == null) return null;

            var compInput = inputs.FirstOrDefault(i => ((string)i["Destination"]).Equals("LocalControlSignalIn", StringComparison.OrdinalIgnoreCase) || ((string)i["Destination"]).Equals("TargetPosition", StringComparison.OrdinalIgnoreCase));
            if (compInput != null)
            {
                return (string)compInput["Source"];
            }
            return null;
        }

        private static string FindBestMatch(List<string> headers, string comp, string stateType, JObject systemMap)
        {
            string compUpper = comp.ToUpper();
            // Alias for mistyped components
            if (compUpper == "23VA001") compUpper = "23VA0001";
            if (compUpper == "23LIC002") compUpper = "23LIC0002";

            var candidates = new List<string>();

            if (stateType == "MassFlow") {
                candidates.AddRange(new[] { 
                    $"{compUpper}_pf:MassFlow",
                    $"{compUpper}:MassFlow",
                    $"{compUpper}:OutletStream.f",
                    $"{compUpper}_pf:OutletStream.f",
                    $"{compUpper}:OverflowOutletStream[0].f",
                    $"{compUpper}:FeedOutletStream[0].f"
                });
            } else if (stateType == "Temperature") {
                candidates.AddRange(new[] { 
                    $"{compUpper}:Temperature",
                    $"{compUpper}:OutletStream.t",
                    $"{compUpper}_pf:OutletStream.t",
                    $"{compUpper}:OverflowOutletStream[0].t"
                });
            } else if (stateType == "Pressure") {
                candidates.AddRange(new[] { 
                    $"{compUpper}:Pressure",
                    $"{compUpper}_pf:OutletPressure",
                    $"{compUpper}_pf:OutletStream.p",
                    $"{compUpper}:OutletStream.p",
                    $"{compUpper}:OverflowOutletStream[0].p"
                });
            } else if (stateType == "WaterLevel" || stateType == "TotalLevel" || stateType == "Level") {
                if (stateType == "WaterLevel") candidates.Add($"{compUpper}:LevelHeavyPhaseFeedSideWeir");
                candidates.Add($"{compUpper}:LevelOverflowLiquid");
                candidates.Add($"{compUpper}:LevelFeedSideWeir");
                candidates.Add($"{compUpper}:Level");
                candidates.Add($"{compUpper}:VolumeTopWeirOverflowSide");
            } else if (stateType == "Control") {
                candidates.AddRange(new[] { 
                    $"{compUpper}:Output",
                    $"{compUpper}:ControllerOutput",
                    $"{compUpper}:Opening",
                    $"{compUpper}:LocalControlSignalIn",
                    $"{compUpper}_m:LocalControlSignalIn",
                    $"{compUpper}:TargetPosition"
                });
            } else if (stateType == "Setpoint") {
                candidates.AddRange(new[] { 
                    $"{compUpper}:SetpointUsed",
                    $"{compUpper}:InternalSetpoint",
                    $"{compUpper}:NormalizedFlowSetpoint"
                });
            } else if (stateType == "Measurement") {
                // For exact PID identification, prefer the controller's direct measurement port
                // over the traced physical target to capture transmitter lag/filtering correctly.
                candidates.AddRange(new[] { 
                    $"{compUpper}:Measurement",
                    $"{compUpper}:InletPressureMeasurement",
                    $"{compUpper}:NormalizedFlow"
                });

                // Determine true physical measurement target from KSpiceSystemMap.json as fallback
                string physicalTarget = TracePhysicalMeasurement(systemMap, compUpper);
                if (physicalTarget != null)
                {
                    candidates.Add(physicalTarget);
                }
            } else if (stateType == "DownstreamPressure") {
                candidates.AddRange(new[] { 
                    $"{compUpper}_pf:OutletPressure",
                    $"{compUpper}_pf:OutletStream.p",
                    $"{compUpper}:OutletStream.p",
                    $"{compUpper}:OverflowOutletStream[0].p" 
                });
            } else if (stateType == "UpstreamPressure") {
                candidates.AddRange(new[] { 
                    $"{compUpper}_pf:InletPressure",
                    $"{compUpper}_pf:InletStream.p",
                    $"{compUpper}:InletStream.p",
                    $"{compUpper}:InletPressure" 
                });
            } else if (stateType == "ControlSignal") {
                candidates.AddRange(new[] {
                    $"{compUpper}:LocalControlSignalIn",
                    $"{compUpper}_m:LocalControlSignalIn",
                    $"{compUpper}:Opening",
                    $"{compUpper}:TargetPosition",
                    $"{compUpper}_m:LocalInput"
                });

                string valveControlSrc = TraceValveControlSignal(systemMap, compUpper);
                if (valveControlSrc != null)
                {
                    candidates.Add(valveControlSrc);
                }
            }

            foreach (var c in candidates)
            {
                var match = headers.FirstOrDefault(h => h.Equals(c, StringComparison.OrdinalIgnoreCase));
                if (match != null) return match;
            }

            return null;
        }
    }
}
