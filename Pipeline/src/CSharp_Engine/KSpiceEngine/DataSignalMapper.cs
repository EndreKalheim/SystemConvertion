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
                // If it's a Controller, automatically link Setpoint and Measurement if available
                if (role == "Controller" && stateType == "Control") {
                    string spMatch = FindBestMatch(csvHeaders, comp, "Setpoint", systemMap);
                    if (spMatch != null && !comp.StartsWith("23ASC")) mapping[$"{comp}_Setpoint"] = spMatch;
                    
                    string measMatch = FindBestMatch(csvHeaders, comp, "Measurement", systemMap);
                    if (measMatch != null && !comp.StartsWith("23ASC")) mapping[$"{comp}_Measurement"] = measMatch;
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
                // Determine true physical measurement target from KSpiceSystemMap.json
                string physicalTarget = TracePhysicalMeasurement(systemMap, compUpper);
                if (physicalTarget != null)
                {
                    candidates.Add(physicalTarget);
                }

                candidates.AddRange(new[] { 
                    $"{compUpper}:Measurement",
                    $"{compUpper}:InletPressureMeasurement",
                    $"{compUpper}:NormalizedFlow"
                });
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
