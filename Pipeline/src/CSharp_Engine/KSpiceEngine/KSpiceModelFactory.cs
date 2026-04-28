using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KSpiceEngine
{
    /// <summary>
    /// Phase 2: Builds the TSA equations map (JSON) from a KSpice system map.
    /// Only generates the equation descriptors — no TSA model objects are constructed here.
    /// Actual identification (fitting) happens in DynamicPlantRunner (Phase 4).
    /// </summary>
    public class KSpiceModelFactory
    {
        public static void BuildEquationsMap(string jsonPath, out List<JObject> exportMap)
        {
            System.Console.WriteLine($"Loading System Map: {jsonPath}");
            var jsonContent = File.ReadAllText(jsonPath);
            dynamic systemMap = JsonConvert.DeserializeObject(jsonContent);

            var modelDefinitions = new List<JObject>();

            string[] ignoreTypes = { "Alarm", "Transmitter", "Indicator", "SignalSwitch", "ProfileViewer" };

            // First pass: collect valid base components (de-duplicated, _pf stripped)
            var baseProps = new Dictionary<string, dynamic>();
            foreach (var node in systemMap.Models)
            {
                string t     = node["KSpiceType"];
                string n     = node["Name"];
                string nLow  = n.ToLower();

                if (t == "Network"
                    || ignoreTypes.Any(it => t.Contains(it))
                    || nLow.EndsWith("_m")
                    || nLow.EndsWith("_view")
                    || nLow.StartsWith("pv")
                    || nLow.StartsWith("pf_")
                    || nLow.StartsWith("network-")
                    || nLow.Contains("fe0"))
                    continue;

                string baseName = n.Replace("_pf", "");
                if (!baseProps.ContainsKey(baseName))
                    baseProps[baseName] = node;
            }

            // Second pass: emit one equation descriptor per state per component
            foreach (var entry in baseProps)
            {
                string  baseName = entry.Key;
                dynamic node     = entry.Value;
                string  ktype    = node["KSpiceType"];
                var     pMap     = node["Parameters"] as JObject ?? new JObject();

                void AddState(string suffix, string formula, string[] inputs)
                {
                    var eq = new JObject();
                    eq["ID"]        = $"{baseName}_{suffix}";
                    eq["Component"] = baseName;
                    eq["State"]     = suffix;
                    eq["Formula"]   = formula;
                    eq["Inputs"]    = JArray.FromObject(inputs);
                    eq["Role"]      = suffix == "Control"             ? "Controller"
                                    : (suffix.EndsWith("Level") || suffix == "Pressure") ? "Volume"
                                    : "FlowEquipment";

                    if (pMap["M"]        != null && suffix == "MassFlow") eq["Param"] = $"Cv: {pMap["M"]}";
                    if (pMap["Diameter"] != null && suffix == "Pressure")
                        eq["Param"] = $"D: {pMap["Diameter"]} L: {pMap["Length"]}";

                    modelDefinitions.Add(eq);
                }

                if (ktype.Contains("Separator") || ktype.Contains("Tank"))
                {
                    AddState("Pressure",    "dP/dt = k * (m_in - m_out)",           new[] { "UPSTREAM_FLOW", "DOWNSTREAM_FLOW" });
                    AddState("WaterLevel",  "dL_w/dt = (1/A) * (m_in - m_out)",     new[] { "UPSTREAM_FLOW", "DOWNSTREAM_FLOW" });
                    AddState("OilLevel",    "dL_o/dt = (1/A) * (m_in - m_out)",     new[] { "UPSTREAM_FLOW", "DOWNSTREAM_FLOW" });
                    AddState("TotalLevel",  "dL/dt   = (1/A) * (m_in - m_out)",     new[] { "UPSTREAM_FLOW", "DOWNSTREAM_FLOW" });
                    AddState("Temperature", "T_out = f(T_in, m_in, m_out)",          new[] { "UPSTREAM_FLOW", "UPSTREAM_TEMP", "DOWNSTREAM_FLOW" });
                }
                else if (ktype.Contains("HeatExchanger"))
                {
                    AddState("MassFlow",    "F = sum(m_downstream)",                          new[] { "DOWNSTREAM_FLOW_SUM" });
                    AddState("Pressure",    "P_out = P_in - dP(F)",                           new[] { "UPSTREAM_PRESSURE", $"{baseName}_MassFlow" });
                    AddState("Temperature", "T_out = f(T_in, T_cool, F, m_partner)",          new[] { "UPSTREAM_TEMP", $"{baseName}_MassFlow", "COOLING_TEMP", "PARTNER_FLOW" });
                }
                else if (ktype.Contains("ControlValve") || ktype.Contains("PipeFlow"))
                {
                    AddState("MassFlow",    "F = Cv * sqrt(dP * rho)",               new[] { "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE", "LOCAL_CONTROL" });
                    AddState("Temperature", "T_out = f(T_in, P_in, P_out)",          new[] { "UPSTREAM_TEMP", "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE" });
                }
                else if (ktype.Contains("Compressor") || ktype.Contains("Pump"))
                {
                    AddState("MassFlow",    "F = sum(m_downstream)",                 new[] { "DOWNSTREAM_FLOW_SUM" });
                    AddState("Pressure",    "P_out = P_in + Head(F, N)",             new[] { "UPSTREAM_PRESSURE", $"{baseName}_MassFlow", "LOCAL_CONTROL" });
                    AddState("Temperature", "T_out = T_in * (P_out/P_in)^((k-1)/k)", new[] { "UPSTREAM_TEMP", $"{baseName}_Pressure", "UPSTREAM_PRESSURE" });
                }
                else if (ktype.IndexOf("pid", System.StringComparison.OrdinalIgnoreCase) >= 0
                         || ktype.Contains("GenericASC")
                         || ktype.Contains("Controller"))
                {
                    if (ktype.Contains("GenericASC"))
                        AddState("Control", "ASC: U(t) = f(Flow, Pressure)", new[] { "MEASURED_FLOW", "MEASURED_PRESSURE" });
                    else
                        AddState("Control", "PID: U(t) = Kp*e + Ki*∫e dt",   new[] { "MEASURED_STATE" });
                }
                else
                {
                    // Boundary source / unrecognised type — treated as measured input
                    AddState("MassFlow",    "Boundary Flow",     new string[0]);
                    AddState("Pressure",    "Boundary Pressure", new string[0]);
                    AddState("Temperature", "Boundary Energy",   new string[0]);
                }
            }

            exportMap = modelDefinitions;
        }
    }
}
