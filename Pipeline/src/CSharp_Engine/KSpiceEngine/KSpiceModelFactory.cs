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

                    // ControllerType distinguishes PID from ASC without relying on
                    // component-name heuristics (e.g. "starts with 23ASC").
                    if (suffix == "Control")
                        eq["ControllerType"] = ktype.Contains("GenericASC") ? "ASC" : "PID";

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
                    // Pressure drop across a heat exchanger is small and flow-independent
                    // in practice; using {baseName}_MassFlow as a local_var input created a
                    // cascade: HX_MassFlow → KA0001_MassFlow errors → HX_Pressure errors.
                    // Upstream pressure alone gives a cleaner and more stable CL model.
                    AddState("Pressure",    "P_out ≈ P_in  (small HX dP ignored)",           new[] { "UPSTREAM_PRESSURE" });
                    AddState("Temperature", "T_out = f(T_in, T_cool, F, m_partner)",          new[] { "UPSTREAM_TEMP", $"{baseName}_MassFlow", "COOLING_TEMP", "PARTNER_FLOW" });
                }
                else if (ktype.Contains("ControlValve") || ktype.Contains("PipeFlow") || ktype.Contains("BlockValve"))
                {
                    // BlockValve (ESV, etc.) is modeled as a valve: Q = Cv * sqrt(dP * rho).
                    // When no controller is wired (LOCAL_CONTROL finds no upstream PID/ASC),
                    // DynamicPlantRunner defaults to Assumed_100% (always open).
                    AddState("MassFlow",    "F = Cv * sqrt(dP * rho)",               new[] { "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE", "LOCAL_CONTROL" });
                    AddState("Temperature", "T_out = f(T_in, P_in, P_out)",          new[] { "UPSTREAM_TEMP", "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE" });
                }
                else if (ktype.Contains("Compressor"))
                {
                    // Speed-controlled compressor: flow is driven by suction pressure and
                    // the speed controller (PIC).  Speed is the direct output of the speed
                    // controller so LOCAL_CONTROL gives a near-linear Speed model.
                    AddState("MassFlow",    "F = f(N, P_sep)",                       new[] { "UPSTREAM_PRESSURE", "LOCAL_CONTROL" });
                    AddState("Pressure",    "P_out = P_in + Head(F, N)",             new[] { "SUCTION_PRESSURE", $"{baseName}_MassFlow", "LOCAL_CONTROL" });
                    AddState("Temperature", "T_out = T_in * (P_out/P_in)^((k-1)/k)", new[] { "UPSTREAM_TEMP", $"{baseName}_Pressure", "UPSTREAM_PRESSURE" });
                    // Speed is commanded by the speed controller; identify as linear function
                    // of LOCAL_CONTROL so the closed-loop uses predicted speed, not CSV data.
                    AddState("Speed",       "N = f(speed controller output)",        new[] { "LOCAL_CONTROL" });
                }
                else if (ktype.Contains("Pump"))
                {
                    // Pump flow equals downstream valve flow (series conservation of mass).
                    // LOCAL_CONTROL is intentionally excluded: for pumps without a dedicated
                    // speed controller, the topology builder would fall back to the downstream
                    // valve's level controller (LIC), which is spurious (the level controller
                    // drives the valve, not the pump directly). The downstream flow already
                    // captures the combined effect of all controllers in the series path.
                    //
                    // Pressure does NOT include {baseName}_MassFlow to avoid a closed-loop cycle:
                    //   PA_MassFlow[t] → uses → LV_MassFlow[t-1]
                    //   LV_MassFlow[t] → uses → PA_Pressure[t]
                    //   PA_Pressure[t] → uses → PA_MassFlow[t]   ← cycle with 1-step delay → unstable
                    // Removing MassFlow breaks the loop; order becomes:
                    //   PA_Pressure → LV_MassFlow → PA_MassFlow  (no cycle).
                    //
                    // PUMP_SPEED wires to the predicted Speed state (not CSV boundary) once the
                    // Speed equation exists; the topology builder only adds a boundary node when
                    // speed_key is absent from equation_ids.
                    AddState("MassFlow",    "F = f(F_ds, P_sep)",                    new[] { "UPSTREAM_PRESSURE", "DOWNSTREAM_FLOW" });
                    AddState("Pressure",    "P_out = P_in + Head(F, N)",             new[] { "SUCTION_PRESSURE", "LOCAL_CONTROL", "PUMP_SPEED" });
                    AddState("Temperature", "T_out = f(T_in, P_out, P_in)",          new[] { "UPSTREAM_TEMP", $"{baseName}_Pressure", "UPSTREAM_PRESSURE" });
                    // Motor speed is driven by torque balance (no external speed controller for
                    // fixed-frequency motors).  Model as data-driven function of the operating
                    // point so the closed-loop can predict speed without reading the CSV.
                    AddState("Speed",       "N = f(P_sep, F_ds) motor torque balance", new[] { "UPSTREAM_PRESSURE", "DOWNSTREAM_FLOW" });
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
