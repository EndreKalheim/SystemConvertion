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
            dynamic systemMap = JsonConvert.DeserializeObject(jsonContent)
                ?? throw new System.InvalidOperationException($"Failed to parse {jsonPath}");

            var modelDefinitions = new List<JObject>();

            string[] ignoreTypes = { "Alarm", "Transmitter", "Indicator", "SignalSwitch", "ProfileViewer" };

            // Pre-pass: build component-name → KSpiceType lookup so the valve pass can
            // detect ASC-controlled anti-surge valves regardless of their name.
            var typeByComp = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var node in systemMap.Models)
                typeByComp[(string)node["Name"] ?? ""] = (string)node["KSpiceType"] ?? "";

            // Pre-pass: build downstream adjacency map (component → set of components that list it
            // as an input source). Used by IsInterstageHX to detect inter-stage heat exchangers.
            var downstreamOf = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var node in systemMap.Models)
            {
                string nodeName = (string)node["Name"] ?? "";
                var nodeInputs2 = node["Inputs"] as JArray;
                if (nodeInputs2 == null) continue;
                foreach (var inp in nodeInputs2)
                {
                    string src = (string?)inp["Source"] ?? "";
                    int colon = src.IndexOf(':');
                    if (colon <= 0) continue;
                    string srcComp = src.Substring(0, colon);
                    if (!downstreamOf.ContainsKey(srcComp))
                        downstreamOf[srcComp] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    downstreamOf[srcComp].Add(nodeName);
                }
            }

            // Inverse adjacency map: component → set of components that feed INTO it.
            var upstreamOf = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in downstreamOf)
                foreach (var ds in kv.Value)
                {
                    if (!upstreamOf.ContainsKey(ds))
                        upstreamOf[ds] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    upstreamOf[ds].Add(kv.Key);
                }

            // True if any input to comp is an ASC controller — used to detect anti-surge
            // recirculation valves regardless of plant-specific naming conventions.
            bool IsAscControlledValve(string comp)
            {
                if (!upstreamOf.TryGetValue(comp, out var ups)) return false;
                foreach (var u in ups)
                    if (typeByComp.TryGetValue(u, out var t) &&
                        t.IndexOf("ASC", System.StringComparison.OrdinalIgnoreCase) >= 0)
                        return true;
                return false;
            }

            // Returns true if any Compressor is reachable downstream from compName,
            // skipping anti-surge recirculation paths which create backward loops that
            // would otherwise make HP after-coolers appear inter-stage.
            bool IsInterstageHX(string compName)
            {
                var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var queue   = new Queue<string>();
                if (downstreamOf.TryGetValue(compName, out var ds0))
                    foreach (var d in ds0) queue.Enqueue(d);
                while (queue.Count > 0)
                {
                    string curr = queue.Dequeue();
                    if (!visited.Add(curr)) continue;
                    string baseCurr2 = curr.Replace("_pf", "");
                    if (IsAscControlledValve(baseCurr2) || IsAscControlledValve(curr)) continue;
                    if (typeByComp.TryGetValue(curr, out var ct) && ct.Contains("Compressor"))
                        return true;
                    if (downstreamOf.TryGetValue(curr, out var nxt))
                        foreach (var d in nxt) queue.Enqueue(d);
                }
                return false;
            }

            // Returns true if this ControlValve/PipeFlow is the HP suction pressure junction node:
            // a merge point where ≥ 2 distinct inter-stage heat exchangers converge upstream.
            // A single valve on one LP train path returns false (only 1 inter-stage HX upstream).
            // BFS upstream through transparent K-Spice pipe segments (pv_*, network-*, Network type)
            // until all reachable equipment boundaries are found; requires ≥ 2 inter-stage HX
            // components in the result set.
            bool IsInterstageJunction(string compName)
            {
                var visited  = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var queue    = new Queue<string>();
                var foundHXs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var seedKey in new[] { compName, compName + "_pf" })
                    if (upstreamOf.TryGetValue(seedKey, out var init))
                        foreach (var u in init) queue.Enqueue(u);

                while (queue.Count > 0)
                {
                    string curr = queue.Dequeue();
                    if (!visited.Add(curr)) continue;
                    string baseCurr = curr.Replace("_pf", "");

                    if (!typeByComp.TryGetValue(curr, out var ct) &&
                        !typeByComp.TryGetValue(baseCurr, out ct))
                        ct = "";

                    // Also look up base component type (to classify _pf companions).
                    typeByComp.TryGetValue(baseCurr, out var baseCt);

                    if (ct.Contains("HeatExchanger") && IsInterstageHX(baseCurr))
                    {
                        foundHXs.Add(baseCurr);
                        continue; // record but don't traverse through the HX
                    }

                    // Stop at equipment boundaries. Also stop at _pf companions of ControlValves
                    // (e.g. 23PV_0001_pf is the pipe companion of ControlValve 23PV_0001;
                    // traversing through it would incorrectly route ESV2002 back to HX0001).
                    bool isEquipment = ct.Contains("Compressor") || ct.Contains("Separator")
                        || ct.Contains("Tank") || ct.Contains("Pump")
                        || ct.Contains("ControlValve") || ct.Contains("BlockValve")
                        || ct.Contains("HeatExchanger")
                        || (baseCt ?? "").Contains("ControlValve");
                    if (isEquipment) continue;

                    // Transparent: pipe volumes, PipeFlow companions of non-ControlValves, networks.
                    string low = baseCurr.ToLower();
                    bool isTransparent = low.StartsWith("pv") || low.StartsWith("pf_")
                        || low.StartsWith("network-") || ct == "" || ct.Contains("Network")
                        || ct.Contains("PipeFlow") || ct.Contains("PipeVolume")
                        || curr.EndsWith("_pf", System.StringComparison.OrdinalIgnoreCase);
                    if (isTransparent)
                        foreach (var key in new[] { curr, baseCurr, curr + "_pf" })
                            if (upstreamOf.TryGetValue(key, out var ups))
                                foreach (var u in ups) queue.Enqueue(u);
                }
                // Only a true merge-point junction has ≥ 2 distinct inter-stage HX trains upstream.
                return foundHXs.Count >= 2;
            }

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

                void AddState(string suffix, string[] inputs)
                {
                    var eq = new JObject();
                    eq["ID"]        = $"{baseName}_{suffix}";
                    eq["Component"] = baseName;
                    eq["State"]     = suffix;
                    eq["Inputs"]    = JArray.FromObject(inputs);
                    eq["Role"]      = suffix == "Control"             ? "Controller"
                                    : (suffix.EndsWith("Level") || suffix == "Pressure") ? "Volume"
                                    : "FlowEquipment";

                    // ControllerType distinguishes PID from ASC without relying on
                    // component-name heuristics (e.g. "starts with 23ASC").
                    if (suffix == "Control")
                        eq["ControllerType"] = ktype.Contains("GenericASC") ? "ASC" : "PID";

                    // Embed K-Spice PID tuning so IdentifyPidModel can reference them.
                    // Needed to normalise controller outputs that are in physical units
                    // (e.g. RPM for speed controllers) rather than percent [0, 100].
                    if (suffix == "Control" && !ktype.Contains("GenericASC"))
                    {
                        double? g   = (double?)pMap["Gain"];
                        double? ti  = (double?)pMap["IntegralTime"];
                        double? td  = (double?)pMap["DerivativeTime"];
                        double? rlo = (double?)pMap["RangeLowLimit"];
                        double? rhi = (double?)pMap["RangeHighLimit"];
                        double? olo = (double?)pMap["OutputRangeLowLimit"];
                        double? ohi = (double?)pMap["OutputRangeHighLimit"];
                        if (g  != null) eq["KSpice_Gain"]  = g.Value;
                        if (ti != null) eq["KSpice_Ti_s"]  = ti.Value;
                        if (td != null) eq["KSpice_Td_s"]  = td.Value;
                        if (rlo != null && rhi != null)
                            eq["KSpice_MeasRange"] = rhi.Value - rlo.Value;
                        if (olo != null && ohi != null)
                        {
                            eq["KSpice_OutRangeLow"]  = olo.Value;
                            eq["KSpice_OutRangeHigh"] = ohi.Value;
                        }
                    }

                    if (pMap["M"]        != null && suffix == "MassFlow") eq["Param"] = $"Cv: {pMap["M"]}";
                    if (pMap["Diameter"] != null && suffix == "Pressure")
                        eq["Param"] = $"D: {pMap["Diameter"]} L: {pMap["Length"]}";

                    modelDefinitions.Add(eq);
                }

                if (ktype.Contains("Separator") || ktype.Contains("Tank"))
                {
                    AddState("Pressure",    new[] { "UPSTREAM_FLOW", "DOWNSTREAM_COMPRESSOR_FLOW", "DOWNSTREAM_LIQUID_OUTFLOWS", "ANTISURGE_INFLOW" });
                    AddState("WaterLevel",  new[] { "UPSTREAM_FLOW", "DOWNSTREAM_FLOW" });
                    AddState("OilLevel",    new[] { "UPSTREAM_FLOW", "DOWNSTREAM_FLOW" });
                    // Downstream flows are excluded: including them creates large
                    // OLS canceling gains that blow up CL when outlet flows are mispredicted.
                    // Feed temperature + flow are stable boundaries sufficient for T_sep.
                    AddState("Temperature", new[] { "UPSTREAM_TEMP", "UPSTREAM_FLOW" });
                }
                else if (ktype.Contains("HeatExchanger"))
                {
                    AddState("MassFlow",    new[] { "DOWNSTREAM_FLOW_SUM" });
                    AddState("Pressure",    new[] { "UPSTREAM_PRESSURE" });
                    AddState("Temperature", new[] { "UPSTREAM_TEMP", $"{baseName}_MassFlow", "COOLING_TEMP", "PARTNER_FLOW" });
                }
                else if (ktype.Contains("ControlValve") || ktype.Contains("PipeFlow") || ktype.Contains("BlockValve"))
                {
                    bool isAntiSurge = IsAscControlledValve(baseName) || IsAscControlledValve(baseName + "_pf");
                    bool isInterstageJunction = !isAntiSurge && IsInterstageJunction(baseName);

                    if (isAntiSurge)
                    {
                        // Anti-surge recirculation valve: gas flows from the discharge side
                        // back to the compressor suction. CONTAINER_PRESSURE traverses upstream
                        // through HX/Compressor to find the suction separator pressure for P_out.
                        AddState("MassFlow",    new[] { "UPSTREAM_PRESSURE", "CONTAINER_PRESSURE", "LOCAL_CONTROL" });
                        AddState("Temperature", new[] { "UPSTREAM_TEMP", "UPSTREAM_PRESSURE", "CONTAINER_PRESSURE" });
                    }
                    else if (isInterstageJunction)
                    {
                        // Inter-stage header without a physical accumulation vessel: modeled
                        // as a synthetic mass-balance vessel using the same equation as a
                        // separator. UPSTREAM_FLOW + ANTISURGE_INFLOW act as m_in (+);
                        // DOWNSTREAM_COMPRESSOR_FLOW + SIBLING_ANTISURGE_FLOW act as m_out (−).
                        // NegateOutflows + SeparatorPressureIdentifier handle signs and fit.
                        AddState("Pressure",    new[] { "UPSTREAM_FLOW", "ANTISURGE_INFLOW", "DOWNSTREAM_COMPRESSOR_FLOW", "SIBLING_ANTISURGE_FLOW" });
                        AddState("MassFlow",    new[] { "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE", "LOCAL_CONTROL" });
                        AddState("Temperature", new[] { "UPSTREAM_TEMP", "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE" });
                    }
                    else
                    {
                        // When no controller is wired (LOCAL_CONTROL finds nothing),
                        // DynamicPlantRunner defaults to Assumed_100% (always open).
                        AddState("MassFlow",    new[] { "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE", "LOCAL_CONTROL" });
                        AddState("Temperature", new[] { "UPSTREAM_TEMP", "UPSTREAM_PRESSURE", "DOWNSTREAM_PRESSURE" });
                    }
                }
                else if (ktype.Contains("Compressor"))
                {
                    // Compressor flow uses suction pressure + speed; downstream-flow
                    // approaches failed due to operating-point mismatch between training
                    // and test sets and anti-surge anti-correlation during surge events.
                    AddState("MassFlow",    new[] { "SUCTION_PRESSURE", "LOCAL_CONTROL", $"{baseName}_Pressure", "ANTISURGE_RECIRC_FLOW" });
                    // Anti-surge recirculation is excluded from Pressure to avoid a short
                    // ASC→UV→Pressure→MassFlow→ASC cycle that amplifies CL errors. The
                    // recirculation effect is already captured indirectly via MassFlow.
                    AddState("Pressure",    new[] { "SUCTION_PRESSURE", $"{baseName}_MassFlow", "LOCAL_CONTROL" });
                    AddState("Temperature", new[] { "UPSTREAM_TEMP", $"{baseName}_Pressure", "UPSTREAM_PRESSURE" });
                    AddState("Speed",       new[] { "LOCAL_CONTROL" });
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
                    AddState("MassFlow",    new[] { "DOWNSTREAM_FLOW" });
                    AddState("Pressure",    new[] { "SUCTION_PRESSURE", "LOCAL_CONTROL", "PUMP_SPEED" });
                    AddState("Temperature", new[] { "UPSTREAM_TEMP", $"{baseName}_Pressure", "UPSTREAM_PRESSURE" });
                    // Motor speed is driven by torque balance (no external speed controller for
                    // fixed-frequency motors).  Model as data-driven function of the operating
                    // point so the closed-loop can predict speed without reading the CSV.
                    AddState("Speed",       new[] { "UPSTREAM_PRESSURE", "DOWNSTREAM_FLOW" });
                }
                else if (ktype.IndexOf("pid", System.StringComparison.OrdinalIgnoreCase) >= 0
                         || ktype.Contains("GenericASC")
                         || ktype.Contains("Controller"))
                {
                    if (ktype.Contains("GenericASC"))
                        AddState("Control", new[] { "MEASURED_FLOW", "MEASURED_PRESSURE", "MEASURED_SPEED" });
                    else
                        AddState("Control", new[] { "MEASURED_STATE" });
                }
                else
                {
                    // Boundary source / unrecognised type — treated as measured input.
                    // FlowElements measure DP → flow only; they have no absolute-pressure
                    // or temperature state, so skip those to avoid dead topology edges.
                    if (ktype.Contains("FlowElement") || ktype.Contains("FlowMeter"))
                    {
                        AddState("MassFlow", new string[0]);
                    }
                    else
                    {
                        AddState("MassFlow",    new string[0]);
                        AddState("Pressure",    new string[0]);
                        AddState("Temperature", new string[0]);
                    }
                }
            }

            exportMap = modelDefinitions;
        }
    }
}
