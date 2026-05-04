using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace KSpiceEngine
{
    /// <summary>
    /// Maps a CSV signal name (e.g. "23LIC0001:Measurement", "25ESV0001_pf:InletPressure")
    /// to the model_id whose prediction physically represents that signal.
    ///
    /// During training every model is identified against ground-truth CSV inputs, so the
    /// runner only needed signalMap (model_id → its own primary CSV column). For the
    /// closed-loop test we need the inverse plus chain-tracing: a controller's
    /// "Measurement" port reads via a K-spice AlarmTransmitter from the underlying
    /// physical state, and we want to substitute that state's predicted value instead
    /// of the alarm CSV (which would be cheating).
    ///
    /// The map is built from the K-spice system map by walking each component's Inputs:
    ///
    ///   23LIC0001:Measurement ←(K-spice wiring)← 23LT0001:MeasuredValue
    ///   23LT0001 (AlarmTransmitter): Inputs[Destination=Value] ← 23VA0001:LevelHeavyPhaseFeedSideWeir
    ///   signalMap["23VA0001_WaterLevel"] = "23VA0001:LevelHeavyPhaseFeedSideWeir"
    ///   ⇒ 23LIC0001:Measurement ≡ 23VA0001_WaterLevel (model)
    ///
    /// The traversal is generic: it follows the FIRST signal-bearing Input port of any
    /// component (skipping pure parameter wires like ValveCv, GridProfile, etc.). This
    /// keeps the same code working for new K-spice systems — we don't hard-code any
    /// instrument tag prefixes.
    /// </summary>
    public class SignalEquivalenceMap
    {
        private readonly Dictionary<string, JObject> componentByName;
        private readonly Dictionary<string, string>  csvToModelId;
        // Memoised resolution results so repeat lookups during the simulation loop
        // don't re-walk the system map. Sentinel "" means "we tried but found nothing".
        private readonly Dictionary<string, string>  cache = new(StringComparer.OrdinalIgnoreCase);

        // (PARAM_DESTINATION_PREFIXES previously tried to filter "non-signal" inputs
        // during a generic any-input traversal. The new component-aware traversal
        // doesn't need it — each handled K-spice class names its passthrough port
        // explicitly.)

        public SignalEquivalenceMap(JArray models, Dictionary<string, string> signalMap, ISet<string> validModelIds = null)
        {
            componentByName = new Dictionary<string, JObject>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in models)
            {
                string name = (string)m["Name"] ?? "";
                if (!componentByName.ContainsKey(name))
                    componentByName[name] = (JObject)m;
            }

            // Inverse signalMap: CSV column → model_id. Only include entries where
            // the model_id is an actual predicted model — signalMap also contains
            // convenience entries (e.g. "23LIC0001_Measurement") that name CSV columns
            // for trainer lookup but never get a prediction array. Including those
            // would short-circuit the transmitter trace and silently fall back to raw
            // CSV (the cheating we're trying to stop).
            csvToModelId = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in signalMap)
            {
                if (validModelIds != null && !validModelIds.Contains(kv.Key)) continue;
                if (!csvToModelId.ContainsKey(kv.Value))
                    csvToModelId[kv.Value] = kv.Key;
            }
        }

        /// <summary>The base CSV-to-model_id map (signalMap inverse), no chain tracing.</summary>
        public IReadOnlyDictionary<string, string> CsvToModelId => csvToModelId;

        /// <summary>
        /// Resolve any CSV signal name to a model_id by walking the K-spice wiring graph.
        /// Returns null when no equivalent modelled state exists (true boundary signal).
        /// </summary>
        public string TryResolve(string csvSignal)
        {
            if (string.IsNullOrEmpty(csvSignal)) return null;
            if (cache.TryGetValue(csvSignal, out string cached))
                return cached.Length == 0 ? null : cached;

            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string result = ResolveRecursive(csvSignal, visited);
            cache[csvSignal] = result ?? "";
            return result;
        }

        private string ResolveRecursive(string signal, HashSet<string> visited)
        {
            if (string.IsNullOrEmpty(signal)) return null;

            // Direct hit: this signal IS a model's primary output.
            if (csvToModelId.TryGetValue(signal, out string direct)) return direct;

            // Cycle guard.
            if (!visited.Add(signal)) return null;

            // Parse "Comp:Port".
            int colon = signal.IndexOf(':');
            if (colon <= 0) return null;
            string comp = signal.Substring(0, colon);
            string port = signal.Substring(colon + 1);

            // Look up using the full name including any _pf suffix — _pf variants ARE
            // registered in componentByName (they come from the full models array) and
            // carry the InletStream inputs that their base component lacks.
            if (!componentByName.TryGetValue(comp, out JObject compObj)) return null;
            string ktype = (string)compObj["KSpiceType"] ?? "";
            var inputs = (JArray)compObj["Inputs"];
            if (inputs == null || inputs.Count == 0) return null;

            // Only trace through specific *passthrough* component classes. Anything
            // that physically transforms its inputs (compressors, heat exchangers,
            // separators, valves, motors) gets ZERO tracing — their outputs are not
            // equivalent to any single input. Walking those would let the trace
            // wander into unrelated parts of the plant (we hit one such bug where
            // 23TIC0003.Measurement traced through a compressor's Speed input back
            // to the gas-pressure controller).
            //
            // Passthrough classes:
            //   * Transmitter: single Value input drives all outputs.
            //   * PidController: Measurement port has a single defining source.
            //   * PipeVolume: pressure is the same at all inlets (isobaric node);
            //     for T/F only single-inlet volumes are traceable.
            //   * Valve/BlockValve/PipeFlow: inlet and outlet pressure are ~equal
            //     at measurement timescale — trace InletStream upstream.
            //   * Separator/Tank: all outlet pressure ports equal the vessel pressure.

            bool isPressurePort = port.Equals("Pressure", StringComparison.OrdinalIgnoreCase)
                                  || port.EndsWith(".p", StringComparison.OrdinalIgnoreCase);

            if (ktype.IndexOf("Transmitter", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Pick the Value input. Many transmitters expose multiple outputs
                // (MeasuredValue, AlarmHigh, etc.) — they all derive from "Value".
                foreach (var inp in inputs)
                {
                    string dst = (string)inp["Destination"] ?? "";
                    if (string.Equals(dst, "Value", StringComparison.OrdinalIgnoreCase))
                        return ResolveRecursive((string)inp["Source"], visited);
                }
                return null;
            }

            if (ktype.IndexOf("Controller", StringComparison.OrdinalIgnoreCase) >= 0
                || ktype.IndexOf("Pid", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Only the Measurement port has a clean equivalence — Setpoint is
                // operator input, ControllerOutput is the controller's *output*
                // (which is itself modelled as the controller's prediction), and
                // DcsFeedback / InputSwitch are HMI plumbing.
                if (string.Equals(port, "Measurement", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var inp in inputs)
                    {
                        string dst = (string)inp["Destination"] ?? "";
                        if (string.Equals(dst, "Measurement", StringComparison.OrdinalIgnoreCase))
                            return ResolveRecursive((string)inp["Source"], visited);
                    }
                }
                return null;
            }

            if (ktype.IndexOf("PipeVolume", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                // Collect all InletStream sources from this PipeVolume.
                var inlets = new List<JObject>();
                foreach (var inp in inputs)
                {
                    string dst = (string)inp["Destination"] ?? "";
                    if (!dst.StartsWith("InletStream", StringComparison.OrdinalIgnoreCase)) continue;
                    inlets.Add((JObject)inp);
                }
                if (inlets.Count == 0) return null;

                // For Pressure queries (including .p stream ports), all inlets share
                // the same pressure node — try each in order. For T/F, multi-inlet
                // mixing requires a dedicated model; only trace single-inlet volumes.
                bool isPressureQuery = isPressurePort;
                if (inlets.Count > 1 && !isPressureQuery)
                    return null;

                foreach (var sole in inlets)
                {
                    string upstream = (string)sole["Source"]; // e.g. "23ESV0002_pf:OutletStream"
                    int uc = upstream.IndexOf(':');

                    if (isPressureQuery && uc > 0)
                    {
                        // Prefer querying the upstream component's canonical Pressure
                        // port first — this keeps the port name consistent through the
                        // chain so subsequent handlers receive a clean "Pressure" query.
                        string upComp = upstream.Substring(0, uc);
                        var hit = ResolveRecursive($"{upComp}:Pressure", visited);
                        if (hit != null) return hit;
                    }

                    string mapped = MapPortToStreamSuffix(upstream, isPressureQuery ? "Pressure" : port);
                    if (mapped != null)
                    {
                        var hit = ResolveRecursive(mapped, visited);
                        if (hit != null) return hit;
                    }
                    var fallback = ResolveRecursive(upstream, visited);
                    if (fallback != null) return fallback;

                    if (!isPressureQuery) break; // single-inlet path already exhausted
                }
                return null;
            }

            // Valve / BlockValve / PipeFlow: pressure passes through the valve body
            // without transformation at measurement timescale — trace InletStream upstream.
            // (Temperature and flow are NOT equivalent across a valve.)
            if (isPressurePort && (
                ktype.IndexOf("Valve",    StringComparison.OrdinalIgnoreCase) >= 0 ||
                ktype.IndexOf("PipeFlow", StringComparison.OrdinalIgnoreCase) >= 0))
            {
                foreach (var inp in inputs)
                {
                    string dst = (string)inp["Destination"] ?? "";
                    if (!dst.StartsWith("InletStream", StringComparison.OrdinalIgnoreCase)) continue;
                    string upstream = (string)inp["Source"]; // e.g. "pv_23L0003A:OutletStream[0]"
                    int uc = upstream.IndexOf(':');
                    if (uc > 0)
                    {
                        // Canonical Pressure port — keeps the port name clean through the chain.
                        string upComp = upstream.Substring(0, uc);
                        var hit = ResolveRecursive($"{upComp}:Pressure", visited);
                        if (hit != null) return hit;
                    }
                    string mapped = MapPortToStreamSuffix(upstream, "Pressure");
                    if (mapped != null)
                    {
                        var hit = ResolveRecursive(mapped, visited);
                        if (hit != null) return hit;
                    }
                    var fallback = ResolveRecursive(upstream, visited);
                    if (fallback != null) return fallback;
                }
                return null;
            }

            // Separator/Tank: any pressure-typed outlet port is equivalent to the
            // vessel's own Pressure state (one shared pressure node).
            if (isPressurePort && (
                ktype.IndexOf("Separator", StringComparison.OrdinalIgnoreCase) >= 0 ||
                ktype.IndexOf("Tank",      StringComparison.OrdinalIgnoreCase) >= 0))
            {
                return ResolveRecursive($"{comp}:Pressure", visited);
            }

            return null;
        }

        // Convert a (stream-bearing source, requested state port) pair into the
        // K-spice convention of `Comp:Stream.suffix` so the inverted signal map can
        // resolve it directly. Returns null when the conversion isn't well-defined.
        private static string MapPortToStreamSuffix(string upstream, string port)
        {
            if (string.IsNullOrEmpty(upstream) || !upstream.Contains(':')) return null;
            string suffix = port switch
            {
                _ when port.Equals("Temperature", StringComparison.OrdinalIgnoreCase) => ".t",
                _ when port.Equals("Pressure",    StringComparison.OrdinalIgnoreCase) => ".p",
                _ when port.Equals("MassFlow",    StringComparison.OrdinalIgnoreCase) => ".f",
                _ => null
            };
            if (suffix == null) return null;
            // upstream looks like "23HX0001:OutletStream" or "...:OutletStream[0]" — both
            // legal in K-spice. The state-suffix attaches to the stream port name.
            return upstream + suffix;
        }
    }
}
