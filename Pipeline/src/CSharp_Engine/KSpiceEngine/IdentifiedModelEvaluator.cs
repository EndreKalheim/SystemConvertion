using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine
{
    /// <summary>
    /// Evaluates a single identified model (one entry from CS_Identified_Parameters.json)
    /// step-by-step on a stream of inputs, without re-identifying. Used by both:
    ///   - OpenLoopTestRunner — drives the model with CSV ground-truth inputs from a
    ///     held-out dataset (Test 2: generalisation on unseen data).
    ///   - ClosedLoopRunner   — drives the model with predictions from neighbour
    ///     models (Test 1: closed-loop simulation of the whole plant).
    ///
    /// One instance wraps one identified model. Construct, WarmStart, then Iterate per step.
    /// </summary>
    public class IdentifiedModelEvaluator
    {
        public string ID { get; }
        public string ModelType { get; }

        // Ordered list of (source keys, role) the model expects per step. The runner
        // uses these to look up which CSV column / which neighbour prediction to feed.
        // Format depends on ModelType — see ResolveInputContract below.
        // Settable so the ClosedLoopRunner can rewire valve inputs to use topology
        // edges (predicted upstream pressures) instead of the SimInputs (raw CSV
        // pressures) saved during training.
        public List<InputSlot> InputContract { get; set; }

        // Optional fall-through: the CSV signal mapping key for this model's output
        // (e.g. "23VA0001_Pressure"). Used by Boundary/Fallback to copy truth when
        // available, and by ClosedLoopRunner to warm-start from CSV.
        public string? OutputSignalKey { get; }

        // Underlying TSA / custom model object (one of these is non-null after construction).
        private UnitModel? unitModel;
        private CustomModels.ValvePhysicsModel? valveModel;
        private CustomModels.AntiSurgePhysicalModel? ascModel;
        private CustomModels.HeatExchangerTemperatureModel? hxTempModel;
        // Direct incremental PI state — replaces PidController to allow anti-windup.
        private bool pidActive;
        private double pidKp;
        private double pidTi_s;
        private double pidEprev;   // e(k-1) for incremental formula
        private double pidUnorm;   // previous output normalised to [0,100]

        // Integrator-style state for IdentifyLinear_IntegratedFlow.
        private double[]? integratorAccum;       // running ∫signal·dt per input
        private double[]? integratorLinearGains; // gains in the order of InputContract
        private bool[]?   integratorIsOutflow;   // negate if outflow
        private double    integratorBias;
        private double[]? integratorU0;          // mean of integrated input from training (offset)
        private double[]? integratorCurvatures;  // optional curvature gains: c*(accum-U0)^2/UNorm
        private double[]? integratorUNorm;       // normalization for curvature denominator
        // Anchors the integrator to the warm-start truth at t=0 so the model's
        // trajectory starts at the right value instead of the trainer's bias-Σg·U0
        // baseline. Without this anchor every closed-loop run would step by
        // (truth[0] − model_baseline) at t=1, throwing every downstream model off.
        private double   integratorAnchor;

        // NetMassBalance: single-gain mass balance, one accumulator over net (in − out).
        // Differs from IntegratedFlow by collapsing all input streams into one summed
        // net flow, eliminating the multicollinear per-stream gains that caused the
        // gas-pressure runaway when individual flow predictions drifted.
        private double netBalanceAccum;
        private double netBalanceGain;
        private bool[]? netBalanceIsOutflow;
        private double netBalanceAnchor;

        private bool pidPrimed;
        private double pidYMin = 0.0;
        private double pidYMax = 100.0;

        // Last good output for fallback returns.
        private double lastOutput;

        public class InputSlot
        {
            // Source key in the SignalMapping (e.g. "23VA0001_Pressure", "25ESV0001_MassFlow")
            // or a special role token: "@Setpoint", "@Measurement", "@P_in", "@P_out",
            // "@Flow", "@U" (the runner resolves these from comp + signal_map).
            public string SourceKey = "";
            // Edge label from topology, e.g. "m_in", "m_out", "P_out". Used for outflow
            // detection in IntegratedFlow models (m_out → negate).
            public string Label = "";
            // True if this slot represents an outflow whose sign must be flipped before
            // being fed to the integrator level model.
            public bool IsOutflow;
        }

        public IdentifiedModelEvaluator(string id, JObject p)
        {
            this.ID = id;
            this.ModelType = (string?)p["ModelType"] ?? "Unknown";
            this.InputContract = ResolveInputContract(id, p);
            this.OutputSignalKey = (string?)p["__OutputSignalKey"];
            BuildUnderlyingModel(p);
        }

        // ── Input-contract resolution ──────────────────────────────────────────
        // The contract tells the runner which series to feed in which order. The
        // *runner* handles "where does that series come from" — CSV column for
        // open-loop, neighbour prediction (or CSV for boundaries) for closed-loop.
        private static List<InputSlot> ResolveInputContract(string modelId, JObject p)
        {
            string mt = (string?)p["ModelType"] ?? "";
            int li = modelId.LastIndexOf('_');
            string comp = li > 0 ? modelId.Substring(0, li) : modelId;

            var slots = new List<InputSlot>();

            switch (mt)
            {
                case "PID":
                    slots.Add(new InputSlot { SourceKey = $"@Setpoint:{comp}",   Label = "y_set"  });
                    slots.Add(new InputSlot { SourceKey = $"@Measurement:{comp}", Label = "y_meas" });
                    return slots;

                case "AntiSurgePhysicalModel":
                    // ASC always wants three signals in this order: P_in, P_out, MassFlow.
                    // The trainer used FindInputSignals to pick them; in test runs we
                    // re-resolve via the topology in the runner.
                    slots.Add(new InputSlot { SourceKey = $"@P_in:{comp}",  Label = "P_in"  });
                    slots.Add(new InputSlot { SourceKey = $"@P_out:{comp}", Label = "P_out" });
                    slots.Add(new InputSlot { SourceKey = $"@Flow:{comp}",  Label = "MF"    });
                    return slots;

                case "ValvePhysicsModel":
                    // SimInputs were saved with the original CSV column names
                    // (P_in, P_out, U). Use those directly.
                    var simInputs = (JArray?)p["SimInputs"];
                    if (simInputs != null && simInputs.Count >= 3)
                    {
                        slots.Add(new InputSlot { SourceKey = (string?)simInputs[0] ?? "", Label = "P_in"  });
                        slots.Add(new InputSlot { SourceKey = (string?)simInputs[1] ?? "", Label = "P_out" });
                        slots.Add(new InputSlot { SourceKey = (string?)simInputs[2] ?? "", Label = "U"     });
                    }
                    return slots;

                case "HeatExchangerTemperatureModel":
                {
                    slots.Add(new InputSlot { SourceKey = $"@HX_Tin:{comp}",        Label = "UPSTREAM_TEMP"   });
                    // GasFlowSourceId: if the identifier was trained against a specific model
                    // (e.g. 23KA0001_MassFlow) instead of what the topology routes, use that
                    // same model ID directly so closed-loop uses the same signal as training.
                    string? gasFlowSrc = (string?)p["GasFlowSourceId"];
                    slots.Add(new InputSlot { SourceKey = gasFlowSrc ?? $"@HX_Flow:{comp}", Label = "MassFlow" });
                    slots.Add(new InputSlot { SourceKey = $"@HX_CoolTemp:{comp}",    Label = "COOLING_TEMP"    });
                    slots.Add(new InputSlot { SourceKey = $"@HX_PartnerFlow:{comp}", Label = "PARTNER_FLOW"    });
                    return slots;
                }

                case "IdentifyLinear":
                case "UnitIdentifier":
                case "UnitIdentifier_SignedFlows":
                case "UnitIdentifier_NetSignedFlow":
                case "IdentifyLinear_IntegratedFlow":
                case "NetMassBalance":
                    // InputNames is a list like "25ESV0001_MassFlow(m_in)". Strip the
                    // parens and keep the resolved key + label so the runner can look
                    // each one up in the signal map (or in current/prev predictions).
                    var inputNames = (JArray?)p["InputNames"];
                    if (inputNames != null)
                    {
                        foreach (var nameTok in inputNames)
                        {
                            string raw = (string?)nameTok ?? "";
                            int paren = raw.IndexOf('(');
                            string key = paren > 0 ? raw.Substring(0, paren).Trim() : raw.Trim();
                            string label = "";
                            if (paren > 0)
                            {
                                int close = raw.IndexOf(')', paren);
                                if (close > paren) label = raw.Substring(paren + 1, close - paren - 1);
                            }
                            // The trainer can append "_neg" to outflow names for the
                            // integrator level model — strip it. The IsOutflow flag
                            // captures the same semantic.
                            if (key.EndsWith("_neg")) key = key.Substring(0, key.Length - 4);
                            bool isOutflow = label == "m_out" || label == "m_sum" || label.StartsWith("mass_out");
                            slots.Add(new InputSlot { SourceKey = key, Label = label, IsOutflow = isOutflow });
                        }
                    }
                    return slots;

                case "Boundary":
                case "Fallback":
                    return slots; // no inputs needed; runner copies truth

                default:
                    return slots;
            }
        }

        private void BuildUnderlyingModel(JObject p)
        {
            switch (ModelType)
            {
                case "PID":
                {
                    pidKp   = (double?)p["Kp"]    ?? 0.0;
                    pidTi_s = (double?)p["Ti_s"]  ?? double.PositiveInfinity;
                    pidYMin = (double?)p["YMin"]   ?? 0.0;
                    pidYMax = (double?)p["YMax"]   ?? 100.0;
                    pidActive = true;
                    return;
                }

                case "AntiSurgePhysicalModel":
                {
                    ascModel = new CustomModels.AntiSurgePhysicalModel(
                        ID, new[] { "P_in", "P_out", "Flow" }, ID);
                    var arch = (string)p["Architecture"] ?? "LinearOLS";
                    ascModel.modelParameters.Architecture = arch;
                    if (arch == "KickBased")
                    {
                        ascModel.modelParameters.SurgeProxy_MF_Coeff       = (double?)p["SurgeProxy_MF_Coeff"]       ?? 1.0;
                        ascModel.modelParameters.SurgeProxy_a              = (double?)p["SurgeProxy_a"]              ?? 0.0;
                        ascModel.modelParameters.SurgeProxy_b              = (double?)p["SurgeProxy_b"]              ?? -35.0;
                        ascModel.modelParameters.KickThreshold             = (double?)p["KickThreshold"]             ?? 0.0;
                        ascModel.modelParameters.HoldThreshold             = (double?)p["HoldThreshold"]             ?? 0.0;
                        ascModel.modelParameters.KickRate_PrcPerSec        = (double?)p["KickRate_PrcPerSec"]        ?? 0.0;
                        ascModel.modelParameters.KickGain_PrcPerSecPerUnit = (double?)p["KickGain_PrcPerSecPerUnit"] ?? 0.0;
                        ascModel.modelParameters.RampDown_PrcPerMin        = (double?)p["RampDown_PrcPerMin"]        ?? 60.0;
                        ascModel.modelParameters.RampDecay_Tau_s           = (double?)p["RampDecay_Tau_s"]           ?? 0.0;
                        ascModel.modelParameters.SurgeMargin_LP_Tau_s      = (double?)p["SurgeMargin_LP_Tau_s"]      ?? 0.0;
                    }
                    else
                    {
                        var fNames = (JArray)p["FeatureNames"];
                        var fWts   = (JArray)p["FeatureWeights"];
                        if (fNames != null) ascModel.modelParameters.FeatureNames   = fNames.Select(t => (string)t).ToArray();
                        if (fWts   != null) ascModel.modelParameters.FeatureWeights = fWts.Select(t => (double)t).ToArray();
                        ascModel.modelParameters.LPFilter_Tau_s = (double?)p["LPFilter_Tau_s"] ?? 0.0;
                        ascModel.modelParameters.OpenTime_s     = (double?)p["OpenTime_s"]     ?? 5.0;
                        ascModel.modelParameters.CloseTime_s    = (double?)p["CloseTime_s"]    ?? 60.0;
                    }
                    return;
                }

                case "ValvePhysicsModel":
                {
                    valveModel = new CustomModels.ValvePhysicsModel(
                        ID, new[] { "P_in", "P_out", "U" }, ID);
                    valveModel.modelParameters.Cv                  = (double?)p["Cv"]                  ?? 100.0;
                    valveModel.modelParameters.DensityTuningFactor = (double?)p["DensityTuningFactor"] ?? 1.0;
                    return;
                }

                case "HeatExchangerTemperatureModel":
                {
                    hxTempModel = new CustomModels.HeatExchangerTemperatureModel(
                        ID, new[] { "Tin", "MassFlow", "CoolTemp", "PartnerFlow" }, ID);
                    hxTempModel.modelParameters.Subtype       = (string?)p["Subtype"]        ?? "GasSide";
                    hxTempModel.modelParameters.Bias          = (double?)p["Bias"]          ?? 0.0;
                    hxTempModel.modelParameters.Alpha         = (double?)p["Alpha"]         ?? 0.5;
                    hxTempModel.modelParameters.GainGas       = (double?)p["GainGas"]       ?? 0.0;
                    hxTempModel.modelParameters.GainWater     = (double?)p["GainWater"]     ?? 0.0;
                    hxTempModel.modelParameters.CapacityRatio = (double?)p["CapacityRatio"] ?? 1.0;
                    return;
                }

                case "IdentifyLinear":
                case "UnitIdentifier":
                case "UnitIdentifier_SignedFlows":
                case "UnitIdentifier_NetSignedFlow":
                {
                    var unitParams = new UnitParameters
                    {
                        TimeConstant_s = (double?)p["TimeConstant_s"] ?? 0.0,
                        TimeDelay_s    = (double?)p["TimeDelay_s"]    ?? 0.0,
                        Bias           = (double?)p["Bias"]           ?? 0.0
                    };
                    var gains = (JArray)p["LinearGains"];
                    if (gains != null) unitParams.LinearGains = gains.Select(t => (double)t).ToArray();
                    var u0 = (JArray)p["U0"];
                    if (u0 != null) unitParams.U0 = u0.Select(t => (double)t).ToArray();
                    var unorm = (JArray)p["UNorm"];
                    if (unorm != null) unitParams.UNorm = unorm.Select(t => (double)t).ToArray();
                    var curv = (JArray)p["Curvatures"];
                    if (curv != null) unitParams.Curvatures = curv.Select(t => (double)t).ToArray();

                    unitModel = new UnitModel(unitParams, ID);
                    // NetSignedFlow: single virtual input "F_net" — the unitModel needs one ID
                    var inputNames = ModelType == "UnitIdentifier_NetSignedFlow"
                        ? new[] { "F_net" }
                        : InputContract.Select(s => s.SourceKey).ToArray();
                    unitModel.SetInputIDs(inputNames);
                    return;
                }

                case "IdentifyLinear_IntegratedFlow":
                {
                    integratorBias = (double?)p["Bias"] ?? 0.0;
                    var gains = (JArray)p["LinearGains"];
                    integratorLinearGains = gains?.Select(t => (double)t).ToArray() ?? new double[0];
                    integratorIsOutflow = InputContract.Select(s => s.IsOutflow).ToArray();
                    integratorAccum = new double[InputContract.Count];
                    var u0 = (JArray)p["U0"];
                    integratorU0 = u0?.Select(t => (double)t).ToArray()
                                   ?? new double[integratorLinearGains.Length];
                    var curv = (JArray)p["Curvatures"];
                    if (curv != null && curv.Any(t => !double.IsNaN((double)t)))
                        integratorCurvatures = curv.Select(t => (double)t).ToArray();
                    var unorm = (JArray)p["UNorm"];
                    integratorUNorm = unorm?.Select(t => (double)t).ToArray();
                    return;
                }

                case "NetMassBalance":
                {
                    netBalanceGain = (double?)p["Gain"] ?? 0.0;
                    netBalanceIsOutflow = InputContract.Select(s => s.IsOutflow).ToArray();
                    netBalanceAccum = 0.0;
                    return;
                }

            }
        }

        /// <summary>
        /// Reset internal model state and prime initial output (used at t=0).
        /// initialOutput should be the CSV ground-truth value at t=0 for the model's
        /// output signal — gives the prediction a correct starting point so error
        /// accumulation in closed-loop is measured from t=0.
        /// </summary>
        public void WarmStart(double initialOutput)
        {
            lastOutput = initialOutput;
            pidPrimed  = false;
            if (ascModel != null)      ascModel.WarmStart(null, initialOutput);
            if (valveModel != null)    valveModel.WarmStart(null, initialOutput);
            if (hxTempModel != null)   hxTempModel.WarmStart(null, initialOutput);
            if (integratorAccum != null)
            {
                Array.Clear(integratorAccum, 0, integratorAccum.Length);
                // Anchor: trainer fitted y(t) = Bias + Σ gain·(accum(t) − U0). Starting
                // accum=0 gives the trainer's natural t=0 prediction (≠ truth in general).
                // Save the difference so subsequent Iterate calls produce
                //   y(t) = initialOutput + Σ gain·(accum(t) − U0_t0)
                // i.e. the integrator follows changes from the warm-start point.
                integratorAnchor = initialOutput;
            }
            if (ModelType == "NetMassBalance")
            {
                netBalanceAccum  = 0.0;
                netBalanceAnchor = initialOutput;
            }
        }

        /// <summary>
        /// Run one timestep. inputs[] is in InputContract order.
        /// </summary>
        public double Iterate(double[] inputs, double timeBase_s, double bias = 0.0)
        {
            if (ModelType == "Boundary" || ModelType == "Fallback")
                return lastOutput; // runner is responsible for filling these from CSV

            if (pidActive)
            {
                if (inputs == null || inputs.Length < 2) return lastOutput;
                double sp = inputs[0];
                double pv = inputs[1];
                double yRange = pidYMax - pidYMin;
                bool hasPhysRange = yRange > 100 + 1e-6;
                if (!pidPrimed)
                {
                    pidUnorm = hasPhysRange
                        ? Math.Max(0, Math.Min(100, (lastOutput - pidYMin) / yRange * 100.0))
                        : Math.Max(0, Math.Min(100, lastOutput));
                    pidEprev = sp - pv;
                    pidPrimed = true;
                    return lastOutput;
                }
                double e = sp - pv;
                double dtOverTi = pidTi_s > 1e-9 ? timeBase_s / pidTi_s : 0.0;
                double du = pidKp * ((e - pidEprev) + dtOverTi * e);
                pidUnorm = Math.Max(0, Math.Min(100, pidUnorm + du));
                pidEprev = e;
                double y = hasPhysRange ? pidUnorm / 100.0 * yRange + pidYMin : pidUnorm;
                lastOutput = y;
                return y;
            }

            if (ascModel != null)
            {
                if (inputs == null || inputs.Length < 3) return lastOutput;
                double y = ascModel.Iterate(inputs, timeBase_s)[0];
                lastOutput = y;
                return y;
            }

            if (valveModel != null)
            {
                if (inputs == null || inputs.Length < 3) return lastOutput;
                double y = valveModel.Iterate(inputs, timeBase_s)[0];
                lastOutput = y;
                return y;
            }

            if (hxTempModel != null)
            {
                if (inputs == null || inputs.Length < 1) return lastOutput;
                double y = hxTempModel.Iterate(inputs, timeBase_s)[0];
                lastOutput = y;
                return y;
            }

            if (unitModel != null)
            {
                if (inputs == null) return lastOutput;
                // UnitIdentifier_NetSignedFlow: sum all individual flows (outflows negated)
                // into a single net-flow scalar, then pass as the sole UnitModel input.
                // This eliminates the ±K / ∓K multicollinearity from per-stream OLS.
                // UnitIdentifier_SignedFlows: per-stream gains; negate outflows, pass full array.
                // Standard UnitIdentifier: raw inputs — no negation.
                double[] adjInputs = inputs;
                if (ModelType == "UnitIdentifier_NetSignedFlow" && InputContract.Count > 0)
                {
                    double net = 0.0;
                    for (int i = 0; i < InputContract.Count && i < inputs.Length; i++)
                    {
                        double v = inputs[i];
                        if (double.IsNaN(v)) v = 0.0;
                        if (InputContract[i].IsOutflow) v = -v;
                        net += v;
                    }
                    adjInputs = new double[] { net };
                }
                else if (ModelType == "UnitIdentifier_SignedFlows"
                    && InputContract.Count > 0 && inputs.Length >= InputContract.Count
                    && InputContract.Any(s => s.IsOutflow))
                {
                    adjInputs = inputs.ToArray();
                    for (int i = 0; i < InputContract.Count && i < adjInputs.Length; i++)
                        if (InputContract[i].IsOutflow) adjInputs[i] = -adjInputs[i];
                }
                double y = unitModel.Iterate(adjInputs, timeBase_s)[0];
                if (double.IsNaN(y)) return lastOutput;
                lastOutput = y;
                return y;
            }

            if (integratorAccum != null && integratorLinearGains != null)
            {
                if (inputs == null || inputs.Length < integratorAccum.Length) return lastOutput;
                // Pure integrator from warm-start anchor:
                //   y(t) = y(0) + Σ gain_i · ∫_0^t F_signed_i dτ
                // The gains are slopes (dy/d(accum)) which are independent of the
                // trainer's Bias and U0 — those just absorb operating-point offset
                // during fitting. Anchoring at truth[0] gives the correct initial
                // condition; the gains then drive change from there. This is the
                // textbook mass-balance form, not a regression curve.
                double y = integratorAnchor;
                for (int i = 0; i < integratorAccum.Length; i++)
                {
                    double v = inputs[i];
                    if (double.IsNaN(v)) v = 0.0;
                    if (integratorIsOutflow[i]) v = -v;
                    integratorAccum[i] += v * timeBase_s;
                    y += integratorLinearGains[i] * integratorAccum[i];
                    // Optional curvature: c*(accum - U0)^2 / UNorm
                    if (integratorCurvatures != null && i < integratorCurvatures.Length &&
                        !double.IsNaN(integratorCurvatures[i]))
                    {
                        double dev   = integratorAccum[i] - (integratorU0 != null && i < integratorU0.Length ? integratorU0[i] : 0.0);
                        double uNorm = (integratorUNorm != null && i < integratorUNorm.Length && integratorUNorm[i] > 0) ? integratorUNorm[i] : 1.0;
                        y += integratorCurvatures[i] * dev * dev / uNorm;
                    }
                }
                lastOutput = y;
                return y;
            }

            if (ModelType == "NetMassBalance" && netBalanceIsOutflow != null)
            {
                if (inputs == null || inputs.Length < netBalanceIsOutflow.Length) return lastOutput;
                // Single-gain mass balance:
                //   y(t) = y(0) + Gain · ∫(Σ m_in − Σ m_out) dt
                // One physical gain on the *net* flow — no per-stream weighting that
                // could turn into the +K / −K mode pair we saw with multi-input OLS.
                double net = 0.0;
                for (int i = 0; i < netBalanceIsOutflow.Length; i++)
                {
                    double v = inputs[i];
                    if (double.IsNaN(v)) v = 0.0;
                    if (netBalanceIsOutflow[i]) v = -v;
                    net += v;
                }
                netBalanceAccum += net * timeBase_s;
                double y = netBalanceAnchor + netBalanceGain * netBalanceAccum;
                lastOutput = y;
                return y;
            }

            return lastOutput;
        }
    }
}
