using System;
using System.Collections.Generic;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class AntiSurgeParameters : ModelParametersBaseClass
    {
        // ── Architecture label / branch selector ───────────────────────────
        //   "LinearOLS"  – static linear surge proxy + LP + asymmetric rate-limit
        //                   (the safe baseline; fits the trace by curve-following).
        //   "KickBased"  – TSA PidAntiSurgeParams-style controller:
        //                   while in-surge, accumulate kickPrcPerSec·dt → fast open;
        //                   while safe, ramp down at ffRampDownRatePrcPerMin.
        //                   Captures *why* the valve opens and *when* it can close.
        public string Architecture { get; set; } = "LinearOLS";

        // ── Linear-OLS (architecture = LinearOLS) ───────────────────────────
        //   target = Σ w_i · feature_i(P_in, P_out, MF). Vocabulary in
        //   EvaluateFeature(): "P_in", "P_out", "MF", "DP", "MF2",
        //   "DP_over_MF2", "Const".
        public string[] FeatureNames   { get; set; }
        public double[] FeatureWeights { get; set; }
        public double LPFilter_Tau_s   { get; set; } = 0.0;
        public double OpenTime_s       { get; set; } = 5.0;
        public double CloseTime_s      { get; set; } = 60.0;

        // ── Kick-based (architecture = KickBased) ──────────────────────────
        // surge_distance = MF + SurgeProxy_a · DP + SurgeProxy_b   (DP = P_out-P_in)
        //
        // Three regimes via hysteresis (Schmitt trigger), KickThreshold ≤ HoldThreshold:
        //   surge_distance < KickThreshold   →  IN-SURGE: kick the valve open
        //   surge_distance > HoldThreshold   →  SAFE: ramp down (slow close)
        //   between the two                  →  HOLD: u stays at u(k-1)
        //
        // The hold band reproduces the K-Spice held-open plateau: once kicked open,
        // the operating point typically returns to a marginal region near the surge
        // line (not yet far away enough to be "fully safe"), so the controller
        // refuses to close. Only when the system has clearly recovered does the
        // slow ramp-down begin.
        //
        // Kick rate is the TSA PidAntiSurgeParams kickPrcPerSec (constant) plus an
        // optional K-Spice FastProportionalGain-style proportional term:
        //   rate = KickRate_PrcPerSec  +  KickGain · max(0, KickThreshold - surge_distance)
        public double SurgeProxy_a              { get; set; } = 0.0;
        public double SurgeProxy_b              { get; set; } = -35.0;
        public double KickThreshold            { get; set; } = 0.0;
        public double HoldThreshold            { get; set; } = 0.0;   // ≥ KickThreshold; defines hold band
        public double KickRate_PrcPerSec        { get; set; } = 0.0;   // constant baseline kick
        public double KickGain_PrcPerSecPerUnit { get; set; } = 0.0; // proportional kick gain

        // ── Ramp-down shape ────────────────────────────────────────────────
        // Combined linear + exponential decay:
        //   rate = (RampDown_PrcPerMin / 60)  +  uPrev / RampDecay_Tau_s
        // Linear part = constant floor that ensures u always reaches 0.
        // Exponential part = u-proportional, fast at high u and slow at low u
        // (matches the K-Spice trace's curved decay: rapid 52→20 then slow 20→0).
        // Set RampDecay_Tau_s = 0 to disable the exponential term (pure linear).
        public double RampDown_PrcPerMin        { get; set; } = 60.0;
        public double RampDecay_Tau_s           { get; set; } = 0.0;

        // ── Surge-margin memory ────────────────────────────────────────────
        // Optional first-order LP filter on the surge_distance signal. Gives
        // the controller a "memory" of recent surge events: brief excursions
        // out of the surge region don't immediately kill the held-open state.
        // Effectively extends the HOLD band's *temporal* width. Tau in seconds.
        // Set to 0 to use the raw surge_distance directly.
        public double SurgeMargin_LP_Tau_s      { get; set; } = 0.0;

        // ── Output bounds ──────────────────────────────────────────────────
        public double UMax { get; set; } = 100.0;
        public double UMin { get; set; } = 0.0;
    }

    /// <summary>
    /// Anti-Surge Controller surrogate. Two architectures share the same
    /// (P_in, P_out, MF) input contract; the runtime branch is chosen by
    /// AntiSurgeParameters.Architecture.
    ///
    ///   "KickBased" (recommended): mimics TSA's PidAntiSurgeParams logic
    ///     and the K-Spice GenericASC behavior — fast kick on surge entry,
    ///     held-open while disturbed, slow ramp-down once recovered.
    ///
    ///   "LinearOLS": legacy static-linear-proxy + LP + rate-limit. Fits
    ///     the trace numerically but the OLS weights tend to overfit
    ///     P_out noise (huge cancelling weights), so it doesn't generalise.
    /// </summary>
    public class AntiSurgePhysicalModel : ModelBaseClass, ISimulatableModel
    {
        public AntiSurgeParameters modelParameters = new AntiSurgeParameters();
        private double uPrev = 0.0;
        private double targetLP = 0.0;
        private bool lpInitialized = false;
        private double surgeMarginLP = 0.0;
        private bool surgeMarginLPInitialized = false;

        public AntiSurgePhysicalModel()
        {
            this.processModelType = ModelType.SubProcess;
        }

        public AntiSurgePhysicalModel(string id, string[] inputIDs, string outputID = null) : this()
        {
            this.ID = id;
            this.outputID = outputID ?? id;
            this.ModelInputIDs = inputIDs;
        }

        public override int GetLengthOfInputVector() { return (this.ModelInputIDs != null) ? this.ModelInputIDs.Length : 0; }
        public override SignalType GetOutputSignalType() { return SignalType.Output_Y; }
        public bool IsModelSimulatable(out string explanationStr) { explanationStr = "OK"; return true; }

        public ISimulatableModel Clone(string cloneID)
        {
            return new AntiSurgePhysicalModel(cloneID, (string[])this.ModelInputIDs.Clone(), this.outputID)
            {
                modelParameters = new AntiSurgeParameters()
                {
                    Architecture        = this.modelParameters.Architecture,
                    FeatureNames        = (string[])this.modelParameters.FeatureNames?.Clone(),
                    FeatureWeights      = (double[])this.modelParameters.FeatureWeights?.Clone(),
                    LPFilter_Tau_s      = this.modelParameters.LPFilter_Tau_s,
                    OpenTime_s          = this.modelParameters.OpenTime_s,
                    CloseTime_s         = this.modelParameters.CloseTime_s,
                    SurgeProxy_a              = this.modelParameters.SurgeProxy_a,
                    SurgeProxy_b              = this.modelParameters.SurgeProxy_b,
                    KickThreshold            = this.modelParameters.KickThreshold,
                    HoldThreshold            = this.modelParameters.HoldThreshold,
                    KickRate_PrcPerSec        = this.modelParameters.KickRate_PrcPerSec,
                    KickGain_PrcPerSecPerUnit = this.modelParameters.KickGain_PrcPerSecPerUnit,
                    RampDown_PrcPerMin        = this.modelParameters.RampDown_PrcPerMin,
                    RampDecay_Tau_s           = this.modelParameters.RampDecay_Tau_s,
                    SurgeMargin_LP_Tau_s      = this.modelParameters.SurgeMargin_LP_Tau_s,
                    UMax                = this.modelParameters.UMax,
                    UMin                = this.modelParameters.UMin
                }
            };
        }

        public static double EvaluateFeature(string name, double P_in, double P_out, double MF)
        {
            switch (name)
            {
                case "P_in":        return P_in;
                case "P_out":       return P_out;
                case "MF":          return MF;
                case "DP":          return P_out - P_in;
                case "MF2":         return MF * MF;
                case "DP_over_MF2": return (P_out - P_in) / Math.Max(MF * MF, 1.0);
                case "Const":       return 1.0;
                default:
                    throw new ArgumentException($"Unknown ASC feature '{name}'");
            }
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length < 3) return new double[] { badDataID };

            double P_in     = inputsU[0];
            double P_out    = inputsU[1];
            double MassFlow = inputsU[2];

            if (P_in == badDataID || P_out == badDataID || MassFlow == badDataID || P_in <= 0.1)
                return new double[] { uPrev };

            var p = modelParameters;
            double uOut;

            if (p.Architecture == "KickBased")
            {
                // ── Surge proxy: distance from surge line ──────────────────
                // surge_distance < KickThreshold ⇒ in-surge ⇒ kick the valve
                double DP = P_out - P_in;
                double surgeDistanceRaw = MassFlow + p.SurgeProxy_a * DP + p.SurgeProxy_b;

                // Optional LP filter on surge margin — gives the controller
                // a "memory" so brief excursions out of surge don't end the hold.
                double surgeDistance;
                if (p.SurgeMargin_LP_Tau_s > 1e-9)
                {
                    if (!surgeMarginLPInitialized) { surgeMarginLP = surgeDistanceRaw; surgeMarginLPInitialized = true; }
                    double alpha = 1.0 - Math.Exp(-timeBase_s / p.SurgeMargin_LP_Tau_s);
                    surgeMarginLP += alpha * (surgeDistanceRaw - surgeMarginLP);
                    surgeDistance = surgeMarginLP;
                }
                else
                {
                    surgeDistance = surgeDistanceRaw;
                }

                if (surgeDistance < p.KickThreshold)
                {
                    // IN-SURGE: kick. Constant rate plus optional proportional term.
                    double err = p.KickThreshold - surgeDistance; // > 0 inside surge
                    double kickRatePrcPerSec = p.KickRate_PrcPerSec
                                             + p.KickGain_PrcPerSecPerUnit * err;
                    uOut = uPrev + kickRatePrcPerSec * timeBase_s;
                    if (uOut > p.UMax) uOut = p.UMax;
                }
                else if (surgeDistance > p.HoldThreshold && uPrev > p.UMin)
                {
                    // SAFE: combined linear + exponential close.
                    //   rate = constant_floor + uPrev / decay_tau
                    // Linear floor ensures u eventually reaches 0; exponential
                    // term makes early decay (high u) fast and tail (low u) slow,
                    // matching the K-Spice trace shape.
                    double linearRate = p.RampDown_PrcPerMin / 60.0;
                    double expRate    = (p.RampDecay_Tau_s > 1e-9) ? uPrev / p.RampDecay_Tau_s : 0.0;
                    double rampPerSec = linearRate + expRate;
                    uOut = uPrev - rampPerSec * timeBase_s;
                    if (uOut < p.UMin) uOut = p.UMin;
                }
                else
                {
                    // HOLD band: operating point still in marginal region — refuse
                    // to close further. Reproduces the K-Spice held-open plateau.
                    uOut = uPrev;
                }
            }
            else // LinearOLS (default fallback)
            {
                if (p.FeatureNames == null || p.FeatureWeights == null
                    || p.FeatureNames.Length != p.FeatureWeights.Length)
                    return new double[] { uPrev };

                // 1. Static linear surge proxy
                double target = 0.0;
                for (int i = 0; i < p.FeatureNames.Length; i++)
                    target += p.FeatureWeights[i] * EvaluateFeature(p.FeatureNames[i], P_in, P_out, MassFlow);
                if (target > p.UMax) target = p.UMax;
                if (target < p.UMin) target = p.UMin;

                // 2. Optional first-order low-pass on the target
                double targetSmooth;
                if (p.LPFilter_Tau_s > 1e-9)
                {
                    if (!lpInitialized) { targetLP = target; lpInitialized = true; }
                    double alpha = 1.0 - Math.Exp(-timeBase_s / p.LPFilter_Tau_s);
                    targetLP = targetLP + alpha * (target - targetLP);
                    targetSmooth = targetLP;
                }
                else { targetSmooth = target; }

                // 3. Asymmetric rate-limited follower
                double openRate  = (p.UMax - p.UMin) / Math.Max(1e-6, p.OpenTime_s);
                double closeRate = (p.UMax - p.UMin) / Math.Max(1e-6, p.CloseTime_s);
                double change = targetSmooth - uPrev;
                if (change > 0)
                    uOut = uPrev + Math.Min(change, openRate  * timeBase_s);
                else
                    uOut = uPrev - Math.Min(-change, closeRate * timeBase_s);
                if (uOut > p.UMax) uOut = p.UMax;
                if (uOut < p.UMin) uOut = p.UMin;
            }

            uPrev = uOut;
            return new double[] { Math.Round(uOut, 4) };
        }

        public void WarmStart(double[] inputs, double output)
        {
            uPrev = Math.Max(modelParameters.UMin, Math.Min(modelParameters.UMax, output));
            targetLP = uPrev;
            lpInitialized = true;
            surgeMarginLPInitialized = false; // initialized lazily on first Iterate
        }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[] givenInputValues=null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) { return uPrev; }
    }
}
