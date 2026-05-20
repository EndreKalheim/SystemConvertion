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
        // HP-filter: slow LP tau for HP_PR = LP_fast(PR) - LP_slow(PR).
        // At steady state HP_PR=0 regardless of absolute PR → regime-shift robust.
        // Set to 0 to use raw LP_PR (original LP variant).
        public double HPFilter_Tau_s   { get; set; } = 0.0;
        public double OpenTime_s       { get; set; } = 5.0;
        public double CloseTime_s      { get; set; } = 60.0;

        // ── Kick-based (architecture = KickBased) ──────────────────────────
        // surge_distance = SurgeProxy_MF_Coeff·MF + SurgeProxy_a·PressureRatio + SurgeProxy_b  (PressureRatio=P_out/P_in)
        //
        // Uses pressure *ratio* (not difference) because surge is physically determined
        // by compressor head (lift), which is fundamentally a ratio phenomenon. This avoids
        // false positives at high absolute pressures and correctly prioritizes flow-to-ratio
        // combinations that actually risk surge.
        //
        // The three regression coefficients are stored normalised (divided by the
        // std-dev of the surge distance over training), so the surge distance is
        // always in O(1) units regardless of which physical variable dominates the
        // surge margin.  KickThreshold = 0 separates in-surge (< 0) from safe (≥ 0).
        //
        // Three regimes via hysteresis (Schmitt trigger), KickThreshold ≤ HoldThreshold:
        //   surge_distance < KickThreshold   →  IN-SURGE: kick the valve open
        //   surge_distance > HoldThreshold   →  SAFE: ramp down (slow close)
        //   between the two                  →  HOLD: u stays at u(k-1)
        //
        // Kick rate is the TSA PidAntiSurgeParams kickPrcPerSec (constant) plus an
        // optional K-Spice FastProportionalGain-style proportional term:
        //   rate = KickRate_PrcPerSec  +  KickGain · max(0, KickThreshold - surge_distance)
        public double SurgeProxy_MF_Coeff       { get; set; } = 1.0;  // coefficient on MassFlow (default 1 = legacy)
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
        private enum SurgePhase
        {
            Hold,
            Kick,
            Decay,
        }

        public AntiSurgeParameters modelParameters = new AntiSurgeParameters();
        private double uPrev = 0.0;
        private double targetLP = 0.0;
        private bool lpInitialized = false;
        private double mfLP = 0.0;           // ARX: LP state for MF input
        private bool mfLPInitialized = false;
        private double prLPSlow = 0.0;       // ARX HP-filter: slow LP state for PR
        private bool prLPSlowInitialized = false;
        private double adaptedConst = 0.0;  // ARX HP-filter: runtime-adapted const for regime-shift
        private bool constAdapted = false;
        private double surgeMarginLP = 0.0;
        private bool surgeMarginLPInitialized = false;
        private bool wasInSurge = false;
        private SurgePhase surgePhase = SurgePhase.Hold;

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
                    HPFilter_Tau_s      = this.modelParameters.HPFilter_Tau_s,
                    OpenTime_s          = this.modelParameters.OpenTime_s,
                    CloseTime_s         = this.modelParameters.CloseTime_s,
                    SurgeProxy_MF_Coeff       = this.modelParameters.SurgeProxy_MF_Coeff,
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
                case "PR":          return (P_in > 0.1) ? P_out / P_in : 1.0;  // Pressure ratio (KickBased uses this)
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
                double ApplyDecay(double surgeDistance)
                {
                    double linearRate = p.RampDown_PrcPerMin / 60.0;
                    double expRate    = (p.RampDecay_Tau_s > 1e-9) ? uPrev / p.RampDecay_Tau_s : 0.0;
                    double rampPerSec = linearRate + expRate;
                    double uNext = uPrev - rampPerSec * timeBase_s;
                    if (uNext < p.UMin) uNext = p.UMin;
                    surgePhase = SurgePhase.Decay;
                    wasInSurge = false;
                    return uNext;
                }

                // ── Surge proxy: distance from surge line ──────────────────
                // surge_distance < KickThreshold ⇒ in-surge ⇒ kick the valve
                // Use pressure *ratio* (P_out/P_in) instead of difference because
                // surge risk is determined by compressor head (lift), a ratio phenomenon.
                double PressureRatio = (P_in > 0.1) ? P_out / P_in : 1.0;
                double surgeDistanceRaw = p.SurgeProxy_MF_Coeff * MassFlow + p.SurgeProxy_a * PressureRatio + p.SurgeProxy_b;

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
                    // If we're just entering surge, give a short burst to open faster.
                    bool entering = !wasInSurge;
                    uOut = uPrev + kickRatePrcPerSec * timeBase_s;
                    if (entering)
                    {
                        // burst multiplier (tunable via identification if desired)
                        uOut += 5.0 * kickRatePrcPerSec * timeBase_s;
                    }
                    if (uOut > p.UMax) uOut = p.UMax;
                    wasInSurge = true;
                    surgePhase = SurgePhase.Kick;
                }
                else if (surgePhase == SurgePhase.Decay)
                {
                    // Once decay has started, keep decaying until a new kick.
                    // This prevents the valve from re-entering HOLD mid-close and
                    // getting stranded above zero.
                    uOut = ApplyDecay(surgeDistance);
                }
                else
                {
                    if (surgePhase == SurgePhase.Kick)
                    {
                        // Kick has already happened; move into a true hold only if
                        // the compressor remains in the hysteresis band. Holding is
                        // a pause between kick and recovery, not a permanent state.
                        if (surgeDistance <= p.HoldThreshold)
                        {
                            surgePhase = SurgePhase.Hold;
                            wasInSurge = false;
                            uOut = uPrev;
                        }
                        else
                        {
                            uOut = ApplyDecay(surgeDistance);
                        }
                    }
                    else if (surgeDistance > p.HoldThreshold && uPrev > p.UMin)
                    {
                        // Recovery is confirmed: switch to decay and keep closing.
                        uOut = ApplyDecay(surgeDistance);
                    }
                    else
                    {
                        // Hold is only allowed while the signal remains in the
                        // hysteresis band. If we already started decaying, the
                        // decay state owns the tail until a new kick occurs.
                        surgePhase = SurgePhase.Hold;
                        wasInSurge = false;
                        uOut = uPrev;
                    }
                }
            }
            else if (p.Architecture == "ARX")
            {
                // ARX: U[t] = α·U[t-1] + β·PR_feature[t] + γ·LP(MF[t]) + const
                // PR_feature = LP_fast(PR) when HPFilter_Tau_s=0, else HP_PR = LP_fast - LP_slow.
                // HP_PR is 0 at any steady state → regime-shift robust across operating points.
                // FeatureWeights = [alpha, beta_PR, beta_MF, const]
                double prRaw   = (P_in > 0.1) ? P_out / P_in : 1.0;
                double lpAlpha = (p.LPFilter_Tau_s > 1e-9) ? 1.0 - Math.Exp(-timeBase_s / p.LPFilter_Tau_s) : 1.0;
                if (!lpInitialized)   { targetLP = prRaw;    lpInitialized   = true; }
                if (!mfLPInitialized) { mfLP     = MassFlow; mfLPInitialized = true; }
                targetLP += lpAlpha * (prRaw    - targetLP);
                mfLP     += lpAlpha * (MassFlow - mfLP);

                double prFeature;
                if (p.HPFilter_Tau_s > 1e-9)
                {
                    double slowAlpha = 1.0 - Math.Exp(-timeBase_s / p.HPFilter_Tau_s);
                    if (!prLPSlowInitialized) { prLPSlow = prRaw; prLPSlowInitialized = true; }
                    prLPSlow += slowAlpha * (prRaw - prLPSlow);
                    prFeature = targetLP - prLPSlow;

                    // Adaptive const: on the first step after WarmStart, fix the steady-state
                    // output to uPrev (the actual initial valve position) at the current MF.
                    // This makes the model regime-shift robust: training (valve≈UMin>0) and
                    // testset (valve=0%) both start at their real initial state and stay there
                    // between surge events, regardless of the OLS-fitted const from training.
                    if (!constAdapted)
                    {
                        adaptedConst = uPrev * (1.0 - p.FeatureWeights[0]) - p.FeatureWeights[2] * mfLP;
                        constAdapted = true;
                    }
                }
                else
                {
                    prFeature = targetLP;
                }

                double constTerm = (p.HPFilter_Tau_s > 1e-9) ? adaptedConst : p.FeatureWeights[3];
                uOut = p.FeatureWeights[0] * uPrev
                     + p.FeatureWeights[1] * prFeature
                     + p.FeatureWeights[2] * mfLP
                     + constTerm;
                // Asymmetric rate-limiter: real ASC valves open fast, close slowly.
                // Slow close (CloseTime_s) breaks the CL oscillation feedback loop.
                double rlOpen  = (p.OpenTime_s  > 0) ? 100.0 / p.OpenTime_s  * timeBase_s : 100.0;
                double rlClose = (p.CloseTime_s > 0) ? 100.0 / p.CloseTime_s * timeBase_s : 100.0;
                double rlDelta = uOut - uPrev;
                if (rlDelta >  rlOpen)  uOut = uPrev + rlOpen;
                if (rlDelta < -rlClose) uOut = uPrev - rlClose;
                // Use 0 not UMin: centering anchors U_ss=UMin at training mean;
                // at lower testset PR the model computes sub-UMin values that are
                // physically correct (valve fully closed), so clip to 0, not UMin.
                uOut = Math.Max(0.0, Math.Min(p.UMax, uOut));
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
            // ARX has no hard UMin floor (centering anchors U_ss=UMin at nominal inputs).
            // Clamping to UMin here would start free-running from the wrong state when
            // the testset begins with the valve fully closed (output=0 < training UMin).
            double lo = (modelParameters.Architecture == "ARX") ? 0.0 : modelParameters.UMin;
            uPrev = Math.Max(lo, Math.Min(modelParameters.UMax, output));

            if (modelParameters.Architecture == "ARX")
            {
                // Initialize LP states from the actual initial PR.
                // Both fast and slow LP filters must start at the same value so HP_PR = 0
                // at the evaluation start point regardless of operating-point level.
                if (inputs != null && inputs.Length >= 2 && inputs[0] > 0.1)
                {
                    double prRaw = inputs[1] / inputs[0];
                    targetLP = prRaw;
                    prLPSlow = prRaw;
                    lpInitialized        = true;
                    prLPSlowInitialized  = true;
                }
                else
                {
                    // Inputs not available yet (IdentifiedModelEvaluator calls WarmStart(null, y0)).
                    // Leave both LP filters uninitialized so they pick up the real initial PR
                    // on the first Iterate() call. Avoids a spurious HP_PR spike from
                    // (targetLP=uPrev) - (prLPSlow=realPR) at evaluation start.
                    lpInitialized        = false;
                    prLPSlowInitialized  = false;
                }
            }
            else
            {
                targetLP = uPrev;
                prLPSlow = 0.0;
                lpInitialized        = true;
                prLPSlowInitialized  = false;
            }
            mfLPInitialized = false;          // initialized lazily on first Iterate
            surgeMarginLPInitialized = false; // initialized lazily on first Iterate
            constAdapted = false;             // re-adapt HP const on first Iterate after WarmStart
            surgePhase = SurgePhase.Hold;
            wasInSurge = false;
        }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[] givenInputValues=null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) { return uPrev; }
    }
}
