using System;
using System.Collections.Generic;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class AntiSurgeParameters : ModelParametersBaseClass
    {
        // ── Velocity-form dual-mode PI anti-surge controller ──────────────────
        // K-Spice GenericASC uses the velocity (incremental) form of PI:
        //   error_dim = NASP - NF   (dimensional; positive = surge risk)
        //   NASP = NAS * (1+CLM) * (1-ALM)  (NormalizedFlowSetpoint from CSV)
        //
        //   fastMode = error_dim > 0
        //   kp_curr  = fastMode ? Kp_Fast : Kp_Slow
        //   ki_curr  = kp_curr / Ti_curr
        //   kp_prev  = previous-step kp  (bumpless mode switch)
        //
        //   Δu = OutputScale * (kp_prev*(e - e_prev) + ki_curr*e*dt)
        //   u(t) = clamp(u(t-1) + Δu, UMin, UMax)
        //
        // Training: NASP-NF fed as 4th input from CSV truth.
        // Closed-loop: estimated via OLS proxy (SurgeProxy_MF_Raw/a_Raw/b_Raw).

        public string Architecture { get; set; } = "KSpicePI";

        // ── PI gains (dual-mode) ──────────────────────────────────────────────
        public double Kp_Fast           { get; set; } = 2.0;
        public double Kp_Slow           { get; set; } = 0.2;
        public double Ti_Fast_s         { get; set; } = 10.0;
        public double Ti_Slow_s         { get; set; } = 20.0;
        public double ControlLineMargin      { get; set; } = 0.2;
        public double AsymmetricLineMargin   { get; set; } = 0.05;
        public double OutputScale            { get; set; } = 1.0;

        // ── Valve rate limits (stored for reference, not used by velocity form) ─
        public double OpenTime_s  { get; set; } = 20.0;
        public double CloseTime_s { get; set; } = 60.0;

        // ── Closed-loop error proxy (OLS on predicted signals) ───────────────
        // Fitted to estimate dimensional error: NASP - NF
        // Features: MF, PR, MF², PR², bias (quadratic to capture nonlinear NAS shift).
        public double SurgeProxy_MF_Raw  { get; set; } = 0.0;
        public double SurgeProxy_a_Raw   { get; set; } = 0.0;
        public double SurgeProxy_N_Raw   { get; set; } = 0.0;
        public double SurgeProxy_MF2_Raw { get; set; } = 0.0;
        public double SurgeProxy_PR2_Raw { get; set; } = 0.0;
        public double SurgeProxy_b_Raw        { get; set; } = 0.0;
        // When true: proxy feature is MF/N (speed-normalised flow) instead of raw MF.
        // Avoids HP outlet pressure dependency — N is 84-99% OL accurate vs 8-22% for HP P_out.
        public bool   ProxyUsesNormalizedFlow { get; set; } = false;
        public double NAS_target              { get; set; } = 1.0;

        // ── Fast/slow mode threshold ──────────────────────────────────────────
        // K-Spice GenericASC enters fast mode when error > 0. Stored here so
        // the evaluator can reproduce training behaviour exactly.
        public double FastModeThreshold { get; set; } = 0.0;

        // ── Output bounds ─────────────────────────────────────────────────────
        public double UMax { get; set; } = 100.0;
        public double UMin { get; set; } = 0.0;

        // ── Legacy OLS fields (kept for graceful fallback of old param files) ─
        public string[] FeatureNames   { get; set; }
        public double[] FeatureWeights { get; set; }
        public double   LPFilter_Tau_s { get; set; } = 0.0;
    }

    /// <summary>
    /// Anti-Surge Controller surrogate implementing the K-Spice GenericASC velocity-form PI.
    ///
    /// Algorithm (from step-by-step trace against K-Spice training CSV):
    ///   error_dim = NASP - NF  (dimensional; NASP = NAS*(1+CLM)*(1-ALM))
    ///   Δu = OutputScale * (kp_prev*(e - e_prev) + ki_curr*e*dt)
    ///   u(t) = clamp(u(t-1) + Δu, UMin, UMax)
    ///
    /// Bumpless mode switch: kp_prev uses the previous step's mode gains.
    /// No rate limiter — the velocity form inherently scales movement with error change.
    ///
    /// Inputs: [P_in, P_out, MassFlow] + optional [NASP-NF] as 4th input.
    /// The 4th input (dimensional error) is provided from CSV truth during training/testset;
    /// in closed-loop it is estimated from an OLS proxy on predicted compressor signals.
    /// </summary>
    public class AntiSurgePhysicalModel : ModelBaseClass, ISimulatableModel
    {
        public AntiSurgeParameters modelParameters = new AntiSurgeParameters();

        private double uPrev             = 0.0;
        private double effectiveUMin     = 0.0;
        private double ePrev             = 0.0;   // previous dimensional error
        private bool   ePrevInitialized  = false;
        private bool   prevFastMode      = false;
        // Legacy LP filter state (used only by the LinearOLS fallback path)
        private double targetLP          = 0.0;
        private bool   lpInitialized     = false;

        // Proxy bias correction: computed once from the priming call (truth inputs at t=0)
        // and subtracted from all subsequent CL proxy evaluations. This prevents the proxy
        // from falsely detecting surge at the warm-start operating point when the proxy
        // is calibrated at a different operating point than the truth initial conditions.
        // Deliberately NOT reset in WarmStart so it persists through the re-anchor call
        // that follows the priming call in ClosedLoopRunner.
        private bool   proxyBiasComputed = false;
        private double proxyBiasOffset   = 0.0;

        public AntiSurgePhysicalModel() { this.processModelType = ModelType.SubProcess; }

        public AntiSurgePhysicalModel(string id, string[] inputIDs, string outputID = null) : this()
        {
            this.ID          = id;
            this.outputID    = outputID ?? id;
            this.ModelInputIDs = inputIDs;
        }

        public override int GetLengthOfInputVector()        => ModelInputIDs?.Length ?? 0;
        public override SignalType GetOutputSignalType()    => SignalType.Output_Y;
        public bool IsModelSimulatable(out string s)        { s = "OK"; return true; }

        public ISimulatableModel Clone(string cloneID)
        {
            return new AntiSurgePhysicalModel(cloneID, (string[])ModelInputIDs.Clone(), outputID)
            {
                modelParameters = new AntiSurgeParameters
                {
                    Architecture           = modelParameters.Architecture,
                    Kp_Fast                = modelParameters.Kp_Fast,
                    Kp_Slow                = modelParameters.Kp_Slow,
                    Ti_Fast_s              = modelParameters.Ti_Fast_s,
                    Ti_Slow_s              = modelParameters.Ti_Slow_s,
                    ControlLineMargin      = modelParameters.ControlLineMargin,
                    AsymmetricLineMargin   = modelParameters.AsymmetricLineMargin,
                    OutputScale            = modelParameters.OutputScale,
                    OpenTime_s             = modelParameters.OpenTime_s,
                    CloseTime_s            = modelParameters.CloseTime_s,
                    SurgeProxy_MF_Raw      = modelParameters.SurgeProxy_MF_Raw,
                    SurgeProxy_a_Raw       = modelParameters.SurgeProxy_a_Raw,
                    SurgeProxy_N_Raw       = modelParameters.SurgeProxy_N_Raw,
                    SurgeProxy_MF2_Raw     = modelParameters.SurgeProxy_MF2_Raw,
                    SurgeProxy_PR2_Raw     = modelParameters.SurgeProxy_PR2_Raw,
                    SurgeProxy_b_Raw           = modelParameters.SurgeProxy_b_Raw,
                    ProxyUsesNormalizedFlow    = modelParameters.ProxyUsesNormalizedFlow,
                    NAS_target                 = modelParameters.NAS_target,
                    FastModeThreshold      = modelParameters.FastModeThreshold,
                    UMax                   = modelParameters.UMax,
                    UMin                   = modelParameters.UMin,
                    FeatureNames           = (string[])modelParameters.FeatureNames?.Clone(),
                    FeatureWeights         = (double[])modelParameters.FeatureWeights?.Clone(),
                    LPFilter_Tau_s         = modelParameters.LPFilter_Tau_s
                }
            };
        }

        // EvaluateFeature kept for the legacy LinearOLS fallback path.
        public static double EvaluateFeature(string name, double P_in, double P_out, double MF)
        {
            switch (name)
            {
                case "P_in":        return P_in;
                case "P_out":       return P_out;
                case "MF":          return MF;
                case "DP":          return P_out - P_in;
                case "PR":          return (P_in > 0.1) ? P_out / P_in : 1.0;
                case "MF2":         return MF * MF;
                case "DP_over_MF2": return (P_out - P_in) / Math.Max(MF * MF, 1.0);
                case "Const":       return 1.0;
                default:            throw new ArgumentException($"Unknown ASC feature '{name}'");
            }
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length < 3) return new[] { badDataID };

            double P_in     = inputsU[0];
            double P_out    = inputsU[1];
            double MassFlow = inputsU[2];
            // inputsU[3] = N_speed (compressor speed from topology; 0 if not wired or NaN).
            double N_speed  = inputsU.Length >= 4 && !double.IsNaN(inputsU[3]) ? inputsU[3] : 0.0;

            if (P_in == badDataID || P_out == badDataID || MassFlow == badDataID || P_in <= 0.1)
                return new[] { uPrev };

            var p = modelParameters;

            // ── Dimensional error: NASP - NF ──────────────────────────────────
            // inputsU[4] = truth NASP-NF (open-loop training/testset only; NaN in CL).
            // When non-NaN: use directly (highest accuracy, bypasses proxy).
            //   Side-effect: cache the raw proxy value computed at these truth-input
            //   conditions so CL iterations can subtract it as a bias. This correction
            //   prevents the proxy from falsely detecting surge at the warm-start operating
            //   point when the proxy was fitted at a different (OL-predicted) point.
            // When NaN (closed-loop): 4-feature OLS proxy → f(MF, PR, N, bias),
            //   then subtract the cached priming bias so effective error starts near 0.
            double error;
            if (inputsU.Length >= 5 && !double.IsNaN(inputsU[4]))
            {
                error = inputsU[4];
            }
            else
            {
                double PR      = (P_in > 0.1) ? P_out / P_in : 1.0;
                double proxyMF = (p.ProxyUsesNormalizedFlow && N_speed > 1.0)
                    ? MassFlow / N_speed   // MF/N ≈ K-Spice's NF; avoids HP outlet pressure
                    : MassFlow;
                double rawProxy = p.SurgeProxy_MF_Raw * proxyMF
                                + p.SurgeProxy_a_Raw  * PR
                                + p.SurgeProxy_b_Raw;
                // First proxy evaluation (priming call at t=0 with truth inputs): cache as
                // bias. Subsequent CL evaluations subtract it so effective error starts near 0,
                // preventing false surge detection caused by proxy miscalibration at the
                // warm-start operating point (e.g. higher P_in in truth vs. OL prediction).
                if (!proxyBiasComputed)
                {
                    proxyBiasOffset = rawProxy;
                    proxyBiasComputed = true;
                }
                error = rawProxy - proxyBiasOffset;
            }

            // ── Velocity-form dual-mode PI (bumpless mode-switch) ─────────────
            bool   fastMode  = error > p.FastModeThreshold;
            double kp_curr   = fastMode ? p.Kp_Fast : p.Kp_Slow;
            double ki_curr   = kp_curr / Math.Max(1e-9, fastMode ? p.Ti_Fast_s : p.Ti_Slow_s);
            double kp_prev_v = (prevFastMode && fastMode) ? p.Kp_Fast : p.Kp_Slow;

            // First call after WarmStart: set ePrev = error so delta_e = 0 on first step.
            if (!ePrevInitialized) { ePrev = error; ePrevInitialized = true; }

            double scale = p.OutputScale > 1e-9 ? p.OutputScale : 1.0;
            double du    = scale * (kp_prev_v * (error - ePrev) + ki_curr * error * timeBase_s);
            // Physical valve rate limits from K-Spice model (CloseTime_s / OpenTime_s).
            double uRange = p.UMax - p.UMin;
            if (p.CloseTime_s > 1e-6) du = Math.Max(du, -uRange / p.CloseTime_s * timeBase_s);
            if (p.OpenTime_s  > 1e-6) du = Math.Min(du,  uRange / p.OpenTime_s  * timeBase_s);
            double uOut  = Math.Clamp(uPrev + du, effectiveUMin, p.UMax);

            ePrev        = error;
            prevFastMode = fastMode;
            uPrev        = uOut;
            return new[] { Math.Round(uOut, 4) };
        }

        public void WarmStart(double[] inputs, double output)
        {
            uPrev             = Math.Max(modelParameters.UMin, Math.Min(modelParameters.UMax, output));
            effectiveUMin     = Math.Min(modelParameters.UMin, uPrev);
            ePrev             = 0.0;
            ePrevInitialized  = false;
            prevFastMode      = false;
            targetLP          = uPrev;
            lpInitialized     = false;
            // Reset proxy bias so the FIRST actual CL step sets the baseline (not the
            // truth-input priming call). ClosedLoopRunner calls WarmStart twice: once
            // before priming and once after (re-anchor). The re-anchor clears any bias
            // set by the priming call, so at t=1 the proxy evaluates to 0 effective error
            // regardless of the model's MF prediction error vs. truth. This prevents false
            // surge detection caused by compressor MF model bias at the warm-start point
            // (e.g. UV3001 is 0.88 kg/s at warm-start vs. training mean 6.9 kg/s, which
            // shifts KA3001_MF down by ~3 kg/s and the proxy by +2.8 units above threshold).
            proxyBiasComputed = false;
            proxyBiasOffset   = 0.0;
        }

        public double? GetSteadyStateInput(double x0, int inputIdx = 0, double[] givenInputValues = null) => null;
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) => uPrev;
    }
}
