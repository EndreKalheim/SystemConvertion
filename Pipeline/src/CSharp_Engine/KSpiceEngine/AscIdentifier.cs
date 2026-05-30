using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace KSpiceEngine
{
    /// <summary>
    /// Identifies parameters for the Anti-Surge Controller.
    ///
    /// Model: dual-mode PI with rate limiting (standard industrial anti-surge control).
    ///   Fast mode (NF below control line): Kp_Fast, Ti_Fast_s — opens valve aggressively.
    ///   Slow mode (NF above control line): Kp_Slow, Ti_Slow_s — closes valve gently.
    ///   Rate limits (OpenTime_s / CloseTime_s) and anti-windup.
    ///
    /// Parameters are read from the K-Spice model file (they are our configuration data).
    /// An OLS proxy for NF-NAS is also fitted so closed-loop evaluation can estimate the
    /// control-line error from predicted compressor signals when truth NF/NAS are absent.
    /// </summary>
    internal static class AscIdentifier
    {
        internal static (double[] predictions, JObject modelParams) Identify(
            string id,
            string comp,
            double[] Y_true,
            double timeBase_s,
            List<(string name, double[] data)> inputCols,
            Dictionary<string, double[]> predictions,
            JArray models,
            string selectedKspiceModelPath,
            Dictionary<string, double[]> dataset)
        {
            int M = Y_true.Length;

            // ── 1. Resolve P_in, P_out, Flow from topology input columns ──────
            // Pre-scan: pull out the speed column (y_speed) before pressure/flow
            // classification so RPM values don't corrupt the pressure sort.
            double[]? speedFromTopology = null;
            string?   speedColName      = null;
            foreach (var col in inputCols)
            {
                if (col.name.IndexOf("y_speed", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    col.name.IndexOf("_Speed",  StringComparison.OrdinalIgnoreCase) >= 0)
                { speedFromTopology = col.data; speedColName = col.name; break; }
            }

            double[]? flow = null, pIn = null, pOut = null;
            string?   flowName = null, pInName = null, pOutName = null;
            var pressCands = new List<(double[] data, string name, double mean)>();

            foreach (var col in inputCols)
            {
                if (col.name == speedColName) continue;  // speed handled separately
                string n = col.name;
                bool isFlowDp   = n.IndexOf("FlowDP",    StringComparison.OrdinalIgnoreCase) >= 0;
                bool isKspiceFlow = n.IndexOf("KSpice:",  StringComparison.OrdinalIgnoreCase) >= 0 &&
                                   (n.IndexOf("Flow",        StringComparison.OrdinalIgnoreCase) >= 0 ||
                                    n.IndexOf("MassFlow",    StringComparison.OrdinalIgnoreCase) >= 0 ||
                                    n.IndexOf("Performance", StringComparison.OrdinalIgnoreCase) >= 0);

                if (isFlowDp || isKspiceFlow)
                {
                    if (flow == null || isFlowDp) { flow = col.data; flowName = col.name; }
                    continue;
                }
                if (n.IndexOf("InletPressure",  StringComparison.OrdinalIgnoreCase) >= 0)
                { pressCands.Insert(0, (col.data, col.name, col.data.Average())); continue; }
                if (n.IndexOf("OutletPressure", StringComparison.OrdinalIgnoreCase) >= 0)
                { pressCands.Add((col.data, col.name, col.data.Average())); continue; }
                if (col.name.Contains("Flow") || col.name.Contains("Mass"))
                { if (flow == null) { flow = col.data; flowName = col.name; } }
                else
                { pressCands.Add((col.data, col.name, col.data.Average())); }
            }

            if (pressCands.Count >= 2)
            {
                var sorted = pressCands.OrderBy(p => p.mean).ToList();
                pIn = sorted[0].data;                  pInName  = sorted[0].name;
                pOut = sorted[sorted.Count - 1].data;  pOutName = sorted[sorted.Count - 1].name;
            }
            else if (pressCands.Count == 1)
            { pIn = pressCands[0].data; pInName = pressCands[0].name; }

            if (pIn == null || pOut == null || flow == null)
            {
                Console.WriteLine($"[WARNING] {id}: ASC missing P_in, P_out or Flow inputs.");
                return ((double[])Y_true.Clone(),
                        new JObject { ["ModelType"] = "Fallback",
                                      ["Reason"]    = "ASC missing P_in, P_out or Flow inputs" });
            }

            // ── 2. Override with already-predicted signals where available ─────
            double[]? TryGetPred(string? colName)
            {
                if (colName == null) return null;
                int paren = colName.IndexOf('(');
                string key = paren > 0 ? colName.Substring(0, paren).Trim() : colName.Trim();
                return predictions.TryGetValue(key, out var pred) && pred != null ? pred : null;
            }
            double[] pIn_pred  = TryGetPred(pInName)  ?? pIn;
            double[] pOut_pred = TryGetPred(pOutName) ?? pOut;
            double[] flow_pred = TryGetPred(flowName) ?? flow;
            bool usingPredicted = pIn_pred != pIn || pOut_pred != pOut || flow_pred != flow;
            Console.WriteLine($"[Model] {id}: ASC inputs: pIn={pInName}, pOut={pOutName}, flow={flowName}  (predicted override: {usingPredicted})");

            // ── 3. Check for truth NF/NAS signals ────────────────────────────
            string nfKey  = $"{comp}:NormalizedFlow";
            string nasKey = $"{comp}:NormalizedAsymmetricLimit";
            bool haveKspiceSurge = dataset.ContainsKey(nfKey) && dataset.ContainsKey(nasKey);

            // ── 4. Read PI parameters from K-Spice model file ─────────────────
            double closeTime_s = 60.0, openTime_s = 20.0;
            double controlLineMargin = 0.2;
            double kpFast = 2.0, kpSlow = 0.2, tiFast_s = 10.0, tiSlow_s = 20.0;
            bool hasKSpicePIParams = false;
            Console.WriteLine($"[Model] {id}: selected K-Spice model path = {selectedKspiceModelPath}");

            foreach (var m in models)
            {
                string rn = ((string?)m["Name"] ?? "").Replace("_pf", "", StringComparison.OrdinalIgnoreCase);
                if (!string.Equals(rn, comp, StringComparison.OrdinalIgnoreCase)) continue;

                bool sourceMatches = false;
                if (!string.IsNullOrEmpty(selectedKspiceModelPath))
                {
                    var src = (string?)m["SourceFile"] ?? (string?)m["SourceMdl"];
                    if (src != null)
                    {
                        try { sourceMatches = string.Equals(Path.GetFileName(src),
                                  Path.GetFileName(selectedKspiceModelPath),
                                  StringComparison.OrdinalIgnoreCase); }
                        catch { }
                    }
                }

                var prm = m["Parameters"] as JObject;
                if (prm == null) break;

                if (prm["ControlLineMargin"] != null)
                    controlLineMargin = (double?)prm["ControlLineMargin"] ?? controlLineMargin;

                if (prm["FastProportionalGain"] != null)
                {
                    kpFast   = (double?)prm["FastProportionalGain"] ?? kpFast;
                    kpSlow   = (double?)prm["SlowProportionalGain"] ?? 0.2;
                    tiFast_s = (double?)prm["FastIntegralTime"]     ?? 10.0;
                    tiSlow_s = (double?)prm["SlowIntegralTime"]     ?? 20.0;
                    openTime_s = (double?)prm["OpenTime"]           ?? 20.0;
                    hasKSpicePIParams = kpFast > 0.001;
                }

                if (prm["CloseTime"] != null && (sourceMatches || string.IsNullOrEmpty(selectedKspiceModelPath)))
                {
                    closeTime_s = (double?)prm["CloseTime"] ?? closeTime_s;
                    break;
                }
                break;
            }

            // ── 5. Identify dual-mode PI model ────────────────────────────────
            if (hasKSpicePIParams && haveKspiceSurge)
            {
                return IdentifyDualModePI(id, comp, Y_true, pIn, pOut, flow,
                                          pIn_pred, pOut_pred, flow_pred,
                                          closeTime_s, openTime_s, timeBase_s,
                                          dataset, nfKey, nasKey, M,
                                          controlLineMargin, kpFast, kpSlow, tiFast_s, tiSlow_s,
                                          speedFromTopology ?? Array.Empty<double>());
            }

            Console.WriteLine($"[WARNING] {id}: KSpicePI params available={hasKSpicePIParams}, NF/NAS available={haveKspiceSurge} — using ConstantUMin fallback");
            return ConstantUMinFallback(id, Y_true, M, "KSpicePI params or NF/NAS signals not available");
        }

        // ── Dual-mode PI identification ──────────────────────────────────────

        private static (double[] predictions, JObject modelParams) IdentifyDualModePI(
            string id, string comp,
            double[] Y_true,
            double[] pIn,      double[] pOut,      double[] flow,
            double[] pIn_pred, double[] pOut_pred, double[] flow_pred,
            double closeTime_s, double openTime_s, double timeBase_s,
            Dictionary<string, double[]> dataset,
            string nfKey, string nasKey, int M,
            double controlLineMargin,
            double kpFast, double kpSlow, double tiFast_s, double tiSlow_s,
            double[] speedArr)
        {
            double[] nfArr  = dataset[nfKey]  ?? throw new InvalidOperationException($"Missing NF data for {comp}");
            double[] nasArr = dataset[nasKey] ?? throw new InvalidOperationException($"Missing NAS data for {comp}");

            // Compute asym = NAS*(1+CLM)*(1-ALM)/NAS = (1+CLM)*(1-ALM)
            // K-Spice uses NormalizedFlowSetpoint = NAS*asym as the open trigger; the
            // error signal is normalized: error = asym - NF/NAS  (positive = surge risk).
            // We derive asym empirically from the NormalizedFlowSetpoint column if available,
            // otherwise from CLM and ALM=0.05 (system-map default).
            string naspKey = $"{comp}:NormalizedFlowSetpoint";
            double asym;
            if (dataset.TryGetValue(naspKey, out var naspArr))
            {
                double sumRatio = 0.0;
                for (int i = 0; i < M; i++) sumRatio += (nasArr[i] > 0.1) ? naspArr[i] / nasArr[i] : 1.0;
                asym = sumRatio / M;
            }
            else
            {
                asym = (1.0 + controlLineMargin) * (1.0 - 0.05); // ALM default = 0.05
            }
            double alm = 1.0 - asym / (1.0 + controlLineMargin);

            // Pre-compute truth error (dimensional): error = NASP - NF
            // Positive = below open trigger = surge risk = fast mode.
            var truthError = new double[M];
            double nasMean = 0.0, nfMean = 0.0;
            for (int i = 0; i < M; i++)
            {
                truthError[i] = (naspArr != null) ? naspArr[i] - nfArr[i] : 0.0;
                nasMean += nasArr[i];
                nfMean  += nfArr[i];
            }
            nasMean /= M;
            nfMean  /= M;

            // Fit surge proxy: proxy(MF, PR[, N]) ≈ NASP - NF (dimensional error).
            // Features are mean-centred and std-scaled before OLS so ridge regularisation
            // is numerically fair across all features regardless of physical units.
            // If speedArr is present a 3-predictor fit (MF, PR, N) is used; otherwise 2
            // predictors (MF, PR).  Intercept is recovered from feature means.
            // Sign guard: reject and zero all coefficients if MF coeff is non-negative
            // (positive MF → more surge risk is physically impossible and causes CL runaway).

            // Proxy uses PREDICTED signals (pIn_pred/pOut_pred/flow_pred) rather than truth.
            // This aligns proxy training with the closed-loop regime:
            //   LP ASCs: predicted ≈ truth (usingPredicted=False) → no change.
            //   HP ASCs: pOut_pred = KA_Pressure_pred (8-22% OL) → ridge OLS finds near-zero
            //            β_PR because noisy PR has low signal → proxy relies on MF + bias
            //            → stable in CL even when HP pressure is poorly predicted.
            bool proxyUsesNormFlow = false;   // MF/N disabled: speed unreliable in CL

            double raw_mf = 0.0, raw_a = 0.0, raw_n = 0.0, raw_b = 0.0;
            {
                int nFeat = 2;   // (MF, PR)

                // Pass 1 — feature means and target mean (use PREDICTED signals)
                double muMF = 0, muPR = 0, muErr = 0;
                for (int i = 0; i < M; i++)
                {
                    double pr = (pIn_pred[i] > 0.1) ? pOut_pred[i] / pIn_pred[i] : 1.0;
                    muMF  += flow_pred[i];
                    muPR  += pr;
                    muErr += (naspArr != null) ? naspArr[i] - nfArr[i] : 0.0;
                }
                muMF /= M; muPR /= M; muErr /= M;

                // Pass 2 — feature stds
                double vMF = 0, vPR = 0;
                for (int i = 0; i < M; i++)
                {
                    double pr  = (pIn_pred[i] > 0.1) ? pOut_pred[i] / pIn_pred[i] : 1.0;
                    double mf  = flow_pred[i];
                    vMF += (mf - muMF) * (mf - muMF);
                    vPR += (pr - muPR) * (pr - muPR);
                }
                double sMF = Math.Sqrt(vMF / M + 1e-10);
                double sPR = Math.Sqrt(vPR / M + 1e-10);

                // Pass 3 — build XᵀX and Xᵀy on centred/scaled features
                double[,] XtX = new double[nFeat, nFeat];
                double[]  Xty = new double[nFeat];
                for (int i = 0; i < M; i++)
                {
                    double pr     = (pIn_pred[i] > 0.1) ? pOut_pred[i] / pIn_pred[i] : 1.0;
                    double mf     = flow_pred[i];
                    double target = ((naspArr != null) ? naspArr[i] - nfArr[i] : 0.0) - muErr;
                    double[] row  = { (mf - muMF) / sMF, (pr - muPR) / sPR };
                    for (int p = 0; p < nFeat; p++)
                    {
                        Xty[p] += row[p] * target;
                        for (int q = 0; q < nFeat; q++) XtX[p, q] += row[p] * row[q];
                    }
                }
                // Ridge: λ = 0.01 * M
                for (int p = 0; p < nFeat; p++) XtX[p, p] += 0.01 * M;

                double[]? betaN = DynamicPlantRunner.SolveLinearSystem(XtX, Xty, nFeat);
                if (betaN != null && betaN[0] < 0)  // sign guard: higher MF → less surge risk
                {
                    raw_mf = betaN[0] / sMF;
                    raw_a  = betaN[1] / sPR;
                    raw_n  = 0.0;
                    raw_b  = muErr - raw_mf * muMF - raw_a * muPR;
                    Console.WriteLine($"[Model] {id}: Surge proxy (2-pred, norm, pred-aligned): MF={raw_mf:G3}, PR={raw_a:G3}, bias={raw_b:G3}");
                }
                else if (betaN != null)
                    Console.WriteLine($"[WARNING] {id}: Proxy MF coeff non-negative ({betaN[0]/sMF:G4}) — proxy zeroed");
            }

            double uMinLocal = 0.0;

            // Effective PI gains — will be grid-searched below.
            // K-Spice's gains are calibrated to its internal (normalised) error units, which
            // differ from the dimensional NASP-NF we extract from the CSV.  A grid search
            // recovers the effective values that minimise SSE on the training trajectory.
            double kpFastEff = kpFast, tiFastEff_s = tiFast_s;

            // Run the dual-mode PI model on training data with truth error at input[4].
            // Input layout mirrors Iterate(): [P_in, P_out, MF, N_speed, truth_error].
            double[] SimPI(double outputScale)
            {
                var m = new CustomModels.AntiSurgePhysicalModel(id, new[] { "P_in", "P_out", "Flow", "Speed" }, id);
                m.modelParameters.Kp_Fast           = kpFastEff;
                m.modelParameters.Kp_Slow           = kpSlow;
                m.modelParameters.Ti_Fast_s         = tiFastEff_s;
                m.modelParameters.Ti_Slow_s         = tiSlow_s;
                m.modelParameters.OpenTime_s        = openTime_s;
                m.modelParameters.CloseTime_s       = closeTime_s;
                m.modelParameters.OutputScale       = outputScale;
                m.modelParameters.ControlLineMargin      = controlLineMargin;
                m.modelParameters.AsymmetricLineMargin   = alm;
                m.modelParameters.NAS_target             = nasMean;
                m.modelParameters.UMin              = uMinLocal;
                m.WarmStart(null, Y_true[0]);
                double[] ySim = new double[M];
                for (int i = 0; i < M; i++)
                {
                    double n_i = speedArr != null && i < speedArr.Length ? speedArr[i] : 0.0;
                    ySim[i] = m.Iterate(new[] { pIn_pred[i], pOut_pred[i], flow_pred[i], n_i, truthError[i] }, timeBase_s)[0];
                }
                return ySim;
            }

            double tssGlobal = 0.0;
            { double mean = Y_true.Average(); for (int i = 0; i < M; i++) { double d = Y_true[i] - mean; tssGlobal += d * d; } }

            // Uncapped R² for internal comparison (comparison with capped -100 breaks when all candidates are bad).
            double ComputeSSE(double[] yPred)
            {
                double sse = 0;
                for (int i = 0; i < M; i++) { double r = Y_true[i] - yPred[i]; sse += r * r; }
                return sse;
            }
            double ComputeFit(double[] yPred) =>
                tssGlobal > 1e-12 ? Math.Max(-100.0, 100.0 * (1.0 - ComputeSSE(yPred) / tssGlobal)) : 0.0;

            // 4D grid-search: fast-mode (Kp_Fast × Ti_Fast) × slow-mode (Kp_Slow × Ti_Slow).
            // K-Spice's gains are in its internal normalised-error units, which differ from
            // the dimensional NASP-NF in the CSV — the grid recovers the effective values.
            // Slow-mode gains control valve-close rate when safe, which dominates the fit
            // when surge events are rare in the training window.  864 combos total — each
            // SimPI call is a fast linear loop, so total time is still < 2 s per ASC.
            {
                double bestSSE    = double.PositiveInfinity;
                double bestKp     = kpFastEff, bestTi      = tiFastEff_s;
                double bestKpSlow = kpSlow,    bestTiSlow  = tiSlow_s;
                foreach (double tiSlowTry in new[] { 10.0, 20.0, 40.0, 80.0 })
                foreach (double kpSlowTry in new[] { 0.1, 0.2, 0.4 })
                foreach (double tiTry in new[] { 5.0, 10.0, 20.0, 40.0, 80.0, 160.0 })
                foreach (double kpTry in new[] { 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 1.0, 1.5, 2.0, 3.0, 5.0, 8.0 })
                {
                    kpFastEff   = kpTry;     tiFastEff_s = tiTry;
                    kpSlow      = kpSlowTry; tiSlow_s    = tiSlowTry;
                    double sse = ComputeSSE(SimPI(1.0));
                    if (sse < bestSSE)
                    {
                        bestSSE    = sse;
                        bestKp     = kpTry;     bestTi     = tiTry;
                        bestKpSlow = kpSlowTry; bestTiSlow = tiSlowTry;
                    }
                }
                kpFastEff   = bestKp;     tiFastEff_s = bestTi;
                kpSlow      = bestKpSlow; tiSlow_s    = bestTiSlow;
            }

            // Try OutputScale=1.0 first (the 4D grid used it as the reference)
            double[] yBase   = SimPI(1.0);
            double   sseBase = ComputeSSE(yBase);

            // If fit is poor, scan a few candidate scales via OLS.
            double bestScale = 1.0;
            double[] yFinal = yBase;
            double   sseFinal = sseBase;

            if (sseBase > 0.01 * tssGlobal)   // fit < 99% — try OLS scale
            {
                double sumPIY = 0, sumPI2 = 0;
                for (int i = 0; i < M; i++) { sumPIY += yBase[i] * Y_true[i]; sumPI2 += yBase[i] * yBase[i]; }
                double olsScale = sumPI2 > 1e-9 ? sumPIY / sumPI2 : 1.0;
                olsScale = Math.Max(0.1, Math.Min(10.0, olsScale));

                double[] yScaled  = SimPI(olsScale);
                double   sseScaled = ComputeSSE(yScaled);
                if (sseScaled < sseFinal)
                {
                    bestScale = olsScale;
                    yFinal    = yScaled;
                    sseFinal  = sseScaled;
                }
            }
            double fitFinal = ComputeFit(yFinal);

            Console.WriteLine($"[Model] {id}: DualModePI(velocity) OutputScale={bestScale:G4}  fit={fitFinal:F2}%  " +
                              $"(Kp_fast={kpFastEff:G4}[mdl:{kpFast}], Ti_fast={tiFastEff_s:G4}s[mdl:{tiFast_s}s], " +
                              $"Kp_slow={kpSlow}, Ti_slow={tiSlow_s}s, CLM={controlLineMargin:F2})");

            // Compute UKickMax for the open-loop runner's 4th-input injection gate
            double uKickMax = 0.0;
            for (int i = 0; i < M; i++)
                if (truthError[i] > 0 && Y_true[i] > uKickMax) uKickMax = Y_true[i];
            uKickMax = Math.Min(uKickMax + 5.0, 100.0);

            var modelParams = new JObject
            {
                ["ModelType"]                 = "AntiSurgePhysicalModel",
                ["Architecture"]              = "KSpicePI",
                ["Kp_Fast"]                   = kpFastEff,
                ["Kp_Slow"]                   = kpSlow,
                ["Ti_Fast_s"]                 = tiFastEff_s,
                ["Ti_Slow_s"]                 = tiSlow_s,
                ["OpenTime_s"]                = openTime_s,
                ["CloseTime_s"]               = closeTime_s,
                ["OutputScale"]               = bestScale,
                ["ControlLineMargin"]         = controlLineMargin,
                ["AsymmetricLineMargin"]      = alm,
                ["NAS_mean"]                  = nasMean,
                // SurgeProxy_* fits dimensional error: NASP - NF.
                // When ProxyUsesNormalizedFlow=true, SurgeProxy_MF_Raw coeff applies to MF/N.
                ["SurgeProxy_MF_Raw"]         = raw_mf,
                ["SurgeProxy_a_Raw"]          = raw_a,
                ["SurgeProxy_N_Raw"]          = raw_n,   // always 0; kept for compat
                ["SurgeProxy_b_Raw"]          = raw_b,
                ["ProxyUsesNormalizedFlow"]    = proxyUsesNormFlow,
                // Kept for runner compatibility
                ["SurgeProxy_MeanDist"]       = 0.0,
                ["SurgeProxy_StdDist"]        = 1.0,
                ["UKickMax"]                  = uKickMax,
                ["UMin"]                      = uMinLocal,
                ["NasSignalKey"]              = naspArr != null ? naspKey : "",
                ["NfSignalKey"]               = nfKey,
                ["FastModeThreshold"]         = 1.0,
                ["FitScore"]                  = fitFinal,
                ["Formula"]                   = $"DualModePI(velocity-form, dim error=NASP-NF); "
                                              + $"fast(Kp={kpFastEff:G4}[mdl:{kpFast}],Ti={tiFastEff_s:G4}s); "
                                              + $"slow(Kp={kpSlow},Ti={tiSlow_s}s); "
                                              + $"scale={bestScale:G4}; proxy=pred-MF+PR"
            };

            return (yFinal, modelParams);
        }

        // ── Constant-UMin fallback ────────────────────────────────────────────

        private static (double[] predictions, JObject modelParams) ConstantUMinFallback(
            string id, double[] Y_true, int M, string reason)
        {
            double sortedMin5 = Y_true.OrderBy(y => y).Skip((int)(M * 0.05)).First();
            double uMin = (sortedMin5 >= 2.0) ? sortedMin5 : 0.0;
            double[] constY = Y_true.Select(_ => uMin).ToArray();
            double mean = Y_true.Average();
            double sse = constY.Zip(Y_true, (pr, tr) => (tr - pr) * (tr - pr)).Sum();
            double tss = Y_true.Select(y => (y - mean) * (y - mean)).Sum();
            double fit = tss > 1e-12 ? Math.Max(-100.0, 100.0 * (1.0 - sse / tss)) : 0.0;

            Console.WriteLine($"  [ASC] {id}: ConstantUMin={uMin:F1}% (fit={fit:F1}%) — {reason}");

            return (constY, new JObject
            {
                ["ModelType"]    = "ConstantUMin",
                ["Architecture"] = "ConstantUMin",
                ["Value"]        = uMin,
                ["UMin"]         = uMin,
                ["FitScore"]     = fit,
                ["Formula"]      = $"U(t) = {uMin:F1}% — {reason}"
            });
        }
    }
}
