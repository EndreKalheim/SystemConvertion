using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace KSpiceEngine
{
    /// <summary>
    /// Identifies parameters for an Anti-Surge Controller from K-Spice CSV data.
    /// Benchmarks several OLS architectures then fits a physics-based KickBased model
    /// and returns whichever achieves the best open-loop fit score.
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

            // 1. Resolve P_in, P_out, Flow from topology input columns
            double[] flow = null, pIn = null, pOut = null;
            string   flowName = null, pInName = null, pOutName = null;
            var pressCands = new List<(double[] data, string name, double mean)>();

            foreach (var col in inputCols)
            {
                string n = col.name;
                bool isFlowDp = n.IndexOf("FlowDP", StringComparison.OrdinalIgnoreCase) >= 0;
                bool isKspiceFlow = n.IndexOf("KSpice:", StringComparison.OrdinalIgnoreCase) >= 0 &&
                                    (n.IndexOf("Flow",        StringComparison.OrdinalIgnoreCase) >= 0 ||
                                     n.IndexOf("MassFlow",    StringComparison.OrdinalIgnoreCase) >= 0 ||
                                     n.IndexOf("Performance", StringComparison.OrdinalIgnoreCase) >= 0);

                if (isFlowDp || isKspiceFlow)
                {
                    if (flow == null || isFlowDp) { flow = col.data; flowName = col.name; }
                    continue;
                }
                if (n.IndexOf("InletPressure", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    pressCands.Insert(0, (col.data, col.name, col.data.Average()));
                    continue;
                }
                if (n.IndexOf("OutletPressure", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    pressCands.Add((col.data, col.name, col.data.Average()));
                    continue;
                }
                if (col.name.Contains("Flow") || col.name.Contains("Mass"))
                {
                    if (flow == null) { flow = col.data; flowName = col.name; }
                }
                else
                {
                    pressCands.Add((col.data, col.name, col.data.Average()));
                }
            }

            if (pressCands.Count >= 2)
            {
                var sorted = pressCands.OrderBy(p => p.mean).ToList();
                pIn = sorted[0].data;                     pInName  = sorted[0].name;
                pOut = sorted[sorted.Count - 1].data;    pOutName = sorted[sorted.Count - 1].name;
            }
            else if (pressCands.Count == 1)
            {
                pIn = pressCands[0].data; pInName = pressCands[0].name;
            }

            if (pIn == null || pOut == null || flow == null)
            {
                Console.WriteLine($"[WARNING] {id}: ASC missing P_in, P_out or Flow inputs.");
                return ((double[])Y_true.Clone(),
                        new JObject { ["ModelType"] = "Fallback", ["Reason"] = "ASC missing P_in, P_out or Flow inputs" });
            }

            // 2. Override with already-predicted signals where available
            double[] TryGetPred(string colName)
            {
                if (colName == null) return null;
                int paren = colName.IndexOf('(');
                string key = paren > 0 ? colName.Substring(0, paren).Trim() : colName.Trim();
                return predictions.TryGetValue(key, out var pred) && pred != null ? pred : null;
            }
            double[] pIn_pred   = TryGetPred(pInName)   ?? pIn;
            double[] pOut_pred  = TryGetPred(pOutName)  ?? pOut;
            double[] flow_pred  = TryGetPred(flowName)  ?? flow;
            bool usingPredicted = pIn_pred != pIn || pOut_pred != pOut || flow_pred != flow;
            Console.WriteLine($"[Model] {id}:   ASC inputs: pIn={pInName}, pOut={pOutName}, flow={flowName}  (predicted override: {usingPredicted})");

            // 3. Read CloseTime from K-Spice model parameters. Prefer parameters
            //    originating from the selected K-Spice .mdl when available.
            double closeTime_s = 60.0;
            Console.WriteLine($"[Model] {id}: selected K-Spice model path = {selectedKspiceModelPath}");
            foreach (var m in models)
            {
                string rn = ((string)m["Name"]).Replace("_pf", "", StringComparison.OrdinalIgnoreCase);
                if (string.Equals(rn, comp, StringComparison.OrdinalIgnoreCase))
                {
                    // If the system map carries a SourceFile or SourceMdl marker, prefer
                    // entries that match the selected K-Spice model path. Otherwise fall
                    // back to the first matching component entry.
                    bool sourceMatches = false;
                    if (!string.IsNullOrEmpty(selectedKspiceModelPath))
                    {
                        var src = (string)m["SourceFile"] ?? (string)m["SourceMdl"] ?? null;
                        if (src != null)
                        {
                            try { sourceMatches = string.Equals(Path.GetFileName(src), Path.GetFileName(selectedKspiceModelPath), StringComparison.OrdinalIgnoreCase); }
                            catch { sourceMatches = false; }
                        }
                    }
                    var prm = m["Parameters"] as JObject;
                    if (prm?["CloseTime"] != null && (sourceMatches || string.IsNullOrEmpty(selectedKspiceModelPath)))
                    {
                        closeTime_s = (double)prm["CloseTime"];
                        break;
                    }
                }
            }

            // 4. Run OLS multi-architecture benchmark.
            // The benchmark tracks two winners:
            //   bestFit/bestParams  — overall best OLS (may be SurgeProxy, LinearOLS, etc.)
            //   bestPRFit/bestPRParams — best LinearPR candidate (scale-invariant, safe)
            var benchmark = new JArray();
            double bestFit   = double.NegativeInfinity;
            double[] bestY   = null;
            JObject bestParams = null;
            double bestPRFit   = double.NegativeInfinity;
            double[] bestPRY   = null;
            JObject bestPRParams = null;
            RunOlsBenchmark(id, Y_true, pIn, pOut, flow, closeTime_s, timeBase_s, M,
                            ref benchmark, ref bestFit, ref bestY, ref bestParams,
                            ref bestPRFit, ref bestPRY, ref bestPRParams);

            // 5. Fit physics-based KickBased model.
            // Selection priority (from highest to lowest):
            //   (a) KickBased — if its training fit >= best LinearPR fit.
            //       KickBased is always preferred when it is competitive because it handles
            //       surge events as a state machine (correct physical structure).
            //   (b) LinearPR (PR=P_out/P_in, MF, Const) — if it beats KickBased.
            //       LinearPR is scale-invariant: weights don't blow up when operating
            //       pressure shifts between training and test/CL.
            //   (c) Best overall OLS (LinearOLS, SurgeProxy, etc.) — fallback when neither
            //       KickBased nor LinearPR achieves positive fit.
            //       NOTE: LinearOLS/SurgeProxy use raw P_out/DP_over_MF2 which can explode
            //       at different operating points — only use as last resort.
            var kick = IdentifyKickBased(id, comp, Y_true, pIn, pOut, flow,
                                         pIn_pred, pOut_pred, flow_pred,
                                         closeTime_s, timeBase_s, dataset, M);
            if (kick.benchmarkEntry != null) benchmark.Add(kick.benchmarkEntry);

            // UMin is needed for active-period fit and over-triggering checks.
            double sortedMin5 = Y_true.OrderBy(y => y).Skip((int)(Y_true.Length * 0.05)).First();
            double estimatedUMin = (sortedMin5 >= 2.0) ? sortedMin5 : 0.0;
            Console.WriteLine($"  [ASC] {id}: estimatedUMin={estimatedUMin:F1}% (5th-pct={sortedMin5:F1}%)");

            // Active-period fit: how well does KickBased track the valve when it is actively
            // moving? Good overall fit via zero-baseline does not mean it tracks active control.
            double activeFitKick = -100.0;
            int nActiveSamples = 0;
            if (kick.y != null)
            {
                nActiveSamples = Y_true.Count(y => y > estimatedUMin + 5.0);
                if (nActiveSamples > 10)
                {
                    double meanActive = Y_true.Where(y => y > estimatedUMin + 5.0).Average();
                    double ssea = 0, tssa = 0;
                    for (int i = 0; i < M; i++)
                    {
                        if (Y_true[i] > estimatedUMin + 5.0)
                        {
                            double r  = Y_true[i] - kick.y[i]; ssea += r * r;
                            double dm = Y_true[i] - meanActive; tssa += dm * dm;
                        }
                    }
                    activeFitKick = tssa > 1e-12 ? Math.Max(-100, 100.0 * (1 - ssea / tssa)) : -100.0;
                }
                Console.WriteLine($"  [ASC] {id}: KickBased active-period fit={activeFitKick:F2}%  (nActive={nActiveSamples}/{M})");
            }

            // Over-triggering check: KickBased predicts surge far more often than truth.
            bool kickOverTriggering = false;
            if (kick.y != null && kick.fitScore > 0.0)
            {
                double predSurgeFrac = kick.y.Count(y => y > estimatedUMin + 5.0) / (double)M;
                double trueSurgeFrac = Y_true.Count(y => y > estimatedUMin + 5.0) / (double)M;
                kickOverTriggering = predSurgeFrac > Math.Max(trueSurgeFrac * 3.0, 0.25);
                if (kickOverTriggering)
                    Console.WriteLine($"  [ASC] {id}: KickBased over-triggers (pred={predSurgeFrac:P1} vs truth={trueSurgeFrac:P1}) -- will use fallback");
                else
                    Console.WriteLine($"  [ASC] {id}: KickBased surge fraction OK (pred={predSurgeFrac:P1} vs truth={trueSurgeFrac:P1})");
            }

            // ARX fallback: U[t] = alpha*U[t-1] + beta*LP(PR[t]) + gamma*LP(MF[t]) + const
            // Identified with teacher forcing; free-running evaluation gives honest CL estimate.
            var arx = IdentifyARX(id, Y_true, pIn, pOut, flow, timeBase_s, M,
                                   openTime_s: 5.0, closeTime_s: closeTime_s);

            // Selection: (a) KickBased if active-period fit > 50% and not over-triggering;
            //            (b) ARX if free-run fit > 0 and > 15% of best LinearPR training fit;
            //            (c) LinearPR; (d) best OLS.
            // KickBased threshold is 50%: 30-50% active-period fit means marginal tracking;
            // ARX is more CL-stable in those borderline cases (alpha cap prevents runaway).
            bool kickIsGood = kick.fitScore > 0.0 && activeFitKick > 50.0 && !kickOverTriggering;
            // ARX has alpha≈0.95 memory; when inputs are from predicted models (not CSV),
            // error accumulation destabilises CL. Restrict to direct-input ASCs only.
            bool arxWins    = arx.y != null && arx.fitScore > 0.0
                              && arx.fitScore > Math.Max(0.0, bestPRFit) * 0.50
                              && !usingPredicted;

            if (kickIsGood)
            {
                bestFit = kick.fitScore; bestY = kick.y; bestParams = kick.@params;
                Console.WriteLine($"  [ASC] {id}: -> KickBased selected (activeFit={activeFitKick:F1}%)");
            }
            else if (arxWins)
            {
                bestFit = arx.fitScore; bestY = arx.y; bestParams = arx.@params;
                Console.WriteLine($"  [ASC] {id}: -> ARX selected (fit={arx.fitScore:F1}%, KickBased activeFit={activeFitKick:F1}%)");
            }
            else if (bestPRFit > double.NegativeInfinity)
            {
                bestFit = bestPRFit; bestY = bestPRY; bestParams = bestPRParams;
                Console.WriteLine($"  [ASC] {id}: -> LinearPR selected (fit={bestPRFit:F1}%)");
            }
            else
            {
                Console.WriteLine($"  [ASC] {id}: -> Best OLS selected (fit={bestFit:F1}%)");
            }

            if (bestY == null || bestParams == null)
                return ((double[])Y_true.Clone(),
                        new JObject { ["ModelType"] = "Fallback", ["Reason"] = "All ASC candidates failed" });

            bestParams["UMin"] = estimatedUMin;

            // Override with constant-UMin for quiet ASCs or over-triggering KickBased.
            double surgeFracQ = Y_true.Count(y => y > estimatedUMin + 5.0) / (double)M;
            Console.WriteLine($"  [ASC] {id}: surgeFraction (> UMin+5%)={surgeFracQ:P1}");
            bool quietNoModel = surgeFracQ < 0.05 && !kickIsGood && (arx.y == null || arx.fitScore < 0);
            if (quietNoModel || kickOverTriggering)
            {
                double[] constY = Y_true.Select(_ => estimatedUMin).ToArray();
                double   tss    = Y_true.Select(y => y - Y_true.Average()).Select(d => d * d).Sum();
                double   sse    = constY.Zip(Y_true, (pred, t) => (t - pred) * (t - pred)).Sum();
                double   cFit   = tss > 1e-12 ? Math.Max(-100, 100.0 * (1.0 - sse / tss)) : 0;
                bestFit    = cFit;
                bestY      = constY;
                bestParams = new JObject
                {
                    ["ModelType"]    = "ConstantUMin",
                    ["Architecture"] = "ConstantUMin",
                    ["Value"]        = estimatedUMin,
                    ["UMin"]         = estimatedUMin,
                    ["FitScore"]     = cFit,
                    ["Formula"]      = kickOverTriggering
                        ? $"U(t) = {estimatedUMin:F1}% (KickBased over-triggers -- surgeFrac={surgeFracQ:P1})"
                        : $"U(t) = {estimatedUMin:F1}% (quiet ASC -- surgeFrac={surgeFracQ:P1})"
                };
                string reason = kickOverTriggering ? "KickBased over-triggering" : "quiet ASC";
                Console.WriteLine($"  [ASC] {id}: {reason} -- using constant UMin={estimatedUMin:F1}%");
            }

            bestParams["Benchmark"] = benchmark;
            Console.WriteLine($"[SUCCESS] {id}: best architecture = {bestParams["Architecture"]}, fit={bestFit:F2}%");
            return (bestY, bestParams);
        }

        // ── OLS benchmark ────────────────────────────────────────────────────

        private static void RunOlsBenchmark(
            string id, double[] Y_true,
            double[] pIn, double[] pOut, double[] flow,
            double closeTime_s, double timeBase_s, int M,
            ref JArray benchmark, ref double bestFit, ref double[] bestY, ref JObject bestParams,
            ref double bestPRFit, ref double[] bestPRY, ref JObject bestPRParams)
        {
            var linearFeats = new[] { "P_out", "MF", "P_in", "Const" };
            var dpFeats     = new[] { "DP", "MF", "Const" };
            var surgeFeats  = new[] { "DP", "MF", "MF2", "DP_over_MF2", "Const" };
            var prFeats     = new[] { "PR", "MF", "Const" };
            var prNlFeats   = new[] { "PR", "MF", "MF2", "Const" };
            var candidates  = new List<(string label, string[] features, double lpTau)>
            {
                ("LinearOLS",        linearFeats, 0.0),
                ("LinearOLS_LP1s",   linearFeats, 1.0),
                ("LinearOLS_LP2s",   linearFeats, 2.0),
                ("LinearOLS_LP3s",   linearFeats, 3.0),
                ("LinearOLS_LP5s",   linearFeats, 5.0),
                ("LinearDP_LP2s",    dpFeats,     2.0),
                ("LinearDP_LP3s",    dpFeats,     3.0),
                ("SurgeProxy_LP2s",  surgeFeats,  2.0),
                ("SurgeProxy_LP3s",  surgeFeats,  3.0),
                ("LinearPR_LP2s",    prFeats,     2.0),
                ("LinearPR_LP3s",    prFeats,     3.0),
                ("LinearPR_NL_LP2s", prNlFeats,   2.0),
            };

            foreach (var cand in candidates)
            {
                int K = cand.features.Length;
                double[,] XtX = new double[K, K];
                double[]  Xty = new double[K];
                double[]  feat = new double[K];
                for (int i = 0; i < M; i++)
                {
                    for (int k = 0; k < K; k++)
                        feat[k] = CustomModels.AntiSurgePhysicalModel
                                      .EvaluateFeature(cand.features[k], pIn[i], pOut[i], flow[i]);
                    for (int p = 0; p < K; p++)
                    {
                        Xty[p] += feat[p] * Y_true[i];
                        for (int q = 0; q < K; q++) XtX[p, q] += feat[p] * feat[q];
                    }
                }
                for (int p = 0; p < K; p++) XtX[p, p] += 1e-9;

                double[] w = DynamicPlantRunner.SolveLinearSystem(XtX, Xty, K);
                if (w == null) continue;

                var model = new CustomModels.AntiSurgePhysicalModel(id, new[] { "P_in", "P_out", "Flow" }, id);
                model.modelParameters.Architecture   = cand.label;
                model.modelParameters.FeatureNames   = cand.features;
                model.modelParameters.FeatureWeights = w;
                model.modelParameters.LPFilter_Tau_s = cand.lpTau;
                model.modelParameters.CloseTime_s    = closeTime_s;
                model.WarmStart(null, Y_true[0]);

                double[] y_sim = new double[M];
                double sse = 0, mean = Y_true.Average(), tss = 0;
                for (int i = 0; i < M; i++)
                {
                    y_sim[i] = model.Iterate(new[] { pIn[i], pOut[i], flow[i] }, timeBase_s)[0];
                    double r  = Y_true[i] - y_sim[i]; sse += r * r;
                    double dm = Y_true[i] - mean;      tss += dm * dm;
                }
                double fit = tss > 0 ? Math.Max(-100, 100 * (1 - sse / tss)) : 0;

                var weightsJson = new JObject();
                for (int k = 0; k < K; k++) weightsJson[cand.features[k]] = w[k];
                benchmark.Add(new JObject
                {
                    ["Architecture"]   = cand.label,
                    ["FeatureNames"]   = JArray.FromObject(cand.features),
                    ["LPFilter_Tau_s"] = cand.lpTau,
                    ["FitScore"]       = fit,
                    ["Weights"]        = weightsJson
                });
                Console.WriteLine($"[Model] {id}:   {cand.label,-18} fit={fit,6:F2}%  τ_LP={cand.lpTau:F1}s  weights=[{string.Join(", ", cand.features.Zip(w, (n, v) => $"{n}={v:G3}"))}]");

                var entryParams = new JObject
                {
                    ["ModelType"]      = "AntiSurgePhysicalModel",
                    ["Architecture"]   = cand.label,
                    ["FeatureNames"]   = JArray.FromObject(cand.features),
                    ["FeatureWeights"] = JArray.FromObject(w),
                    ["LPFilter_Tau_s"] = cand.lpTau,
                    ["OpenTime_s"]     = model.modelParameters.OpenTime_s,
                    ["CloseTime_s"]    = model.modelParameters.CloseTime_s,
                    ["FitScore"]       = fit,
                    ["Formula"]        = $"target = Σ w·feature(P_in,P_out,MF) over [{string.Join(", ", cand.features)}]; LP(τ={cand.lpTau:F1}s) → asymmetric rate-limit (open {100.0 / model.modelParameters.OpenTime_s:F1} %/s, close {100.0 / closeTime_s:F2} %/s)."
                };
                if (fit > bestFit)
                {
                    bestFit    = fit;
                    bestY      = y_sim;
                    bestParams = entryParams;
                }
                if (cand.label.StartsWith("LinearPR") && fit > bestPRFit)
                {
                    bestPRFit    = fit;
                    bestPRY      = y_sim;
                    bestPRParams = entryParams;
                }
            }
        }

        // ── KickBased physics model ──────────────────────────────────────────

        private static (double[] y, double fitScore, JObject @params, JObject benchmarkEntry) IdentifyKickBased(
            string id, string comp,
            double[] Y_true,
            double[] pIn,      double[] pOut,      double[] flow,
            double[] pIn_pred, double[] pOut_pred, double[] flow_pred,
            double closeTime_s, double timeBase_s,
            Dictionary<string, double[]> dataset, int M)
        {
            // Estimate operational floor for the level-based in-surge criterion below.
            double sortedMin5Local = Y_true.OrderBy(y => y).Skip((int)(Y_true.Length * 0.05)).First();
            double uMinLocal = (sortedMin5Local >= 2.0) ? sortedMin5Local : 0.0;
            const double surgeDeadZone = 5.0;  // % above UMin floor

            // Derive in-surge mask from K-Spice internals if available, else heuristic
            bool[] inSurge = new bool[M];
            string nfKey   = $"{comp}:NormalizedFlow";
            string nasKey  = $"{comp}:NormalizedAsymmetricLimit";
            bool haveKspiceSurge = dataset.ContainsKey(nfKey) && dataset.ContainsKey(nasKey);
            if (haveKspiceSurge)
            {
                double[] nf  = dataset[nfKey];
                double[] nas = dataset[nasKey];
                for (int i = 0; i < M; i++) inSurge[i] = nf[i] < nas[i];
            }
            else
            {
                for (int i = 1; i < M; i++)
                    inSurge[i] = (Y_true[i] - Y_true[i - 1]) / timeBase_s > 0.5   // actively opening
                                 || Y_true[i] > uMinLocal + surgeDeadZone;           // sustained elevation
            }

            int nSurge = 0, nSafe = 0;
            for (int i = 0; i < M; i++) { if (inSurge[i]) nSurge++; else nSafe++; }

            // Fit linear surge-margin proxy from predicted signals using pressure RATIO (not difference).
            // Surge is fundamentally a ratio phenomenon (compressor head/lift), so P_out/P_in is the
            // physically correct metric; pressure difference at high absolute pressure is misleading.
            double[,] XtX = new double[3, 3];
            double[]  Xty = new double[3];
            if (haveKspiceSurge)
            {
                double[] nfArr  = dataset[nfKey];
                double[] nasArr = dataset[nasKey];
                for (int i = 0; i < M; i++)
                {
                    double margin = nfArr[i] - nasArr[i];
                    double pressureRatio = (pIn_pred[i] > 0.1) ? pOut_pred[i] / pIn_pred[i] : 1.0;
                    double[] row  = { flow_pred[i], pressureRatio, 1.0 };
                    for (int p = 0; p < 3; p++) { Xty[p] += row[p] * margin; for (int q = 0; q < 3; q++) XtX[p, q] += row[p] * row[q]; }
                }
            }
            else
            {
                double wSurge = (nSurge > 0 && nSafe > 0) ? 0.5 * M / Math.Max(1, nSurge) : 1.0;
                double wSafe  = (nSurge > 0 && nSafe > 0) ? 0.5 * M / Math.Max(1, nSafe)  : 1.0;
                for (int i = 0; i < M; i++)
                {
                    double t = inSurge[i] ? -1.0 : +1.0;
                    double w = inSurge[i] ? wSurge : wSafe;
                    double pressureRatio = (pIn_pred[i] > 0.1) ? pOut_pred[i] / pIn_pred[i] : 1.0;
                    double[] row = { flow_pred[i], pressureRatio, 1.0 };
                    for (int p = 0; p < 3; p++) { Xty[p] += w * row[p] * t; for (int q = 0; q < 3; q++) XtX[p, q] += w * row[p] * row[q]; }
                }
            }
            for (int p = 0; p < 3; p++) XtX[p, p] += 1e-9;
            double[] beta = DynamicPlantRunner.SolveLinearSystem(XtX, Xty, 3);

            double surgeProxy_mf = 1.0, surgeProxy_a = 0.0, surgeProxy_b = -35.0, kickThreshold = 0.0;
            if (beta != null && nSurge > 5 && nSafe > 5)
            {
                double sumSd = 0, sumSd2 = 0;
                for (int i = 0; i < M; i++)
                {
                    double pressureRatio = (pIn_pred[i] > 0.1) ? pOut_pred[i] / pIn_pred[i] : 1.0;
                    double sd = beta[0] * flow_pred[i] + beta[1] * pressureRatio + beta[2];
                    sumSd += sd; sumSd2 += sd * sd;
                }
                double meanSd = sumSd / M;
                double stdSd  = Math.Sqrt(Math.Max(1e-12, sumSd2 / M - meanSd * meanSd));
                double scale  = Math.Max(1e-6, stdSd);
                surgeProxy_mf = beta[0] / scale;
                surgeProxy_a  = beta[1] / scale;
                surgeProxy_b  = (beta[2] - meanSd) / scale;  // mean-centered: proxy ≈ 0 at training op-point
            }

            // Estimate kick and ramp rates from trace statistics
            var rises = new List<double>();
            var falls = new List<double>();
            for (int i = 1; i < M; i++)
            {
                double dy = (Y_true[i] - Y_true[i - 1]) / timeBase_s;
                if (inSurge[i]  && dy >  0.2 && Y_true[i]     < 99.5) rises.Add(dy);
                if (!inSurge[i] && dy < -0.2 && Y_true[i - 1] >  5.0) falls.Add(-dy);
            }

            double Pct(List<double> xs, double q)
            {
                if (xs.Count == 0) return double.NaN;
                xs.Sort();
                int idx = (int)Math.Floor(q * (xs.Count - 1));
                return xs[Math.Max(0, Math.Min(xs.Count - 1, idx))];
            }
            double kickRate = double.IsNaN(Pct(rises, 0.9))  ? 8.0  : Pct(rises, 0.9);
            double rampDown = double.IsNaN(Pct(falls, 0.75)) ? 60.0 : Pct(falls, 0.75) * 60.0;

            // Grid search + coordinate-descent refinement
            double peakRef = 0.0;
            for (int i = 0; i < M; i++) if (Y_true[i] > peakRef) peakRef = Y_true[i];

            (double fit, double composite, double[] y) Simulate(double kr, double kg, double rd, double tauR, double tauM, double kThr, double hThr, double peakP)
            {
                var m = new CustomModels.AntiSurgePhysicalModel(id, new[] { "P_in", "P_out", "Flow" }, id);
                m.modelParameters.Architecture              = "KickBased";
                m.modelParameters.SurgeProxy_MF_Coeff       = surgeProxy_mf;
                m.modelParameters.SurgeProxy_a              = surgeProxy_a;
                m.modelParameters.SurgeProxy_b              = surgeProxy_b;
                m.modelParameters.KickThreshold             = kThr;
                m.modelParameters.HoldThreshold             = hThr;
                m.modelParameters.KickRate_PrcPerSec        = kr;
                m.modelParameters.KickGain_PrcPerSecPerUnit = kg;
                m.modelParameters.RampDown_PrcPerMin        = rd;
                m.modelParameters.RampDecay_Tau_s           = tauR;
                m.modelParameters.SurgeMargin_LP_Tau_s      = tauM;
                m.WarmStart(null, Y_true[0]);
                double sse = 0, mean = Y_true.Average(), tss = 0, peakSim = 0;
                double[] ySim = new double[M];
                for (int i = 0; i < M; i++)
                {
                    double yv = m.Iterate(new[] { pIn_pred[i], pOut_pred[i], flow_pred[i] }, timeBase_s)[0];
                    ySim[i] = yv;
                    if (yv > peakSim) peakSim = yv;
                    double r  = Y_true[i] - yv;   sse += r * r;
                    double dm = Y_true[i] - mean;  tss += dm * dm;
                }
                double f = tss > 0 ? Math.Max(-100, 100 * (1 - sse / tss)) : 0;
                return (f, f - peakP * Math.Abs(peakSim - peakRef), ySim);
            }

            double[] kickRateGrid = { 0.0, 2.0, 4.0, 6.0, 8.0, 10.0 };
            double[] gainGrid     = { 0.0, 0.5, 1.0, 1.5, 2.0 };
            double[] rampGrid     = { 0.0, 15.0, 30.0, 60.0 };
            double[] tauRampGrid  = { 0.0, 5.0, 10.0, 20.0, 30.0, 60.0, 100.0, 200.0 };
            double[] thrGrid      = { -2.0, -1.0, -0.5, 0.0 };
            double[] holdDeltas   = { 0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0 };
            double[] tauMargGrid  = { 0.0, 5.0, 15.0, 30.0 };
            const double peakPenalty = 0.4;

            double bestComposite = double.NegativeInfinity;
            double bestKickFit = 0, bestKickRate = 0, bestKickGain = 0;
            double bestRamp = rampDown, bestRampTau = 0, bestMargLP = 0;
            double bestThr = kickThreshold, bestHoldThr = kickThreshold;

            foreach (double kr   in kickRateGrid)
            foreach (double kg   in gainGrid)
            foreach (double rd   in rampGrid)
            foreach (double tauR in tauRampGrid)
            foreach (double tauM in tauMargGrid)
            foreach (double thr  in thrGrid)
            foreach (double hDelta in holdDeltas)
            {
                if (kr == 0.0 && kg == 0.0) continue;
                if (rd == 0.0 && tauR == 0.0) continue;
                var (f, score, _) = Simulate(kr, kg, rd, tauR, tauM, thr, thr + hDelta, peakPenalty);
                if (score > bestComposite)
                {
                    bestComposite = score; bestKickFit = f;
                    bestKickRate = kr; bestKickGain = kg; bestRamp = rd;
                    bestRampTau = tauR; bestMargLP = tauM; bestThr = thr; bestHoldThr = thr + hDelta;
                }
            }
            double gridFit = bestKickFit;

            double[] paramVec   = { bestKickRate, bestKickGain, bestRamp, bestRampTau, bestMargLP, bestThr, bestHoldThr };
            double[] paramSteps = { 1.0, 0.25, 5.0, 20.0, 5.0, 1.0, 2.0 };
            double[] paramMin   = { 0.0, 0.0,  0.0,  0.0,  0.0, -20.0, -20.0 };
            double[] paramMax   = { 100.0, 5.0, 300.0, 500.0, 60.0, 10.0, 50.0 };
            int nMoves = 0;

            for (int iter = 0; iter < 60; iter++)
            {
                bool improved = false;
                for (int p = 0; p < paramVec.Length; p++)
                {
                    foreach (double dir in new[] { -1.0, +1.0 })
                    {
                        double trial = paramVec[p] + dir * paramSteps[p];
                        if (trial < paramMin[p] || trial > paramMax[p]) continue;
                        double tmp = paramVec[p]; paramVec[p] = trial;
                        if (paramVec[6] < paramVec[5]) { paramVec[p] = tmp; continue; }
                        if (paramVec[0] == 0 && paramVec[1] == 0) { paramVec[p] = tmp; continue; }
                        if (paramVec[2] == 0 && paramVec[3] == 0) { paramVec[p] = tmp; continue; }
                        var (f, sc, _) = Simulate(paramVec[0], paramVec[1], paramVec[2], paramVec[3], paramVec[4], paramVec[5], paramVec[6], peakPenalty);
                        if (sc > bestComposite + 1e-6) { bestComposite = sc; bestKickFit = f; improved = true; nMoves++; }
                        else paramVec[p] = tmp;
                    }
                }
                if (!improved)
                {
                    bool anyAlive = false;
                    for (int p = 0; p < paramSteps.Length; p++) { paramSteps[p] *= 0.5; if (paramSteps[p] > 1e-3) anyAlive = true; }
                    if (!anyAlive) break;
                }
            }
            bestKickRate = paramVec[0]; bestKickGain = paramVec[1]; bestRamp     = paramVec[2];
            bestRampTau  = paramVec[3]; bestMargLP   = paramVec[4]; bestThr      = paramVec[5]; bestHoldThr = paramVec[6];
            Console.WriteLine($"[Model] {id}:   KickBased grid={gridFit:F2}% → after {nMoves} coord-descent moves: fit={bestKickFit:F2}%");

            var (_, _, yKick) = Simulate(bestKickRate, bestKickGain, bestRamp, bestRampTau, bestMargLP, bestThr, bestHoldThr, peakPenalty);
            string surgeSrc = haveKspiceSurge ? "K-Spice NormalizedFlow vs NormalizedAsymmetricLimit"
                                              : "Y_true rising-velocity heuristic";
            Console.WriteLine($"[Model] {id}:   KickBased final    fit={bestKickFit,6:F2}%  surge_dist={surgeProxy_mf:F4}·MF+{surgeProxy_a:F4}·PR+{surgeProxy_b:F4}, kThr={bestThr:F2}, holdThr={bestHoldThr:F2}, kr={bestKickRate:F2}%/s, kg={bestKickGain:F3}/unit, ramp={bestRamp:F1}%/min, τ_decay={bestRampTau:F1}s, τ_marg={bestMargLP:F1}s  [PR=P_out/P_in]");

            var entry = new JObject
            {
                ["Architecture"]              = "KickBased",
                ["SurgeProxy_MF_Coeff"]       = surgeProxy_mf,
                ["SurgeProxy_a"]              = surgeProxy_a,
                ["SurgeProxy_b"]              = surgeProxy_b,
                ["KickThreshold"]             = bestThr,
                ["HoldThreshold"]             = bestHoldThr,
                ["KickRate_PrcPerSec"]        = bestKickRate,
                ["KickGain_PrcPerSecPerUnit"] = bestKickGain,
                ["RampDown_PrcPerMin"]        = bestRamp,
                ["RampDecay_Tau_s"]           = bestRampTau,
                ["SurgeMargin_LP_Tau_s"]      = bestMargLP,
                ["FitScore"]                  = bestKickFit,
                ["SurgeTruthSource"]          = surgeSrc
            };

            if (bestKickFit <= 0.0) return (yKick, bestKickFit, null, entry);

            var kickParams = new JObject
            {
                ["ModelType"]                 = "AntiSurgePhysicalModel",
                ["Architecture"]              = "KickBased",
                ["SurgeProxy_MF_Coeff"]       = surgeProxy_mf,
                ["SurgeProxy_a"]              = surgeProxy_a,
                ["SurgeProxy_b"]              = surgeProxy_b,
                ["KickThreshold"]             = bestThr,
                ["HoldThreshold"]             = bestHoldThr,
                ["KickRate_PrcPerSec"]        = bestKickRate,
                ["KickGain_PrcPerSecPerUnit"] = bestKickGain,
                ["RampDown_PrcPerMin"]        = bestRamp,
                ["RampDecay_Tau_s"]           = bestRampTau,
                ["SurgeMargin_LP_Tau_s"]      = bestMargLP,
                ["FitScore"]                  = bestKickFit,
                ["SurgeTruthSource"]          = surgeSrc,
                ["Formula"]                   = $"surge_distance = {surgeProxy_mf:F4}·MF + {surgeProxy_a:F4}·PR + {surgeProxy_b:F4}  [PR=P_out/P_in]"
                                                + (bestMargLP > 0 ? $", LP-filtered (τ={bestMargLP:F1}s)" : "")
                                                + $";  KICK if <{bestThr:F2}: u += ({bestKickRate:F2} + {bestKickGain:F3}·max(0,{bestThr:F2}−surge_distance)) %/s·dt"
                                                + $";  HOLD if [{bestThr:F2},{bestHoldThr:F2}]: u unchanged"
                                                + $";  RAMP if >{bestHoldThr:F2}: u -= ({bestRamp:F1}/60"
                                                + (bestRampTau > 0 ? $" + u/{bestRampTau:F1}" : "") + ") %/s·dt"
            };
            return (yKick, bestKickFit, kickParams, entry);
        }

        // ---- ARX: U[t] = alpha*U[t-1] + beta*LP(PR) + gamma*LP(MF) + const ----

        private static (double[] y, double fitScore, JObject @params) IdentifyARX(
            string id,
            double[] Y_true,
            double[] pIn, double[] pOut, double[] flow,
            double timeBase_s, int M,
            double openTime_s = 5.0,
            double closeTime_s = 60.0)
        {
            double bestFit = double.NegativeInfinity;
            double[] bestY = null;
            JObject bestParams = null;

            foreach (double tau in new[] { 2.0, 3.0, 5.0 })
            {
                double lpAlpha = (tau > 1e-9) ? 1.0 - Math.Exp(-timeBase_s / tau) : 1.0;

                double[] lpPR = new double[M];
                double[] lpMF = new double[M];
                lpPR[0] = (pIn[0] > 0.1) ? pOut[0] / pIn[0] : 1.0;
                lpMF[0] = flow[0];
                for (int i = 1; i < M; i++)
                {
                    double prRaw = (pIn[i] > 0.1) ? pOut[i] / pIn[i] : 1.0;
                    lpPR[i] = lpPR[i - 1] + lpAlpha * (prRaw - lpPR[i - 1]);
                    lpMF[i] = lpMF[i - 1] + lpAlpha * (flow[i] - lpMF[i - 1]);
                }

                // OLS with teacher forcing: columns = [Y_true[t-1], LP_PR[t], LP_MF[t], 1]
                const int K = 4;
                double[,] XtX = new double[K, K];
                double[]  Xty = new double[K];
                for (int i = 1; i < M; i++)
                {
                    double[] row = { Y_true[i - 1], lpPR[i], lpMF[i], 1.0 };
                    for (int p = 0; p < K; p++)
                    {
                        Xty[p] += row[p] * Y_true[i];
                        for (int q = 0; q < K; q++) XtX[p, q] += row[p] * row[q];
                    }
                }
                for (int p = 0; p < K; p++) XtX[p, p] += 1e-9;

                double[] w = DynamicPlantRunner.SolveLinearSystem(XtX, Xty, K);
                if (w == null) continue;

                // Cap alpha at 0.95: reduces steady-state PR gain from ~44 to ~37 for ASC1001
                // (natural alpha=0.958) and leaves ASC0001 (alpha=0.942) unchanged.
                // Rate-limiter handles high-frequency oscillation; alpha cap handles slow drift.
                w[0] = Math.Min(0.95, Math.Max(0.0, w[0]));

                // Cap effective PR gain dU_ss/dPR = w[1]/(1-w[0]) to limit CL feedback amplification.
                // Uncapped gain ~44 amplifies small CL PR errors into sustained slow oscillation.
                // Cap at 25: PR change of 0.3 unit → delta_U_ss = 7.5%, still tracking genuine surge.
                const double maxEffGain = 25.0;
                double effGain = (1.0 - w[0]) > 1e-6 ? w[1] / (1.0 - w[0]) : 100.0;
                if (effGain > maxEffGain && w[1] > 0)
                    w[1] = maxEffGain * (1.0 - w[0]);

                // Operating-point centering: force U_ss = UMin at mean training inputs.
                // Without this, small PR/MF shifts at testset time cause the ARX steady-state
                // to drift, giving -100% test fit even when CL training fit is good.
                double sortedMin5Arx = Y_true.OrderBy(y => y).Skip((int)(Y_true.Length * 0.05)).First();
                double uMinArx = (sortedMin5Arx >= 2.0) ? sortedMin5Arx : 0.0;
                double meanLpPR = lpPR.Average();
                double meanLpMF = lpMF.Average();
                w[3] = uMinArx * (1.0 - w[0]) - w[1] * meanLpPR - w[2] * meanLpMF;

                // FREE-RUNNING simulation for honest CL estimate.
                // Asymmetric rate-limiter matches real valve behavior: slow close (60s)
                // prevents rapid oscillation; fast open (5s) preserves surge response.
                double maxOpen_s  = openTime_s  > 0 ? 100.0 / openTime_s  * timeBase_s : 100.0;
                double maxClose_s = closeTime_s > 0 ? 100.0 / closeTime_s * timeBase_s : 100.0;
                double[] ySim = new double[M];
                ySim[0] = Y_true[0];
                double prLP_fr = (pIn[0] > 0.1) ? pOut[0] / pIn[0] : 1.0;
                double mfLP_fr = flow[0];
                for (int i = 1; i < M; i++)
                {
                    double prRaw = (pIn[i] > 0.1) ? pOut[i] / pIn[i] : 1.0;
                    prLP_fr += lpAlpha * (prRaw - prLP_fr);
                    mfLP_fr += lpAlpha * (flow[i] - mfLP_fr);
                    double uNext = w[0] * ySim[i - 1] + w[1] * prLP_fr + w[2] * mfLP_fr + w[3];
                    double delta = uNext - ySim[i - 1];
                    if (delta >  maxOpen_s)  uNext = ySim[i - 1] + maxOpen_s;
                    if (delta < -maxClose_s) uNext = ySim[i - 1] - maxClose_s;
                    ySim[i] = Math.Max(0.0, Math.Min(100.0, uNext));
                }

                double mean = Y_true.Average();
                double sse = 0, tss = 0;
                for (int i = 0; i < M; i++)
                {
                    double r  = Y_true[i] - ySim[i]; sse += r * r;
                    double dm = Y_true[i] - mean;     tss += dm * dm;
                }
                double fit = tss > 0 ? Math.Max(-100, 100.0 * (1 - sse / tss)) : 0;

                Console.WriteLine($"[Model] {id}:   ARX_LP{tau:F0}s  fit(free-run)={fit:F2}%  alpha={w[0]:F3}  beta_PR={w[1]:F4}  beta_MF={w[2]:F4}  const={w[3]:F4}");

                if (fit > bestFit)
                {
                    bestFit = fit;
                    bestY   = ySim;
                    bestParams = new JObject
                    {
                        ["ModelType"]      = "AntiSurgePhysicalModel",
                        ["Architecture"]   = "ARX",
                        ["FeatureNames"]   = new JArray("U_prev", "LP_PR", "LP_MF", "Const"),
                        ["FeatureWeights"] = new JArray(w[0], w[1], w[2], w[3]),
                        ["LPFilter_Tau_s"] = tau,
                        ["OpenTime_s"]     = openTime_s,
                        ["CloseTime_s"]    = closeTime_s,
                        ["FitScore"]       = fit,
                        ["Formula"]        = $"U[t]={w[0]:F3}*U[t-1]+{w[1]:F4}*LP(PR,{tau:F0}s)+{w[2]:F4}*LP(MF,{tau:F0}s)+{w[3]:F4} [rate-limited open={openTime_s:F0}s close={closeTime_s:F0}s]"
                    };
                }
            }

            return (bestY, bestFit > double.NegativeInfinity ? bestFit : -100.0, bestParams);
        }
    }
}