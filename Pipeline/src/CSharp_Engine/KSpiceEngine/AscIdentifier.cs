using System;
using System.Collections.Generic;
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

            // 3. Read CloseTime from K-Spice model parameters
            double closeTime_s = 60.0;
            foreach (var m in models)
            {
                string rn = ((string)m["Name"]).Replace("_pf", "", StringComparison.OrdinalIgnoreCase);
                if (string.Equals(rn, comp, StringComparison.OrdinalIgnoreCase))
                {
                    var prm = m["Parameters"] as JObject;
                    if (prm?["CloseTime"] != null) closeTime_s = (double)prm["CloseTime"];
                    break;
                }
            }

            // 4. Run OLS multi-architecture benchmark.
            // Results are stored as a fallback: if KickBased fails (fitScore == 0) the
            // best OLS candidate is returned instead.  The benchmark array is always
            // attached to the output JSON so the plotter can compare all architectures.
            // OLS is intentionally overridden by KickBased whenever fitScore > 0 because
            // static OLS weights cause false kick-opens in closed-loop simulation.
            var benchmark = new JArray();
            double bestFit = double.NegativeInfinity;
            double[] bestY = null;
            JObject bestParams = null;
            RunOlsBenchmark(id, Y_true, pIn, pOut, flow, closeTime_s, timeBase_s, M,
                            ref benchmark, ref bestFit, ref bestY, ref bestParams);

            // 5. Fit physics-based KickBased model
            var kick = IdentifyKickBased(id, comp, Y_true, pIn, pOut, flow,
                                         pIn_pred, pOut_pred, flow_pred,
                                         closeTime_s, timeBase_s, dataset, M);
            if (kick.benchmarkEntry != null) benchmark.Add(kick.benchmarkEntry);
            if (kick.fitScore > 0.0)
            {
                bestFit    = kick.fitScore;
                bestY      = kick.y;
                bestParams = kick.@params;
            }

            if (bestY == null || bestParams == null)
                return ((double[])Y_true.Clone(),
                        new JObject { ["ModelType"] = "Fallback", ["Reason"] = "All ASC candidates failed" });

            bestParams["Benchmark"] = benchmark;
            Console.WriteLine($"[SUCCESS] {id}: best architecture = {bestParams["Architecture"]}, fit={bestFit:F2}%");
            return (bestY, bestParams);
        }

        // ── OLS benchmark ────────────────────────────────────────────────────

        private static void RunOlsBenchmark(
            string id, double[] Y_true,
            double[] pIn, double[] pOut, double[] flow,
            double closeTime_s, double timeBase_s, int M,
            ref JArray benchmark, ref double bestFit, ref double[] bestY, ref JObject bestParams)
        {
            var linearFeats = new[] { "P_out", "MF", "P_in", "Const" };
            var dpFeats     = new[] { "DP", "MF", "Const" };
            var surgeFeats  = new[] { "DP", "MF", "MF2", "DP_over_MF2", "Const" };
            var candidates  = new List<(string label, string[] features, double lpTau)>
            {
                ("LinearOLS",       linearFeats, 0.0),
                ("LinearOLS_LP1s",  linearFeats, 1.0),
                ("LinearOLS_LP2s",  linearFeats, 2.0),
                ("LinearOLS_LP3s",  linearFeats, 3.0),
                ("LinearOLS_LP5s",  linearFeats, 5.0),
                ("LinearDP_LP2s",   dpFeats,     2.0),
                ("LinearDP_LP3s",   dpFeats,     3.0),
                ("SurgeProxy_LP2s", surgeFeats,  2.0),
                ("SurgeProxy_LP3s", surgeFeats,  3.0),
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

                if (fit > bestFit)
                {
                    bestFit    = fit;
                    bestY      = y_sim;
                    bestParams = new JObject
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
                    inSurge[i] = (Y_true[i] - Y_true[i - 1]) / timeBase_s > 1.5;
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
                    double dp = pOut_pred[i] - pIn_pred[i];
                    double sd = beta[0] * flow_pred[i] + beta[1] * dp + beta[2];
                    sumSd += sd; sumSd2 += sd * sd;
                }
                double meanSd = sumSd / M;
                double stdSd  = Math.Sqrt(Math.Max(1e-12, sumSd2 / M - meanSd * meanSd));
                double scale  = Math.Max(1e-6, stdSd);
                surgeProxy_mf = beta[0] / scale;
                surgeProxy_a  = beta[1] / scale;
                surgeProxy_b  = beta[2] / scale;
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
            double[] tauRampGrid  = { 0.0, 30.0, 60.0, 100.0, 200.0 };
            double[] thrGrid      = { -2.0, -1.0, -0.5, 0.0 };
            double[] holdDeltas   = { 0.5, 1.0, 1.5, 2.0, 3.0, 5.0 };
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
            double[] paramMax   = { 25.0, 5.0, 300.0, 500.0, 60.0, 10.0, 50.0 };
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
                                                + $";  HOLD if [{bestThr:F2},{bestHoldThr:F2}]: u unchanged (max {120.0:F0}s then slow ramp)"
                                                + $";  RAMP if >{bestHoldThr:F2}: u -= ({bestRamp:F1}/60"
                                                + (bestRampTau > 0 ? $" + u/{bestRampTau:F1}" : "") + ") %/s·dt"
            };
            return (yKick, bestKickFit, kickParams, entry);
        }
    }
}
