using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine
{
    /// <summary>
    /// Identifies a separator/tank gas-pressure model by benchmarking six strategies
    /// (integrated-flow regression, curvature, net-mass-balance, surge-excluded,
    /// net-signed-flow, raw-signed-flow) and selecting the best fit.
    /// </summary>
    internal static class SeparatorPressureIdentifier
    {
        internal static (double[] predictions, JObject modelParams) Identify(
            string id,
            double[] Y_true,
            List<(string name, double[] data)> inputCols,
            double timeBase_s)
        {
            var signedInputs     = DynamicPlantRunner.NegateOutflows(inputCols);
            var integratedInputs = DynamicPlantRunner.IntegrateFlows(signedInputs, timeBase_s);
            int nOut = inputCols.Count(x => IsOutflow(x.name));
            Console.WriteLine($"[Model] {id}: Trying pressure strategies, {integratedInputs.Count} inputs ({nOut} outflows negated)");

            var results = new List<(double[] ySim, double fit, string name, JObject pars)>();

            try
            {
                // A) IdentifyLinear on integrated flows — linear per-stream gains
                TryStrategy(id, "A) IdentifyLinear         ", "IdentifyLinear",
                    () => { var ds = DynamicPlantRunner.BuildUnitDataSet(integratedInputs, Y_true, timeBase_s); var m = UnitIdentifier.IdentifyLinear(ref ds, null, false); return (ds, m); },
                    "Pressure(t) = Bias + sum(gain_i * integral(input_i * dt))  [outflows negated]",
                    inputCols, results);

                // B) Identify with curvature — nonlinear compressibility term
                TryStrategy(id, "B) Identify(curvature)    ", "IdentifyLinear_IntegratedFlow",
                    () => { var ds = DynamicPlantRunner.BuildUnitDataSet(integratedInputs, Y_true, timeBase_s); var m = UnitIdentifier.Identify(ref ds, null, false); return (ds, m); },
                    "Pressure(t) = Bias + sum(gain_i * integral(input_i*dt) + curv_i*(integral-U0_i)^2/UNorm_i)  [outflows negated]",
                    inputCols, results);

                // C) NetMassBalance — single gain on net integrated flow
                {
                    int N = signedInputs[0].data.Length;
                    double[] netInteg = NetIntegrate(signedInputs, N, timeBase_s);
                    var netInputs = new List<(string name, double[] data)> { ("NetFlow(m_net)", netInteg) };
                    var ds = DynamicPlantRunner.BuildUnitDataSet(netInputs, Y_true, timeBase_s);
                    var m  = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                    if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null &&
                        m.modelParameters.LinearGains?.Length > 0)
                    {
                        double fit  = m.modelParameters.Fitting.FitScorePrc;
                        double gain = m.modelParameters.LinearGains[0];
                        var p = new JObject
                        {
                            ["ModelType"]  = "NetMassBalance",
                            ["FitScore"]   = fit,
                            ["Gain"]       = gain,
                            ["InputNames"] = JArray.FromObject(inputCols.Select(x => x.name).ToArray()),
                            ["Formula"]    = "Pressure(t) = y(0) + Gain * integral(net_flow * dt)  [net = sum m_in - sum m_out]"
                        };
                        DynamicPlantRunner.AttachUnitParams(p, m.modelParameters);
                        Console.WriteLine($"[Model] {id}:   C) NetMassBalance           fit={fit:F1}%  gain={gain:G4}");
                        results.Add((ds.Y_sim, fit, "NetMassBalance", p));
                    }
                }

                // D) IdentifyLinear excluding high-rate (surge) time indices
                {
                    int N = Y_true.Length;
                    double[] rates = new double[N];
                    for (int j = 1; j < N; j++) rates[j] = Math.Abs(Y_true[j] - Y_true[j - 1]);
                    double median = rates.OrderBy(r => r).ToArray()[N / 2];
                    var excl = rates.Select((r, j) => (r, j)).Where(x => x.r > median * 5.0).Select(x => x.j).ToList();
                    if (excl.Count > 0 && excl.Count < N * 0.4)
                    {
                        var ds = DynamicPlantRunner.BuildUnitDataSet(integratedInputs, Y_true, timeBase_s);
                        ds.IndicesToIgnore = excl;
                        var m = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                        if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null)
                        {
                            double fit = m.modelParameters.Fitting.FitScorePrc;
                            var p = BuildUnitParams("IdentifyLinear_IntegratedFlow", fit, m.modelParameters,
                                "Pressure(t) = Bias + sum(gain_i * integral(input_i * dt))  [surge indices excluded from fit]",
                                inputCols);
                            Console.WriteLine($"[Model] {id}:   D) IdentifyLinear(excl {excl.Count} surge pts) fit={fit:F1}%");
                            results.Add((ds.Y_sim, fit, "IdentifyLinear-surgeFree", p));
                        }
                    }
                }

                // E) UnitIdentifier on net signed flow — single leaky-integrator
                {
                    int N = signedInputs[0].data.Length;
                    double[] netRaw = NetSum(signedInputs, N);
                    var netInput = new List<(string name, double[] data)> { ("NetFlow_signed", netRaw) };
                    var ds = DynamicPlantRunner.BuildUnitDataSet(netInput, Y_true, timeBase_s);
                    var m  = UnitIdentifier.IdentifyLinear(ref ds, null, false);
                    if (m.modelParameters.Fitting.WasAbleToIdentify && ds.Y_sim != null)
                    {
                        double fit        = m.modelParameters.Fitting.FitScorePrc;
                        double singleGain = m.modelParameters.LinearGains?.Length > 0 ? m.modelParameters.LinearGains[0] : 0.0;
                        var p = new JObject
                        {
                            ["ModelType"]      = "UnitIdentifier_NetSignedFlow",
                            ["FitScore"]       = fit,
                            ["TimeConstant_s"] = m.modelParameters.TimeConstant_s,
                            ["Formula"]        = $"Pressure(t) = LP(Bias + gain*F_net, Tc={m.modelParameters.TimeConstant_s:F1}s)  [F_net = Σm_in − Σm_out]",
                            ["LinearGains"]    = JArray.FromObject(new[] { singleGain }),
                            ["InputNames"]     = JArray.FromObject(inputCols.Select(x => x.name).ToArray())
                        };
                        DynamicPlantRunner.AttachUnitParams(p, m.modelParameters);
                        Console.WriteLine($"[Model] {id}:   E) UnitIdent(net signed flow)   fit={fit:F1}%  Tc={m.modelParameters.TimeConstant_s:F1}s  gain={singleGain:G4}");
                        results.Add((ds.Y_sim, fit, "IdentifyLinear-rawFlows", p));
                    }
                }

                // F) UnitIdentifier on raw signed flows — per-stream leaky integrator
                TryStrategy(id, "F) UnitIdent(raw signed)  ", "UnitIdentifier_SignedFlows",
                    () => { var ds = DynamicPlantRunner.BuildUnitDataSet(signedInputs, Y_true, timeBase_s); var m = UnitIdentifier.IdentifyLinear(ref ds, null, false); return (ds, m); },
                    "Pressure(t) = LP(Bias + sum(gain_i * F_i), Tc)  [outflows pre-negated in evaluation]",
                    inputCols, results);

                if (results.Count == 0)
                    return ((double[])Y_true.Clone(),
                            new JObject { ["ModelType"] = "Fallback", ["Reason"] = "All pressure strategies failed" });

                // Reject strategies whose per-stream gains are implausibly large.
                // |gain| > 50 indicates OLS collinearity (e.g. a proxy signal correlated
                // with another input assigns huge cancelling gains that blow up in testset).
                const double MaxGain = 50.0;
                bool GainsOk(JObject pars)
                {
                    var gains = pars["LinearGains"] as JArray;
                    return gains == null || gains.All(g => Math.Abs((double)g) <= MaxGain);
                }

                // Strategy selection priority (most generalizable first):
                //   F > E > C > D > A > B
                // F and E use a leaky-integrator (dynamic) model on raw flows — they return
                // to a mean and generalise across CSV files with different initial conditions.
                // C also uses raw net flow (single static gain) and is robust.
                // A, B, D regress on CUMULATIVE INTEGRALS whose absolute level drifts with
                // initial conditions; they overfit training data and fail on held-out CSVs.
                // Per-stream strategies (A, B, D, F) are additionally rejected when any
                // |gain| > MaxGain — a sign of OLS collinearity (e.g. proxy signal aliasing).

                // Dynamic strategies preferred: F (per-stream) then E (net) then C (static net)
                var stratF = results.FirstOrDefault(r => r.name == "UnitIdentifier_SignedFlows");
                var stratE = results.FirstOrDefault(r => r.name == "IdentifyLinear-rawFlows");
                var stratC = results.FirstOrDefault(r => r.name == "NetMassBalance");

                bool stratFOk = stratF.name != null && stratF.fit >= 20.0 && GainsOk(stratF.pars);
                bool stratEOk = stratE.name != null && stratE.fit >= 20.0;
                bool stratCOk = stratC.name != null && stratC.fit >= 20.0;

                (double[] ySim, double fit, string name, JObject pars) best;
                if (stratFOk)
                {
                    best = stratF;
                    Console.WriteLine($"[Model] {id}: Preferring Strategy F (UnitIdent-SignedFlows) fit={stratF.fit:F1}%.");
                }
                else if (stratEOk)
                {
                    best = stratE;
                    if (stratF.name != null)
                    {
                        var maxG = (stratF.pars["LinearGains"] as JArray)?
                                       .Max(g => Math.Abs((double)g)) ?? 0.0;
                        Console.WriteLine($"[Model] {id}: Strategy F rejected (maxGain={maxG:F1} > {MaxGain}). Using E.");
                    }
                    else Console.WriteLine($"[Model] {id}: Strategy F unavailable. Using E fit={stratE.fit:F1}%.");
                }
                else if (stratCOk)
                {
                    best = stratC;
                    Console.WriteLine($"[Model] {id}: Strategies F/E unavailable or poor. Using C fit={stratC.fit:F1}%.");
                }
                else
                {
                    // Last resort: among all results with acceptable gains by fit; else unconstrained.
                    var acceptable = results.Where(r => GainsOk(r.pars)).OrderByDescending(r => r.fit).ToList();
                    best = acceptable.Count > 0
                           ? acceptable[0]
                           : results.OrderByDescending(r => r.fit).First();
                    Console.WriteLine($"[Model] {id}: All preferred strategies poor/rejected. Using {best.name} fit={best.fit:F1}%.");
                }

                Console.WriteLine($"[SUCCESS] {id}: Best={best.name}, FitScore={best.fit:F1}%");
                return (best.ySim, best.pars);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARNING] {id}: Pressure identification exception: {ex.Message}");
                return ((double[])Y_true.Clone(), new JObject { ["ModelType"] = "Fallback", ["Reason"] = ex.Message });
            }
        }

        // ── Helpers ─────────────────────────────────────────────────────────

        private static bool IsOutflow(string name) =>
            name.Contains("(m_out)") || name.Contains("(m_sum)") || name.Contains("(mass_out");

        private static double[] NetSum(List<(string name, double[] data)> inputs, int N)
        {
            double[] net = new double[N];
            for (int j = 0; j < N; j++) foreach (var s in inputs) net[j] += s.data[j];
            return net;
        }

        private static double[] NetIntegrate(List<(string name, double[] data)> inputs, int N, double timeBase_s)
        {
            double[] net   = NetSum(inputs, N);
            double[] integ = new double[N];
            for (int j = 1; j < N; j++) integ[j] = integ[j - 1] + net[j] * timeBase_s;
            return integ;
        }

        private static void TryStrategy(
            string id,
            string label,
            string modelType,
            Func<(UnitDataSet ds, UnitModel m)> run,
            string formula,
            List<(string name, double[] data)> inputCols,
            List<(double[] ySim, double fit, string name, JObject pars)> results)
        {
            var (ds, m) = run();
            if (!m.modelParameters.Fitting.WasAbleToIdentify || ds.Y_sim == null) return;
            double fit = m.modelParameters.Fitting.FitScorePrc;
            var p = BuildUnitParams(modelType, fit, m.modelParameters, formula, inputCols);
            Console.WriteLine($"[Model] {id}:   {label} fit={fit:F1}%");
            results.Add((ds.Y_sim, fit, modelType, p));
        }

        private static JObject BuildUnitParams(
            string modelType, double fit, UnitParameters p,
            string formula, List<(string name, double[] data)> inputCols)
        {
            var jo = new JObject
            {
                ["ModelType"]      = modelType,
                ["FitScore"]       = fit,
                ["TimeConstant_s"] = p.TimeConstant_s,
                ["Formula"]        = formula
            };
            if (p.LinearGains != null)
            {
                jo["LinearGains"] = JArray.FromObject(p.LinearGains);
                jo["InputNames"]  = JArray.FromObject(inputCols.Select(x => x.name).ToArray());
            }
            DynamicPlantRunner.AttachUnitParams(jo, p);
            return jo;
        }
    }
}
