using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace KSpiceEngine
{
    /// <summary>
    /// Identifies parameters for a heat-exchanger outlet temperature model.
    ///
    /// GasSide (hot gas being cooled, e.g. 23HX0001):
    ///   T_out = Bias + Alpha * T_gas_in + GainGas * F_gas + GainWater * F_water
    ///   Four-parameter OLS. GainWater must be negative (more cooling water → lower T_out);
    ///   if OLS gives a positive value (PID endogeneity), falls back to NTU-effectiveness
    ///   physics estimate and refits the remaining three parameters.
    ///
    /// WaterSide (cold water being heated, e.g. 23HX0001s):
    ///   T_out = T_cold + CapacityRatio * (T_gas_out - T_cold)
    ///   Energy-balance form with one fitted parameter (CapacityRatio).
    /// </summary>
    internal static class HeatExchangerIdentifier
    {
        /// <param name="gasFlowOverride">
        /// Optional (sourceModelId, data) pair to substitute for the F_gas slot (inputs[1]).
        /// When provided, the GasSide model is trained against this signal instead of whatever
        /// the topology routed as the local_var input. The source model ID is stored in
        /// GasFlowSourceId so the closed-loop evaluator uses the same signal.
        /// </param>
        public static (double[] pred, JObject pars) Identify(
            string id, double[] Y_true,
            List<(string name, double[] data)> inputCols, double timeBase_s,
            (string modelId, double[] data)? gasFlowOverride = null)
        {
            if (inputCols.Count < 3)
            {
                Console.WriteLine($"[WARNING] {id}: HX temperature needs at least 3 inputs, found {inputCols.Count}.");
                return ((double[])Y_true.Clone(),
                        new JObject { ["ModelType"] = "Fallback", ["Reason"] = "HX temperature needs at least 3 inputs" });
            }

            try
            {
                int n = Y_true.Length;
                for (int k = 0; k < Math.Min(inputCols.Count, 3); k++)
                    n = Math.Min(n, inputCols[k].data.Length);
                if (n < 5)
                    return ((double[])Y_true.Clone(),
                            new JObject { ["ModelType"] = "Fallback", ["Reason"] = "Not enough samples for HX fit" });

                // Subtype detection: if mean T_in > mean Y the hot stream is being cooled (GasSide).
                // If mean T_in < mean Y the cold stream is being heated (WaterSide).
                double sumTin = 0, sumY = 0;
                int validN = 0;
                for (int i = 0; i < n; i++)
                {
                    if (double.IsNaN(inputCols[0].data[i]) || double.IsNaN(Y_true[i])) continue;
                    sumTin += inputCols[0].data[i];
                    sumY   += Y_true[i];
                    validN++;
                }
                if (validN < 1)
                    return ((double[])Y_true.Clone(),
                            new JObject { ["ModelType"] = "Fallback", ["Reason"] = "No valid samples for HX fit" });
                double meanTin = sumTin / validN;
                double meanY   = sumY   / validN;

                bool isWaterSide = meanTin < meanY;
                Console.WriteLine($"[Model] {id}: HX subtype={(isWaterSide ? "WaterSide" : "GasSide")} (mean Tin={meanTin:F1} °C, mean Y={meanY:F1} °C)");

                // Apply optional F_gas override: substitute inputCols[1] with the override signal.
                string gasFlowSourceId = null;
                if (!isWaterSide && gasFlowOverride.HasValue && inputCols.Count >= 2)
                {
                    var ov = gasFlowOverride.Value;
                    inputCols = new List<(string, double[])>(inputCols)
                        { [1] = ($"{ov.modelId}(local_var)", ov.data) };
                    gasFlowSourceId = ov.modelId;
                    Console.WriteLine($"[Model] {id}: F_gas slot overridden → {ov.modelId}");
                }

                double[] pred = new double[Y_true.Length];
                pred[0] = Y_true[0];
                JObject pars;

                if (!isWaterSide)
                    pars = IdentifyGasSide(id, Y_true, inputCols, n, pred, timeBase_s, gasFlowSourceId);
                else
                    pars = IdentifyWaterSide(id, Y_true, inputCols, n, pred, timeBase_s);

                return (pred, pars);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARNING] {id}: HX temperature identification failed: {ex.Message}");
                return ((double[])Y_true.Clone(),
                        new JObject { ["ModelType"] = "Fallback", ["Reason"] = ex.Message });
            }
        }

        // ── GasSide: T_out = Bias + Alpha * T_in + GainGas * F_gas + GainWater * F_water ──
        //
        // inputs[0] = T_gas_in  (hot stream inlet temperature)
        // inputs[1] = F_gas     (gas mass flow — more load → higher T_out, GainGas > 0)
        // inputs[2] = T_cool    — ignored (avoids HX0001 ↔ HX0001s circular dependency)
        // inputs[3] = F_water   (cold-water partner flow, TIC0003 control variable, GainWater < 0)
        //
        // GainWater sign: MUST be negative. OLS often gives positive due to PID endogeneity
        // (F_water is controlled to chase T_out → spurious positive correlation). Falls back
        // to NTU-effectiveness physics estimate and refits the other three parameters.
        private static JObject IdentifyGasSide(
            string id, double[] Y_true,
            List<(string name, double[] data)> inputCols,
            int n, double[] pred, double timeBase_s, string gasFlowSourceId = null)
        {
            bool hasFgas   = inputCols.Count >= 2;
            bool hasFwater = inputCols.Count >= 4;

            double s1 = 0, sTin = 0, sTin2 = 0, sY = 0, sTinY = 0;
            double sFg = 0, sFg2 = 0, sTinFg = 0, sFgY = 0;
            double sFw = 0, sFw2 = 0, sTinFw = 0, sFwY = 0;
            double sFgFw = 0;
            int cnt = 0;
            for (int i = 1; i < n; i++)
            {
                double tin = inputCols[0].data[i - 1];
                double fg  = hasFgas   ? inputCols[1].data[Math.Min(i - 1, inputCols[1].data.Length - 1)] : 0.0;
                double fw  = hasFwater ? inputCols[3].data[Math.Min(i - 1, inputCols[3].data.Length - 1)] : 0.0;
                double y   = Y_true[i];
                if (double.IsNaN(tin) || double.IsNaN(y)) continue;
                if (hasFgas   && double.IsNaN(fg)) continue;
                if (hasFwater && double.IsNaN(fw)) continue;
                s1++; sTin += tin; sTin2 += tin * tin; sY += y; sTinY += tin * y;
                sFg += fg; sFg2 += fg * fg; sTinFg += tin * fg; sFgY += fg * y;
                sFw += fw; sFw2 += fw * fw; sTinFw += tin * fw; sFwY += fw * y;
                sFgFw += fg * fw;
                cnt++;
            }
            if (cnt < 4)
                return new JObject { ["ModelType"] = "Fallback", ["Reason"] = "GasSide: too few valid samples" };

            double bias, alpha, gainGas = 0.0, gainWater = 0.0;
            {
                // 4×4 normal equations: [1, T_in, F_gas, F_water] → [Bias, Alpha, GainGas, GainWater]
                double[,] A = {
                    { s1,    sTin,   sFg,    sFw    },
                    { sTin,  sTin2,  sTinFg, sTinFw },
                    { sFg,   sTinFg, sFg2,   sFgFw  },
                    { sFw,   sTinFw, sFgFw,  sFw2   }
                };
                double[] b   = { sY, sTinY, sFgY, sFwY };
                double[] sol = DynamicPlantRunner.SolveLinearSystem(A, b, 4);
                bias      = sol[0];
                alpha     = sol[1];
                gainGas   = hasFgas   ? sol[2] : 0.0;
                gainWater = hasFwater ? sol[3] : 0.0;
            }

            // GainWater must be negative (more cold water → lower T_out).
            // A positive OLS result signals PID endogeneity → NTU-effectiveness fallback.
            if (hasFwater && gainWater >= 0)
            {
                const double T_COLD_BOUNDARY       = 15.0;
                const double EFFECTIVENESS_EXPONENT = 0.4;
                double meanTinOp = sTin / cnt;
                double meanYOp   = sY   / cnt;
                double meanFwOp  = sFw  / cnt;
                double dTmax     = Math.Max(meanTinOp - T_COLD_BOUNDARY, 1.0);
                double epsilon   = Math.Max(0.0, meanTinOp - meanYOp) / dTmax;
                gainWater        = -EFFECTIVENESS_EXPONENT * epsilon * dTmax / Math.Max(meanFwOp, 1.0);
                Console.WriteLine($"[Model] {id}: OLS GainWater was positive (PID endogeneity). "
                                + $"Using physics estimate: {gainWater:G4} (ε={epsilon:F3}, meanFw={meanFwOp:F1})");

                // Refit Bias, Alpha, GainGas with F_water contribution removed.
                double s1r = 0, sTinr = 0, sTin2r = 0, sFgr = 0, sFg2r = 0, sTinFgr = 0;
                double sYr = 0, sTinYr = 0, sFgYr = 0;
                for (int i = 1; i < n; i++)
                {
                    double tin = inputCols[0].data[i - 1];
                    double fg  = hasFgas   ? inputCols[1].data[Math.Min(i - 1, inputCols[1].data.Length - 1)] : 0.0;
                    double fw  = hasFwater ? inputCols[3].data[Math.Min(i - 1, inputCols[3].data.Length - 1)] : 0.0;
                    double y   = Y_true[i] - gainWater * fw;
                    if (double.IsNaN(tin) || double.IsNaN(y) || double.IsNaN(fg) || double.IsNaN(fw)) continue;
                    s1r++; sTinr += tin; sTin2r += tin * tin; sFgr += fg; sFg2r += fg * fg; sTinFgr += tin * fg;
                    sYr += y; sTinYr += tin * y; sFgYr += fg * y;
                }
                double[,] Ar = {
                    { s1r,   sTinr,   sFgr    },
                    { sTinr, sTin2r,  sTinFgr },
                    { sFgr,  sTinFgr, sFg2r   }
                };
                double[] br   = { sYr, sTinYr, sFgYr };
                double[] solr = DynamicPlantRunner.SolveLinearSystem(Ar, br, 3);
                bias    = solr[0];
                alpha   = solr[1];
                gainGas = hasFgas ? solr[2] : 0.0;
            }

            var model = new CustomModels.HeatExchangerTemperatureModel(
                id, new[] { "UPSTREAM_TEMP", "MassFlow", "COOLING_TEMP", "PARTNER_FLOW" }, id);
            model.modelParameters.Subtype   = "GasSide";
            model.modelParameters.Bias      = bias;
            model.modelParameters.Alpha     = alpha;
            model.modelParameters.GainGas   = gainGas;
            model.modelParameters.GainWater = gainWater;
            model.WarmStart(null, Y_true[0]);

            for (int i = 1; i < Y_true.Length; i++)
            {
                double tin = inputCols[0].data[Math.Min(i - 1, inputCols[0].data.Length - 1)];
                double fg  = hasFgas   ? inputCols[1].data[Math.Min(i - 1, inputCols[1].data.Length - 1)] : 0.0;
                double fw  = hasFwater ? inputCols[3].data[Math.Min(i - 1, inputCols[3].data.Length - 1)] : 0.0;
                pred[i] = model.Iterate(new[] { tin, fg, 0.0, fw }, timeBase_s)[0];
            }

            double fitScore = FitScore(Y_true, pred);
            Console.WriteLine($"[SUCCESS] {id}: GasSide FitScore={fitScore:F1}% "
                            + $"(Bias={bias:G4}, Alpha={alpha:G4}, GainGas={gainGas:G4}, GainWater={gainWater:G4})");

            var pars = new JObject
            {
                ["ModelType"]  = "HeatExchangerTemperatureModel",
                ["Subtype"]    = "GasSide",
                ["FitScore"]   = fitScore,
                ["Bias"]       = bias,
                ["Alpha"]      = alpha,
                ["GainGas"]    = gainGas,
                ["GainWater"]  = gainWater,
                ["Formula"]    = "T_out = Bias + Alpha * T_in + GainGas * F_gas + GainWater * F_water",
                ["InputNames"] = JArray.FromObject(inputCols.Select(x => x.name).ToArray())
            };
            if (gasFlowSourceId != null)
                pars["GasFlowSourceId"] = gasFlowSourceId;
            return pars;
        }

        // ── WaterSide: T_out = T_cold + CapacityRatio * (T_gas_out - T_cold) ──
        //
        // inputs[0] = T_cold_boundary (constant cold-water inlet, ~15 °C)
        // inputs[2] = T_gas_out       (gas-side HX outlet, already predicted this timestep)
        // CapacityRatio ≈ (m_gas * Cp_gas) / (m_water * Cp_water) — fitted from data.
        // No circular dependency: HX0001_Temperature is computed before HX0001s in topo order.
        private static JObject IdentifyWaterSide(
            string id, double[] Y_true,
            List<(string name, double[] data)> inputCols,
            int n, double[] pred, double timeBase_s)
        {
            double sXX = 0, sXY = 0;
            int cnt = 0;
            for (int i = 1; i < n; i++)
            {
                double tcold = inputCols[0].data[i - 1];
                double tgas  = inputCols[2].data[i - 1];
                double y     = Y_true[i];
                if (double.IsNaN(tcold) || double.IsNaN(tgas) || double.IsNaN(y)) continue;
                double dx = tgas - tcold;
                double dy = y    - tcold;
                sXX += dx * dx;
                sXY += dx * dy;
                cnt++;
            }
            if (cnt < 2)
                return new JObject { ["ModelType"] = "Fallback", ["Reason"] = "WaterSide: too few valid samples" };

            double capacityRatio = sXX > 1e-10 ? sXY / sXX : 1.0;

            var model = new CustomModels.HeatExchangerTemperatureModel(
                id, new[] { "UPSTREAM_TEMP", "MassFlow", "COOLING_TEMP", "PARTNER_FLOW" }, id);
            model.modelParameters.Subtype       = "WaterSide";
            model.modelParameters.CapacityRatio = capacityRatio;
            model.WarmStart(null, Y_true[0]);

            for (int i = 1; i < Y_true.Length; i++)
            {
                double tcold = inputCols[0].data[Math.Min(i - 1, inputCols[0].data.Length - 1)];
                double fwat  = inputCols.Count > 1 ? inputCols[1].data[Math.Min(i - 1, inputCols[1].data.Length - 1)] : 0.0;
                double tgas  = inputCols[2].data[Math.Min(i - 1, inputCols[2].data.Length - 1)];
                pred[i] = model.Iterate(new[] { tcold, fwat, tgas, 0.0 }, timeBase_s)[0];
            }

            double fitScore = FitScore(Y_true, pred);
            Console.WriteLine($"[SUCCESS] {id}: WaterSide FitScore={fitScore:F1}% (CapacityRatio={capacityRatio:G4})");

            return new JObject
            {
                ["ModelType"]     = "HeatExchangerTemperatureModel",
                ["Subtype"]       = "WaterSide",
                ["FitScore"]      = fitScore,
                ["CapacityRatio"] = capacityRatio,
                ["Formula"]       = "T_out = T_cold + CapacityRatio * (T_gas_out - T_cold)",
                ["InputNames"]    = JArray.FromObject(inputCols.Select(x => x.name).ToArray())
            };
        }

        private static double FitScore(double[] yTrue, double[] yPred)
        {
            double meanTrue = 0; int cnt = 0;
            foreach (var v in yTrue) { if (!double.IsNaN(v)) { meanTrue += v; cnt++; } }
            if (cnt == 0) return 0;
            meanTrue /= cnt;
            double sse = 0, tss = 0;
            for (int i = 0; i < yTrue.Length; i++)
            {
                if (double.IsNaN(yTrue[i]) || double.IsNaN(yPred[i])) continue;
                double r = yTrue[i] - yPred[i]; sse += r * r;
                double d = yTrue[i] - meanTrue;  tss += d * d;
            }
            return tss > 0 ? Math.Max(-100, 100.0 * (1.0 - sse / tss)) : 0;
        }
    }
}
