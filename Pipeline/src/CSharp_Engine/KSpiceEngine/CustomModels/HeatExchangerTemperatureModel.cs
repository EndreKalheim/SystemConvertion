using System;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class HeatExchangerTemperatureParameters : ModelParametersBaseClass
    {
        public string Subtype { get; set; } = "GasSide";

        // GasSide: T_out = Bias + Alpha * T_in + GainGas * F_gas + GainWater * F_water
        //   inputs[0] = T_gas_in  (hot stream inlet, e.g. compressor outlet)
        //   inputs[1] = F_gas     (gas mass flow — more load → higher outlet temp, GainGas > 0)
        //   inputs[2] = T_cool    — ignored (avoids HX0001 ↔ HX0001s circular dependency)
        //   inputs[3] = F_water   (cold-water partner flow, TIC0003 control variable, GainWater < 0)
        public double Bias      { get; set; } = 0.0;
        public double Alpha     { get; set; } = 0.5;
        public double GainGas   { get; set; } = 0.0;
        public double GainWater { get; set; } = 0.0;

        // WaterSide: T_out = T_cold + CapacityRatio * (T_gas_out - T_cold)
        //   inputs[0] = T_cold_boundary (constant cold-water inlet, e.g. COLD0001 at ~15 °C)
        //   inputs[2] = T_gas_out (already-predicted gas-side outlet from this timestep)
        //   CapacityRatio ≈ (m_gas * Cp_gas) / (m_water * Cp_water) — fitted from data
        public double CapacityRatio { get; set; } = 1.0;
    }

    /// <summary>
    /// Two-subtype surrogate for a heat-exchanger outlet temperature.
    ///
    /// GasSide (hot gas being cooled, e.g. 23HX0001):
    ///   T_out = Bias + Alpha * T_gas_in
    ///   Single-input linear model. Does NOT reference the cooling-water outlet temperature,
    ///   which eliminates the HX0001 ↔ HX0001s circular dependency.
    ///
    /// WaterSide (cold water being heated, e.g. 23HX0001s):
    ///   T_out = T_cold_in + CapacityRatio * (T_gas_out - T_cold_in)
    ///   Energy-balance form with one fitted parameter. T_gas_out is the already-predicted
    ///   gas-side output from this timestep, so there is no circular dependency.
    /// </summary>
    public class HeatExchangerTemperatureModel : ModelBaseClass, ISimulatableModel
    {
        public HeatExchangerTemperatureParameters modelParameters = new HeatExchangerTemperatureParameters();
        private double lastOutput;

        public HeatExchangerTemperatureModel()
        {
            this.processModelType = ModelType.SubProcess;
        }

        public HeatExchangerTemperatureModel(string id, string[] inputIDs, string outputID = null) : this()
        {
            this.ID = id;
            this.outputID = outputID ?? id;
            this.ModelInputIDs = inputIDs;
        }

        public override int GetLengthOfInputVector() => this.ModelInputIDs?.Length ?? 0;
        public override SignalType GetOutputSignalType() => SignalType.Output_Y;
        public bool IsModelSimulatable(out string explanationStr) { explanationStr = "OK"; return true; }

        public ISimulatableModel Clone(string cloneID)
        {
            return new HeatExchangerTemperatureModel(cloneID, (string[])this.ModelInputIDs.Clone(), this.outputID)
            {
                modelParameters = new HeatExchangerTemperatureParameters
                {
                    Subtype       = this.modelParameters.Subtype,
                    Bias          = this.modelParameters.Bias,
                    Alpha         = this.modelParameters.Alpha,
                    GainGas       = this.modelParameters.GainGas,
                    GainWater     = this.modelParameters.GainWater,
                    CapacityRatio = this.modelParameters.CapacityRatio
                }
            };
        }

        public void WarmStart(double[] inputs, double output)
        {
            lastOutput = output;
        }

        public double[] Iterate(double[] inputs, double timeBase_s, double badDataID = -9999)
        {
            var p = modelParameters;

            if (p.Subtype == "WaterSide")
            {
                if (inputs == null || inputs.Length < 3) return new double[] { lastOutput };
                double tcold = inputs[0]; // cold boundary (~15 °C)
                double thot  = inputs[2]; // gas-side outlet, already predicted this step
                if (double.IsNaN(tcold) || double.IsNaN(thot) ||
                    tcold == badDataID   || thot == badDataID)
                    return new double[] { lastOutput };
                lastOutput = tcold + p.CapacityRatio * (thot - tcold);
                return new double[] { lastOutput };
            }

            // GasSide
            if (inputs == null || inputs.Length < 1) return new double[] { lastOutput };
            double tin = inputs[0]; // hot gas inlet temperature
            if (double.IsNaN(tin) || tin == badDataID) return new double[] { lastOutput };
            double fg = inputs.Length >= 2 ? inputs[1] : 0.0; // gas mass flow (load)
            if (double.IsNaN(fg) || fg == badDataID) fg = 0.0;
            double fw = inputs.Length >= 4 ? inputs[3] : 0.0; // cold-water partner flow
            if (double.IsNaN(fw) || fw == badDataID) fw = 0.0;
            lastOutput = p.Bias + p.Alpha * tin + p.GainGas * fg + p.GainWater * fw;
            return new double[] { lastOutput };
        }

        public double? GetSteadyStateInput(double x0, int inputIdx = 0, double[] givenInputValues = null) => null;
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) => lastOutput;
    }
}
