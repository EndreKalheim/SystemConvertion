using System;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class ThermalMixingParameters : ModelParametersBaseClass
    {
        /// <summary>Baseline hold-up mass when levels are unavailable (kg).</summary>
        public double M0 { get; set; } = 8000.0;
        /// <summary>Mass per unit water level (heavy phase) for hold-up scaling.</summary>
        public double MassPerWaterLevel { get; set; } = 6000.0;
        /// <summary>Secondary scaling from total liquid level (lighter weight).</summary>
        public double MassPerTotalLevel { get; set; } = 1500.0;
        public double Cp { get; set; } = 4180.0;
        /// <summary>Linear cooling toward ambient: (W/kg) * (T - T_amb) / (M_eff*Cp) per second.</summary>
        public double HeatLossPerHoldup_k { get; set; } = 0.00012;
        public double AmbientTemperature_K { get; set; } = 288.15;
    }

    public class ThermalMixingModel : ModelBaseClass, ISimulatableModel
    {
        public ThermalMixingParameters modelParameters = new ThermalMixingParameters();

        private double currentTemperature;
        private bool isInitialized;

        /// <summary>
        /// Runtime inputs: m_in, T_in, m_out, water level, total level (IDs are symbolic for tracing only).
        /// </summary>
        public ThermalMixingModel(string id, string massInID, string tempInID, string massOutID, string levelWaterID, string levelTotalID, string outputTempID = null)
        {
            this.ID = id;
            this.outputID = outputTempID ?? id;
            this.ModelInputIDs = new string[] { massInID, tempInID, massOutID, levelWaterID, levelTotalID };
            this.processModelType = ModelType.SubProcess;
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length < 3)
                return new double[] { badDataID };

            double m_in = inputsU[0];
            double t_in = inputsU[1];
            double m_out = inputsU[2];
            double waterLevel = inputsU.Length >= 4 ? inputsU[3] : -1;
            double totalLevel = inputsU.Length >= 5 ? inputsU[4] : -1;

            if (m_in == badDataID || t_in == badDataID || m_out == badDataID)
                return new double[] { badDataID };

            if (!isInitialized)
            {
                currentTemperature = t_in;
                isInitialized = true;
            }

            double wl = waterLevel >= 0 ? waterLevel : 0;
            double tl = totalLevel >= 0 ? totalLevel : 0;
            double mEff = modelParameters.M0
                + modelParameters.MassPerWaterLevel * wl
                + modelParameters.MassPerTotalLevel * tl;
            if (mEff < modelParameters.M0 * 0.05)
                mEff = modelParameters.M0 * 0.05;

            double cp = modelParameters.Cp <= 1 ? 4180.0 : modelParameters.Cp;

            // Enthalpy-like net: inflow brings feed enthalpy; outflow removes at current T (simple lump).
            double phiIn = m_in * (t_in - currentTemperature);
            
            // The energy impact on bulk temperature purely from mixing:
            double dT_conv = phiIn * timeBase_s / mEff;

            double tAmb = modelParameters.AmbientTemperature_K;
            double dT_loss = modelParameters.HeatLossPerHoldup_k * (currentTemperature - tAmb) * timeBase_s;

            currentTemperature += dT_conv - dT_loss;

            return new double[] { currentTemperature };
        }

        public void WarmStart(double[] inputs, double output)
        {
            if (!double.IsNaN(output))
            {
                currentTemperature = output;
                isInitialized = true;
            }
        }

        public double? GetSteadyStateInput(double x0, int inputIdx = 0, double[] givenInputValues = null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) { return null; }

        public override SignalType GetOutputSignalType()
        {
            return SignalType.Output_Y;
        }

        public bool IsModelSimulatable(out string explanationStr)
        {
            explanationStr = "OK";
            if (modelParameters.M0 <= 0)
            {
                explanationStr = "M0 must be positive.";
                return false;
            }
            return true;
        }

        public ISimulatableModel Clone(string ID)
        {
            var lt = this.ModelInputIDs.Length > 4 ? this.ModelInputIDs[4] : "TotalLevel";
            return new ThermalMixingModel(ID, this.ModelInputIDs[0], this.ModelInputIDs[1], this.ModelInputIDs[2], this.ModelInputIDs[3], lt, this.outputID)
            {
                modelParameters = new ThermalMixingParameters
                {
                    M0 = this.modelParameters.M0,
                    MassPerWaterLevel = this.modelParameters.MassPerWaterLevel,
                    MassPerTotalLevel = this.modelParameters.MassPerTotalLevel,
                    Cp = this.modelParameters.Cp,
                    HeatLossPerHoldup_k = this.modelParameters.HeatLossPerHoldup_k,
                    AmbientTemperature_K = this.modelParameters.AmbientTemperature_K
                }
            };
        }

        public override int GetLengthOfInputVector()
        {
            return 5;
        }
    }
}
