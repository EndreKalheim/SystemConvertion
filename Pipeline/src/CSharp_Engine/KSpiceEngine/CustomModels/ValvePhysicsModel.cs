using System;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class ValveParameters : ModelParametersBaseClass
    {
        public double Cv { get; set; } = 100.0;
        // Used to tune the unmodelled density/units factor
        public double DensityTuningFactor { get; set; } = 1.0;
    }

    public class ValvePhysicsModel : ModelBaseClass, ISimulatableModel
    {
        public ValveParameters modelParameters = new ValveParameters();

        public ValvePhysicsModel()
        {
            this.processModelType = ModelType.SubProcess;
        }

        public ValvePhysicsModel(string id, string[] inputIDs, string? outputID = null) : this()
        {
            this.ID = id;
            this.outputID = outputID ?? id;
            this.ModelInputIDs = inputIDs;
        }

        public override int GetLengthOfInputVector() { return (this.ModelInputIDs != null) ? this.ModelInputIDs.Length : 0; }
        public override SignalType GetOutputSignalType() { return SignalType.Output_Y; }
        public bool IsModelSimulatable(out string explanationStr) { explanationStr = "OK"; return true; }

        public void WarmStart(double[] inputs, double output)
        {
            // Stateless physics model, no internal state to warm start
        }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[]? givenInputValues=null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) { return null; }

        public ISimulatableModel Clone(string cloneID)
        {
            return new ValvePhysicsModel(cloneID, (string[])this.ModelInputIDs.Clone(), this.outputID)
            {
                modelParameters = new ValveParameters()
                {
                    Cv = this.modelParameters.Cv,
                    DensityTuningFactor = this.modelParameters.DensityTuningFactor
                }
            };
        }

        // Inputs expected: P_in, P_out, U (0..100 or 0..1)
        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length < 3) return new double[] { badDataID };

            double pIn = inputsU[0];
            double pOut = inputsU[1];
            double u = inputsU[2];

            // Treat NaN and the bad-data sentinel uniformly as "no data" → no flow.
            if (double.IsNaN(pIn) || double.IsNaN(pOut) || double.IsNaN(u) ||
                pIn == badDataID || pOut == badDataID || u == badDataID)
            {
                return new double[] { 0 };
            }

            // K-Spice control signals are always on a 0-100 % scale, including small values
            // like 0.6 that mean 0.6 % (not 60 %). Always normalise by 100 — the previous
            // "if (u > 1) u /= 100" heuristic mis-scaled near-closed samples by 100x.
            double uNormalized = u / 100.0;
            if (uNormalized < 0) uNormalized = 0;
            if (uNormalized > 1.0) uNormalized = 1.0;

            // Reverse flow is not modelled; clamp dP to keep sqrt real.
            double deltaP = pIn - pOut;
            if (deltaP < 0) deltaP = 0;

            // MassFlow = Cv * U * sqrt(dP) * tuning_factor
            double flow = modelParameters.Cv * uNormalized * Math.Sqrt(deltaP) * modelParameters.DensityTuningFactor;

            return new double[] { flow };
        }
    }
}