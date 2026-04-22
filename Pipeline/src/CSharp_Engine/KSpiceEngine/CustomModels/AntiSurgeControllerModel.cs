using System;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class ASCParameters : ModelParametersBaseClass
    {
        public double Kp { get; set; } = -1.0; 
        public double Ti { get; set; } = 10.0;
        public double Td { get; set; } = 0.0;
        public double FastOpenMultiplier { get; set; } = 5.0; // If surging, open valve 5x faster
    }

    public class AntiSurgeControllerModel : ModelBaseClass, ISimulatableModel
    {
        public ASCParameters modelParameters = new ASCParameters();
        
        private double lastOutput = 0;
        private double lastError = 0;
        private bool isInitialized = false;

        public AntiSurgeControllerModel(string id, string spID, string pvID, string outputID = null)
        {
            this.ID = id;
            this.outputID = outputID ?? id;
            this.ModelInputIDs = new string[] { spID, pvID };
            this.processModelType = ModelType.SubProcess;
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length < 2)
                return new double[] { badDataID };

            double sp = inputsU[0];
            double pv = inputsU[1];

            if (sp == badDataID || pv == badDataID)
                return new double[] { badDataID };

            // e(k) = SP - PV
            // If PV (flow) drops below SP (surge limit + margin), error > 0, need to OPEN valve (increase output)
            double error = sp - pv;

            if (!isInitialized)
            {
                lastError = error;
                isInitialized = true;
                // If not warm started, assume valve is closed
                lastOutput = 0;
            }

            // Asymmetric gain: open fast, close slow
            double effectiveKp = modelParameters.Kp;
            if (error > 0) { // surging!
                effectiveKp *= modelParameters.FastOpenMultiplier;
            }

            // Incremental PI formulation
            double deltaP = effectiveKp * (error - lastError);
            double deltaI = (modelParameters.Ti > 0) ? (effectiveKp / modelParameters.Ti) * error * timeBase_s : 0;
            
            double deltaU = deltaP + deltaI;
            double output = lastOutput + deltaU;

            // Anti-windup / clamping (0 to 100 usually, or 0 to 1)
            if (output > 100) output = 100;
            if (output < 0) output = 0;

            lastOutput = output;
            lastError = error;

            return new double[] { output };
        }

        public void WarmStart(double[] inputs, double output) 
        { 
            if (!double.IsNaN(output)) {
                lastOutput = output;
                isInitialized = true;
            }
        }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[] givenInputValues=null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) { return null; }

        public override SignalType GetOutputSignalType() { return SignalType.Output_Y; }
        public bool IsModelSimulatable(out string explanationStr) { explanationStr = "OK"; return true; }

        public ISimulatableModel Clone(string ID) 
        { 
            return new AntiSurgeControllerModel(ID, this.ModelInputIDs[0], this.ModelInputIDs[1], this.outputID)
            {
                modelParameters = new ASCParameters { 
                    Kp = this.modelParameters.Kp,
                    Ti = this.modelParameters.Ti,
                    Td = this.modelParameters.Td,
                    FastOpenMultiplier = this.modelParameters.FastOpenMultiplier
                }
            };
        }

        public override int GetLengthOfInputVector() { return 2; }
    }
}
