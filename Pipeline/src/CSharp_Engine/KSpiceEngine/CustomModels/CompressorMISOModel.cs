using System;
using TimeSeriesAnalysis.Dynamic;
using TimeSeriesAnalysis; // Need this for SignalType maybe?

namespace KSpiceEngine.CustomModels
{
    public class CompressorParameters : ModelParametersBaseClass
    {
        public double K { get; set; } = 0.005;
    }

    public class CompressorMISOModel : ModelBaseClass, ISimulatableModel
    {
        public CompressorParameters modelParameters = new CompressorParameters();

        public CompressorMISOModel(string id, string pInletID, string speedID, string mFlowID)
        {
            this.ID = id;
            this.SetProcessModelType(ModelType.SubProcess);

            // Need to set outputID explicitly for ModelBaseClass (maybe not strictly but safe)
            this.outputID = mFlowID; 

            this.ModelInputIDs = new string[] { pInletID, speedID };
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length < 2)
                return new double[] { badDataID };

            double pInlet = inputsU[0];
            double speed = inputsU[1];
            
            if (pInlet == badDataID || speed == badDataID) return new double[] { badDataID };

            // Simplified compressor curve: massflow ~ K * speed * sqrt(pInlet)
            double output = modelParameters.K * speed * Math.Sqrt(Math.Max(0, pInlet));
            return new double[] { output };
        }

        public void WarmStart(double[] initialInputs, double initialOutput)
        {
        }

        public double? GetSteadyStateInput(double x0, int inputIdx = 0, double[] givenInputValues = null)
        {
            return 0;
        }

        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999)
        {
            if (u0 == null || u0.Length < 2) return badDataID;
            return modelParameters.K * u0[1] * Math.Sqrt(Math.Max(0, u0[0]));
        }

        public override SignalType GetOutputSignalType()
        {
            return SignalType.Output_Y; 
        }

        public bool IsModelSimulatable(out string explanationStr)
        {
            explanationStr = "OK";
            return true;
        }

        public ISimulatableModel Clone(string cloneID)
        {
            var clone = new CompressorMISOModel(cloneID, this.ModelInputIDs[0], this.ModelInputIDs[1], this.outputID);
            clone.modelParameters.K = this.modelParameters.K;
            return clone;
        }

        public override int GetLengthOfInputVector()
        {
            return 2;
        }
    }
}
