using System;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class AntiSurgeParameters : ModelParametersBaseClass
    {
        public double Kp { get; set; } = 5.0;
        public double Ti_s { get; set; } = 10.0;
        public double FlowGain { get; set; } = 3.0;
        public double SurgeGain1 { get; set; } = 15.0;
        public double SurgeGain2 { get; set; } = -8.9;
        public double SetpointMargin { get; set; } = 0.2;
    }

    public class AntiSurgePhysicalModel : ModelBaseClass, ISimulatableModel
    {
        public AntiSurgeParameters modelParameters = new AntiSurgeParameters();
        private double integralError = 0.0;
        private double uPrev = 0.0;

        public AntiSurgePhysicalModel()
        {
            this.processModelType = ModelType.SubProcess;
        }

        public AntiSurgePhysicalModel(string id, string[] inputIDs, string outputID = null) : this()
        {
            this.ID = id;
            this.outputID = outputID ?? id;
            this.ModelInputIDs = inputIDs;
        }

        public override int GetLengthOfInputVector() { return (this.ModelInputIDs != null) ? this.ModelInputIDs.Length : 0; }

        public override SignalType GetOutputSignalType() { return SignalType.Output_Y; }
        public bool IsModelSimulatable(out string explanationStr) { explanationStr = "OK"; return true; }
        public ISimulatableModel Clone(string cloneID) 
        { 
            return new AntiSurgePhysicalModel(cloneID, (string[])this.ModelInputIDs.Clone(), this.outputID) 
            { 
                 modelParameters = new AntiSurgeParameters() 
                 {
                     Kp = this.modelParameters.Kp,
                     Ti_s = this.modelParameters.Ti_s,
                     FlowGain = this.modelParameters.FlowGain,
                     SurgeGain1 = this.modelParameters.SurgeGain1,
                     SurgeGain2 = this.modelParameters.SurgeGain2,
                     SetpointMargin = this.modelParameters.SetpointMargin
                 }
            };
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length < 3) return new double[] { badDataID };
            
            double P_in = inputsU[0];
            double P_out = inputsU[1];
            double MassFlow = inputsU[2];
            
            if (P_in == badDataID || P_out == badDataID || MassFlow == badDataID || P_in <= 0.1)
                return new double[] { uPrev };

            double pr = P_out / P_in;
            double surgeLimit = modelParameters.SurgeGain1 * pr + modelParameters.SurgeGain2;
            double normFlow = MassFlow * modelParameters.FlowGain / Math.Sqrt(P_in);
            double setpoint = surgeLimit * (1.0 + modelParameters.SetpointMargin);
            double error = setpoint - normFlow;

            double dT = timeBase_s;
            double integralIncrement = error * dT;
            
            if ((uPrev >= 100 && error > 0) || (uPrev <= 0 && error < 0))
            {}
            else { integralError += integralIncrement; }

            double P_term = modelParameters.Kp * error;
            double I_term = (modelParameters.Kp / modelParameters.Ti_s) * integralError;
            
            double uOut = P_term + I_term;
            if (uOut > 100) uOut = 100;
            if (uOut < 0) uOut = 0;
            
            uPrev = uOut;
            return new double[] { Math.Round(uOut, 4) };
        }

        public void WarmStart(double[] inputs, double output)
        {
            uPrev = output;
            if (modelParameters.Kp != 0 && modelParameters.Ti_s > 0) {
                 integralError = output / (modelParameters.Kp / modelParameters.Ti_s);
            }
        }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[] givenInputValues=null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) { return uPrev; }
    }
}