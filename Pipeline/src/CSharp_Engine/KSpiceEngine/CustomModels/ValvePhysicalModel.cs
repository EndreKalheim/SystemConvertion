using System;
using System.Linq;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class ValvePhysicalParameters : ModelParametersBaseClass
    {
        public double M { get; set; } = 1.0;
    }

    public class ValvePhysicalModel : ModelBaseClass, ISimulatableModel
    {
        public ValvePhysicalParameters modelParameters = new ValvePhysicalParameters();
        
        public ValvePhysicalModel()
        {
            this.processModelType = ModelType.SubProcess;
        }

        public ValvePhysicalModel(string id, string[] inputIDs, string outputID = null)
             : this()
        {
            this.ID = id;
            this.outputID = outputID ?? id;
            this.ModelInputIDs = inputIDs;
        }

        // Backward compatibility constructor
        public ValvePhysicalModel(string id, string uID, string pID, string outputID = null)
             : this(id, new string[] { uID, pID }, outputID)
        { }

        public override int GetLengthOfInputVector()
        {
            return (this.ModelInputIDs != null) ? this.ModelInputIDs.Length : 0;
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null || inputsU.Length == 0)
                return new double[] { badDataID };

            double pIn = inputsU[0];
            double pOut = 0;
            double u = 100.0;
            double level = 0.0;

            for (int i = 1; i < this.ModelInputIDs.Length; i++)
            {
                if (i >= inputsU.Length) continue;

                string idLower = this.ModelInputIDs[i].ToLower();
                if (idLower.Contains("level")) level = inputsU[i] == badDataID ? 0 : inputsU[i];
                else if (idLower.Contains("signal") || idLower.Contains("opening") || idLower.Contains("controlleroutput") || idLower.Contains("position") || idLower.Contains("output") || idLower.Contains("ic0") || idLower.Contains("tic") || idLower.Contains("lic") || idLower.Contains("fic") || idLower.Contains("pic")) u = inputsU[i];
                else pOut = inputsU[i] == badDataID ? 0 : inputsU[i]; 
            }

            if (pIn == badDataID || u == badDataID)
                return new double[] { badDataID };

            // Very simple grey-box formula for deltaP. Liquid level adds hydrostatic head.
            double deltaP = (pIn + level * 0.1) - pOut; 
            deltaP = Math.Max(0, deltaP);

            // Flow = M * (U/100) * sqrt(deltaP)
            double flow = modelParameters.M * Math.Max(0, u / 100.0) * Math.Sqrt(deltaP);

            if (double.IsNaN(flow) || double.IsInfinity(flow))
                flow = 0;

            return new double[] { flow };
        }

        public void WarmStart(double[] inputs, double output) { }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[] givenInputValues=null) { return null; }

        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999)
        {
            var output = Iterate(u0, 1, badDataID);
            if (output[0] == badDataID) return null;
            return output[0];
        }

        public override SignalType GetOutputSignalType() { return SignalType.Output_Y; }

        public bool IsModelSimulatable(out string explanationStr)
        {
            explanationStr = "OK";
            if (modelParameters.M <= 0)
            {
                explanationStr = "M parameter must be positive.";
                return false;
            }
            return true;
        }

        public ISimulatableModel Clone(string cloneID)
        {
            return new ValvePhysicalModel(cloneID, (string[])this.ModelInputIDs.Clone(), this.outputID)
            {
                modelParameters = new ValvePhysicalParameters { M = this.modelParameters.M }
            };
        }
    }

    public class ValvePhysicalIdentifier
    {
        public static double Identify(TimeSeriesAnalysis.TimeSeriesDataSet dataset, string[] inputIDs, string outFlowID)
        {
            double mSum = 0;
            int mCount = 0;
            
            var pInData = dataset.GetValues(inputIDs[0]);
            int len = dataset.GetLength() ?? 0;
            double[] pOutData = new double[len];
            double[] levelData = new double[len];
            double[] uData = new double[len];
            for(int k=0; k<len; k++) uData[k] = 100.0;

            for (int i = 1; i < inputIDs.Length; i++)
            {
                string idLower = inputIDs[i].ToLower();
                var data = dataset.GetValues(inputIDs[i]);
                if (data == null) continue;

                if (idLower.Contains("level")) levelData = data;
                else if (idLower.Contains("signal") || idLower.Contains("opening") || idLower.Contains("controlleroutput") || idLower.Contains("position") || idLower.Contains("output") || idLower.Contains("ic0") || idLower.Contains("tic") || idLower.Contains("lic") || idLower.Contains("fic") || idLower.Contains("pic")) uData = data;
                else pOutData = data;
            }

            var flowData = dataset.GetValues(outFlowID);
            if (flowData == null || pInData == null) return 1.0;

            for (int i = 0; i < len; i++)
            {
                double dp = (pInData[i] + levelData[i] * 0.1) - pOutData[i];
                if (uData[i] > 1.0 && dp > 0 && Math.Abs(flowData[i]) > 0.001)
                {
                    double m_inst = flowData[i] / ((uData[i] / 100.0) * Math.Sqrt(dp));
                    mSum += m_inst;
                    mCount++;
                }
            }

            if (mCount > 0) return mSum / mCount;
            return 1.0; 
        }
    }
}
