using System;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class TankLevelParameters : ModelParametersBaseClass
    {
        public double GeometryFactor { get; set; } = 1.0;
        public double InitialLevel { get; set; } = 0.0;
        public double[] FlowSigns { get; set; } = new double[0]; // Used as FlowWeights 
    }

    public class TankLevelModel : ModelBaseClass, ISimulatableModel
    {
        public TankLevelParameters modelParameters = new TankLevelParameters(); 

        private double currentLevel = 0;
        private bool isInitialized = false;

        // Adjusted constructor to support array initialization if needed
        public TankLevelModel(string id, string[] inputs, string outputLevelID = null)
        {
            this.ID = id;
            this.outputID = outputLevelID ?? id;

            this.ModelInputIDs = inputs;

            this.processModelType = ModelType.SubProcess;
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (inputsU == null)
                return new double[] { badDataID };

            if (!isInitialized)
            {
                currentLevel = modelParameters.InitialLevel;
                isInitialized = true;
            }

            double levelToReturn = currentLevel; // Explicit Euler: return state at t, then integrate for t+1

            double sumDelta = 0;
            for (int i = 0; i < inputsU.Length; i++)
            {
                if (inputsU[i] == badDataID || double.IsNaN(inputsU[i]) || double.IsInfinity(inputsU[i]))
                    continue;

                double weight = 0.0;
                if (modelParameters.FlowSigns != null && i < modelParameters.FlowSigns.Length)
                {
                    weight = modelParameters.FlowSigns[i];
                }
                
                sumDelta += inputsU[i] * weight;
            }

            currentLevel += sumDelta * timeBase_s;

            if (double.IsNaN(currentLevel) || double.IsInfinity(currentLevel))
                currentLevel = 0; 
                
            return new double[] { levelToReturn };
        }

        public void WarmStart(double[] inputs, double output) 
        { 
            if (!double.IsNaN(output)) {
                currentLevel = output;
                isInitialized = true;
            }
        }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[] givenInputValues=null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) { return modelParameters.InitialLevel; }

        public override SignalType GetOutputSignalType() 
        { 
            return SignalType.Output_Y; 
        }

        public bool IsModelSimulatable(out string explanationStr) 
        { 
            explanationStr = "OK"; 
            return true; 
        }

        public ISimulatableModel Clone(string ID) 
        { 
            return new TankLevelModel(ID, this.ModelInputIDs, this.outputID)
            {
                modelParameters = new TankLevelParameters { 
                    GeometryFactor = this.modelParameters.GeometryFactor,
                    InitialLevel = this.modelParameters.InitialLevel,
                    FlowSigns = this.modelParameters.FlowSigns
                }
            };
        }

        public override int GetLengthOfInputVector() 
        { 
            return 2; 
        }
    }

    public class TankLevelIdentifier
    {
        public static TankLevelParameters Identify(TimeSeriesAnalysis.TimeSeriesDataSet dataset, string[] inIDs, string levelID)
        {
            var p = new TankLevelParameters();
            var levels = dataset.GetValues(levelID);
            
            if (levels != null && levels.Length > 0)
            {
                p.InitialLevel = levels[0];
            }
            
            p.FlowSigns = new double[inIDs.Length];
            for(int i=0; i<inIDs.Length; i++) { p.FlowSigns[i] = 1.0; }
            p.GeometryFactor = 1.0; 

            var timeBase = dataset.GetTimeBase();
            if (timeBase == 0) timeBase = 1.0;

            if (levels == null || inIDs.Length == 0) return p;

            int numSamples = levels.Length;
            double[,] U = new double[numSamples, inIDs.Length];

            // Integrate flows so we can fit Level = Sum(k_i * Int_Flow_i) + c
            for (int i = 0; i < inIDs.Length; i++)
            {
                var inputData = dataset.GetValues(inIDs[i]);
                if (inputData == null || inputData.Length < numSamples) continue;

                double currentIntegral = 0;
                for (int j = 0; j < numSamples; j++)
                {
                    currentIntegral += inputData[j] * timeBase;
                    U[j, i] = currentIntegral; 
                }
            }

            var unitData = new TimeSeriesAnalysis.Dynamic.UnitDataSet();
            unitData.Y_meas = levels;  // Do not use DiffY. Fit raw level!
            unitData.U = U;            // Input is integrated flow
            unitData.CreateTimeStamps(timeBase);

            var fit = TimeSeriesAnalysis.Dynamic.UnitIdentifier.IdentifyLinearAndStatic(ref unitData, new TimeSeriesAnalysis.Dynamic.FittingSpecs(), false);

            if (fit.modelParameters.LinearGains != null && fit.modelParameters.LinearGains.Length == inIDs.Length)
            {
                for (int i = 0; i < inIDs.Length; i++)
                {
                    p.FlowSigns[i] = fit.modelParameters.LinearGains[i];
                }
            }
            
            return p;
        }
    }
}
