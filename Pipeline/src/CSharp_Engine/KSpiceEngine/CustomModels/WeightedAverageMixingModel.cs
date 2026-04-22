using System;
using System.Linq;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine.CustomModels
{
    public class WeightedAverageMixingParameters : ModelParametersBaseClass
    {
        public int NumberOfInlets { get; set; } = 2;
        public double TimeConstant_s { get; set; } = 33.25; // Tuned with Level & Pressure covariates (Global Search)
        public double AmbientTemp { get; set; } = 3.03;    
        public double InitialTemperature { get; set; } = 41.5; 
        public double[] CpFactors { get; set; } = null; 
        public double LevelWeight { get; set; } = -0.6701;
        public double PressureWeight { get; set; } = 5.7197;
    }

    public class WeightedAverageMixingModel : ModelBaseClass, ISimulatableModel
    {
        public WeightedAverageMixingParameters modelParameters = new WeightedAverageMixingParameters();
        
        private double previousTemperature;
        private bool isInitialized = false;

        public WeightedAverageMixingModel() { previousTemperature = modelParameters.InitialTemperature; }

        public WeightedAverageMixingModel(string id, string[] inputs)
        {
            this.ID = id;
            this.outputID = id;
            this.ModelInputIDs = inputs;
            
            // Count only flow inlets, ignore covariates like Level and Pressure
            int inletCount = 0;
            if (inputs != null) {
                foreach (string s in inputs) {
                    if (s.ToLower().Contains("massflow")) inletCount++;
                }
            }
            this.modelParameters.NumberOfInlets = inletCount > 0 ? inletCount : 2;

            this.processModelType = ModelType.SubProcess;
        }

        public double[] Iterate(double[] inputsU, double timeBase_s, double badDataID = -9999)
        {
            if (!isInitialized)
            {
                previousTemperature = modelParameters.InitialTemperature;
            }

            if (inputsU == null || inputsU.Length < modelParameters.NumberOfInlets * 2)
                return new double[] { badDataID };

            double num = 0;
            double den = 0;
            bool allBad = true;

            for (int i = 0; i < modelParameters.NumberOfInlets; i++)
            {
                double flow = inputsU[i * 2];
                double temp = inputsU[i * 2 + 1];

                double cp = 1.0;
                if (modelParameters.CpFactors != null && i < modelParameters.CpFactors.Length) {
                    cp = modelParameters.CpFactors[i];
                } else if (this.ModelInputIDs != null && i * 2 < this.ModelInputIDs.Length) {
                    // Specific physical workaround for gas-vs-liquid streams if factors are omitted
                    string inId = this.ModelInputIDs[i * 2].ToLower();
                    if (inId.Contains("23uv0001")) {
                        cp = 0.001; // Tuned ratio for UV gas line vs ESV multiphase
                    } else if (inId.Contains("25esv")) {
                        cp = 1.0; // main multiphase feed
                    }
                }

                if (flow != badDataID && temp != badDataID)
                {
                    if (flow > 0.001) // avoid noise
                    {
                        num += flow * cp * temp;
                        den += flow * cp;
                        allBad = false;
                    }
                }
            }

            double result;
            double nominalMassFlow = 185.18; // Tuned nominal mass flow rate from 25ESV when operational
            double tauLoss = modelParameters.TimeConstant_s * 97.44; // Tuned slower ambient heat loss

            // Extract optional covariate inputs if mapped (4 = Level, 5 = Pressure)
            double currentLevel = 567.0;
            double currentPressure = 27.6;
            if (inputsU.Length > modelParameters.NumberOfInlets * 2 && inputsU[modelParameters.NumberOfInlets * 2] != badDataID) {
                currentLevel = inputsU[modelParameters.NumberOfInlets * 2];
            }
            if (inputsU.Length > modelParameters.NumberOfInlets * 2 + 1 && inputsU[modelParameters.NumberOfInlets * 2 + 1] != badDataID) {
                currentPressure = inputsU[modelParameters.NumberOfInlets * 2 + 1];
            }

            // Covariate physics based on Level and Pressure
            double levelFactor = 1.0 + (modelParameters.LevelWeight * (currentLevel / 567.0));
            levelFactor = Math.Max(levelFactor, 0.01);
            double effectiveAmbient = modelParameters.AmbientTemp + (modelParameters.PressureWeight * (currentPressure - 27.6));
            tauLoss = tauLoss * levelFactor;
            
            if (allBad || den < 0.0001) {
                // If there's no flow, the tank solely loses heat to the ambient surroundings over a longer time constant
                result = previousTemperature + (effectiveAmbient - previousTemperature) * (timeBase_s / tauLoss);
            } else {
                // Determine the instantaneous target incoming mixture temperature
                double targetTemp = num / den;
                
                // Real physics: The tank has a massive heat capacity (mass).
                // The time constant defined in the JSON (e.g. 25 seconds) corresponds to the thermal inertia when nominal flow (~140 kg/s) is entering the tank.
                // If only a trickle of flow enters (like when 25ESV is off but 23UV is on), the effective mixing time constant for the whole tank becomes enormous.
                double effectiveMixingTau = modelParameters.TimeConstant_s * levelFactor * (nominalMassFlow / den);
                
                result = previousTemperature + (targetTemp - previousTemperature) * (timeBase_s / effectiveMixingTau);
                
                // Add the passive heat loss to ambient simultaneously
                result += (effectiveAmbient - result) * (timeBase_s / tauLoss);
            }

            previousTemperature = result;
            isInitialized = true;
            return new double[] { result };
        }

        public void WarmStart(double[] inputs, double output) {
            if (!double.IsNaN(output)) {
                previousTemperature = output;
                isInitialized = true;
            }
        }

        public double? GetSteadyStateInput(double x0, int inputIdx=0, double[] givenInputValues=null) { return null; }
        public double? GetSteadyStateOutput(double[] u0, double badDataID = -9999) 
        { 
            return modelParameters.InitialTemperature;
        }

        public override SignalType GetOutputSignalType() { return SignalType.Output_Y; }

        public bool IsModelSimulatable(out string explanationStr) 
        { 
            explanationStr = "OK"; 
            return true; 
        }

        public ISimulatableModel Clone(string ID) 
        { 
            return new WeightedAverageMixingModel(ID, this.ModelInputIDs)
            {
                modelParameters = new WeightedAverageMixingParameters { 
                    NumberOfInlets = this.modelParameters.NumberOfInlets,
                    TimeConstant_s = this.modelParameters.TimeConstant_s,
                    AmbientTemp = this.modelParameters.AmbientTemp
                }
            };
        }

        public override int GetLengthOfInputVector() 
        { 
            return modelParameters.NumberOfInlets * 2; 
        }
    }
}
