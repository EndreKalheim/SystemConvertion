namespace KSpiceEngine.CustomModels
{
    /// <summary>
    /// Represents a flow stream combining Mass, Temperature, and Pressure 
    /// for accurate physical routing in parallel through the TSA MISO blocks.
    /// </summary>
    public struct StreamVector
    {
        public double MassFlow { get; set; }
        public double Temperature { get; set; }
        public double Pressure { get; set; }

        public StreamVector(double massFlow, double temperature, double pressure)
        {
            MassFlow = massFlow;
            Temperature = temperature;
            Pressure = pressure;
        }

        public override string ToString()
        {
            return $"[M: {MassFlow:F2}, T: {Temperature:F2}, P: {Pressure:F2}]";
        }
    }
}
