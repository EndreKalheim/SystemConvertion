using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using TimeSeriesAnalysis.Dynamic;

namespace KSpiceEngine
{
    class Program
    {
        static void Main(string[] args)
        {
            // Parse CLI arguments
            // --mode  equations | simulate | all  (default: all)
            // --map   path to KSpiceSystemMap.json
            // --csv   path to KspiceSim.csv
            string mode    = "all";
            string mapPath = null;
            string csvPath = null;

            for (int i = 0; i < args.Length - 1; i++)
            {
                if (args[i] == "--mode") mode    = args[i + 1];
                if (args[i] == "--map")  mapPath = args[i + 1];
                if (args[i] == "--csv")  csvPath = args[i + 1];
            }

            // Default paths: relative to the build-output directory (same as before)
            string exeDir = AppDomain.CurrentDomain.BaseDirectory;
            string pipelineRoot = Path.GetFullPath(Path.Combine(exeDir, @"..\..\..\..\..\.."));

            if (string.IsNullOrEmpty(mapPath))
                mapPath = Path.Combine(pipelineRoot, "data", "extracted", "KSpiceSystemMap.json");

            if (string.IsNullOrEmpty(csvPath))
                csvPath = Path.Combine(pipelineRoot, "data", "raw", "KspiceSim.csv");

            string diagDir = Path.Combine(pipelineRoot, "output", "diagrams");
            string outDir  = Path.Combine(pipelineRoot, "output");

            Console.WriteLine("===================================================");
            Console.WriteLine($" K-Spice Engine — Mode: {mode.ToUpper()}");
            Console.WriteLine("===================================================");
            Console.WriteLine($"  Map : {mapPath}");
            Console.WriteLine($"  CSV : {csvPath}");

            if (!File.Exists(mapPath))
            {
                Console.WriteLine($"[ERROR] System map not found: {mapPath}");
                Console.WriteLine("        Run Phase 1 first: python src/Parser/KSpiceParser.py ...");
                return;
            }

            try
            {
                // ── Phase 2: Equations ────────────────────────────────────────
                if (mode == "equations" || mode == "all")
                {
                    Console.WriteLine("\n[Phase 2] Generating equations...");

                    var plant = KSpiceModelFactory.BuildPlantFromMap(mapPath, out List<JObject> tsaEquationsMap);

                    Directory.CreateDirectory(diagDir);
                    string eqMapPath = Path.Combine(diagDir, "TSA_Equations.json");
                    File.WriteAllText(eqMapPath, JsonConvert.SerializeObject(tsaEquationsMap, Formatting.Indented));
                    Console.WriteLine($"[SUCCESS] TSA Equations -> {eqMapPath}");

                    if (File.Exists(csvPath))
                    {
                        string headerLine = File.ReadLines(csvPath).First();
                        var csvHeaders = new List<string>(headerLine.Split(',').Select(h => h.Trim()));
                        var dynamicEqs = tsaEquationsMap.Cast<dynamic>().ToList();
                        var signalMap  = DataSignalMapper.GenerateMapping(csvHeaders, dynamicEqs, mapPath);

                        string sigMapPath = Path.Combine(diagDir, "SignalMapping.json");
                        File.WriteAllText(sigMapPath, JsonConvert.SerializeObject(signalMap, Formatting.Indented));
                        Console.WriteLine($"[SUCCESS] Signal Mapping -> {sigMapPath}");
                    }
                    else
                    {
                        Console.WriteLine($"[WARN] CSV not found at {csvPath} — signal mapping skipped");
                    }
                }

                // ── Phase 4: Simulate ─────────────────────────────────────────
                if (mode == "simulate" || mode == "all")
                {
                    Console.WriteLine("\n[Phase 4] Running simulation and system ID...");

                    string eqMapPath  = Path.Combine(diagDir, "TSA_Equations.json");
                    string sigMapPath = Path.Combine(diagDir, "SignalMapping.json");
                    string predPath   = Path.Combine(outDir, "CS_Predictions.csv");

                    if (!File.Exists(eqMapPath))
                    {
                        Console.WriteLine("[ERROR] TSA_Equations.json missing — run Phase 2 (equations) first.");
                        return;
                    }
                    if (!File.Exists(sigMapPath))
                    {
                        Console.WriteLine("[ERROR] SignalMapping.json missing — run Phase 2 (equations) first.");
                        return;
                    }

                    string topoPath = Path.Combine(diagDir, "TSA_Explicit_Topology.json");
                    if (!File.Exists(topoPath))
                    {
                        Console.WriteLine("[ERROR] TSA_Explicit_Topology.json missing — run Phase 3 (topology) first.");
                        Console.WriteLine("        python src/Visualization/EquationTopologyBuilder.py");
                        return;
                    }

                    DynamicPlantRunner.RunPlantSimulations(csvPath, mapPath, eqMapPath, sigMapPath, predPath);
                    Console.WriteLine($"[SUCCESS] Predictions -> {predPath}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n[ERROR] {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }
    }
}
