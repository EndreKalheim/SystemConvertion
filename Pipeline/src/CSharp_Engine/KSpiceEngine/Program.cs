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
            // --mode  equations | simulate | testset | closedloop | all  (default: all)
            // --map   path to KSpiceSystemMap.json
            // --csv   path to KspiceSim.csv (training data — used by equations & simulate)
            // --testcsv path to held-out CSV (used by testset & closedloop modes)
            string mode    = "all";
            string mapPath = null;
            string csvPath = null;
            string testCsvPath = null;
            // Output filename suffix — lets the same closedloop mode write to either
            // CS_Predictions_ClosedLoop.csv (default) or CS_Predictions_ClosedLoop_Train.csv
            // when run on the training CSV, so the two runs don't clobber each other.
            string suffix = "";

            for (int i = 0; i < args.Length - 1; i++)
            {
                if (args[i] == "--mode")    mode    = args[i + 1];
                if (args[i] == "--map")     mapPath = args[i + 1];
                if (args[i] == "--csv")     csvPath = args[i + 1];
                if (args[i] == "--testcsv") testCsvPath = args[i + 1];
                if (args[i] == "--suffix")  suffix = args[i + 1];
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

                    KSpiceModelFactory.BuildEquationsMap(mapPath, out List<JObject> tsaEquationsMap);

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

                // ── Test 2: open-loop on held-out CSV (frozen models) ─────────
                if (mode == "testset")
                {
                    Console.WriteLine("\n[Test 2] Frozen-model evaluation on held-out CSV...");
                    string eqMapPath  = Path.Combine(diagDir, "TSA_Equations.json");
                    string sigMapPath = Path.Combine(diagDir, "SignalMapping.json");
                    string topoPath   = Path.Combine(diagDir, "TSA_Explicit_Topology.json");
                    string paramsPath = Path.Combine(outDir, "CS_Identified_Parameters.json");
                    string predPath   = Path.Combine(outDir, "CS_Predictions_TestSet.csv");

                    if (!File.Exists(paramsPath))
                    {
                        Console.WriteLine("[ERROR] CS_Identified_Parameters.json missing — run --mode simulate first.");
                        return;
                    }
                    if (string.IsNullOrEmpty(testCsvPath) || !File.Exists(testCsvPath))
                    {
                        Console.WriteLine($"[ERROR] --testcsv path not found: {testCsvPath ?? "(none)"}");
                        return;
                    }

                    OpenLoopTestRunner.Run(testCsvPath, mapPath, eqMapPath, sigMapPath, paramsPath, topoPath, predPath);
                }

                // ── Test 1: closed-loop simulation ────────────────────────────
                if (mode == "closedloop")
                {
                    Console.WriteLine("\n[Test 1] Closed-loop simulation...");
                    string eqMapPath  = Path.Combine(diagDir, "TSA_Equations.json");
                    string sigMapPath = Path.Combine(diagDir, "SignalMapping.json");
                    string topoPath   = Path.Combine(diagDir, "TSA_Explicit_Topology.json");
                    string paramsPath = Path.Combine(outDir, "CS_Identified_Parameters.json");
                    string predPath   = Path.Combine(outDir, $"CS_Predictions_ClosedLoop{suffix}.csv");

                    if (!File.Exists(paramsPath))
                    {
                        Console.WriteLine("[ERROR] CS_Identified_Parameters.json missing — run --mode simulate first.");
                        return;
                    }

                    // Default to the held-out CSV if --testcsv was passed; otherwise use --csv (training).
                    string runCsv = !string.IsNullOrEmpty(testCsvPath) && File.Exists(testCsvPath) ? testCsvPath : csvPath;
                    if (string.IsNullOrEmpty(runCsv) || !File.Exists(runCsv))
                    {
                        Console.WriteLine($"[ERROR] No CSV available for closed-loop run.");
                        return;
                    }

                    ClosedLoopRunner.Run(runCsv, mapPath, eqMapPath, sigMapPath, paramsPath, topoPath, predPath);
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
