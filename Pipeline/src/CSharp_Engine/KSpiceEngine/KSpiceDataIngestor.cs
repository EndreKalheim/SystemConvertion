using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Globalization;
using TimeSeriesAnalysis;

namespace KSpiceEngine
{
    /// <summary>
    /// Phase 3: The Universal Auto-Mapping Ingestor.
    /// Reads K-Spice CSV exports, cleans headers dynamically, 
    /// and constructs a fully mapped TimeSeriesDataSet.
    /// </summary>
    public class KSpiceDataIngestor
    {
        public static TimeSeriesDataSet ReadRawCsv(string csvPath)
        {
            Console.WriteLine($"Ingesting Raw System Data: {csvPath}");
            var dataSet = new TimeSeriesDataSet();
            
            if (!File.Exists(csvPath))
            {
                Console.WriteLine($"[ERROR] File not found: {csvPath}");
                return dataSet;
            }

            var lines = File.ReadAllLines(csvPath).Where(l => !string.IsNullOrWhiteSpace(l)).ToArray();
            if (lines.Length < 2) return dataSet;

            // Step 1: Clean and standardize headers (strip spaces)
            var rawHeaders = lines[0].Split(',');
            var cleanHeaders = rawHeaders.Select(h => h.Trim()).ToArray();

            // Setup data accumulators
            var columns = new List<double>[cleanHeaders.Length];
            for(int i = 0; i < cleanHeaders.Length; i++)
            {
                columns[i] = new List<double>(lines.Length);
            }

            // Step 2: Parse raw floats regardless of locale
            for(int i = 1; i < lines.Length; i++)
            {
                var rowData = lines[i].Split(',');
                for(int j = 0; j < cleanHeaders.Length; j++)
                {
                    double value = double.NaN;
                    if (j < rowData.Length)
                    {
                        var cellRaw = rowData[j].Trim();
                        // Parse numbers natively (true/false convert to 1/0)
                        if (cellRaw.Equals("true", StringComparison.OrdinalIgnoreCase)) value = 1.0;
                        else if (cellRaw.Equals("false", StringComparison.OrdinalIgnoreCase)) value = 0.0;
                        else double.TryParse(cellRaw, NumberStyles.Any, CultureInfo.InvariantCulture, out value);
                    }
                    columns[j].Add(value);
                }
            }

            // Step 3: Register every signal as an agnostic vector
            for(int i = 0; i < cleanHeaders.Length; i++)
            {
                string headerName = cleanHeaders[i];
                if (headerName.Equals("ModelTime", StringComparison.OrdinalIgnoreCase))
                {
                    dataSet.Add("Time_s", columns[i].ToArray());
                    // Create base timestamps assuming 1s diff (as K-Spice often does), 
                    // or let the TSA library handle Time_s inherently.
                    dataSet.CreateTimestamps(1.0, columns[i].Count);
                }
                else
                {
                    dataSet.Add(headerName, columns[i].ToArray());
                }
            }

            Console.WriteLine($"[SUCCESS] Ingested {cleanHeaders.Length} telemetry signals with {columns[0].Count} data points each.");
            return dataSet;
        }
    }
}