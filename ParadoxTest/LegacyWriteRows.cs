using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using static ParadoxTest.LegacyWriteRows;

namespace ParadoxTest
{
    public static class LegacyWriteRows
    {

        /// <summary>
        /// Path to WriteRows.exe. Machine-specific, read from the
        /// git-ignored LegacyWriteRows.local.config (see LegacyWriteRows.local.config.example).
        /// Empty when unset/missing.
        /// </summary>
        public static string WriteRowsExeFilePath => Configuration.GetWriteRowsExePath();

        /// <summary>
        /// This is what we feed in to the tool via json file. It can be an array of these values.
        /// </summary>
        public class WriteRowInput
        {
            public Guid RowId { get; set; } // Example: "rowId": "aff3befe-5ff0-48ea-b040-aa178b35ffaa"  - This is our internal unique identifier for the row to keep track of our writes. It is *not* the database identifier for the row.
            public string TableName { get; set; } // Example: "tableName": "clientversionlog.db"  - it doesn't really have to be the full path.
            public Dictionary<string, object> ColumnValues { get; set; } /* Example: "columnValues": {
      "Id": 0,
      "MajorVersion": 1,
      "MinorVersion": 0,
      "VersionName": "1.0.0",
      "VersionBuildDate": "2023-06-01T12:00:00Z",
      "ComputerName": "MyComputer",
      "UserName": "MyUser",
      "ServerBaseDir": "C:\\Server\\Base\\Dir",
      "ClientId": 12345,
      "OperatingSystem": "Windows 10",
      "DateTime": "2024-06-01T12:00:00Z"
            }
             - Note that autoinc values (some ids) should be zero but they will return a different value in the output.
            */
    }
        

    /// <summary>
    /// This is what we get back from the tool from the output json file. It is a super set of the input however in some cases the column values may be different (i.e. if autoinc field it may now be set). It can be an array of these values.
    /// </summary>
        public class WriteRowOutput : WriteRowInput
    {
            public string Status { get; set; } // Example: "status": "Success" or "Failure"
            public string ErrorMessage { get; set; } // Example: "errorMessage": "Some error message if status is Failure"
            public DateTime WriteTimestampUtc { get; set; } // Example: "writeTimestampUtc": "2024-06-01T12:00:00Z"  - This is the timestamp when the write operation was performed.}

            
        }

        public static bool WriteRow(string tableName, Dictionary<string, object> columnValues)
        {
            return WriteRows(new List<(string TableName, Dictionary<string, object> ColumnValues)> { (tableName, columnValues) });
        }

        public static bool WriteRows(IEnumerable<(string tableName, Dictionary<string, object> columnValues)> tableNamesAndColumnValues)
        {
            var inputJsonFilePath = Path.Combine(Path.GetTempPath(), "writeRowInput.json");
            var outputJsonFilePath = Path.Combine(Path.GetTempPath(), "writeRowOutput.json");

            File.Delete(outputJsonFilePath); // Delete the output file if it exists, so we can check for its existence later.

            var inputs = new List<WriteRowInput>();

            foreach (var (tableName, columnValues) in tableNamesAndColumnValues)
            {
                var input = new WriteRowInput
                {
                    RowId = Guid.NewGuid(),
                    TableName = tableName,
                    ColumnValues = columnValues
                };
                inputs.Add(input);
            }

            var inputJson = System.Text.Json.JsonSerializer.Serialize(inputs);
            File.WriteAllText(inputJsonFilePath, inputJson);
            // Call the external tool to write the row
            var processInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = WriteRowsExeFilePath,
                Arguments = $"-i \"{inputJsonFilePath}\" -o \"{outputJsonFilePath}\" -s", // -s = silent so it doesn't wait for input
                RedirectStandardOutput = false,
                UseShellExecute = true,
                CreateNoWindow = true
            };
            using (var process = System.Diagnostics.Process.Start(processInfo))
            {
                process.WaitForExit();
            }
            if (!File.Exists(outputJsonFilePath))
            {
                throw new Exception("Output file was not created by the external tool.");
            }
            var outputJson = File.ReadAllText(outputJsonFilePath);

            var output = System.Text.Json.JsonSerializer.Deserialize<List<WriteRowOutput>>(
                outputJson,
                new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            foreach (var item in output)
            {
                if (item.Status?.Equals("Success", StringComparison.OrdinalIgnoreCase) != true)
                {
                    throw new Exception($"Failed to write row: {item.ErrorMessage}");
                }
            }

            // TODO: consider outputing the output to a log file or returning it to the caller for further processing if needed. We would need to keep the values in the same order as the input so that the caller can match them up. For now we just return true if all writes were successful.

            return true;
        }

    }
}
