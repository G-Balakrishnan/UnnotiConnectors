using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Core.Logging
{
    public class ImportCsvLogger
    {
        private readonly string _filePath;
        private static readonly object _lock = new object();

        public ImportCsvLogger(string logsFolder, string fileName)
        {
            Directory.CreateDirectory(logsFolder);
            _filePath = Path.Combine(logsFolder, fileName);

            if (!File.Exists(_filePath))
            {
                File.WriteAllText(_filePath,
                    "RowNumber,UniqueValue,Status,HttpStatus,Remarks,TimestampUtc\n");
            }
        }

        public void WriteRow(
            int rowNumber,
            string uniqueValue,
            string status,
            string httpStatus,
            string remarks)
        {
            lock (_lock)
            {
                var line = string.Join(",",
                    rowNumber,
                    Escape(uniqueValue),
                    status,
                    httpStatus,
                    Escape(remarks),
                    DateTime.UtcNow.ToString("o"));

                File.AppendAllText(_filePath, line + Environment.NewLine);
            }
        }

        private static string Escape(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
                return "";

            value = value.Replace("\"", "\"\"");
            return $"\"{value}\"";
        }
    }

}
