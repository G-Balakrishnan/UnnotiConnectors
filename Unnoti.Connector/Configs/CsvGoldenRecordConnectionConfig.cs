using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Core.IContracts;

namespace Unnoti.Connector.Configs
{
    public class CsvGoldenRecordConnectionConfig : IConnectionConfig
    {
        public string InputFolder { get; set; }
        public string ArchiveFolder { get; set; }
        public string FieldMapperFilePath { get; set; }

        public string ApiBaseUrl { get; set; }
        public string ApiKey { get; set; }

        public string LogFolderPath { get; set; }
        public string UniqueIdType { get; set; }
        public int BatchSize { get; set; } = 100;
    }

    public class CsvFieldMapping
    {
        public string Csv_Header { get; set; }
        public string Grs_Field_Key { get; set; }
        public string Data_Type { get; set; } // string, number, date, bool
        public bool IsUnique { get; set; }
    }
    public class CsvFieldMapperConfig
    {
        public List<CsvFieldMapping> Field_Mappings { get; set; }
    }

}
