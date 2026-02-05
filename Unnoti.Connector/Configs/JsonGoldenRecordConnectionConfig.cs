using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Connector.Configs
{
    public class JsonGoldenRecordConnectionConfig
    {
        public string InputFilePath { get; set; }          // JSON file path
        public string FieldMapperFilePath { get; set; }    // Mapper json
        public int BatchSize { get; set; } = 50;

        public string ApiBaseUrl { get; set; }
        public string ApiKey { get; set; }

        public string UniqueIdType { get; set; }            // GR unique id type
        public string LogFolderPath { get; set; }
        public string ArchiveFolder { get; set; }
    }
}
