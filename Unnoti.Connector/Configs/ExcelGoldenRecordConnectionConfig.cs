using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Core.IContracts;

namespace Unnoti.Connector.Configs
{
    public class ExcelGoldenRecordConnectionConfig : IConnectionConfig
    {
        public string InputFolder { get; set; }
        public string ArchiveFolder { get; set; }
        public string FieldMapperFilePath { get; set; }

        public string ApiBaseUrl { get; set; }
        public string ApiKey { get; set; }

        public string LogFolderPath { get; set; }

        public int BatchSize { get; set; } = 100;
    }

}
