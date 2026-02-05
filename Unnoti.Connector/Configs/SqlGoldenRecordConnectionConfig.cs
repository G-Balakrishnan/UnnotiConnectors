using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Core.IContracts;

namespace Unnoti.Connector.Configs
{
    public class SqlGoldenRecordConnectionConfig : IConnectionConfig
    {
        public string ConnectionString { get; set; }
        public string Query { get; set; }
        public string FieldMapperFilePath { get; set; }

        public string ApiBaseUrl { get; set; }
        public string ApiKey { get; set; }

        public string LogFolderPath { get; set; }
        public int BatchSize { get; set; } = 100;
    }
}
