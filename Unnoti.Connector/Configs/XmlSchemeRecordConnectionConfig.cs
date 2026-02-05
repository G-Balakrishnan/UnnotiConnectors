using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Connector.Configs
{
    public class XmlSchemeRecordConnectionConfig
    {
        // ===================== INPUT =====================

        /// <summary>
        /// Full path to the XML input file
        /// </summary>
        public string InputFilePath { get; set; }

        /// <summary>
        /// XPath that selects one logical record node
        /// Example: /Records/Record
        /// </summary>
        public string RecordXPath { get; set; }

        /// <summary>
        /// Mapper configuration (same CsvFieldMapperConfig used everywhere)
        /// </summary>
        public string FieldMapperFilePath { get; set; }

        // ===================== BATCHING =====================

        /// <summary>
        /// Number of records to send per API batch
        /// </summary>
        public int BatchSize { get; set; } = 50;

        // ===================== API =====================

        /// <summary>
        /// Base API URL (example: http://192.168.0.11:8025/gateway)
        /// </summary>
        public string ApiBaseUrl { get; set; }

        /// <summary>
        /// API Key if required (kept for parity with other connectors)
        /// </summary>
        public string ApiKey { get; set; }

        // ===================== SCHEME-SPECIFIC =====================

        /// <summary>
        /// Workflow key to be injected into every SchemeRecordPayload
        /// </summary>
        public string WorkflowKey { get; set; }

        /// <summary>
        /// Unique identifier type used for UniqueData
        /// Example: AADHAR, APPLICATION_ID
        /// </summary>
        public string UniqueIdType { get; set; }

        // ===================== LOGGING =====================

        /// <summary>
        /// Folder where CSV audit logs will be written
        /// </summary>
        public string LogFolderPath { get; set; }

        /// <summary>
        /// Optional archive folder for processed XML files
        /// </summary>
        public string ArchiveFolder { get; set; }
    }
}
