using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.XPath;
using Unnoti.Connector.Base;
using Unnoti.Connector.Configs;
using Unnoti.Core;
using Unnoti.Core.Config;
using Unnoti.Core.DTOs;
using Unnoti.Core.Execution;
using Unnoti.Core.IContracts;
using Unnoti.Core.Logging;
using Unnoti.Core.Services;

namespace Unnoti.Connector.Connectors.XML
{
    public class XmlSchemeRecordConnector : ConnectorBase, IConnector, IConfigurableConnector
    {
        public override string Name => "XML → Scheme Record Importer";
        public string ConnectorKey => "XML_SCHEME_RECORD";
        public override string ConnectorType => "XmlSchemeRecordConnector";
        public override string IconPath => "pack://application:,,,/Assets/Icons/xml.png";

        private LogService _logger;

        // =====================================================
        // ENTRY POINT
        // =====================================================
        public Task<ExecutionResult> ExecuteAsync(
            string connectorConfigPath,
            LogService logger,
            CancellationToken token)
        {
            _logger = logger;
            var result = Execute(connectorConfigPath, token);
            return Task.FromResult(result);
        }

        // =====================================================
        // CORE EXECUTION (SYNC)
        // =====================================================
        private ExecutionResult Execute(
            string connectorConfigPath,
            CancellationToken token)
        {
            InitializeResult();

            var cfg = JsonConvert.DeserializeObject<XmlSchemeRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath))
                ?? throw new InvalidOperationException("Invalid XML Scheme connector config");

            var mapper = JsonConvert.DeserializeObject<CsvFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath))
                ?? throw new InvalidOperationException("Invalid mapper config");

            Directory.CreateDirectory(cfg.LogFolderPath);

            _logger.Info($"Loading XML file: {cfg.InputFilePath}");

            var document = XDocument.Load(cfg.InputFilePath);
            var records = document.XPathSelectElements(cfg.RecordXPath).ToList();

            _logger.Info($"Records found: {records.Count}");

            var importService = new SchemeRecordImportService(
                cfg.ApiBaseUrl,
                cfg.ApiKey,
                _logger);

            var logFilename =
                "XML_SCHEME_" +
                DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") +
                "_ImportLog.csv";

            var batch = new List<SchemeRecordPayload>();
            int rowNumber = 0;

            foreach (var record in records)
            {
                token.ThrowIfCancellationRequested();
                rowNumber++;

                try
                {
                    var payload = BuildPayloadFromXml(
                        record,
                        mapper,
                        cfg,
                        rowNumber);

                    batch.Add(payload);

                    if (batch.Count >= cfg.BatchSize)
                    {
                        SendBatch(
                            importService,
                            batch,
                            cfg.LogFolderPath,
                            logFilename,
                            rowNumber - batch.Count + 1,
                            token);

                        batch.Clear();
                    }
                }
                catch (Exception ex)
                {
                    _logger.Error($"Row {rowNumber}: {ex.Message}");
                    LogError($"Row {rowNumber}: {ex.Message}");
                }
            }

            if (batch.Any())
            {
                SendBatch(
                    importService,
                    batch,
                    cfg.LogFolderPath,
                    logFilename,
                    rowNumber - batch.Count + 1,
                    token);

                batch.Clear();
            }

            // Optional archive
            if (!string.IsNullOrWhiteSpace(cfg.ArchiveFolder))
            {
                Directory.CreateDirectory(cfg.ArchiveFolder);

                var target = Path.Combine(
                    cfg.ArchiveFolder,
                    Path.GetFileName(cfg.InputFilePath));

                if (File.Exists(target))
                    File.Delete(target);

                // File.Move(cfg.InputFilePath, target);
            }

            return Complete();
        }

        // =====================================================
        // SEND BATCH (SYNC HTTP)
        // =====================================================
        private void SendBatch(
            SchemeRecordImportService service,
            List<SchemeRecordPayload> batch,
            string logsFolder,
            string logFileName,
            int startingRow,
            CancellationToken token)
        {
            var csvLogger = new ImportCsvLogger(logsFolder, logFileName);
            int row = startingRow;

            foreach (var payload in batch)
            {
                token.ThrowIfCancellationRequested();

                var uniqueValue = payload.UniqueData
                    .FirstOrDefault()?.GoldenRecordUniqueId ?? "UNKNOWN";

                var result = service.Send(payload);

                _logger.Info($"Row {row} → {result.HttpStatus}");

                csvLogger.WriteRow(
                    row,
                    uniqueValue,
                    result.IsSuccess ? "SUCCESS" : "FAILURE",
                    result.HttpStatus,
                    result.ResponseText);

                row++;
            }
        }

        // =====================================================
        // PAYLOAD BUILDER (XML → SchemeRecordPayload)
        // =====================================================
        private SchemeRecordPayload BuildPayloadFromXml(
            XElement element,
            CsvFieldMapperConfig mapper,
            XmlSchemeRecordConnectionConfig cfg,
            int rowNumber)
        {
            var payload = new SchemeRecordPayload
            {
                WorkflowKey = cfg.WorkflowKey,   // ⭐ REQUIRED ADDON
                FieldValues = new List<FieldValue>(),
                UniqueData = new List<UniqueData>()
            };

            foreach (var map in mapper.Field_Mappings)
            {
                var valueElement = element.XPathSelectElement(map.Csv_Header);
                if (valueElement == null)
                    continue;

                var raw = valueElement.Value?.Trim();
                if (string.IsNullOrWhiteSpace(raw))
                    continue;

                try
                {
                    if (map.IsUnique)
                    {
                        payload.UniqueData.Add(new UniqueData
                        {
                            GoldenRecordUniqueId = raw,
                            UniqueIdType = cfg.UniqueIdType
                        });
                        continue;
                    }

                    var field = new FieldValue
                    {
                        FieldKey = map.Grs_Field_Key
                    };

                    switch (map.Data_Type.ToLowerInvariant())
                    {
                        case "string":
                            field.StringValue = raw;
                            break;
                        case "number":
                            field.NumericValue = decimal.Parse(raw);
                            break;
                        case "date":
                            field.DateValue = DateTime.Parse(raw);
                            break;
                        case "bool":
                            field.BoolValue = bool.Parse(raw);
                            break;
                        default:
                            throw new Exception($"Unsupported data type {map.Data_Type}");
                    }

                    payload.FieldValues.Add(field);
                }
                catch (Exception ex)
                {
                    throw new Exception(
                        $"Row {rowNumber}, XPath '{map.Csv_Header}', Value '{raw}' invalid: {ex.Message}");
                }
            }

            if (!payload.UniqueData.Any())
                throw new Exception($"Row {rowNumber} missing unique identifiers");

            return payload;
        }

        // =====================================================
        // ERROR LOGGING
        // =====================================================
        protected override void LogError(string message)
        {
            File.AppendAllText("xml-scheme-errors.log", message + Environment.NewLine);
        }

        // =====================================================
        // CONFIG SCHEMA
        // =====================================================
        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema() =>
            new[]
            {
                new ConfigFieldDefinition
                {
                    Key = "ConnectionConfig",
                    Label = "XML → Scheme Record Import Connector Configuration Path",
                    FieldType = ConfigFieldType.ConnectionConfigJson,
                    IsRequired = true,
                    ModelType = typeof(XmlSchemeRecordConnectionConfig),
                    DefaultValue =
                        "E:\\Work\\Samples\\UnnotiCommandTool\\Configs\\XML\\Scheme\\config.json"
                }
            };

        public IConnector GetConfigType() => throw new NotImplementedException();
        public string GetConnectorConfigurations() => throw new NotImplementedException();
    }
}
