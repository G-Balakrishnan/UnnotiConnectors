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
    public class XmlGoldenRecordConnector : ConnectorBase, IConnector, IConfigurableConnector
    {
        public override string Name => "XML → Golden Record Importer";
        public string ConnectorKey => "XML_GOLDEN_RECORD";
        public override string ConnectorType => "XmlGoldenRecordConnector";
        public override string IconPath => "pack://application:,,,/Assets/Icons/xml.png";

        private LogService _logger;

        public Task<ExecutionResult> ExecuteAsync(
            string connectorConfigPath,
            LogService logger,
            CancellationToken token)
        {
            _logger = logger;
            var result = Execute(connectorConfigPath, token);
            return Task.FromResult(result);
        }

        private ExecutionResult Execute(
            string connectorConfigPath,
            CancellationToken token)
        {
            InitializeResult();

            var cfg = JsonConvert.DeserializeObject<XmlGoldenRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath))
                ?? throw new InvalidOperationException("Invalid XML Golden config");

            var mapper = JsonConvert.DeserializeObject<CsvFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath))
                ?? throw new InvalidOperationException("Invalid mapper config");

            Directory.CreateDirectory(cfg.LogFolderPath);

            var doc = XDocument.Load(cfg.InputFilePath);
            var records = doc.XPathSelectElements(cfg.RecordXPath).ToList();

            var importService = new GoldenRecordImportService(cfg.ApiBaseUrl, cfg.ApiKey);

            var logFilename =
                "XML_GOLDEN_" + DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") + "_ImportLog.csv";

            var batch = new List<GoldenRecordPayload>();
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
            }

            return Complete();
        }

        // ================= SEND BATCH =================

        private void SendBatch(
            GoldenRecordImportService service,
            List<GoldenRecordPayload> batch,
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

        // ================= PAYLOAD BUILDER =================

        private GoldenRecordPayload BuildPayloadFromXml(
            XElement element,
            CsvFieldMapperConfig mapper,
            XmlGoldenRecordConnectionConfig cfg,
            int rowNumber)
        {
            var payload = new GoldenRecordPayload
            {
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

        protected override void LogError(string message)
        {
            File.AppendAllText("xml-golden-errors.log", message + Environment.NewLine);
        }

        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema() =>
            new[]
            {
                new ConfigFieldDefinition
                {
                    Key = "ConnectionConfig",
                    Label = "XML → Golden Record Import Connector Configuration Path",
                    FieldType = ConfigFieldType.ConnectionConfigJson,
                    IsRequired = true,
                    ModelType = typeof(XmlGoldenRecordConnectionConfig)
                }
            };

        public IConnector GetConfigType() => throw new NotImplementedException();
        public string GetConnectorConfigurations() => throw new NotImplementedException();
    }
}
