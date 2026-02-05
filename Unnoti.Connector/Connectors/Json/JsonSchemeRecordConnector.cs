using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Unnoti.Connector.Base;
using Unnoti.Connector.Configs;
using Unnoti.Core;
using Unnoti.Core.Config;
using Unnoti.Core.DTOs;
using Unnoti.Core.Execution;
using Unnoti.Core.IContracts;
using Unnoti.Core.Logging;
using Unnoti.Core.Services;

namespace Unnoti.Connector.Connectors.JSON
{
    public class JsonSchemeRecordConnector : ConnectorBase, IConnector, IConfigurableConnector
    {
        public override string Name => "JSON → Scheme Record Importer";
        public string ConnectorKey => "JSON_SCHEME_RECORD";
        public override string ConnectorType => "JsonSchemeRecordConnector";
        public override string IconPath => "pack://application:,,,/Assets/Icons/json.png";

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

            var cfg = JsonConvert.DeserializeObject<JsonSchemeRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath))
                ?? throw new InvalidOperationException("Invalid JSON Scheme config");

            var mapper = JsonConvert.DeserializeObject<CsvFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath))
                ?? throw new InvalidOperationException("Invalid mapper config");

            Directory.CreateDirectory(cfg.LogFolderPath);

            var jsonText = File.ReadAllText(cfg.InputFilePath);
            var records = JArray.Parse(jsonText);

            var importService = new SchemeRecordImportService(
                cfg.ApiBaseUrl,
                cfg.ApiKey,
                _logger);

            var logFilename =
                "JSON_SCHEME_" + DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") + "_ImportLog.csv";

            var batch = new List<SchemeRecordPayload>();
            int rowNumber = 0;

            foreach (var item in records)
            {
                token.ThrowIfCancellationRequested();
                rowNumber++;

                try
                {
                    var payload = BuildPayloadFromJson(
                        item,
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

            return Complete();
        }

        // ===================== SEND BATCH =====================

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

        // ===================== PAYLOAD BUILDER =====================

        private SchemeRecordPayload BuildPayloadFromJson(
            JToken json,
            CsvFieldMapperConfig mapper,
            JsonSchemeRecordConnectionConfig cfg,
            int rowNumber)
        {
            var payload = new SchemeRecordPayload
            {
                WorkflowKey = cfg.WorkflowKey, // ⭐ addon param
                FieldValues = new List<FieldValue>(),
                UniqueData = new List<UniqueData>()
            };

            foreach (var map in mapper.Field_Mappings)
            {
                var token = ResolveJsonPath(json, map.Csv_Header);
                if (token == null)
                    continue;

                var raw = token.ToString().Trim();
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
                        $"Row {rowNumber}, Path '{map.Csv_Header}', Value '{raw}' invalid: {ex.Message}");
                }
            }

            if (!payload.UniqueData.Any())
                throw new Exception($"Row {rowNumber} missing unique identifiers");

            return payload;
        }

        private JToken ResolveJsonPath(JToken root, string path)
        {
            try
            {
                return root.SelectToken(path);
            }
            catch
            {
                return null;
            }
        }

        protected override void LogError(string message)
        {
            File.AppendAllText("json-scheme-errors.log", message + Environment.NewLine);
        }

        // ===================== CONFIG =====================

        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema() =>
            new[]
            {
                new ConfigFieldDefinition
                {
                    Key = "ConnectionConfig",
                    Label = "JSON → Scheme Record Import Connector Configuration Path",
                    FieldType = ConfigFieldType.ConnectionConfigJson,
                    IsRequired = true,
                    ModelType = typeof(JsonSchemeRecordConnectionConfig),
                    DefaultValue =
                        "E:\\Work\\Samples\\UnnotiCommandTool\\Configs\\JSON\\Scheme\\config.json"
                }
            };

        public IConnector GetConfigType() => throw new NotImplementedException();
        public string GetConnectorConfigurations() => throw new NotImplementedException();
    }
}
