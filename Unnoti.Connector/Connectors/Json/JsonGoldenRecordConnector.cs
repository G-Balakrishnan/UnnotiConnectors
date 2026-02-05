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
    public class JsonGoldenRecordConnector : ConnectorBase, IConnector, IConfigurableConnector
    {
        public override string Name => "JSON → Golden Record Importer";
        public string ConnectorKey => "JSON_GOLDEN_RECORD";
        public override string ConnectorType => "JsonGoldenRecordConnector";
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

            var cfg = JsonConvert.DeserializeObject<JsonGoldenRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath))
                ?? throw new InvalidOperationException("Invalid JSON Golden Record config");

            var mapper = JsonConvert.DeserializeObject<CsvFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath))
                ?? throw new InvalidOperationException("Invalid mapper config");

            Directory.CreateDirectory(cfg.LogFolderPath);

            var jsonText = File.ReadAllText(cfg.InputFilePath);
            var jsonArray = JArray.Parse(jsonText);

            var importService = new GoldenRecordImportService(cfg.ApiBaseUrl, cfg.ApiKey);

            var logFilename =
                "JSON_GOLDEN_" + DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") + "_ImportLog.csv";

            var batch = new List<GoldenRecordPayload>();
            int rowNumber = 0;

            foreach (var tokenItem in jsonArray)
            {
                token.ThrowIfCancellationRequested();
                rowNumber++;

                try
                {
                    var payload = BuildPayloadFromJson(
                        tokenItem,
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
            GoldenRecordImportService service,
            List<GoldenRecordPayload> batch,
            string logsFolder,
            string logFileName,
            int startingRowNumber,
            CancellationToken token)
        {
            var csvLogger = new ImportCsvLogger(logsFolder, logFileName);
            int row = startingRowNumber;

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

        private GoldenRecordPayload BuildPayloadFromJson(
            JToken json,
            CsvFieldMapperConfig mapper,
            JsonGoldenRecordConnectionConfig cfg,
            int rowNumber)
        {
            var payload = new GoldenRecordPayload
            {
                FieldValues = new List<FieldValue>(),
                UniqueData = new List<UniqueData>()
            };

            foreach (var map in mapper.Field_Mappings)
            {
                var valueToken = ResolveJsonPath(json, map.Csv_Header);
                if (valueToken == null)
                    continue;

                var raw = valueToken.ToString().Trim();
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

        // ===================== JSON PATH RESOLVER =====================

        private JToken ResolveJsonPath(JToken root, string path)
        {
            try
            {
                // Supports: a.b.c , items[0].id
                return root.SelectToken(path);
            }
            catch
            {
                return null;
            }
        }

        protected override void LogError(string message)
        {
            File.AppendAllText("json-golden-errors.log", message + Environment.NewLine);
        }

        // ===================== CONFIG =====================

        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema() =>
            new[]
            {
                new ConfigFieldDefinition
                {
                    Key = "ConnectionConfig",
                    Label = "JSON → Golden Record Import Connector Configuration Path",
                    FieldType = ConfigFieldType.ConnectionConfigJson,
                    IsRequired = true,
                    ModelType = typeof(JsonGoldenRecordConnectionConfig),
                    DefaultValue =
                        "E:\\Work\\Samples\\UnnotiCommandTool\\Configs\\JSON\\GoldenRecord\\config.json"
                }
            };

        public IConnector GetConfigType() => throw new NotImplementedException();
        public string GetConnectorConfigurations() => throw new NotImplementedException();
    }
}
