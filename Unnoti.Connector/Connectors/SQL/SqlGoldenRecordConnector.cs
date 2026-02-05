using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
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

namespace Unnoti.Connector.Connectors.SQL
{
    public class SqlGoldenRecordConnector : ConnectorBase, IConnector, IConfigurableConnector
    {
        public override string Name => "SQL → Golden Record Importer";
        public string ConnectorKey => "SQL_GOLDEN_RECORD";
        public override string ConnectorType => "SqlGoldenRecordConnector";

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

            var cfg = JsonConvert.DeserializeObject<SqlGoldenRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath))
                ?? throw new InvalidOperationException("Invalid SQL connector config");

            var mapper = JsonConvert.DeserializeObject<CsvFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath))
                ?? throw new InvalidOperationException("Invalid mapper config");

            Directory.CreateDirectory(cfg.LogFolderPath);

            var importService = new GoldenRecordImportService(cfg.ApiBaseUrl, cfg.ApiKey);

            var logFilename =
                "SQL_" + DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") + "_ImportLog.csv";

            using (var con = new SqlConnection(cfg.ConnectionString))
            {
                con.Open();

                using (var cmd = new SqlCommand(cfg.Query, con))
                using (var reader = cmd.ExecuteReader())
                {
                    var batch = new List<GoldenRecordPayload>();
                    int rowNumber = 0;

                    while (reader.Read())
                    {
                        token.ThrowIfCancellationRequested();
                        rowNumber++;

                        try
                        {
                            var payload = BuildPayloadFromReader(
                                reader,
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
                }
            }

            return Complete();
        }

        // ================= BATCH SEND (SYNC) =================

        private void SendBatch(
            GoldenRecordImportService service,
            List<GoldenRecordPayload> batch,
            string logsFolder,
            string logFileName,
            int startingRowNumber,
            CancellationToken token)
        {
            var csvLogger = new ImportCsvLogger(logsFolder, logFileName);
            int rowNumber = startingRowNumber;

            foreach (var payload in batch)
            {
                token.ThrowIfCancellationRequested();

                var uniqueValue = payload.UniqueData?
                    .FirstOrDefault()?.GoldenRecordUniqueId ?? "UNKNOWN";

                var result = service.Send(payload);

                _logger.Info($"Row {rowNumber} → {result.HttpStatus}");

                csvLogger.WriteRow(
                    rowNumber,
                    uniqueValue,
                    result.IsSuccess ? "SUCCESS" : "FAILURE",
                    result.HttpStatus,
                    result.ResponseText);

                rowNumber++;
            }
        }

        // ================= PAYLOAD BUILDER =================

        private GoldenRecordPayload BuildPayloadFromReader(
            SqlDataReader reader,
            CsvFieldMapperConfig mapper,
            SqlGoldenRecordConnectionConfig cfg,
            int rowNumber)
        {
            var payload = new GoldenRecordPayload
            {
                FieldValues = new List<FieldValue>(),
                UniqueData = new List<UniqueData>()
            };

            foreach (var map in mapper.Field_Mappings)
            {
                if (!ColumnExists(reader, map.Csv_Header))
                    continue;

                var raw = reader[map.Csv_Header]?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(raw))
                    continue;

                try
                {
                    if (map.IsUnique)
                    {
                        payload.UniqueData.Add(new UniqueData
                        {
                            GoldenRecordUniqueId = raw,
                            UniqueIdType = map.Grs_Field_Key
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
                        $"Row {rowNumber}, Column '{map.Csv_Header}', Value '{raw}' invalid: {ex.Message}");
                }
            }

            if (!payload.UniqueData.Any())
                throw new Exception($"Row {rowNumber} missing unique identifiers");

            return payload;
        }

        private bool ColumnExists(SqlDataReader reader, string columnName)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i).Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        protected override void LogError(string message)
        {
            File.AppendAllText("sql-errors.log", message + Environment.NewLine);
        }

        // ================= CONFIG =================

        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema() =>
            new[]
            {
                new ConfigFieldDefinition
                {
                    Key = "ConnectionConfig",
                    Label = "SQL → Golden Record Import Connector Config Path",
                    FieldType = ConfigFieldType.ConnectionConfigJson,
                    IsRequired = true,
                    ModelType = typeof(SqlGoldenRecordConnectionConfig),
                    DefaultValue = "E:\\Work\\Samples\\UnnotiCommandTool\\Configs\\SQL\\config\\config.json"
                }
            };

        public IConnector GetConfigType() => throw new NotImplementedException();
        public string GetConnectorConfigurations() => throw new NotImplementedException();
    }
}
