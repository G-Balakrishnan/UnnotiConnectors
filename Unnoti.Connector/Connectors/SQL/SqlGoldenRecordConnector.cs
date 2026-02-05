using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Unnoti.Connector.Base;
using Unnoti.Connector.Configs;
using Unnoti.Core;
using Unnoti.Core.Config;
using Unnoti.Core.DTOs;
using Unnoti.Core.Execution;
using Unnoti.Core.IContracts;
using Unnoti.Core.Services;

namespace Unnoti.Connector.Connectors.SQL
{
    public class SqlGoldenRecordConnector : ConnectorBase, IConnector,IConfigurableConnector
    {
        public override string Name => "SQL → Golden Record Importer";
        public string ConnectorKey => "SQL_GOLDEN_RECORD";

        public override string ConnectorType => "SqlGoldenRecordConnector";

        public async Task<ExecutionResult> ExecuteAsync(
            string connectorConfigPath,
            CancellationToken token)
        {
     

            var cfg = JsonConvert.DeserializeObject<SqlGoldenRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath))
                ?? throw new InvalidOperationException("Invalid SQL connector config");

            var mapper = JsonConvert.DeserializeObject<CsvFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath))
                ?? throw new InvalidOperationException("Invalid mapper config");

            Directory.CreateDirectory(cfg.LogFolderPath);

            var logStamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var successLog = Path.Combine(cfg.LogFolderPath, $"{logStamp}_success.log");
            var failureLog = Path.Combine(cfg.LogFolderPath, $"{logStamp}_failure.log");

            var importService = new GoldenRecordImportService(cfg.ApiBaseUrl, cfg.ApiKey);

            using (var con = new SqlConnection(cfg.ConnectionString))
            {
                await con.OpenAsync(token);

                using (var cmd = new SqlCommand(cfg.Query, con))
                {
                    using (var reader = await cmd.ExecuteReaderAsync(token))
                    {

                        var batch = new List<GoldenRecordPayload>();
                        var rowNumber = 0;

                        while (await reader.ReadAsync(token))
                        {
                            rowNumber++;

                            await ExecuteRecordAsync(async () =>
                            {
                                var payload = BuildPayloadFromReader(
                                    reader,
                                    mapper,
                                    cfg,
                                    rowNumber);

                                batch.Add(payload);

                                if (batch.Count >= cfg.BatchSize)
                                {
                                    await SendBatch(importService, batch, token);
                                    batch.Clear();
                                }

                                File.AppendAllText(
                                    successLog,
                                    $"Row {rowNumber} SUCCESS{Environment.NewLine}");

                            }, $"Row-{rowNumber}");
                        }
                        if (batch.Any())
                        {
                            await SendBatch(importService, batch, token);
                            batch.Clear();
                        }
                    }
                }
               
            }
        

            return Complete();
        }

        private async Task SendBatch(
            GoldenRecordImportService service,
            List<GoldenRecordPayload> batch,
            CancellationToken token)
        {
            foreach (var payload in batch)
                await service.SendAsync(payload, token);
        }

        protected override void LogError(string message)
        {
            File.AppendAllText("sql-errors.log", message + Environment.NewLine);
        }

        private GoldenRecordPayload BuildPayloadFromReader(
            SqlDataReader reader,
            CsvFieldMapperConfig mapper,
            SqlGoldenRecordConnectionConfig cfg,
            int rowNumber)
        {
            var payload = new GoldenRecordPayload();

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

        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema() =>
       new[]
       {
            new ConfigFieldDefinition
            {
                Key = "ConnectionConfig",
                Label = "SQL Connector to Import for Goldren Record ConfigPath",
                FieldType = ConfigFieldType.ConnectionConfigJson,
                IsRequired = true,
                ModelType = typeof(SqlGoldenRecordConnectionConfig),
                DefaultValue="E:\\Work\\Samples\\UnnotiCommandTool\\Configs\\SQL\\config\\config.json"
            }
       };

        public IConnector GetConfigType()
        {
            throw new NotImplementedException();
        }

        public string GetConnectorConfigurations()
        {
            throw new NotImplementedException();
        }
    }
}

