using Newtonsoft.Json;
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

namespace Unnoti.Connector.Connectors.CSV
{
    public class CsvGoldenRecordConnector : ConnectorBase, IConnector, IConfigurableConnector
    {
        public override string Name => "CSV → Golden Record Importer";
        public string ConnectorKey => "CSV_GOLDEN_RECORD";

        public override string ConnectorType => "CsvGoldenRecordConnector";

        public async Task<ExecutionResult> ExecuteAsync(
            string connectorConfigPath,
            CancellationToken token)
        {

            var cfg = JsonConvert.DeserializeObject<CsvGoldenRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath))
                ?? throw new InvalidOperationException("Invalid CSV connector config");

            var mapper = JsonConvert.DeserializeObject<CsvFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath))
                ?? throw new InvalidOperationException("Invalid mapper config");

            Directory.CreateDirectory(cfg.LogFolderPath);

            var logStamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var successLog = Path.Combine(cfg.LogFolderPath, $"{logStamp}_success.log");
            var failureLog = Path.Combine(cfg.LogFolderPath, $"{logStamp}_failure.log");

            var importService = new GoldenRecordImportService(cfg.ApiBaseUrl, cfg.ApiKey);

            foreach (var file in Directory.GetFiles(cfg.InputFolder, "*.csv"))
            {
                InitializeResult();
                var lines = File.ReadAllLines(file);
                if (lines.Length < 2) continue;

                var headers = lines[0].Split(',').Select(h => h.Trim()).ToList();
                var batch = new List<GoldenRecordPayload>();
                string logFilename = Path.GetFileNameWithoutExtension(file) + "_" + DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") + "_ImportLog.csv";
                for (int row = 1; row < lines.Length; row++)
                {
                    var cols = lines[row].Split(',');

                    await ExecuteRecordAsync(async () =>
                    {
                        var payload = BuildPayloadFromCsv(
                            headers,
                            cols,
                            mapper,
                            cfg,
                            row + 1);

                        batch.Add(payload);

                        if (batch.Count >= cfg.BatchSize)
                        {
                            await SendBatch(importService, batch, cfg.LogFolderPath, logFilename, row + 1 - batch.Count, token);
                            batch.Clear();
                        }

                        File.AppendAllText(
                            successLog,
                            $"Row {row + 1} SUCCESS{Environment.NewLine}");

                    }, $"Row-{row + 1}");

                }

                if (batch.Any())
                {
                    await SendBatch(importService, batch, cfg.LogFolderPath, logFilename,  batch.Count, token);
                    batch.Clear();
                }

                var target = Path.Combine(cfg.ArchiveFolder, Path.GetFileName(file));
                if (File.Exists(target))
                    File.Delete(target);

                File.Move(file, target);
            }

            return Complete();
        }

        private async Task SendBatch(
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
                var uniqueValue = payload.UniqueData?
                    .FirstOrDefault()?.GoldenRecordUniqueId ?? "UNKNOWN";

                var result = await service.SendAsync(payload, token);

                csvLogger.WriteRow(
                    rowNumber,
                    uniqueValue,
                    result.IsSuccess ? "SUCCESS" : "FAILURE",
                    result.HttpStatus,
                    result.ResponseText);

                rowNumber++;
            }
        }



        protected override void LogError(string message)
        {
            File.AppendAllText("csv-errors.log", message + Environment.NewLine);
        }

        private GoldenRecordPayload BuildPayloadFromCsv(
            List<string> headers,
            string[] values,
            CsvFieldMapperConfig mapper,
            CsvGoldenRecordConnectionConfig cfg,
            int rowNumber)
        {
            var payload = new GoldenRecordPayload() { FieldValues = new List<FieldValue>(), UniqueData = new List<UniqueData>() };

            foreach (var map in mapper.Field_Mappings)
            {
                var idx = headers.IndexOf(map.Csv_Header);
                if (idx == -1) continue;

                var raw = values.Length > idx ? values[idx]?.Trim() : null;
                if (string.IsNullOrWhiteSpace(raw)) continue;

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
                        $"Row {rowNumber}, Column '{map.Csv_Header}', Value '{raw}' invalid: {ex.Message}");
                }
            }

            if (!payload.UniqueData.Any())
                throw new Exception($"Row {rowNumber} missing unique identifiers");

            return payload;
        }

        public IConnector GetConfigType()
        {
            throw new NotImplementedException();
        }

        public string GetConnectorConfigurations()
        {
            throw new NotImplementedException();
        }

        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema()
        {
            return new[]
  {
            new ConfigFieldDefinition
            {
                Key = "ConnectionConfig",
                Label = "CSV To Goldren Record Import Connector Configuration Path",
                FieldType = ConfigFieldType.ConnectionConfigJson,
                IsRequired = true,
                ModelType = typeof(CsvGoldenRecordConnectionConfig),
                DefaultValue="E:\\Work\\Samples\\UnnotiCommandTool\\Configs\\CSV\\GoldrenRecord\\config\\config.json"
            }
          };
        }
    }
}
