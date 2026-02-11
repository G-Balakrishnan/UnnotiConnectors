using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using OfficeOpenXml;
using OfficeOpenXml.FormulaParsing.LexicalAnalysis;
using System;
using System.Collections.Generic;
using System.ComponentModel;
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


namespace Unnoti.Connector.Connectors.Excel
{
   
    public class ExcelGoldenRecordConnector : ConnectorBase, IConnector, IConfigurableConnector
    {
        public override string Name => "Excel → Golden Record Importer";
        public string ConnectorKey => "EXCEL_GOLDEN_RECORD";
        public override string ConnectorType => "ExcelGoldenRecordConnector";

        public override string IconPath => "pack://application:,,,/Assets/Icons/excel.png";

        private LogService logger;
      
        public Task<ExecutionResult> ExecuteAsync(
            string connectorConfigPath,
            LogService logger,
            CancellationToken token)
        {
            this.logger = logger;
            var result = Execute(connectorConfigPath, token);
            return Task.FromResult(result);
        }

        private ExecutionResult Execute(
            string connectorConfigPath,
            CancellationToken token)
        {
            //ExcelPackage.LicenseContext = OfficeOpenXml.LicenseContext.NonCommercial;

            var cfg = JsonConvert.DeserializeObject<ExcelGoldenRecordConnectionConfig>(
                File.ReadAllText(connectorConfigPath));

            var mapper = JsonConvert.DeserializeObject<ExcelFieldMapperConfig>(
                File.ReadAllText(cfg.FieldMapperFilePath));

            Directory.CreateDirectory(cfg.LogFolderPath);

            var importService = new GoldenRecordImportService(cfg.ApiBaseUrl, cfg.ApiKey);

            foreach (var file in Directory.GetFiles(cfg.InputFolder, "*.xlsx"))
            {
                InitializeResult();

                using (var package = new ExcelPackage(new FileInfo(file)))
                {
                    foreach (var sheetMapper in mapper.Sheets)
                    {
                        var sheet = package.Workbook.Worksheets
                            .FirstOrDefault(s =>
                                s.Name.Equals(sheetMapper.SheetName,
                                StringComparison.OrdinalIgnoreCase));

                        if (sheet == null)
                            continue;

                        if (!string.IsNullOrEmpty(sheetMapper.Json_Field_Key))
                            ProcessTableControlSheet(sheet, sheetMapper, cfg, importService, token);
                        else
                            ProcessNormalSheet(sheet, sheetMapper, cfg, importService, token);
                    }
                }

                var target = Path.Combine(cfg.ArchiveFolder, Path.GetFileName(file));
                if (File.Exists(target))
                    File.Delete(target);

                // File.Move(file, target);
            }

            return Complete();
        }

        // ============================
        // NORMAL SHEET
        // ============================

        private void ProcessNormalSheet(
            ExcelWorksheet sheet,
            ExcelSheetMapper mapper,
            ExcelGoldenRecordConnectionConfig cfg,
            GoldenRecordImportService importService,
            CancellationToken token)
        {
            var headers = GetHeaders(sheet);

            var batch = new List<GoldenRecordPayload>();

            for (int row = 2; row <= sheet.Dimension.End.Row; row++)
            {
                token.ThrowIfCancellationRequested();

                var rowDict = ReadRow(sheet, headers, row);

                var payload = BuildPayload(rowDict, mapper, row);

                batch.Add(payload);

                if (batch.Count >= cfg.BatchSize)
                {
                    SendBatch(importService, batch, token);
                    batch.Clear();
                }
            }

            if (batch.Any())
                SendBatch(importService, batch, token);
        }

        // ============================
        // TABLE CONTROL SHEET
        // ============================

        private void ProcessTableControlSheet(
            ExcelWorksheet sheet,
            ExcelSheetMapper mapper,
            ExcelGoldenRecordConnectionConfig cfg,
            GoldenRecordImportService importService,
            CancellationToken token)
        {
            var headers = GetHeaders(sheet);
            var rows = new List<Dictionary<string, string>>();

            for (int row = 2; row <= sheet.Dimension.End.Row; row++)
                rows.Add(ReadRow(sheet, headers, row));

            var uniqueMappings = mapper.Field_Mappings
                .Where(x => x.IsUnique)
                .ToList();

            var jsonColumns = mapper.Field_Mappings
                .Where(x => !x.IsUnique)
                .ToList();

            var grouped = rows.GroupBy(r =>
                string.Join("|", uniqueMappings
                    .Select(u => r.ContainsKey(u.Excel_Header)
                        ? r[u.Excel_Header]
                        : "")));

            var batch = new List<GoldenRecordPayload>();

            foreach (var group in grouped)
            {
                token.ThrowIfCancellationRequested();

                var firstRow = group.First();

                var payload = new GoldenRecordPayload
                {
                    FieldValues = new List<FieldValue>(),
                    UniqueData = new List<UniqueData>()
                };

                // Add unique values
                foreach (var u in uniqueMappings)
                {
                    payload.UniqueData.Add(new UniqueData
                    {
                        GoldenRecordUniqueId = firstRow[u.Excel_Header],
                        UniqueIdType = u.Grs_Field_Key
                    });
                }

                // Build JSON list
                var jsonList = new List<Dictionary<string, object>>();

                foreach (var row in group)
                {
                    var obj = new Dictionary<string, object>();

                    foreach (var col in jsonColumns)
                    {
                        if (row.ContainsKey(col.Excel_Header))
                            obj[col.Excel_Header] = row[col.Excel_Header];
                    }

                    jsonList.Add(obj);
                }

                payload.FieldValues.Add(new FieldValue
                {
                    FieldKey = mapper.Json_Field_Key,
                    JsonValue = JsonConvert.SerializeObject(jsonList)
                });

                batch.Add(payload);

                if (batch.Count >= cfg.BatchSize)
                {
                    SendBatch(importService, batch, token);
                    batch.Clear();
                }
            }

            if (batch.Any())
                SendBatch(importService, batch, token);
        }

        // ============================
        // COMMON METHODS
        // ============================

        private GoldenRecordPayload BuildPayload(
            Dictionary<string, string> rowData,
            ExcelSheetMapper mapper,
            int rowNumber)
        {
            var payload = new GoldenRecordPayload
            {
                FieldValues = new List<FieldValue>(),
                UniqueData = new List<UniqueData>()
            };

            foreach (var map in mapper.Field_Mappings)
            {
                if (!rowData.TryGetValue(map.Excel_Header, out var raw))
                    continue;

                if (string.IsNullOrWhiteSpace(raw))
                    continue;

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

                switch (map.Data_Type?.ToLowerInvariant())
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
                        field.StringValue = raw;
                        break;
                }

                payload.FieldValues.Add(field);
            }

            if (!payload.UniqueData.Any())
                throw new Exception($"Row {rowNumber} missing unique identifiers");

            return payload;
        }

        private List<string> GetHeaders(ExcelWorksheet sheet)
        {
            var headers = new List<string>();

            for (int col = 1; col <= sheet.Dimension.End.Column; col++)
                headers.Add(sheet.Cells[1, col].Text.Trim());

            return headers;
        }

        private Dictionary<string, string> ReadRow(
            ExcelWorksheet sheet,
            List<string> headers,
            int row)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            for (int col = 1; col <= headers.Count; col++)
                dict[headers[col - 1]] = sheet.Cells[row, col].Text?.Trim();

            return dict;
        }

        private void SendBatch(
            GoldenRecordImportService service,
            List<GoldenRecordPayload> batch,
            CancellationToken token)
        {
            foreach (var payload in batch)
            {
                token.ThrowIfCancellationRequested();
                var result = service.Send(payload);
                logger?.Info(result.ResponseText);
            }
        }

        protected override void LogError(string message)
        {
            File.AppendAllText("scheme-excel-errors.log", message + Environment.NewLine);
        }

        // ===================== CONFIG =====================

        public IReadOnlyList<ConfigFieldDefinition> GetConfigSchema() =>
            new[]
            {
                new ConfigFieldDefinition
                {
                    Key = "ConnectionConfig",
                    Label = "Excel → Goldren Record Import Connector Configuration Path",
                    FieldType = ConfigFieldType.ConnectionConfigJson,
                    IsRequired = true,
                    ModelType = typeof(ExcelGoldenRecordConnectionConfig),
                    DefaultValue =
                        "E:\\Work\\Samples\\UnnotiCommandTool\\Configs\\Excel\\GoldrenRecord\\config\\config.json"
                }
            };

        public IConnector GetConfigType() => throw new NotImplementedException();
        public string GetConnectorConfigurations() => throw new NotImplementedException();
    }
}


    public class ExcelFieldMapping
    {
        public string Excel_Header { get; set; }
        public string Grs_Field_Key { get; set; }
        public string Data_Type { get; set; }
        public bool IsUnique { get; set; }
    }

    public class ExcelSheetMapper
    {
        public string SheetName { get; set; }

        // If this has value → it's a TableControl sheet
        public string Json_Field_Key { get; set; }

        public List<ExcelFieldMapping> Field_Mappings { get; set; }
    }

    public class ExcelFieldMapperConfig
    {
        public List<ExcelSheetMapper> Sheets { get; set; }
    }

