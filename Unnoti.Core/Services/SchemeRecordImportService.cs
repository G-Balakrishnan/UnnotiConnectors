using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Unnoti.Core.Base;
using Unnoti.Core.DTOs;
using Unnoti.Core.Logging;

namespace Unnoti.Core.Services
{
    public class SchemeRecordImportService
    {
        private readonly HttpClient _client;
        private readonly LogService _logger;

        public SchemeRecordImportService(string baseUrl, LogService logger)
        {
            _logger = logger;
            _client = new HttpClient
            {
                BaseAddress = new Uri(baseUrl)
            };
        }

        public SchemeRecordImportService(string baseUrl,string apiKey)
        {
            _client = new HttpClient
            {
                BaseAddress = new Uri(baseUrl)
            };
        }

        public async Task<ImportResult> SendAsync(
         SchemeRecordPayload payload,
         CancellationToken cancellationToken)
        {
            try
            {
                var json = JsonConvert.SerializeObject(payload);

                var resp = await _client.PostAsync(
                    "/scheme/ingest",
                    new StringContent(json, Encoding.UTF8, "application/json"),
                    cancellationToken);

                var responseText = await resp.Content.ReadAsStringAsync();

                return new ImportResult()
                {
                    IsSuccess = resp.IsSuccessStatusCode,
                    HttpStatus = ((int)resp.StatusCode).ToString(),
                    ResponseText = responseText
                };
            }
            catch (Exception ex)
            {
                return new ImportResult
                {
                    IsSuccess = false,
                    HttpStatus = "EXCEPTION",
                    ResponseText = ex.Message
                };
            }
        }

    }
}
