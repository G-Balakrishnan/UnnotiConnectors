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
    public class GoldenRecordImportService
    {
        private readonly HttpClient _client;
        private readonly LogService _logger;

        public GoldenRecordImportService(string baseUrl, LogService logger)
        {
            _logger = logger;
            _client = new HttpClient
            {
                BaseAddress = new Uri(baseUrl)
            };
        }
        string _baseurl = "";
        public GoldenRecordImportService(string baseUrl,string apiKey)
        {
            _baseurl = baseUrl.ToLower(); ;
            _client = new HttpClient
            {
                BaseAddress = new Uri(baseUrl)
            };
        }
        public ImportResult Send(GoldenRecordPayload payload)
        {
            var url = _baseurl+ "/ingest"; // full URL

            try
            {
                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(
                        new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

                    var json = JsonConvert.SerializeObject(
                        payload,
                        new JsonSerializerSettings
                        {
                            NullValueHandling = NullValueHandling.Ignore
                        });

                    using (var content = new StringContent(json, Encoding.UTF8, "application/json"))
                    {
                        var response = client
                            .PostAsync(url, content)
                            .GetAwaiter()
                            .GetResult();   // 🔒 SYNC

                        var responseText = response.Content
                            .ReadAsStringAsync()
                            .GetAwaiter()
                            .GetResult();   // 🔒 SYNC

                        return new ImportResult
                        {
                            IsSuccess = response.IsSuccessStatusCode,
                            HttpStatus = ((int)response.StatusCode).ToString(),
                            ResponseText = responseText
                        };
                    }
                }
            }
            catch (Exception ex)
            {
                return new ImportResult
                {
                    IsSuccess = false,
                    HttpStatus = "EXCEPTION",
                    ResponseText = ex.ToString()
                };
            }
        }


        public async Task<ImportResult> SendAsync(
         GoldenRecordPayload payload,
         CancellationToken cancellationToken)
        {
            try
            {
                var json = JsonConvert.SerializeObject(payload);

                var resp = await _client.PostAsync(
                    "/ingest",
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
