using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Text;
using Unnoti.Core.Base;
using Unnoti.Core.DTOs;
using Unnoti.Core.Logging;

namespace Unnoti.Core.Services
{
    public class SchemeRecordImportService
    {
        private readonly string _url;
        private readonly LogService _logger;

        public SchemeRecordImportService(string baseUrl, string apiKey, LogService logger = null)
        {
            _logger = logger;

            _url = baseUrl.TrimEnd('/') + "/scheme/ingest";
        }

        public ImportResult Send(SchemeRecordPayload payload)
        {
            try
            {
                var json = JsonConvert.SerializeObject(
                    payload,
                    new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore
                    });

                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(
                        new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));

                    using (var content = new StringContent(json, Encoding.UTF8, "application/json"))
                    {
                        var response = client
                            .PostAsync(_url, content)
                            .GetAwaiter()
                            .GetResult();   // 🔒 SYNC

                        var responseText = response.Content
                            .ReadAsStringAsync()
                            .GetAwaiter()
                            .GetResult();

                        _logger?.Info($"POST {_url} → {(int)response.StatusCode}");

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
                _logger?.Error(ex.ToString());

                return new ImportResult
                {
                    IsSuccess = false,
                    HttpStatus = "EXCEPTION",
                    ResponseText = ex.ToString()
                };
            }
        }
    }
}
