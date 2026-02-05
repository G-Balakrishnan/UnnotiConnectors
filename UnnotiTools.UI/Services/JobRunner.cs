using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Unnoti.Core.Config;
using Unnoti.Core.Logging;
using Unnoti.Core.Services;
using Unnoti.Core;
using Unnoti.Core.Execution;

namespace UnnotiTools.UI.Services
{
    public static class JobRunner
    {
        public static void Run(JobConfig job, Action<string> logCallback)
        {
            var logger = new LogService(new ILogSink[] {
                new UiLogSink(logCallback)
           });

            try
            {
                    var connector = ConnectorRegistry.Create(job.ConnectorType);

                    connector.ExecuteAsync(
                        job.ConnectorConfig,
                        CancellationToken.None).Wait();
            }
            catch (UnauthorizedAccessException ex)
            {
                logger.Error($"Invalid credentials. {ex.Message}");
            }
            catch (Exception ex)
            {
                logger.Error($"Connector execution failed: {ex}");
            }

        }
    }
}
