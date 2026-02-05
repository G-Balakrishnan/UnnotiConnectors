using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Core.Logging;
using Unnoti.Core.Services;

namespace Unnoti.Core.Execution
{
    public class UnnotiExecutionContext
    {
        public LogService Logger { get; }
        public GoldenRecordImportService ImportService { get; }

        public UnnotiExecutionContext(
            LogService logger,
            GoldenRecordImportService importService)
        {
            Logger = logger;
            ImportService = importService;
        }
    }
}
