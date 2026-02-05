using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Core.Logging
{
    public class LogEntry
    {
        public DateTime Timestamp { get; } = DateTime.Now;
        public string Level { get; }
        public string Message { get; }
        public Exception Exception { get; }

        public LogEntry(string level, string message, Exception ex = null)
        {
            Level = level;
            Message = message;
            Exception = ex;
        }
    }
}
