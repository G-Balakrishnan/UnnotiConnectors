using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Core.Logging
{
    public class LogService
    {
        private readonly IEnumerable<ILogSink> _sinks;

        public LogService(IEnumerable<ILogSink> sinks)
        {
            _sinks = sinks;
        }

        public void Info(string msg) => Write("INFO", msg);
        public void Error(string msg) => Write("ERROR", msg);

        private void Write(string level, string msg)
        {
            var entry = new LogEntry(level, msg);
            foreach (var sink in _sinks)
                sink.Write(entry);
        }
    }
}
