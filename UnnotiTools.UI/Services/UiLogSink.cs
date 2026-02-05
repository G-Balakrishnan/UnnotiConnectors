using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Core.Logging;

namespace UnnotiTools.UI.Services
{
    public class UiLogSink : ILogSink
    {
        private readonly Action<string> _callback;

        public UiLogSink(Action<string> callback)
        {
            _callback = callback;
        }

        public void Write(LogEntry entry)
        {
            _callback($"[{entry.Level}] {entry.Message}");
        }
    }
}
