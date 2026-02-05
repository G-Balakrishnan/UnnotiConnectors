using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Core.Logging
{
    public interface ILogSink
    {
        void Write(LogEntry entry);
    }
}
