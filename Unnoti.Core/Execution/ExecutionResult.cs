using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Core
{
    public class ExecutionResult
    {
        public int Total { get; private set; }
        public int Success { get; private set; }
        public int Failed { get; private set; }

        public DateTime StartedAt { get; } = DateTime.Now;
        public DateTime? FinishedAt { get; private set; }

        public void RecordSuccess()
        {
            Total++; Success++;
        }

        public void RecordFailure()
        {
            Total++; Failed++;
        }

        public void Complete()
        {
            FinishedAt = DateTime.Now;
        }
    }
}
