using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Core.Base
{
    public class ImportResult
    {
        public bool IsSuccess { get; set; }
        public string HttpStatus { get; set; }
        public string ResponseText { get; set; }
    }

}
