using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Core.Config;

namespace Unnoti.Core.IContracts
{
    public interface IConfigurableConnector
    {
        IReadOnlyList<ConfigFieldDefinition> GetConfigSchema();
    }
}
