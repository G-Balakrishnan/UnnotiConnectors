using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unnoti.Core.Config;

namespace UnnotiTools.UI.ViewModels
{
    public class ConnectorItemViewModel
    {
        public string Name { get; }
        public string ConnectorType { get; }
        public IReadOnlyList<ConfigFieldDefinition> Schema { get; }

        public ConnectorItemViewModel(
            string name,
            string type,
            IReadOnlyList<ConfigFieldDefinition> schema)
        {
            Name = name;
            ConnectorType = type;
            Schema = schema;
        }
    }

}
