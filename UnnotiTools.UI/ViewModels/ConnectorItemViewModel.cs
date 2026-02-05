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
        public string Icon => ConnectorType.Contains("SQL") ? "🗄️" : "📄";
        public string IconPath { get; }   // ✅ NEW
        public string Description =>
            ConnectorType.Contains("Scheme") ? "Scheme Record Importer" : "Golden Record Importer";

        public ConnectorItemViewModel(string name, string connectorType,string connectoriconpath, IEnumerable<ConfigFieldDefinition> schema)
        {
            Name = name;
            ConnectorType = connectorType;
            Schema = schema;
            IconPath = connectoriconpath;
        }

        public IEnumerable<ConfigFieldDefinition> Schema { get; }

     
    }

}
