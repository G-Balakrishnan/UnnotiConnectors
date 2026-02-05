using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Unnoti.Core.Config
{
    public enum ConfigFieldType
    {
        Text,
        Number,
        Boolean,
        FilePath,
        ConnectionString,
        ConnectionConfigJson
    }

    public class ConfigFieldDefinition
    {
        public string Key { get; set; }
        public string Label { get; set; }
        public ConfigFieldType FieldType { get; set; }
        public bool IsRequired { get; set; }
        public string DefaultValue { get; set; }
        public Type ModelType { get; set; }
    }
}
