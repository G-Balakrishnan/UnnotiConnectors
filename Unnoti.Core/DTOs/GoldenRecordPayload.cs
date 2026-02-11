using System.Collections.Generic;
using System;

namespace Unnoti.Core.DTOs
{
    public class GoldenRecordPayload
    {
        public string BaseApiUrl { get; set; }
        public List<UniqueData> UniqueData { get; set; } 
        public List<FieldValue> FieldValues { get; set; }
    }
    public class SchemeRecordPayload
    {
        public string BaseApiUrl { get; set; }
        public List<UniqueData> UniqueData { get; set; }
        public List<FieldValue> FieldValues { get; set; }
        public string WorkflowKey { get; set; }
    }
    public class UniqueData
    {
        public string GoldenRecordUniqueId { get; set; }
        public string UniqueIdType { get; set; }
    }

    public class FieldValue
    {
        public string FieldKey { get; set; }
        public string StringValue { get; set; }
        public decimal? NumericValue { get; set; }
        public DateTime? DateValue { get; set; }
        public bool? BoolValue { get; set; }

        public object JsonValue { get; set; }
    }
}
