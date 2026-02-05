public class XmlGoldenRecordConnectionConfig
{
    public string InputFilePath { get; set; }
    public string RecordXPath { get; set; }   // e.g. "/Records/Record"
    public string FieldMapperFilePath { get; set; }
    public int BatchSize { get; set; }
    public string ApiBaseUrl { get; set; }
    public string ApiKey { get; set; }
    public string UniqueIdType { get; set; }
    public string LogFolderPath { get; set; }
}
