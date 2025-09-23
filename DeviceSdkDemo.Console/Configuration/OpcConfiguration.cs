using System.Text.Json.Serialization;

namespace AgentOPC.Console.Configuration
{
    public enum DeviceType
    {
        Press = 0,
        Conveyor = 1,
        QualityStation = 2,
        Compressor = 3
    }
    public class OpcConfiguration
    {
        public OpcServerConfiguration[] OpcServers { get; set; } = Array.Empty<OpcServerConfiguration>();
        public ProductionLine[] ProductionLines { get; set; } = Array.Empty<ProductionLine>();
        public GlobalSettings GlobalSettings { get; set; } = new();
    }

    public class OpcServerConfiguration
    {
        public string Name { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
        public string SecurityPolicy { get; set; } = "None";
        public int ConnectionTimeout { get; set; } = 30000;
        public int SessionTimeout { get; set; } = 60000;
    }

    public class ProductionLine
    {
        public string LineId { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string OpcServerName { get; set; } = string.Empty;
        public TimeSpan DefaultSamplingInterval { get; set; } = TimeSpan.FromSeconds(2);
        public SimplifiedDeviceConfiguration[] Devices { get; set; } = Array.Empty<SimplifiedDeviceConfiguration>();
    }

    // Simplified device configuration - only what's actually needed
    public class SimplifiedDeviceConfiguration
    {
        public string DeviceId { get; set; } = string.Empty;
        public string OpcNodePrefix { get; set; } = string.Empty; // e.g., "Device 1"
        public DeviceType DeviceType { get; set; } = DeviceType.Press; // Default to Press
        public bool Enabled { get; set; } = true;
        public TimeSpan SamplingInterval { get; set; } = TimeSpan.FromSeconds(2);
        public NodeConfiguration[] Nodes { get; set; } = Array.Empty<NodeConfiguration>();
    }

    public class NodeConfiguration
    {
        public string NodeType { get; set; } = string.Empty;
        public string NodeId { get; set; } = string.Empty;
        public string TransmissionType { get; set; } = string.Empty;
        public bool IsWritable { get; set; } = false;
        public string DataType { get; set; } = string.Empty;
    }

    public class GlobalSettings
    {
        public string NodeNamespace { get; set; } = "ns=2;s=";
        public TimeSpan DefaultReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);
        public int MaxReconnectAttempts { get; set; } = 10;
        public bool EnableDetailedLogging { get; set; } = true;
        public string ServiceBusConnectionString { get; set; } = string.Empty;
        public bool EnableCriticalAlerts { get; set; } = true;
    }

    // Runtime models
    public class DeviceMapping
    {
        public string DeviceId { get; set; } = string.Empty;
        public string LineId { get; set; } = string.Empty;
        public string LineName { get; set; } = string.Empty;
        public string OpcServerUrl { get; set; } = string.Empty;
        public string OpcNodePrefix { get; set; } = string.Empty;
        public DeviceType DeviceType { get; set; } = DeviceType.Press;
        public TimeSpan SamplingInterval { get; set; }
        public bool Enabled { get; set; }
        public Dictionary<DataNodeType, StandardDataNode> StandardNodes { get; set; } = new();

    }

}