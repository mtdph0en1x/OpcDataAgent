using System.Text.Json.Serialization;

namespace AgentOPC.Console.Configuration
{
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
        public bool Enabled { get; set; } = true;
        public TimeSpan SamplingInterval { get; set; } = TimeSpan.FromSeconds(2);
    }

    public class GlobalSettings
    {
        public string NodeNamespace { get; set; } = "ns=2;s=";
        public TimeSpan DefaultReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);
        public int MaxReconnectAttempts { get; set; } = 10;
        public bool EnableDetailedLogging { get; set; } = true;
        public bool AutoDiscoverNodes { get; set; } = true;
        public string[] StandardNodeNames { get; set; } = Array.Empty<string>();
    }

    // Runtime models
    public class DeviceMapping
    {
        public string DeviceId { get; set; } = string.Empty;
        public string LineId { get; set; } = string.Empty;
        public string LineName { get; set; } = string.Empty;
        public string OpcServerUrl { get; set; } = string.Empty;
        public string OpcNodePrefix { get; set; } = string.Empty;
        public TimeSpan SamplingInterval { get; set; }
        public bool Enabled { get; set; }
        public List<DiscoveredNode> DiscoveredNodes { get; set; } = new();
    }

    public class DiscoveredNode
    {
        public string NodeName { get; set; } = string.Empty;
        public string NodeId { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public object? LastValue { get; set; }
        public DateTime LastRead { get; set; }
    }
}