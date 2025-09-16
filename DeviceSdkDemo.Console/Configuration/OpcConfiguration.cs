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
        public bool ValidateNodesOnStartup { get; set; } = true;
        public string[] DisabledNodes { get; set; } = Array.Empty<string>(); // Node names to skip
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
        public Dictionary<DataNodeType, StandardDataNode> StandardNodes { get; set; } = new();

        public static Dictionary<DataNodeType, StandardDataNode> CreateStandardNodes(string opcNodePrefix)
        {
            return new Dictionary<DataNodeType, StandardDataNode>
            {
                [DataNodeType.ProductionStatus] = new StandardDataNode
                {
                    NodeType = DataNodeType.ProductionStatus,
                    NodeId = $"ns=2;s={opcNodePrefix}/ProductionStatus",
                    NodeName = "ProductionStatus",
                    TransmissionType = DataTransmissionType.Telemetry,
                    IsWritable = false,
                    DataType = "int"
                },
                [DataNodeType.WorkorderId] = new StandardDataNode
                {
                    NodeType = DataNodeType.WorkorderId,
                    NodeId = $"ns=2;s={opcNodePrefix}/WorkorderId",
                    NodeName = "WorkorderId",
                    TransmissionType = DataTransmissionType.Telemetry,
                    IsWritable = false,
                    DataType = "string"
                },
                [DataNodeType.ProductionRate] = new StandardDataNode
                {
                    NodeType = DataNodeType.ProductionRate,
                    NodeId = $"ns=2;s={opcNodePrefix}/ProductionRate",
                    NodeName = "ProductionRate",
                    TransmissionType = DataTransmissionType.ReportedState,
                    IsWritable = true,
                    DataType = "double"
                },
                [DataNodeType.GoodCount] = new StandardDataNode
                {
                    NodeType = DataNodeType.GoodCount,
                    NodeId = $"ns=2;s={opcNodePrefix}/GoodCount",
                    NodeName = "GoodCount",
                    TransmissionType = DataTransmissionType.Telemetry,
                    IsWritable = false,
                    DataType = "int"
                },
                [DataNodeType.BadCount] = new StandardDataNode
                {
                    NodeType = DataNodeType.BadCount,
                    NodeId = $"ns=2;s={opcNodePrefix}/BadCount",
                    NodeName = "BadCount",
                    TransmissionType = DataTransmissionType.Telemetry,
                    IsWritable = false,
                    DataType = "int"
                },
                [DataNodeType.Temperature] = new StandardDataNode
                {
                    NodeType = DataNodeType.Temperature,
                    NodeId = $"ns=2;s={opcNodePrefix}/Temperature",
                    NodeName = "Temperature",
                    TransmissionType = DataTransmissionType.Telemetry,
                    IsWritable = false,
                    DataType = "double"
                },
                [DataNodeType.DeviceErrors] = new StandardDataNode
                {
                    NodeType = DataNodeType.DeviceErrors,
                    NodeId = $"ns=2;s={opcNodePrefix}/DeviceError",
                    NodeName = "DeviceError",
                    TransmissionType = DataTransmissionType.ReportedState, // Changed to ReportedState, events handled separately
                    IsWritable = false,
                    DataType = "int"
                }
            };
        }
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