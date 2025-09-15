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
        public DeviceConfiguration[] Devices { get; set; } = Array.Empty<DeviceConfiguration>();
    }

    public class DeviceConfiguration
    {
        public string DeviceId { get; set; } = string.Empty;
        public string DeviceName { get; set; } = string.Empty;
        public string DeviceType { get; set; } = string.Empty;
        public bool Enabled { get; set; } = true;
        public TimeSpan SamplingInterval { get; set; } = TimeSpan.FromSeconds(2);
        public string NodePrefix { get; set; } = string.Empty;
        public Sensor[] Sensors { get; set; } = Array.Empty<Sensor>();
    }

    public class Sensor
    {
        public string SensorName { get; set; } = string.Empty;
        public string NodePath { get; set; } = string.Empty;
        public string DataType { get; set; } = "string";
        public string Unit { get; set; } = string.Empty;
        public AlertThresholds? AlertThresholds { get; set; }
        public string AlertDirection { get; set; } = "above"; // "above" or "below"
    }

    public class AlertThresholds
    {
        public double? Warning { get; set; }
        public double? Critical { get; set; }
    }

    public class GlobalSettings
    {
        public string NodeNamespace { get; set; } = "ns=2;s=";
        public TimeSpan DefaultReconnectDelay { get; set; } = TimeSpan.FromSeconds(5);
        public int MaxReconnectAttempts { get; set; } = 10;
        public bool EnableDetailedLogging { get; set; } = true;
    }

    // Runtime models (converted from configuration)
    public class DeviceMapping
    {
        public string DeviceId { get; set; } = string.Empty;
        public string DeviceName { get; set; } = string.Empty;
        public string DeviceType { get; set; } = string.Empty;
        public string LineId { get; set; } = string.Empty;
        public string LineName { get; set; } = string.Empty;
        public string OpcServerUrl { get; set; } = string.Empty;
        public TimeSpan SamplingInterval { get; set; }
        public bool Enabled { get; set; }
        public NodeMapping[] NodeMappings { get; set; } = Array.Empty<NodeMapping>();
    }

    public class NodeMapping
    {
        public string SensorName { get; set; } = string.Empty;
        public string NodeId { get; set; } = string.Empty;
        public string DataType { get; set; } = "string";
        public string Unit { get; set; } = string.Empty;
        public double? WarningThreshold { get; set; }
        public double? CriticalThreshold { get; set; }
        public string AlertDirection { get; set; } = "above";

        // Helper properties
        public bool HasAlerts => WarningThreshold.HasValue || CriticalThreshold.HasValue;
        public bool IsNumeric => DataType == "double" || DataType == "int" || DataType == "float";
    }
}