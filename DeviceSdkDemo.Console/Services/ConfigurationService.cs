using System.Text.Json;
using Microsoft.Extensions.Logging;
using AgentOPC.Console.Configuration;
using Opc.UaFx.Client;
using Opc.UaFx;

namespace AgentOPC.Console.Services
{
    public class ConfigurationService
    {
        private readonly ILogger<ConfigurationService> _logger;
        private OpcConfiguration? _configuration;

        public ConfigurationService(ILogger<ConfigurationService> logger)
        {
            _logger = logger;
        }

        public async Task<OpcConfiguration> LoadConfigurationAsync(string configFilePath = "device-mappings.json")
        {
            try
            {
                if (!File.Exists(configFilePath))
                {
                    _logger.LogError($"Configuration file not found: {configFilePath}");
                    throw new FileNotFoundException($"Configuration file not found: {configFilePath}");
                }

                var jsonString = await File.ReadAllTextAsync(configFilePath);
                var options = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                    ReadCommentHandling = JsonCommentHandling.Skip,
                    AllowTrailingCommas = true
                };

                _configuration = JsonSerializer.Deserialize<OpcConfiguration>(jsonString, options);

                if (_configuration == null)
                {
                    throw new InvalidOperationException("Failed to deserialize configuration");
                }

                ValidateConfiguration(_configuration);
                _logger.LogInformation($"Loaded configuration with {_configuration.ProductionLines.Length} production lines");

                return _configuration;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error loading configuration: {ex.Message}");
                throw;
            }
        }

        public async Task<List<DeviceMapping>> ConvertToDeviceMappingsAsync(OpcConfiguration config)
        {
            var deviceMappings = new List<DeviceMapping>();
            var opcServers = config.OpcServers.ToDictionary(s => s.Name, s => s);

            foreach (var line in config.ProductionLines)
            {
                if (!opcServers.TryGetValue(line.OpcServerName, out var opcServer))
                {
                    _logger.LogWarning($"OPC server '{line.OpcServerName}' not found for line {line.LineId}");
                    continue;
                }

                foreach (var device in line.Devices.Where(d => d.Enabled))
                {
                    var deviceMapping = new DeviceMapping
                    {
                        DeviceId = device.DeviceId,
                        LineId = line.LineId,
                        LineName = line.Name,
                        OpcServerUrl = opcServer.Url,
                        OpcNodePrefix = device.OpcNodePrefix,
                        DeviceType = device.DeviceType,
                        SamplingInterval = device.SamplingInterval,
                        Enabled = device.Enabled
                    };

                    // Initialize nodes from configuration
                    deviceMapping.StandardNodes = CreateStandardNodesFromConfig(device.Nodes);
                    _logger.LogInformation($"Created {deviceMapping.StandardNodes.Count} nodes for {device.DeviceId} ({device.DeviceType})");

                    deviceMappings.Add(deviceMapping);
                    _logger.LogInformation($"Created device mapping: {device.DeviceId}");
                }
            }

            return deviceMappings;
        }

        private Dictionary<DataNodeType, StandardDataNode> CreateStandardNodesFromConfig(NodeConfiguration[] nodeConfigs)
        {
            var standardNodes = new Dictionary<DataNodeType, StandardDataNode>();

            foreach (var nodeConfig in nodeConfigs)
            {
                if (Enum.TryParse<DataNodeType>(nodeConfig.NodeType, true, out var nodeType) &&
                    Enum.TryParse<DataTransmissionType>(nodeConfig.TransmissionType, true, out var transmissionType))
                {
                    var standardNode = new StandardDataNode
                    {
                        NodeType = nodeType,
                        NodeId = nodeConfig.NodeId,
                        NodeName = nodeConfig.NodeType,
                        TransmissionType = transmissionType,
                        IsWritable = nodeConfig.IsWritable,
                        DataType = nodeConfig.DataType
                    };

                    standardNodes[nodeType] = standardNode;
                }
                else
                {
                    _logger.LogWarning($"Invalid node configuration: NodeType='{nodeConfig.NodeType}', TransmissionType='{nodeConfig.TransmissionType}'");
                }
            }

            return standardNodes;
        }


        private void ValidateConfiguration(OpcConfiguration config)
        {
            // Simplified validation - only check what's actually needed
            if (!config.OpcServers.Any())
            {
                throw new InvalidOperationException("No OPC servers defined in configuration");
            }

            foreach (var server in config.OpcServers)
            {
                if (string.IsNullOrEmpty(server.Name))
                    throw new InvalidOperationException("OPC server name cannot be empty");
                if (string.IsNullOrEmpty(server.Url))
                    throw new InvalidOperationException($"OPC server URL cannot be empty for server '{server.Name}'");
            }

            if (!config.ProductionLines.Any())
            {
                throw new InvalidOperationException("No production lines defined in configuration");
            }

            var serverNames = config.OpcServers.Select(s => s.Name).ToHashSet();
            var lineIds = new HashSet<string>();
            var deviceIds = new HashSet<string>();

            foreach (var line in config.ProductionLines)
            {
                if (string.IsNullOrEmpty(line.LineId))
                    throw new InvalidOperationException("Production line ID cannot be empty");

                if (!lineIds.Add(line.LineId))
                    throw new InvalidOperationException($"Duplicate production line ID: {line.LineId}");

                if (!serverNames.Contains(line.OpcServerName))
                    throw new InvalidOperationException($"Invalid OPC server reference '{line.OpcServerName}' in line {line.LineId}");

                foreach (var device in line.Devices)
                {
                    if (string.IsNullOrEmpty(device.DeviceId))
                        throw new InvalidOperationException($"Device ID cannot be empty in line {line.LineId}");

                    if (!deviceIds.Add(device.DeviceId))
                        throw new InvalidOperationException($"Duplicate device ID: {device.DeviceId}");
                }
            }

            _logger.LogInformation("Configuration validation passed");
        }
    }
}