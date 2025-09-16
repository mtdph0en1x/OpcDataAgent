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
                        SamplingInterval = device.SamplingInterval,
                        Enabled = device.Enabled
                    };

                    // Auto-discover nodes if enabled
                    if (config.GlobalSettings.AutoDiscoverNodes)
                    {
                        deviceMapping.DiscoveredNodes = await DiscoverDeviceNodesAsync(
                            opcServer.Url,
                            device.OpcNodePrefix,
                            config.GlobalSettings);
                    }

                    deviceMappings.Add(deviceMapping);
                    _logger.LogInformation($"Created device mapping: {device.DeviceId} with {deviceMapping.DiscoveredNodes.Count} nodes");
                }
            }

            return deviceMappings;
        }

        private async Task<List<DiscoveredNode>> DiscoverDeviceNodesAsync(
            string opcServerUrl,
            string nodePrefix,
            GlobalSettings globalSettings)
        {
            var discoveredNodes = new List<DiscoveredNode>();
            OpcClient? opcClient = null;

            try
            {
                opcClient = new OpcClient(opcServerUrl);
                opcClient.Connect();

                _logger.LogDebug($"Discovering nodes for device prefix: {nodePrefix}");

                // Try each standard node name
                foreach (var nodeName in globalSettings.StandardNodeNames)
                {
                    var nodeId = $"{globalSettings.NodeNamespace}{nodePrefix}/{nodeName}";

                    try
                    {
                        // Test if node exists by trying to read it
                        var readNode = new OpcReadNode(nodeId);
                        var result = opcClient.ReadNode(readNode);

                        if (result.Status.IsGood)
                        {
                            var discoveredNode = new DiscoveredNode
                            {
                                NodeName = nodeName,
                                NodeId = nodeId,
                                DataType = InferDataType(result.Value),
                                LastValue = result.Value,
                                LastRead = DateTime.UtcNow
                            };

                            discoveredNodes.Add(discoveredNode);
                            _logger.LogDebug($"Discovered node: {nodeId} ({discoveredNode.DataType})");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug($"Node {nodeId} not available: {ex.Message}");
                        // Node doesn't exist, skip it
                    }
                }

                _logger.LogInformation($"Discovered {discoveredNodes.Count} nodes for device {nodePrefix}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error discovering nodes for {nodePrefix}: {ex.Message}");
            }
            finally
            {
                opcClient?.Disconnect();
                opcClient?.Dispose();
            }

            return discoveredNodes;
        }

        private string InferDataType(object? value)
        {
            return value switch
            {
                double or float => "double",
                int or short or long => "int",
                bool => "bool",
                string => "string",
                _ => "object"
            };
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