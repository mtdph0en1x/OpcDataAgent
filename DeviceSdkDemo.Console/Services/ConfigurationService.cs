using System.Text.Json;
using Microsoft.Extensions.Logging;
using AgentOPC.Console.Configuration;

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

        public List<DeviceMapping> ConvertToDeviceMappings(OpcConfiguration config)
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
                        DeviceName = device.DeviceName,
                        DeviceType = device.DeviceType,
                        LineId = line.LineId,
                        LineName = line.Name,
                        OpcServerUrl = opcServer.Url,
                        SamplingInterval = device.SamplingInterval,
                        Enabled = device.Enabled,
                        NodeMappings = ConvertSensorsToNodeMappings(device, config.GlobalSettings)
                    };

                    deviceMappings.Add(deviceMapping);
                    _logger.LogDebug($"Created device mapping: {device.DeviceId} on {line.LineId}");
                }
            }

            _logger.LogInformation($"Converted {deviceMappings.Count} device mappings from configuration");
            return deviceMappings;
        }

        private NodeMapping[] ConvertSensorsToNodeMappings(DeviceConfiguration device, GlobalSettings globalSettings)
        {
            return device.Sensors.Select(sensor => new NodeMapping
            {
                SensorName = sensor.SensorName,
                NodeId = $"{globalSettings.NodeNamespace}{device.NodePrefix}/{sensor.NodePath}",
                DataType = sensor.DataType,
                Unit = sensor.Unit,
                WarningThreshold = sensor.AlertThresholds?.Warning,
                CriticalThreshold = sensor.AlertThresholds?.Critical,
                AlertDirection = sensor.AlertDirection
            }).ToArray();
        }

        private void ValidateConfiguration(OpcConfiguration config)
        {
            // Validate OPC servers
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

            // Validate production lines
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