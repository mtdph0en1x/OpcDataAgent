using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Azure.Messaging.ServiceBus;
using Opc.UaFx.Client;
using Newtonsoft.Json;
using Opc.UaFx;
using AgentOPC.Console.Configuration;
using AgentOPC.Console.Services;
using Microsoft.Azure.Devices.Client;
using System.Text;
using Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Shared;
using DeviceClientMessage = Microsoft.Azure.Devices.Client.Message;
using DeviceClientTransportType = Microsoft.Azure.Devices.Client.TransportType;

namespace AgentOPC.Console
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            // Build configuration
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            // Build host
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    services.AddSingleton<IConfiguration>(config);
                    services.AddSingleton(provider =>
                        new ServiceBusClient(config.GetConnectionString("ServiceBus")));
                    services.AddSingleton<ConfigurationService>();
                    services.AddHostedService<OpcDataCollectionService>();
                })
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                    logging.SetMinimumLevel(LogLevel.Information);
                })
                .Build();

            System.Console.WriteLine("=== OPC Data Agent Starting ===");
            System.Console.WriteLine("Configuration-driven OPC UA to IoT Hub + Service Bus bridge");
            System.Console.WriteLine();

            try
            {
                await host.RunAsync();
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Fatal error: {ex.Message}");
                Environment.Exit(1);
            }
        }
    }

    public class OpcDataCollectionService : BackgroundService
    {
        private readonly ILogger<OpcDataCollectionService> _logger;
        private readonly ServiceBusClient _serviceBusClient;
        private readonly ConfigurationService _configService;
        private readonly IConfiguration _configuration;
        private readonly Dictionary<string, OpcClient> _opcClients;
        private readonly Dictionary<string, DeviceClient> _deviceClients;
        private List<DeviceMapping> _deviceMappings = new();

        private readonly string _deviceDataQueue;
        private readonly string _deviceAlertsQueue;

        public OpcDataCollectionService(
            ILogger<OpcDataCollectionService> logger,
            ServiceBusClient serviceBusClient,
            ConfigurationService configService,
            IConfiguration configuration)
        {
            _logger = logger;
            _serviceBusClient = serviceBusClient;
            _configService = configService;
            _configuration = configuration;
            _opcClients = new Dictionary<string, OpcClient>();
            _deviceClients = new Dictionary<string, DeviceClient>(); // Only initialize once

            _deviceDataQueue = _configuration["ServiceBusQueues:DeviceData"] ?? "device-data";
            _deviceAlertsQueue = _configuration["ServiceBusQueues:DeviceAlerts"] ?? "device-alerts";

            _logger.LogInformation($"Using IoT Hub for telemetry");
            _logger.LogInformation($"Using Service Bus queues - Data: {_deviceDataQueue}, Alerts: {_deviceAlertsQueue}");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await InitializeAsync();

            // Start monitoring tasks for each device
            var tasks = _deviceMappings
                .Where(m => m.Enabled)
                .Select(mapping => MonitorDeviceAsync(mapping, stoppingToken))
                .ToList();

            _logger.LogInformation($"Starting {tasks.Count} device monitoring tasks");
            await Task.WhenAll(tasks);
        }

        private async Task InitializeAsync()
        {
            try
            {
                // Load configuration from file
                var opcConfig = await _configService.LoadConfigurationAsync();

                // Convert to device mappings WITH auto-discovery
                _deviceMappings = await _configService.ConvertToDeviceMappingsAsync(opcConfig);

                // Initialize OPC connections
                await InitializeOpcConnectionsAsync(opcConfig.OpcServers);

                // Initialize IoT Hub connections
                await InitializeIoTHubAsync();

                _logger.LogInformation($"Initialization complete:");
                _logger.LogInformation($"  - {opcConfig.OpcServers.Length} OPC servers");
                _logger.LogInformation($"  - {opcConfig.ProductionLines.Length} production lines");
                _logger.LogInformation($"  - {_deviceMappings.Count} enabled devices");
                _logger.LogInformation($"  - {_deviceMappings.Sum(d => d.DiscoveredNodes.Count)} discovered nodes");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Initialization failed: {ex.Message}");
                throw;
            }
        }

        private async Task InitializeOpcConnectionsAsync(OpcServerConfiguration[] opcServers)
        {
            foreach (var serverConfig in opcServers)
            {
                OpcClient? opcClient = null;
                try
                {
                    opcClient = new OpcClient(serverConfig.Url)
                    {
                        SessionTimeout = serverConfig.SessionTimeout
                    };

                    await Task.Run(() => opcClient.Connect());
                    _opcClients[serverConfig.Url] = opcClient;

                    _logger.LogInformation($"Connected to OPC Server: {serverConfig.Name} ({serverConfig.Url})");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Failed to connect to OPC Server {serverConfig.Name}: {ex.Message}");
                    opcClient?.Dispose();
                }
            }
        }

        private async Task InitializeIoTHubAsync()
        {
            try
            {
                var serviceConnectionString = _configuration.GetConnectionString("IoTHubService");
                var registryManager = RegistryManager.CreateFromConnectionString(serviceConnectionString);

                foreach (var deviceMapping in _deviceMappings.Where(d => d.Enabled))
                {
                    try
                    {
                        // Try to get existing device, or create if it doesn't exist
                        var device = await registryManager.GetDeviceAsync(deviceMapping.DeviceId);
                        if (device == null)
                        {
                            _logger.LogInformation($"Creating IoT Hub device: {deviceMapping.DeviceId}");
                            device = await registryManager.AddDeviceAsync(new Microsoft.Azure.Devices.Device(deviceMapping.DeviceId));
                        }

                        // Build connection string for this device
                        var hostName = GetHostNameFromConnectionString(serviceConnectionString);
                        var deviceConnectionString = $"HostName={hostName};DeviceId={deviceMapping.DeviceId};SharedAccessKey={device.Authentication.SymmetricKey.PrimaryKey}";

                        var deviceClient = DeviceClient.CreateFromConnectionString(deviceConnectionString, DeviceClientTransportType.Mqtt);
                        await deviceClient.OpenAsync();

                        // Setup Direct Methods and Device Twin
                        await SetupDeviceClientHandlersAsync(deviceClient, deviceMapping);

                        _deviceClients[deviceMapping.DeviceId] = deviceClient;

                        _logger.LogInformation($"Connected to IoT Hub: {deviceMapping.DeviceId}");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Failed to initialize device {deviceMapping.DeviceId}: {ex.Message}");
                        // Continue with other devices
                    }
                }

                await registryManager.CloseAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to initialize IoT Hub connections: {ex.Message}");
                throw;
            }
        }

        private string GetHostNameFromConnectionString(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentException("Connection string cannot be null or empty", nameof(connectionString));

            var hostNamePart = connectionString.Split(';')
                .FirstOrDefault(part => part.StartsWith("HostName="));
            
            if (hostNamePart == null)
                throw new ArgumentException("Connection string does not contain HostName", nameof(connectionString));

            var hostNameParts = hostNamePart.Split('=');
            if (hostNameParts.Length < 2)
                throw new ArgumentException("Invalid HostName format in connection string", nameof(connectionString));

            return hostNameParts[1];
        }

        private async Task SetupDeviceClientHandlersAsync(DeviceClient deviceClient, DeviceMapping deviceMapping)
        {
            try
            {
                // Setup Direct Methods
                await deviceClient.SetMethodHandlerAsync("GetDeviceStatus", GetDeviceStatusHandler, deviceMapping);
                await deviceClient.SetMethodHandlerAsync("RestartDevice", RestartDeviceHandler, deviceMapping);
                await deviceClient.SetMethodHandlerAsync("UpdateSamplingInterval", UpdateSamplingIntervalHandler, deviceMapping);
                await deviceClient.SetMethodHandlerAsync("GetLastValues", GetLastValuesHandler, deviceMapping);
                await deviceClient.SetMethodHandlerAsync("EmergencyStop", EmergencyStopHandler, deviceMapping);
                await deviceClient.SetMethodHandlerAsync("ResetErrorStatus", ResetErrorStatusHandler, deviceMapping);

                // Setup Device Twin desired properties callback
                await deviceClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertyChangedAsync, deviceMapping);

                // Get current device twin and update reported properties
                var twin = await deviceClient.GetTwinAsync();
                await UpdateReportedPropertiesAsync(deviceClient, deviceMapping);

                _logger.LogInformation($"Device handlers setup complete for {deviceMapping.DeviceId}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to setup device handlers for {deviceMapping.DeviceId}: {ex.Message}");
            }
        }

        // Direct Method Handlers
        private async Task<MethodResponse> GetDeviceStatusHandler(MethodRequest methodRequest, object userContext)
        {
            var deviceMapping = (DeviceMapping)userContext;
            _logger.LogInformation($"GetDeviceStatus method called for device {deviceMapping.DeviceId}");

            var status = new
            {
                DeviceId = deviceMapping.DeviceId,
                LineName = deviceMapping.LineName,
                IsEnabled = deviceMapping.Enabled,
                SamplingInterval = deviceMapping.SamplingInterval.TotalSeconds,
                LastDataCount = deviceMapping.DiscoveredNodes.Count,
                LastUpdateTime = deviceMapping.DiscoveredNodes.Any() ? 
                    deviceMapping.DiscoveredNodes.Max(n => n.LastRead) : DateTime.MinValue,
                Status = "Online"
            };

            var response = JsonConvert.SerializeObject(status);
            return new MethodResponse(Encoding.UTF8.GetBytes(response), 200);
        }

        private async Task<MethodResponse> RestartDeviceHandler(MethodRequest methodRequest, object userContext)
        {
            var deviceMapping = (DeviceMapping)userContext;
            _logger.LogInformation($"RestartDevice method called for device {deviceMapping.DeviceId}");

            try
            {
                // Simulate restart by resetting last read times
                foreach (var node in deviceMapping.DiscoveredNodes)
                {
                    node.LastRead = DateTime.MinValue;
                    node.LastValue = null;
                }

                var response = JsonConvert.SerializeObject(new { 
                    message = $"Device {deviceMapping.DeviceId} restarted successfully",
                    timestamp = DateTime.UtcNow 
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(response), 200);
            }
            catch (Exception ex)
            {
                var errorResponse = JsonConvert.SerializeObject(new { 
                    error = ex.Message,
                    timestamp = DateTime.UtcNow 
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(errorResponse), 500);
            }
        }

        private async Task<MethodResponse> UpdateSamplingIntervalHandler(MethodRequest methodRequest, object userContext)
        {
            var deviceMapping = (DeviceMapping)userContext;
            _logger.LogInformation($"UpdateSamplingInterval method called for device {deviceMapping.DeviceId}");

            try
            {
                var payload = JsonConvert.DeserializeObject<dynamic>(methodRequest.DataAsJson);
                var newInterval = (int)payload.samplingIntervalSeconds;
                
                deviceMapping.SamplingInterval = TimeSpan.FromSeconds(newInterval);
                
                var response = JsonConvert.SerializeObject(new { 
                    message = $"Sampling interval updated to {newInterval} seconds",
                    newInterval = newInterval,
                    timestamp = DateTime.UtcNow 
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(response), 200);
            }
            catch (Exception ex)
            {
                var errorResponse = JsonConvert.SerializeObject(new { 
                    error = ex.Message,
                    timestamp = DateTime.UtcNow 
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(errorResponse), 400);
            }
        }

        private async Task<MethodResponse> GetLastValuesHandler(MethodRequest methodRequest, object userContext)
        {
            var deviceMapping = (DeviceMapping)userContext;
            _logger.LogInformation($"GetLastValues method called for device {deviceMapping.DeviceId}");

            var lastValues = deviceMapping.DiscoveredNodes.ToDictionary(
                node => node.NodeName,
                node => new { 
                    value = node.LastValue,
                    lastRead = node.LastRead,
                    dataType = node.DataType 
                }
            );

            var response = JsonConvert.SerializeObject(new {
                deviceId = deviceMapping.DeviceId,
                lastValues = lastValues,
                timestamp = DateTime.UtcNow
            });
            return new MethodResponse(Encoding.UTF8.GetBytes(response), 200);
        }

        private async Task<MethodResponse> EmergencyStopHandler(MethodRequest methodRequest, object userContext)
        {
            var deviceMapping = (DeviceMapping)userContext;
            _logger.LogInformation($"EmergencyStop method called for device {deviceMapping.DeviceId}");

            try
            {
                // Get the OPC client for this device
                if (!_opcClients.TryGetValue(deviceMapping.OpcServerUrl, out var opcClient))
                {
                    var errorResponse = JsonConvert.SerializeObject(new {
                        error = "OPC client not found",
                        timestamp = DateTime.UtcNow
                    });
                    return new MethodResponse(Encoding.UTF8.GetBytes(errorResponse), 404);
                }

                // Build the method node IDs based on the device prefix
                string methodNodeId = $"ns=2;s={deviceMapping.OpcNodePrefix}/EmergencyStop";
                string objectNodeId = $"ns=2;s={deviceMapping.OpcNodePrefix}";

                // Call the OPC method
                opcClient.CallMethod(objectNodeId, methodNodeId);

                _logger.LogInformation($"Emergency Stop executed successfully for device {deviceMapping.DeviceId}");

                var response = JsonConvert.SerializeObject(new {
                    message = $"Emergency Stop executed successfully for device {deviceMapping.DeviceId}",
                    timestamp = DateTime.UtcNow
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(response), 200);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error executing Emergency Stop for device {deviceMapping.DeviceId}: {ex.Message}");
                var errorResponse = JsonConvert.SerializeObject(new {
                    error = ex.Message,
                    timestamp = DateTime.UtcNow
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(errorResponse), 500);
            }
        }

        private async Task<MethodResponse> ResetErrorStatusHandler(MethodRequest methodRequest, object userContext)
        {
            var deviceMapping = (DeviceMapping)userContext;
            _logger.LogInformation($"ResetErrorStatus method called for device {deviceMapping.DeviceId}");

            try
            {
                // Get the OPC client for this device
                if (!_opcClients.TryGetValue(deviceMapping.OpcServerUrl, out var opcClient))
                {
                    var errorResponse = JsonConvert.SerializeObject(new {
                        error = "OPC client not found",
                        timestamp = DateTime.UtcNow
                    });
                    return new MethodResponse(Encoding.UTF8.GetBytes(errorResponse), 404);
                }

                // Build the method node IDs based on the device prefix
                string methodNodeId = $"ns=2;s={deviceMapping.OpcNodePrefix}/ResetErrorStatus";
                string objectNodeId = $"ns=2;s={deviceMapping.OpcNodePrefix}";

                // Call the OPC method
                opcClient.CallMethod(objectNodeId, methodNodeId);

                _logger.LogInformation($"Error status reset successfully for device {deviceMapping.DeviceId}");

                var response = JsonConvert.SerializeObject(new {
                    message = $"Error status reset successfully for device {deviceMapping.DeviceId}",
                    timestamp = DateTime.UtcNow
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(response), 200);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error resetting status for device {deviceMapping.DeviceId}: {ex.Message}");
                var errorResponse = JsonConvert.SerializeObject(new {
                    error = ex.Message,
                    timestamp = DateTime.UtcNow
                });
                return new MethodResponse(Encoding.UTF8.GetBytes(errorResponse), 500);
            }
        }

        // Device Twin Handlers
        private async Task OnDesiredPropertyChangedAsync(TwinCollection desiredProperties, object userContext)
        {
            var deviceMapping = (DeviceMapping)userContext;
            _logger.LogInformation($"Desired property changed for device {deviceMapping.DeviceId}");

            try
            {
                // Basic operational controls
                if (desiredProperties.Contains("samplingInterval"))
                {
                    var newInterval = (int)desiredProperties["samplingInterval"];
                    deviceMapping.SamplingInterval = TimeSpan.FromSeconds(newInterval);
                    _logger.LogInformation($"Updated sampling interval to {newInterval} seconds for device {deviceMapping.DeviceId}");
                }

                if (desiredProperties.Contains("enabled"))
                {
                    deviceMapping.Enabled = (bool)desiredProperties["enabled"];
                    _logger.LogInformation($"Updated enabled status to {deviceMapping.Enabled} for device {deviceMapping.DeviceId}");
                }

                // Operational mode changes
                if (desiredProperties.Contains("operationalMode"))
                {
                    var mode = (string)desiredProperties["operationalMode"];
                    _logger.LogInformation($"Operational mode changed to {mode} for device {deviceMapping.DeviceId}");
                    
                    // Adjust sampling based on mode
                    switch (mode.ToLower())
                    {
                        case "maintenance":
                            deviceMapping.SamplingInterval = TimeSpan.FromSeconds(30); // Slower sampling
                            break;
                        case "critical":
                            deviceMapping.SamplingInterval = TimeSpan.FromSeconds(1); // Fast sampling
                            break;
                        case "automatic":
                        default:
                            // Keep current interval
                            break;
                    }
                }

                // Configuration updates (ASA/Functions will handle thresholds and alerts)
                if (desiredProperties.Contains("qualityThresholds"))
                {
                    _logger.LogInformation($"Quality thresholds updated for device {deviceMapping.DeviceId}");
                }

                if (desiredProperties.Contains("targetProductionRate"))
                {
                    var rate = (int)desiredProperties["targetProductionRate"];
                    _logger.LogInformation($"Target production rate set to {rate} units/hour for device {deviceMapping.DeviceId}");
                }

                if (desiredProperties.Contains("productionSchedule"))
                {
                    _logger.LogInformation($"Production schedule updated for device {deviceMapping.DeviceId}");
                }

                // Update reported properties to acknowledge the change
                if (_deviceClients.TryGetValue(deviceMapping.DeviceId, out var deviceClient))
                {
                    await UpdateReportedPropertiesAsync(deviceClient, deviceMapping);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing desired property change for device {deviceMapping.DeviceId}: {ex.Message}");
            }
        }

        private async Task UpdateReportedPropertiesAsync(DeviceClient deviceClient, DeviceMapping deviceMapping)
        {
            try
            {
                var reportedProperties = new TwinCollection();
                
                // Basic device info
                reportedProperties["samplingInterval"] = deviceMapping.SamplingInterval.TotalSeconds;
                reportedProperties["enabled"] = deviceMapping.Enabled;
                reportedProperties["lineId"] = deviceMapping.LineId;
                reportedProperties["lineName"] = deviceMapping.LineName;
                reportedProperties["nodeCount"] = deviceMapping.DiscoveredNodes.Count;
                reportedProperties["lastUpdateTime"] = DateTime.UtcNow;
                reportedProperties["agentVersion"] = "2.1";

                // Operational status
                reportedProperties["status"] = new
                {
                    state = deviceMapping.Enabled ? "running" : "stopped",
                    health = "healthy", // Can be derived from OPC connection status
                    uptime = DateTime.UtcNow.Subtract(DateTime.Today).TotalHours, // Hours since midnight
                    connectionStatus = "connected"
                };

                // Current production metrics (calculated from recent data)
                var recentNodes = deviceMapping.DiscoveredNodes.Where(n => n.LastRead > DateTime.UtcNow.AddMinutes(-5));
                reportedProperties["currentMetrics"] = new
                {
                    activeNodeCount = recentNodes.Count(),
                    dataQuality = recentNodes.Any() ? (double)recentNodes.Count() / deviceMapping.DiscoveredNodes.Count : 0.0,
                    lastDataReceived = deviceMapping.DiscoveredNodes.Any() ? 
                        deviceMapping.DiscoveredNodes.Max(n => n.LastRead) : DateTime.MinValue
                };

                // Performance indicators for Azure Functions
                reportedProperties["kpi"] = new
                {
                    efficiency = CalculateEfficiency(deviceMapping), // Custom calculation
                    availability = deviceMapping.Enabled ? 1.0 : 0.0,
                    quality = CalculateQuality(deviceMapping), // Custom calculation
                    oee = CalculateOEE(deviceMapping) // Overall Equipment Effectiveness
                };

                // Status tracking (for ASA/Functions to process)
                reportedProperties["operationalStatus"] = new
                {
                    lastReportTime = DateTime.UtcNow,
                    dataPoints = deviceMapping.DiscoveredNodes.Count,
                    samplingRate = deviceMapping.SamplingInterval.TotalSeconds
                };

                // Configuration acknowledgment
                reportedProperties["configurationStatus"] = new
                {
                    lastConfigUpdate = DateTime.UtcNow,
                    pendingChanges = false,
                    configVersion = "1.0"
                };

                await deviceClient.UpdateReportedPropertiesAsync(reportedProperties);
                _logger.LogDebug($"Updated reported properties for device {deviceMapping.DeviceId}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to update reported properties for device {deviceMapping.DeviceId}: {ex.Message}");
            }
        }

        // Helper methods for KPI calculations
        private double CalculateEfficiency(DeviceMapping deviceMapping)
        {
            // Example: percentage of nodes successfully read in last hour
            var oneHourAgo = DateTime.UtcNow.AddHours(-1);
            var recentReads = deviceMapping.DiscoveredNodes.Count(n => n.LastRead > oneHourAgo);
            return deviceMapping.DiscoveredNodes.Count > 0 ? (double)recentReads / deviceMapping.DiscoveredNodes.Count : 0.0;
        }

        private double CalculateQuality(DeviceMapping deviceMapping)
        {
            // Example: percentage of valid (non-null) readings
            var validValues = deviceMapping.DiscoveredNodes.Count(n => n.LastValue != null);
            return deviceMapping.DiscoveredNodes.Count > 0 ? (double)validValues / deviceMapping.DiscoveredNodes.Count : 0.0;
        }

        private double CalculateOEE(DeviceMapping deviceMapping)
        {
            // Overall Equipment Effectiveness = Availability × Performance × Quality
            var availability = deviceMapping.Enabled ? 1.0 : 0.0;
            var performance = CalculateEfficiency(deviceMapping);
            var quality = CalculateQuality(deviceMapping);
            return availability * performance * quality;
        }

        private async Task MonitorDeviceAsync(DeviceMapping deviceMapping, CancellationToken cancellationToken)
        {
            if (!_opcClients.TryGetValue(deviceMapping.OpcServerUrl, out var opcClient))
            {
                _logger.LogError($"No OPC client found for {deviceMapping.OpcServerUrl}");
                return;
            }

            // Always use IoT Hub for telemetry, Service Bus for alerts
            ITelemetrySender telemetrySender;
            if (_deviceClients.TryGetValue(deviceMapping.DeviceId, out var deviceClient))
            {
                telemetrySender = new IoTHubTelemetrySender(deviceClient, _logger);
            }
            else
            {
                // Fallback to Service Bus if IoT Hub connection failed
                telemetrySender = new ServiceBusTelemetrySender(_serviceBusClient.CreateSender(_deviceDataQueue), _logger);
                _logger.LogWarning($"Using Service Bus fallback for {deviceMapping.DeviceId}");
            }

            await using var alertSender = _serviceBusClient.CreateSender(_deviceAlertsQueue);

            _logger.LogInformation($"Starting monitoring: {deviceMapping.DeviceId} ({deviceMapping.LineName}) via {telemetrySender.GetType().Name}");

            try
            {
                while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Read all sensor nodes for this device
                    var readNodes = deviceMapping.DiscoveredNodes
                        .Select(n => new OpcReadNode(n.NodeId))
                        .ToArray();

                    var values = opcClient.ReadNodes(readNodes).ToArray();

                    // Build device data message
                    var deviceData = new DeviceDataMessage
                    {
                        DeviceId = deviceMapping.DeviceId,
                        DeviceName = deviceMapping.DeviceId, // Use DeviceId as name since DeviceName doesn't exist
                        DeviceType = "OPC", // Default device type since DeviceType doesn't exist in DeviceMapping
                        LineId = deviceMapping.LineId,
                        LineName = deviceMapping.LineName,
                        Timestamp = DateTime.UtcNow,
                        Data = new Dictionary<string, object>()
                    };

                    for (int i = 0; i < deviceMapping.DiscoveredNodes.Count && i < values.Length; i++)
                    {
                        var discoveredNode = deviceMapping.DiscoveredNodes[i];
                        var opcValue = values[i];

                        if (opcValue.Status.IsGood)
                        {
                            var value = ConvertValue(opcValue.Value, discoveredNode.DataType);
                            deviceData.Data[discoveredNode.NodeName] = value;

                            // Update the discovered node with latest value
                            discoveredNode.LastValue = value;
                            discoveredNode.LastRead = DateTime.UtcNow;

                            // Note: No alert checking here - that's handled by Azure Functions
                        }
                        else
                        {
                            _logger.LogWarning($"Bad OPC value for {deviceMapping.DeviceId}.{discoveredNode.NodeName}: {opcValue.Status}");
                            deviceData.Data[discoveredNode.NodeName] = null;
                        }
                    }

                    // Send device data to IoT Hub
                    await telemetrySender.SendTelemetryAsync(deviceData);

                    _logger.LogDebug($"Sent data for {deviceMapping.DeviceId}");
                    await Task.Delay(deviceMapping.SamplingInterval, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error monitoring {deviceMapping.DeviceId}: {ex.Message}");
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
            }
            }
            finally
            {
                await telemetrySender.DisposeAsync();
            }
        }

       

        
        private object ConvertValue(object value, string dataType)
        {
            if (value == null) return null;

            return dataType.ToLower() switch
            {
                "double" or "float" => Convert.ToDouble(value),
                "int" or "integer" => Convert.ToInt32(value),
                "bool" or "boolean" => Convert.ToBoolean(value),
                "string" => value.ToString(),
                _ => value
            };
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping OPC Data Agent...");

            // Close all IoT Hub device clients
            foreach (var deviceClient in _deviceClients.Values)
            {
                try
                {
                    await deviceClient.CloseAsync();
                    deviceClient.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error closing device client: {ex.Message}");
                }
            }

            // Close all OPC clients
            foreach (var opcClient in _opcClients.Values)
            {
                try
                {
                    opcClient.Disconnect();
                    opcClient.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error disconnecting OPC client: {ex.Message}");
                }
            }

            await base.StopAsync(cancellationToken);
        }
    }

    // Telemetry sender abstractions
    public interface ITelemetrySender : IAsyncDisposable
    {
        Task SendTelemetryAsync(DeviceDataMessage deviceData);
    }

    public class IoTHubTelemetrySender : ITelemetrySender
    {
        private readonly DeviceClient _deviceClient;
        private readonly ILogger _logger;

        public IoTHubTelemetrySender(DeviceClient deviceClient, ILogger logger)
        {
            _deviceClient = deviceClient;
            _logger = logger;
        }

        public async Task SendTelemetryAsync(DeviceDataMessage deviceData)
        {
            var messageBody = JsonConvert.SerializeObject(deviceData);
            var message = new DeviceClientMessage(Encoding.UTF8.GetBytes(messageBody));

            // Add properties for ASA routing
            message.Properties["deviceId"] = deviceData.DeviceId;
            message.Properties["lineId"] = deviceData.LineId;
            message.Properties["deviceType"] = deviceData.DeviceType;
            message.Properties["messageType"] = "telemetry";

            await _deviceClient.SendEventAsync(message);
            _logger.LogDebug($"Sent telemetry to IoT Hub: {deviceData.DeviceId}");
        }

        public async ValueTask DisposeAsync()
        {
            // Don't close the device client here - it's managed by the main service
        }
    }

    public class ServiceBusTelemetrySender : ITelemetrySender
    {
        private readonly ServiceBusSender _sender;
        private readonly ILogger _logger;

        public ServiceBusTelemetrySender(ServiceBusSender sender, ILogger logger)
        {
            _sender = sender;
            _logger = logger;
        }

        public async Task SendTelemetryAsync(DeviceDataMessage deviceData)
        {
            await _sender.SendMessageAsync(
                new ServiceBusMessage(JsonConvert.SerializeObject(deviceData))
                {
                    Subject = deviceData.LineId,
                    MessageId = Guid.NewGuid().ToString(),
                    ContentType = "application/json"
                });

            _logger.LogDebug($"Sent telemetry to Service Bus: {deviceData.DeviceId}");
        }

        public async ValueTask DisposeAsync()
        {
            await _sender.DisposeAsync();
        }
    }

    // Enhanced message classes
    public class DeviceDataMessage
    {
        public string DeviceId { get; set; } = string.Empty;
        public string DeviceName { get; set; } = string.Empty;
        public string DeviceType { get; set; } = string.Empty;
        public string LineId { get; set; } = string.Empty;
        public string LineName { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public Dictionary<string, object?> Data { get; set; } = new();
    }

    public class DeviceAlertMessage
    {
        public string DeviceId { get; set; } = string.Empty;
        public string DeviceName { get; set; } = string.Empty;
        public string DeviceType { get; set; } = string.Empty;
        public string LineId { get; set; } = string.Empty;
        public string LineName { get; set; } = string.Empty;
        public string SensorName { get; set; } = string.Empty;
        public string AlertType { get; set; } = string.Empty;
        public string AlertLevel { get; set; } = string.Empty; // "Warning" or "Critical"
        public double CurrentValue { get; set; }
        public double ThresholdValue { get; set; }
        public string Unit { get; set; } = string.Empty;
        public string AlertDirection { get; set; } = string.Empty; // "above" or "below"
        public int Priority { get; set; }
        public DateTime Timestamp { get; set; }
        public string SenderId { get; set; } = string.Empty;

        // Backward compatibility properties for existing Azure Functions
        public double Temperature { get; set; }
        public int ProductionRate { get; set; }
        public int ErrorCount { get; set; }
    }
}