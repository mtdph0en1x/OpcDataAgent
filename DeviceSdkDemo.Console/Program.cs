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
                    services.AddSingleton<OpcDataCollectionService>();
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

                // Convert to device mappings
                _deviceMappings = _configService.ConvertToDeviceMappings(opcConfig);

                // Initialize OPC connections
                await InitializeOpcConnectionsAsync(opcConfig.OpcServers);

                // Initialize IoT Hub connections
                await InitializeIoTHubAsync();

                _logger.LogInformation($"Initialization complete:");
                _logger.LogInformation($"  - {opcConfig.OpcServers.Length} OPC servers");
                _logger.LogInformation($"  - {opcConfig.ProductionLines.Length} production lines");
                _logger.LogInformation($"  - {_deviceMappings.Count} enabled devices");
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
                try
                {
                    var opcClient = new OpcClient(serverConfig.Url)
                    {
                        SessionTimeout = serverConfig.SessionTimeout
                    };

                    opcClient.Connect();
                    _opcClients[serverConfig.Url] = opcClient;

                    _logger.LogInformation($"Connected to OPC Server: {serverConfig.Name} ({serverConfig.Url})");
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Failed to connect to OPC Server {serverConfig.Name}: {ex.Message}");
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
            return connectionString.Split(';')
                .FirstOrDefault(part => part.StartsWith("HostName="))
                ?.Split('=')[1] ?? "";
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

            var alertSender = _serviceBusClient.CreateSender(_deviceAlertsQueue);

            _logger.LogInformation($"Starting monitoring: {deviceMapping.DeviceId} ({deviceMapping.DeviceName}) via {telemetrySender.GetType().Name}");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Read all sensor nodes for this device
                    var readNodes = deviceMapping.NodeMappings
                        .Select(n => new OpcReadNode(n.NodeId))
                        .ToArray();

                    var values = opcClient.ReadNodes(readNodes).ToArray();

                    // Build device data message
                    var deviceData = new DeviceDataMessage
                    {
                        DeviceId = deviceMapping.DeviceId,
                        DeviceName = deviceMapping.DeviceName,
                        DeviceType = deviceMapping.DeviceType,
                        LineId = deviceMapping.LineId,
                        LineName = deviceMapping.LineName,
                        Timestamp = DateTime.UtcNow,
                        Data = new Dictionary<string, object>()
                    };

                    // Process each sensor value
                    for (int i = 0; i < deviceMapping.NodeMappings.Length && i < values.Length; i++)
                    {
                        var nodeMapping = deviceMapping.NodeMappings[i];
                        var opcValue = values[i];

                        if (opcValue.Status.IsGood)
                        {
                            var value = ConvertValue(opcValue.Value, nodeMapping.DataType);
                            deviceData.Data[nodeMapping.SensorName] = value;

                            // Check for alerts
                            await CheckForAlertsAsync(alertSender, deviceMapping, nodeMapping, value);
                        }
                        else
                        {
                            _logger.LogWarning($"Bad OPC value for {deviceMapping.DeviceId}.{nodeMapping.SensorName}: {opcValue.Status}");
                            deviceData.Data[nodeMapping.SensorName] = null;
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

            await telemetrySender.DisposeAsync();
            await alertSender.DisposeAsync();
        }

        private async Task CheckForAlertsAsync(ServiceBusSender alertSender, DeviceMapping deviceMapping, NodeMapping nodeMapping, object value)
        {
            if (!nodeMapping.HasAlerts || !nodeMapping.IsNumeric || value == null)
                return;

            try
            {
                var numericValue = Convert.ToDouble(value);
                string alertLevel = null;
                double? thresholdValue = null;

                // Check critical threshold first
                if (nodeMapping.CriticalThreshold.HasValue)
                {
                    bool criticalAlert = nodeMapping.AlertDirection.ToLower() == "below"
                        ? numericValue < nodeMapping.CriticalThreshold.Value
                        : numericValue > nodeMapping.CriticalThreshold.Value;

                    if (criticalAlert)
                    {
                        alertLevel = "Critical";
                        thresholdValue = nodeMapping.CriticalThreshold.Value;
                    }
                }

                // Check warning threshold if no critical alert
                if (alertLevel == null && nodeMapping.WarningThreshold.HasValue)
                {
                    bool warningAlert = nodeMapping.AlertDirection.ToLower() == "below"
                        ? numericValue < nodeMapping.WarningThreshold.Value
                        : numericValue > nodeMapping.WarningThreshold.Value;

                    if (warningAlert)
                    {
                        alertLevel = "Warning";
                        thresholdValue = nodeMapping.WarningThreshold.Value;
                    }
                }

                // Send alert if threshold exceeded
                if (alertLevel != null && thresholdValue.HasValue)
                {
                    await SendAlertAsync(alertSender, deviceMapping, nodeMapping, numericValue, alertLevel, thresholdValue.Value);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error checking alerts for {deviceMapping.DeviceId}.{nodeMapping.SensorName}: {ex.Message}");
            }
        }

        private async Task SendAlertAsync(ServiceBusSender alertSender, DeviceMapping deviceMapping, NodeMapping nodeMapping,
            double currentValue, string alertLevel, double thresholdValue)
        {
            var alert = new DeviceAlertMessage
            {
                DeviceId = deviceMapping.DeviceId,
                DeviceName = deviceMapping.DeviceName,
                DeviceType = deviceMapping.DeviceType,
                LineId = deviceMapping.LineId,
                LineName = deviceMapping.LineName,
                SensorName = nodeMapping.SensorName,
                AlertType = nodeMapping.SensorName,
                AlertLevel = alertLevel,
                CurrentValue = currentValue,
                ThresholdValue = thresholdValue,
                Unit = nodeMapping.Unit,
                AlertDirection = nodeMapping.AlertDirection,
                Priority = alertLevel == "Critical" ? 5 : 3,
                Timestamp = DateTime.UtcNow,
                SenderId = "OpcDataAgent"
            };

            // Set backward compatibility properties for existing agent functions
            switch (nodeMapping.SensorName.ToLower())
            {
                case "temperature":
                    alert.Temperature = currentValue;
                    break;
                case "productionrate":
                    alert.ProductionRate = (int)currentValue;
                    break;
                case "deviceerror":
                    alert.ErrorCount = (int)currentValue;
                    break;
            }

            var message = new ServiceBusMessage(JsonConvert.SerializeObject(alert))
            {
                Subject = deviceMapping.LineId,
                MessageId = Guid.NewGuid().ToString(),
                ContentType = "application/json",
                TimeToLive = TimeSpan.FromHours(1) // Alerts expire after 1 hour
            };

            // Add custom properties for filtering
            message.ApplicationProperties["DeviceId"] = deviceMapping.DeviceId;
            message.ApplicationProperties["LineId"] = deviceMapping.LineId;
            message.ApplicationProperties["AlertLevel"] = alertLevel;
            message.ApplicationProperties["SensorType"] = nodeMapping.SensorName;

            await alertSender.SendMessageAsync(message);

            var direction = nodeMapping.AlertDirection == "below" ? "below" : "above";
            _logger.LogWarning($"ALERT [{alertLevel}]: {deviceMapping.DeviceId}.{nodeMapping.SensorName} = {currentValue:F2}{nodeMapping.Unit} ({direction} threshold: {thresholdValue:F2}{nodeMapping.Unit})");
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