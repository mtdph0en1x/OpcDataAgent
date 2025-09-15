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
            System.Console.WriteLine("Configuration-driven OPC UA to Service Bus bridge");
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
        private List<DeviceMapping> _deviceMappings = new();

        // Queue names from configuration
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

            // Read queue names from configuration
            _deviceDataQueue = _configuration["ServiceBusQueues:DeviceData"] ?? "device-data";
            _deviceAlertsQueue = _configuration["ServiceBusQueues:DeviceAlerts"] ?? "device-alerts";

            _logger.LogInformation($"Using queues - Data: {_deviceDataQueue}, Alerts: {_deviceAlertsQueue}");
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

        private async Task MonitorDeviceAsync(DeviceMapping deviceMapping, CancellationToken cancellationToken)
        {
            if (!_opcClients.TryGetValue(deviceMapping.OpcServerUrl, out var opcClient))
            {
                _logger.LogError($"No OPC client found for {deviceMapping.OpcServerUrl}");
                return;
            }

            var dataSender = _serviceBusClient.CreateSender(_deviceDataQueue);
            var alertSender = _serviceBusClient.CreateSender(_deviceAlertsQueue);

            _logger.LogInformation($"Starting monitoring: {deviceMapping.DeviceId} ({deviceMapping.DeviceName})");

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

                    // Send device data to Service Bus
                    await dataSender.SendMessageAsync(
                        new ServiceBusMessage(JsonConvert.SerializeObject(deviceData))
                        {
                            Subject = deviceMapping.LineId,
                            MessageId = Guid.NewGuid().ToString(),
                            ContentType = "application/json"
                        });

                    _logger.LogDebug($"Sent data for {deviceMapping.DeviceId}");
                    await Task.Delay(deviceMapping.SamplingInterval, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error monitoring {deviceMapping.DeviceId}: {ex.Message}");
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
            }

            await dataSender.DisposeAsync();
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