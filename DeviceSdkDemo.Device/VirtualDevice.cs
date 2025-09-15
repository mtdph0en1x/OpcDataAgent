using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Shared;
using Newtonsoft.Json;
using System.Text;
using Opc.UaFx.Client;

namespace Device.Device
{
    public class VirtualDevice
    {
        private readonly DeviceClient client;
        private readonly string deviceName;
        private readonly OpcClient opcClient;

        public VirtualDevice(DeviceClient deviceClient, string deviceName, OpcClient opcClient)
        {
            this.client = deviceClient;
            this.deviceName = deviceName;
            this.opcClient = opcClient;
        }

        #region Command Handling 

        public async Task<MethodResponse> HandleEmergencyStopAsync(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine($"[{deviceName}] EMERGENCY STOP command received from agent");
            try
            {
                string methodNodeId = $"ns=2;s={deviceName}/EmergencyStop";
                string objectNodeId = $"ns=2;s={deviceName}";

                opcClient.CallMethod(objectNodeId, methodNodeId);
                Console.WriteLine($"[{deviceName}] Emergency Stop executed on OPC-UA server");

                return new MethodResponse(200);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{deviceName}] Error executing Emergency Stop: {ex.Message}");
                return new MethodResponse(500);
            }
        }

        public async Task<MethodResponse> HandleResetErrorStatusAsync(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine($"[{deviceName}] Reset Error Status command received from agent");
            try
            {
                string methodNodeId = $"ns=2;s={deviceName}/ResetErrorStatus";
                string objectNodeId = $"ns=2;s={deviceName}";

                opcClient.CallMethod(objectNodeId, methodNodeId);
                Console.WriteLine($"[{deviceName}] Error status reset on OPC-UA server");

                return new MethodResponse(200);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{deviceName}] Error resetting status: {ex.Message}");
                return new MethodResponse(500);
            }
        }

        
        public async Task<MethodResponse> HandleAdjustProductionRateAsync(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine($"[{deviceName}] Adjust Production Rate command received from agent");
            try
            {
                var payload = JsonConvert.DeserializeAnonymousType(
                    methodRequest.DataAsJson,
                    new { TargetRate = default(int), Reason = default(string) });

                if (payload?.TargetRate > 0)
                {
                    // Write new production rate to OPC server
                    opcClient.WriteNode($"ns=2;s={deviceName}/ProductionRate", payload.TargetRate);

                    Console.WriteLine($"[{deviceName}] Production rate adjusted to {payload.TargetRate} units/hr");
                    Console.WriteLine($"[{deviceName}] Reason: {payload.Reason}");

                    // Update device twin
                    await UpdateReportedProductionRateAsync(payload.TargetRate);

                    return new MethodResponse(200);
                }
                else
                {
                    Console.WriteLine($"[{deviceName}] Invalid production rate in command");
                    return new MethodResponse(400);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{deviceName}] Error adjusting production rate: {ex.Message}");
                return new MethodResponse(500);
            }
        }

        #endregion

        #region Device Twin 

        public async Task UpdateTwinAsync()
        {
            var twin = await client.GetTwinAsync();
            Console.WriteLine($"[{deviceName}] Initial twin received");

            var reportedProperties = new TwinCollection();
            reportedProperties["DateTimeLastAppLaunch"] = DateTime.Now;
            reportedProperties["DeviceName"] = deviceName;
            reportedProperties["Status"] = "CommandHandlerReady";

            await client.UpdateReportedPropertiesAsync(reportedProperties);
        }

        public async Task UpdateReportedProductionRateAsync(int productionRate)
        {
            var twin = await client.GetTwinAsync();
            var reportedProperties = new TwinCollection();

            reportedProperties["DateTimeLastAppLaunch"] = DateTime.Now;
            reportedProperties["ProductionRate"] = productionRate;

            await client.UpdateReportedPropertiesAsync(reportedProperties);
            Console.WriteLine($"[{deviceName}] Device twin updated - Production Rate: {productionRate}");
        }

        
        public async Task OnDesiredPropertyChanged(TwinCollection desiredProperties, object userContext)
        {
            Console.WriteLine($"[{deviceName}] Agent requested configuration change:");
            Console.WriteLine($"\t{JsonConvert.SerializeObject(desiredProperties)}");

            if (desiredProperties.Contains("ProductionRate"))
            {
                var productionRate = (int)desiredProperties["ProductionRate"];
                Console.WriteLine($"[{deviceName}] Agent setting production rate to: {productionRate}");

                // Apply to OPC-UA server
                opcClient.WriteNode($"ns=2;s={deviceName}/ProductionRate", productionRate);

                // Update reported properties
                var reportedProperties = new TwinCollection();
                reportedProperties["ProductionRate"] = productionRate;
                reportedProperties["DateTimeLastAppLaunch"] = DateTime.Now;

                await client.UpdateReportedPropertiesAsync(reportedProperties);
            }
        }

        #endregion

        #region Initialization

        public async Task InitializeCommandHandlers()
        {
            // Register command handlers for agent communication
            await client.SetMethodHandlerAsync("EmergencyStop", HandleEmergencyStopAsync, opcClient);
            await client.SetMethodHandlerAsync("ResetErrorStatus", HandleResetErrorStatusAsync, opcClient);
            await client.SetMethodHandlerAsync("AdjustProductionRate", HandleAdjustProductionRateAsync, opcClient);

            // Handle configuration changes from agents
            await client.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertyChanged, opcClient);

            Console.WriteLine($"[{deviceName}] Command handlers initialized - ready for agent commands");
        }

        #endregion

        
    }
}