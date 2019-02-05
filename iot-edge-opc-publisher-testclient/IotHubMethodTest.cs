using Newtonsoft.Json;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Threading;

namespace OpcPublisherTestClient
{
    using Microsoft.Azure.Devices;
    using OpcPublisher;
    using System.Linq;
    using System.Net;
    using static Program;

    public class IotHubMethodTest : MethodTestBase
    {
        public IotHubMethodTest(string iotHubConnectionString, string iotHubPublisherDeviceName, string iotHubPublisherModuleName, string testserverUrl,
            int maxShortWaitSec, int maxLongWaitSec, CancellationToken ct) : base("IotHubMethodTest", testserverUrl, maxShortWaitSec, maxLongWaitSec, ct)
        {
            string logPrefix = $"{_logClassPrefix}:IotHubMethodTest: ";
            Logger.Information($"IoTHub connection string: {iotHubConnectionString}");
            Logger.Information($"IoTHub publisher device name: {iotHubPublisherDeviceName}");
            Logger.Information($"IoTHub publisher module name: {iotHubPublisherModuleName}");
            TestserverUrl = testserverUrl;

            // init IoTHub connection
            _iotHubClient = ServiceClient.CreateFromConnectionString(iotHubConnectionString, TransportType.Amqp_WebSocket_Only);
            _publisherDeviceName = iotHubPublisherDeviceName;
            _publisherDevice = new Device(iotHubPublisherDeviceName);
            _publisherModule = null;
            if (!string.IsNullOrEmpty(iotHubPublisherModuleName))
            {
                _publisherModuleName = iotHubPublisherModuleName;
                _publisherModule = new Module(iotHubPublisherDeviceName, iotHubPublisherModuleName);
            }
            TimeSpan responseTimeout = TimeSpan.FromSeconds(300);
            TimeSpan connectionTimeout = TimeSpan.FromSeconds(30);
            _publishNodesMethod = new CloudToDeviceMethod("PublishNodes", responseTimeout, connectionTimeout);
            _unpublishNodesMethod = new CloudToDeviceMethod("UnpublishNodes", responseTimeout, connectionTimeout);
            _unpublishAllNodesMethod = new CloudToDeviceMethod("UnpublishAllNodes", responseTimeout, connectionTimeout);
            _getConfiguredEndpointsMethod = new CloudToDeviceMethod("GetConfiguredEndpoints", responseTimeout, connectionTimeout);
            _getConfiguredNodesOnEndpointMethod = new CloudToDeviceMethod("GetConfiguredNodesOnEndpoint", responseTimeout, connectionTimeout);
            _getDiagnosticInfoMethod = new CloudToDeviceMethod("GetDiagnosticInfo", responseTimeout, connectionTimeout);
            _getInfoMethod = new CloudToDeviceMethod("GetInfo", responseTimeout, connectionTimeout);
            _getDiagnosticLogMethod = new CloudToDeviceMethod("GetDiagnosticLog", responseTimeout, connectionTimeout);
            _getDiagnosticStartupLogMethod = new CloudToDeviceMethod("GetDiagnosticStartupLog", responseTimeout, connectionTimeout);
            _callUnknonwnMethod = new CloudToDeviceMethod("UnknownMethod", responseTimeout, connectionTimeout);
        }

        //
        // Method to add addhoc tests running in exclusive mode.
        //
        protected override void AdhocTest()
        {
        }

        protected override bool PublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl = null)
        {
            string logPrefix = $"{_logClassPrefix}:PublishNodes:";
            bool result = true;
            int retryCount = MAX_RETRY_COUNT;

            try
            {
                PublishNodesMethodRequestModel publishNodesMethodRequestModel = new PublishNodesMethodRequestModel(endpointUrl ?? TestserverUrl);
                foreach (var nodeIdInfo in nodeIdInfos)
                {
                    publishNodesMethodRequestModel.OpcNodes.Add(new OpcNodeOnEndpointModel(nodeIdInfo.Id));
                }
                CloudToDeviceMethodResult methodResult = new CloudToDeviceMethodResult();
                methodResult.Status = (int)HttpStatusCode.NotAcceptable;
                while (methodResult.Status == (int)HttpStatusCode.NotAcceptable && retryCount-- > 0)
                {
                    _publishNodesMethod.SetPayloadJson(JsonConvert.SerializeObject(publishNodesMethodRequestModel));
                    if (string.IsNullOrEmpty(_publisherModuleName))
                    {
                        methodResult = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publishNodesMethod, ct).Result;
                    }
                    else
                    {
                        methodResult = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _publishNodesMethod, ct).Result;
                    }
                    if (methodResult.Status == (int)HttpStatusCode.NotAcceptable)
                    {
                        Thread.Sleep(MaxShortWaitSec * 1000);
                    }
                    else
                    {
                        break;
                    }
                }
                foreach (var nodeIdInfo in nodeIdInfos)
                {
                    if (!nodeIdInfo.Published && methodResult.Status != (int)HttpStatusCode.OK && methodResult.Status != (int)HttpStatusCode.Accepted)
                    {
                        Logger.Warning($"{logPrefix} failed (nodeId: '{nodeIdInfo.Id}', statusCode: '{methodResult.Status}', publishedState: '{nodeIdInfo.Published}')");
                        result = false;
                    }
                    else
                    {
                        nodeIdInfo.Published = true;
                        Logger.Debug($"{logPrefix} succeeded (nodeId: '{nodeIdInfo.Id}', statusCode: '{methodResult.Status}')");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            return result;
        }

        protected override bool UnpublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishNodes:";
            try
            {
                UnpublishNodesMethodRequestModel unpublishNodesMethodRequestModel = new UnpublishNodesMethodRequestModel(endpointUrl);
                foreach (var nodeIdInfo in nodeIdInfos)
                {
                    unpublishNodesMethodRequestModel.OpcNodes.Add(new OpcNodeOnEndpointModel(nodeIdInfo.Id));
                }
                _unpublishNodesMethod.SetPayloadJson(JsonConvert.SerializeObject(unpublishNodesMethodRequestModel));
                CloudToDeviceMethodResult result;
                if (string.IsNullOrEmpty(_publisherModuleName))
                {
                    result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _unpublishNodesMethod, ct).Result;
                }
                else
                {
                    result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _unpublishNodesMethod, ct).Result;
                }
                foreach (var nodeIdInfo in nodeIdInfos)
                {
                    if (nodeIdInfo.Published && result.Status != (int)HttpStatusCode.OK && result.Status != (int)HttpStatusCode.Accepted)
                    {
                        Logger.Warning($"{logPrefix} failed (nodeId: '{nodeIdInfo.Id}', statusCode: '{result.Status}', publishedState: '{nodeIdInfo.Published}')");
                        return false;
                    }
                    else
                    {
                        nodeIdInfo.Published = false;
                        Logger.Debug($"{logPrefix} succeeded (nodeId: '{nodeIdInfo.Id}', statusCode: '{result.Status}'");
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            return false;
        }

        private void UnpublishAllNodes(CancellationToken ct, string endpointUrl = null)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishAllNodes:";
            List<string> endpoints = new List<string>();
            try
            {
                UnpublishAllNodesMethodRequestModel unpublishAllNodesMethodRequestModel = new UnpublishAllNodesMethodRequestModel();
                CloudToDeviceMethodResult result;
                if (string.IsNullOrEmpty(_publisherModuleName))
                {
                    result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _unpublishAllNodesMethod, ct).Result;
                }
                else
                {
                    result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _unpublishAllNodesMethod, ct).Result;
                }
                if (result.Status == (int)HttpStatusCode.OK)
                {
                    Logger.Debug($"{logPrefix} succedded {(string.IsNullOrEmpty(endpointUrl) ? $"(endpoint: {endpointUrl})" : "(all endpoints)")}, statusCode: '{result.Status}'");
                }
                else
                {
                    Logger.Warning($"{logPrefix} failed {(string.IsNullOrEmpty(endpointUrl) ? $"(endpoint: {endpointUrl})" : "(all endpoints)")}, statusCode: '{result.Status}'");
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception ");
            }
        }

        private List<string> GetConfiguredEndpoints(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetConfiguredEndpoints:";
            GetConfiguredEndpointsMethodResponseModel response = null;
            List<string> endpoints = new List<string>();
            try
            {
                GetConfiguredEndpointsMethodRequestModel getConfiguredEndpointsMethodRequestModel = new GetConfiguredEndpointsMethodRequestModel();
                ulong? continuationToken = null;
                while (true)
                {
                    getConfiguredEndpointsMethodRequestModel.ContinuationToken = continuationToken;
                    _getConfiguredEndpointsMethod.SetPayloadJson(JsonConvert.SerializeObject(getConfiguredEndpointsMethodRequestModel));
                    CloudToDeviceMethodResult result;
                    if (string.IsNullOrEmpty(_publisherModuleName))
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _getConfiguredEndpointsMethod, ct).Result;
                    }
                    else
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _getConfiguredEndpointsMethod, ct).Result;
                    }
                    response = JsonConvert.DeserializeObject<GetConfiguredEndpointsMethodResponseModel>(result.GetPayloadAsJson());
                    if (response != null && response.Endpoints != null)
                    {
                        endpoints.AddRange(response.Endpoints.Select(e => e.EndpointUrl).ToList());
                    }
                    if (response == null || response.ContinuationToken == null)
                    {
                        break;
                    }
                    continuationToken = response.ContinuationToken;
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception ");
            }
            Logger.Debug($"{logPrefix} succeeded, got {endpoints.Count} endpoints");
            return endpoints;
        }

        protected override PublishedNodesCollection GetPublishedNodesLegacy(string endpointUrl, CancellationToken ct)
        {
            List<OpcNodeOnEndpointModel> nodeList = GetConfiguredNodesOnEndpoint(endpointUrl, ct);
            PublishedNodesCollection publishedNodes = new PublishedNodesCollection();
            foreach (var node in nodeList)
            {
                NodeLookup nodeLookup = new NodeLookup();
                nodeLookup.EndPointURL = new Uri(endpointUrl);
                nodeLookup.NodeID = new NodeId(node.Id);
                publishedNodes.Add(new NodeLookup());
            }
            return publishedNodes;
        }

        protected override List<OpcNodeOnEndpointModel> GetConfiguredNodesOnEndpoint(string endpointUrl, CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetConfiguredNodesOnEndpoint:";
            GetConfiguredNodesOnEndpointMethodResponseModel response = null;
            List<OpcNodeOnEndpointModel> nodes = new List<OpcNodeOnEndpointModel>();
            try
            {
                GetConfiguredNodesOnEndpointMethodRequestModel getConfiguredNodesOnEndpointMethodRequestModel = new GetConfiguredNodesOnEndpointMethodRequestModel(endpointUrl);
                getConfiguredNodesOnEndpointMethodRequestModel.EndpointUrl = endpointUrl;
                ulong? continuationToken = null;
                while (true)
                {
                    getConfiguredNodesOnEndpointMethodRequestModel.ContinuationToken = continuationToken;
                    _getConfiguredNodesOnEndpointMethod.SetPayloadJson(JsonConvert.SerializeObject(getConfiguredNodesOnEndpointMethodRequestModel));
                    CloudToDeviceMethodResult result;
                    if (string.IsNullOrEmpty(_publisherModuleName))
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _getConfiguredNodesOnEndpointMethod, ct).Result;
                    }
                    else
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _getConfiguredNodesOnEndpointMethod, ct).Result;
                    }
                    if (result.Status == (int)HttpStatusCode.OK)
                    {
                        Logger.Debug($"{logPrefix} succeeded, got {nodes.Count} nodes are published on endpoint '{endpointUrl}')");
                        response = JsonConvert.DeserializeObject<GetConfiguredNodesOnEndpointMethodResponseModel>(result.GetPayloadAsJson());
                        if (response != null && response.OpcNodes != null)
                        {
                            nodes.AddRange(response.OpcNodes);
                        }
                        if (response == null || response.ContinuationToken == null)
                        {
                            break;
                        }
                        continuationToken = response.ContinuationToken;
                    }
                    else
                    {
                        if (result.Status == (int)HttpStatusCode.Gone)
                        {
                            Logger.Debug($"{logPrefix} node configuration has changed for endpoint {endpointUrl}");
                        }
                        else
                        {
                            Logger.Warning($"{logPrefix} failed for endpoint {endpointUrl}");
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            return nodes;
        }

        protected override bool UnpublishAllConfiguredNodes(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishAllConfiguredNodes:";
            CloudToDeviceMethodResult result = null;
            try
            {
                UnpublishAllNodesMethodRequestModel unpublishAllNodesMethodRequestModel = new UnpublishAllNodesMethodRequestModel();
                _unpublishAllNodesMethod.SetPayloadJson(JsonConvert.SerializeObject(unpublishAllNodesMethodRequestModel));
                if (string.IsNullOrEmpty(_publisherModuleName))
                {
                    result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _unpublishAllNodesMethod, ct).Result;
                }
                else
                {
                    result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _unpublishAllNodesMethod, ct).Result;
                }
                if (result.Status == (int)HttpStatusCode.OK)
                {
                    Logger.Debug($"{logPrefix} succeeded");
                }
                else
                {
                    Logger.Warning($"{logPrefix} failed");
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            Logger.Debug($"{logPrefix} succeeded, result: {(HttpStatusCode)result.Status}");
            return (HttpStatusCode)result.Status == HttpStatusCode.OK ? true : false;
        }

        protected override void GetInfo(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetInfo:";
            try
            {
                while (true)
                {
                    CloudToDeviceMethodResult result;
                    if (string.IsNullOrEmpty(_publisherModuleName))
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _getInfoMethod, ct).Result;
                    }
                    else
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _getInfoMethod, ct).Result;
                    }
                    if (result.Status == (int)HttpStatusCode.OK)
                    {
                        Logger.Debug($"{logPrefix} succeeded");
                    }
                    else
                    {
                        Logger.Warning($"{logPrefix} failed");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
        }

        protected override void GetDiagnosticInfo(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetDiagnosticInfo:";
            List<OpcNodeOnEndpointModel> nodes = new List<OpcNodeOnEndpointModel>();
            try
            {
                while (true)
                {
                    CloudToDeviceMethodResult result;
                    if (string.IsNullOrEmpty(_publisherModuleName))
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _getDiagnosticInfoMethod, ct).Result;
                    }
                    else
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _getDiagnosticInfoMethod, ct).Result;
                    }
                    if (result.Status == (int)HttpStatusCode.OK)
                    {
                        Logger.Debug($"{logPrefix} succeeded");
                    }
                    else
                    {
                        Logger.Warning($"{logPrefix} failed");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
        }

        protected override void GetDiagnosticLog(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetDiagnosticLog:";
            List<OpcNodeOnEndpointModel> nodes = new List<OpcNodeOnEndpointModel>();
            try
            {
                while (true)
                {
                    CloudToDeviceMethodResult result;
                    if (string.IsNullOrEmpty(_publisherModuleName))
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _getDiagnosticLogMethod, ct).Result;
                    }
                    else
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _getDiagnosticLogMethod, ct).Result;
                    }
                    if (result.Status == (int)HttpStatusCode.OK)
                    {
                        Logger.Debug($"{logPrefix} succeeded");
                    }
                    else
                    {
                        Logger.Warning($"{logPrefix} failed");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
        }

        protected override void GetDiagnosticStartupLog(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetDiagnosticStartupLog:";
            List<OpcNodeOnEndpointModel> nodes = new List<OpcNodeOnEndpointModel>();
            try
            {
                while (true)
                {
                    CloudToDeviceMethodResult result;
                    if (string.IsNullOrEmpty(_publisherModuleName))
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _getDiagnosticStartupLogMethod, ct).Result;
                    }
                    else
                    {
                        result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _getDiagnosticStartupLogMethod, ct).Result;
                    }
                    if (result.Status == (int)HttpStatusCode.OK)
                    {
                        Logger.Debug($"{logPrefix} succeeded");
                    }
                    else
                    {
                        Logger.Warning($"{logPrefix} failed");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
        }

        protected override void CallUnknownMethod(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:CallUnknownMethod:";
            List<OpcNodeOnEndpointModel> nodes = new List<OpcNodeOnEndpointModel>();
            try
            {
                while (true)
                {
                    CloudToDeviceMethodResult result = null;
                    try
                    {
                        if (string.IsNullOrEmpty(_publisherModuleName))
                        {
                            result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _callUnknonwnMethod, ct).Result;
                        }
                        else
                        {
                            result = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _callUnknonwnMethod, ct).Result;
                        }

                    }
                    catch
                    {
                        if (result?.Status != (int)HttpStatusCode.NotImplemented)
                        {
                            throw;
                        }
                    }
                    if (result.Status == (int)HttpStatusCode.NotImplemented)
                    {
                        Logger.Debug($"{logPrefix} succeeded");
                    }
                    else
                    {
                        Logger.Warning($"{logPrefix} failed");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
        }

        const int MAX_RETRY_COUNT = 3;

        private static string _logClassPrefix = "IotHubMethodTest";

        ServiceClient _iotHubClient;
        string _publisherDeviceName;
        Device _publisherDevice;
        string _publisherModuleName;
        Module _publisherModule;
        CloudToDeviceMethod _publishNodesMethod;
        CloudToDeviceMethod _unpublishNodesMethod;
        CloudToDeviceMethod _unpublishAllNodesMethod;
        CloudToDeviceMethod _getConfiguredEndpointsMethod;
        CloudToDeviceMethod _getConfiguredNodesOnEndpointMethod;
        CloudToDeviceMethod _getInfoMethod;
        CloudToDeviceMethod _getDiagnosticInfoMethod;
        CloudToDeviceMethod _getDiagnosticLogMethod;
        CloudToDeviceMethod _getDiagnosticStartupLogMethod;
        CloudToDeviceMethod _callUnknonwnMethod;
    }
}
