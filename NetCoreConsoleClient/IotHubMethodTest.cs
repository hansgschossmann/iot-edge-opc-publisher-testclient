﻿using Newtonsoft.Json;
using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace NetCoreConsoleClient
{
    using Microsoft.Azure.Devices;
    using OpcPublisher;
    using System.Linq;
    using System.Net;
    using static Program;

    public class IotHubMethodTest : MethodTestBase
    {
        public IotHubMethodTest(string iotHubConnectionString, string iotHubPublisherDeviceName, string iotHubPublisherModuleName, string testserverUrl,
            int maxShortWaitSec, int maxLongWaitSec, CancellationToken ct) : base("IoTHubMethodTest", testserverUrl, maxShortWaitSec, maxLongWaitSec, ct)
        {
            string logPrefix = $"{_logClassPrefix}:IotHubMethodTest: ";
            Logger.Information($"IoTHub connection string: {iotHubConnectionString}");
            Logger.Information($"IoTHub publisher device name: {iotHubPublisherDeviceName}");
            Logger.Information($"IoTHub publisher module name: {iotHubPublisherModuleName}");
            _testserverUrl = testserverUrl;

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
        }

        //
        // Method to add addhoc tests running in exclusive mode.
        //
        protected override void AdhocTest()
        {
        }

        protected override bool PublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl = null)
        {
            string logPrefix = $"{_logClassPrefix}:PublishNodes: ";
            bool result = true;
            try
            {
                PublishNodesMethodRequestModel publishNodesMethodRequestModel = new PublishNodesMethodRequestModel(endpointUrl ?? _testserverUrl);
                foreach (var nodeIdInfo in nodeIdInfos)
                {
                    publishNodesMethodRequestModel.Nodes.Add(new NodeModel(nodeIdInfo.Id));
                }
                _publishNodesMethod.SetPayloadJson(JsonConvert.SerializeObject(publishNodesMethodRequestModel));
                CloudToDeviceMethodResult methodResult;
                if (string.IsNullOrEmpty(_publisherModuleName))
                {
                    methodResult = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publishNodesMethod, ct).Result;
                }
                else
                {
                    methodResult = _iotHubClient.InvokeDeviceMethodAsync(_publisherDeviceName, _publisherModuleName, _publishNodesMethod, ct).Result;
                }
                foreach (var nodeIdInfo in nodeIdInfos)
                {
                    if (!nodeIdInfo.Published && methodResult.Status != (int)HttpStatusCode.OK && methodResult.Status != (int)HttpStatusCode.Accepted)
                    {
                        Logger.Warning($"{logPrefix}failed (nodeId: '{nodeIdInfo.Id}', statusCode: '{methodResult.Status}', publishedState: '{nodeIdInfo.Published}')");
                        result = false;
                    }
                    else
                    {
                        nodeIdInfo.Published = true;
                        Logger.Debug($"{logPrefix}succeeded (nodeId: '{nodeIdInfo.Id}', statusCode: '{methodResult.Status}')");
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix}Exception");
            }
            return result;
        }

        protected override bool UnpublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishNodes: ";
            try
            {
                UnpublishNodesMethodRequestModel unpublishNodesMethodRequestModel = new UnpublishNodesMethodRequestModel(endpointUrl);
                foreach (var nodeIdInfo in nodeIdInfos)
                {
                    unpublishNodesMethodRequestModel.Nodes.Add(new NodeModel(nodeIdInfo.Id));
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
                        Logger.Warning($"{logPrefix}failed (nodeId: '{nodeIdInfo.Id}', statusCode: '{result.Status}', publishedState: '{nodeIdInfo.Published}')");
                        return false;
                    }
                    else
                    {
                        nodeIdInfo.Published = false;
                        Logger.Debug($"{logPrefix}succeeded (nodeId: '{nodeIdInfo.Id}', statusCode: '{result.Status}'");
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix}Exception");
            }
            return false;
        }

        //private void UnpublishAllNodeIdNodes(CancellationToken ct)
        //{
        //    string logPrefix = $"{_logClassPrefix}:UnpublishAllNodeIdNodes: ";
        //    // unpublish all longhaul NodeId nodes.
        //    for (int nodeIndex = 0; nodeIndex < _testServerNodeIds.Count; nodeIndex++)
        //    {
        //        UnpublishNodes(_testServerNodeIds.GetRange(nodeIndex, 1), ct, _testserverUrl);
        //    }
        //    Logger.Verbose($"{logPrefix}completed");
        //}

        //private void UnpublishAllExpandedNodeIdNodes(CancellationToken ct)
        //{
        //    string logPrefix = $"{_logClassPrefix}:UnpublishAllExpandedNodeIdNodes: ";
        //    // unpublish all longhaul ExpandedNodeId nodes.
        //    for (int nodeIndex = 0; nodeIndex < _testServerExpandedNodeIds.Count; nodeIndex++)
        //    {
        //        UnpublishNodes(_testServerExpandedNodeIds.GetRange(nodeIndex, 1), ct, _testserverUrl);
        //    }
        //    Logger.Verbose($"{logPrefix}completed");
        //}

        private void UnpublishAllNodes(CancellationToken ct, string endpointUrl = null)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishAllNodes: ";
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
                Logger.Debug($"{logPrefix}succeeded, status: '{result.Status}'");
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix}Exception ");
            }
        }

        private List<string> GetConfiguredEndpoints(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetConfiguredEndpoints: ";
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
                        endpoints.AddRange(response.Endpoints);
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
               Logger.Fatal(e, $"{logPrefix}Exception ");
            }
            Logger.Debug($"{logPrefix}succeeded, got {endpoints.Count} endpoints");
            return endpoints;
        }

        protected override PublishedNodesCollection GetPublishedNodesLegacy(string endpointUrl, CancellationToken ct)
        {
            List<NodeModel> nodeList = GetConfiguredNodesOnEndpoint(endpointUrl, ct);
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

        protected override List<NodeModel> GetConfiguredNodesOnEndpoint(string endpointUrl, CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetConfiguredNodesOnEndpoint: ";
            GetConfiguredNodesOnEndpointMethodResponseModel response = null;
            List<NodeModel> nodes = new List<NodeModel>();
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
                    response = JsonConvert.DeserializeObject<GetConfiguredNodesOnEndpointMethodResponseModel>(result.GetPayloadAsJson());
                    if (response != null && response.Nodes != null)
                    {
                        nodes.AddRange(response.Nodes);
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
                Logger.Fatal(e, $"{logPrefix}Exception");
            }
            Logger.Debug($"{logPrefix}succeeded, got {nodes.Count} nodes are published on endpoint '{endpointUrl}')");
            return nodes;
        }

        protected override bool UnpublishAllConfiguredNodes(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishAllConfiguredNodes: ";
            CloudToDeviceMethodResult result = null;
            List<NodeModel> nodes = new List<NodeModel>();
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
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix}Exception");
            }
            Logger.Debug($"{logPrefix}succeeded, result: {(HttpStatusCode)result.Status}");
            return (HttpStatusCode)result.Status == HttpStatusCode.OK ? true : false;
        }

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
    }
}
