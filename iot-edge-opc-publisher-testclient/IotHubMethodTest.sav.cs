/* Copyright (c) 1996-2016, OPC Foundation. All rights reserved.
   The source code in this file is covered under a dual-license scenario:
     - RCL: for OPC Foundation members in good-standing
     - GPL V2: everybody else
   RCL license terms accompanied with this source code. See http://opcfoundation.org/License/RCL/1.00/
   GNU General Public License as published by the Free Software Foundation;
   version 2 of the License are accompanied with this source code. See http://opcfoundation.org/License/GPLv2
   This source code is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
*/

using Microsoft.Azure.Devices;
using Newtonsoft.Json;
using Opc.Ua;
using Opc.Ua.Client;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace NetCoreConsoleClient
{
    using static Program;

    public class IotHubMethodTest
    {
        public class PublishNodeMethodData
        {
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public int Version;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string Id;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string EndpointUri;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string PublishInterval;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string SamplingInterval;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string UseSecurity;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string UserName;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string Password;
        }

        public class UnpublishNodeMethodData
        {
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public int Version;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string Id;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string EndpointUri;
        }

        public class GetListOfPublishedNodesMethodData
        {
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public int Version;
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public string EndpointUri;
        }

        // todo check
        [DataContract]
        public partial class NodeLookup
        {
            public NodeLookup()
            {
            }

            [DataMember]
            public Uri EndPointURL;

            [DataMember]
            public NodeId NodeID;
        }

        [CollectionDataContract]
        public partial class PublishedNodesCollection : List<NodeLookup>
        {
            public PublishedNodesCollection()
            {
            }
        }

        private class NodeIdInfo
        {
            public NodeIdInfo(string nodeId)
            {
                _nodeId = nodeId;
                _published = false;
            }

            public string NodeId => _nodeId;

            public bool Published
            {
                get => _published;
                set => _published = true;
            }

            private string _nodeId;
            private bool _published;
        }

        public IotHubMethodTest(string iothubConnectionString, string iothubPublisherDeviceName, string iothubPublisherModuleName, string testserverUrl)
        {
            _testserverUrl = testserverUrl;

            _referenceServerNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TEST_TAG_NUM; i++)
            {
                _referenceServerNodeIds.Add(new NodeIdInfo($"ns=2;s=IotHubMethodTag_{i:D7}"));
            };
            _referenceServerExpandedNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TEST_TAG_NUM; i++)
            {
                _referenceServerExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=IotHubMethodTag_{i:D7}"));
            };

            ServiceClient iothubClient = ServiceClient.CreateFromConnectionString(iothubConnectionString, TransportType.Amqp_WebSocket_Only);
            Device publisherDevice = new Device(iothubPublisherDeviceName);
            Module publisherModule = null;
            if (!string.IsNullOrEmpty(iothubPublisherModuleName))
            {
                publisherModule = new Module(iothubPublisherDeviceName, iothubPublisherModuleName);
            }
            CloudToDeviceMethod publishNodeMethod = new CloudToDeviceMethod("PublishNode");
            CloudToDeviceMethod unpublishNodeMethod = new CloudToDeviceMethod("UnpublishNode");
            CloudToDeviceMethod getListOfPublishedNodesMethod = new CloudToDeviceMethod("GetListOfPublishedNodes");
        }

        public void Run(CancellationToken ct)
        {
            Logger.Information($"Run IoTHub method testing {(_clientRunTime != -1 ? $"for {_clientRunTime / 1000} seconds or" : "till")} CTRL-C is pressed");
            Task.Run(async () => { await RunAllTestsAsync(ct); });
        }

        private async Task RunAllTestsAsync(CancellationToken ct)
        {
            Logger.Information("Run IoTHub method tests");
            // run add/remove test
            await AddRemoveTestAsync();
            // run longhaul test
            var publisherTests = new List<Task>
            {
                //Task.Run(async () => await TestMultiSubscriptionsAsync(ct, )),
                Task.Run(async () => await LonghaulTestAsync(ct, _maxShortWaitSec, _maxLongWaitSec))
            };
            try
            {
                Task.WaitAll(publisherTests.ToArray());
            }
            catch
            {
            }
            Logger.Information($"All tests completed");
        }

        private async Task LonghaulTestAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            var opcPublisherMethodLonghaulTests = new List<Task>
            {
                Task.Run(async () => await PublishNodesLoopAsync(ct, maxShortWaitSec, maxLongWaitSec)),
                Task.Run(async () => await UnpublishNodesLoopAsync(ct, maxShortWaitSec, maxLongWaitSec)),
                Task.Run(async () => await GetListOfPublishedNodesLoopAsync(ct, maxShortWaitSec, maxLongWaitSec))
            };

            // cleanup published nodes
            UnpublishAllNodes();

            await Task.Delay(-1, ct);
            Task.WaitAll(opcPublisherMethodLonghaulTests.ToArray());
        }

        // we do not support different publishing intervals with OPC methods
        //private async Task OpcMultiSubscriptionTestServerAsync(CancellationToken ct, int maxSubscriptionNum)
        //{
        //    List<Subscription> subscriptions = new List<Subscription>();
        //    Logger.Information($"Max subscription test: Create and delete {maxSubscriptionNum}");

        //    for (var publishingIntervalSec = 1; publishingIntervalSec <= _maxSubscriptionNum; publishingIntervalSec++)
        //    {
        //        var subscription = new Subscription(_session.DefaultSubscription) { PublishingInterval = publishingIntervalSec * 1000 };

        //        Logger.Information("Max subscription test: Add a list of items (server current time and status) to the subscription.");
        //        var list = new List<MonitoredItem> {
        //            new MonitoredItem(subscription.DefaultItem)
        //            {
        //                DisplayName = "ServerStatusCurrentTime", StartNodeId = "i="+Variables.Server_ServerStatus_CurrentTime.ToString()
        //            }
        //        };
        //        list.ForEach(i => i.Notification += OnMultiNotification);
        //        subscription.AddItems(list);

        //        Logger.Information("Max subscription test: Add the subscription to the session.");
        //        _session.AddSubscription(subscription);
        //        subscription.Create();
        //        subscriptions.Add(subscription);
        //    }
        //    await Task.Delay(-1, ct);
        //    foreach (var subscription in subscriptions)
        //    {
        //        subscription.Delete(true);
        //    }
        //}


        private async Task AddRemoveTestAsync()
        {
            const int delay = 1000;
            int publishedNodesCount = 0;
            int unpublishCount = 0;
            int publishCount = 0;
            int currentCount = 0;

            publishedNodesCount = GetNumberOfPublishedNodes();
            Logger.Information($"{_testserverUrl} is publishing {publishedNodesCount} node(s).");

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId and validate result
            Logger.Information($"Publish all nodes with NodeId");
            foreach (var node in _referenceServerNodeIds)
            {
                PublishOneNode(node);
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerNodeIds.Count} nodes published");
            }

            return;
            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish nodes with NodeId one at a time and validate result
            Logger.Information($"Publish nodes with NodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _referenceServerNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
                currentCount = GetNumberOfPublishedNodes();
                if (currentCount != publishCount)
                {
                    Logger.Error($"There are {currentCount} nodes published! Expected {publishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerNodeIds.Count} nodes published");
            }

            // unpublish nodes with NodeId one at a time and validate result
            Logger.Information($"Unpublish nodes with NodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _referenceServerNodeIds)
            {
                if (UnpublishOneNode(node))
                {
                    unpublishCount++;
                }
                currentCount = GetNumberOfPublishedNodes();
                if (currentCount != publishedNodesCount - unpublishCount)
                {
                    Logger.Error($"There are {currentCount} nodes published! Expected {publishedNodesCount - unpublishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with ExpandedNodeId and validate result
            Logger.Information($"Publish all nodes with ExpandedNodeId");
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                PublishOneNode(node);
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerExpandedNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerExpandedNodeIds.Count} nodes published");
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }


            // publish nodes with ExpandedNodeId one at a time and validate result
            Logger.Information($"Publish nodes with ExpandedNodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
                currentCount = GetNumberOfPublishedNodes();
                if (currentCount != publishCount)
                {
                    Logger.Error($"There are {currentCount} nodes published! Expected {publishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerExpandedNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerExpandedNodeIds.Count} nodes published");
            }

            // unpublish nodes with ExpandedNodeId one at a time and validate result
            Logger.Information($"Unpublish nodes with ExpandedNodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                if (UnpublishOneNode(node))
                {
                    unpublishCount++;
                }
                currentCount = GetNumberOfPublishedNodes();
                if (currentCount != publishedNodesCount - unpublishCount)
                {
                    Logger.Error($"There are {currentCount} nodes published! Expected {publishedNodesCount - unpublishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and validate result
            Logger.Information($"Publish all nodes with NodeId (first) and ExpandedNodeId (second)");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _referenceServerNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerNodeIds.Count} nodes published");
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and validate result
            Logger.Information($"Publish nodes with NodeId (first) and ExpandedNodeId (second) one at a time and unpublish by NodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            for (int i = 0; i < _referenceServerNodeIds.Count; i++)
            {
                if (PublishOneNode(_referenceServerNodeIds[i]))
                {
                    publishCount++;
                }
                if (PublishOneNode(_referenceServerExpandedNodeIds[i]))
                {
                    publishCount++;
                }
            }
            await Task.Delay(delay);
            for (int i = 0; i < _referenceServerNodeIds.Count; i++)
            {
                if (UnpublishOneNode(_referenceServerNodeIds[i]))
                {
                    unpublishCount++;
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and unpublish by ExpandedNodeId and validate result
            Logger.Information($"Publish nodes with NodeId (first) and ExpandedNodeId (second) one at a time and unpublish by NodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            for (int i = 0; i < _referenceServerNodeIds.Count; i++)
            {
                if (PublishOneNode(_referenceServerNodeIds[i]))
                {
                    publishCount++;
                }
                if (PublishOneNode(_referenceServerExpandedNodeIds[i]))
                {
                    publishCount++;
                }
            }
            await Task.Delay(delay);
            for (int i = 0; i < _referenceServerExpandedNodeIds.Count; i++)
            {
                if (UnpublishOneNode(_referenceServerExpandedNodeIds[i]))
                {
                    unpublishCount++;
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (second) and ExpandedNodeId (first) and validate result
            Logger.Information($"Publish all nodes with NodeId (second) and ExpandedNodeId (first)");
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            foreach (var node in _referenceServerNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerExpandedNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerExpandedNodeIds.Count} nodes published");
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            await Task.Delay(delay);
        }

        private bool PublishOneNode(NodeIdInfo nodeIdInfo)
        {
            try
            {
                VariantCollection inputArguments = new VariantCollection()
                {
                    nodeIdInfo.NodeId,
                    _testserverUrl
                };
                CallMethodRequestCollection requests = new CallMethodRequestCollection();
                CallMethodResultCollection results = null;
                DiagnosticInfoCollection diagnosticInfos = null;
                CallMethodRequest request = new CallMethodRequest();
                request.ObjectId = new NodeId("Methods", 2);
                request.MethodId = new NodeId("PublishNode", 2);
                request.InputArguments = inputArguments;
                requests.Add(request);
                ResponseHeader responseHeader = _session.Call(null, requests, out results, out diagnosticInfos);
                if (!nodeIdInfo.Published && StatusCode.IsBad(results[0].StatusCode))
                {
                    Logger.Warning($"PublishOneNode failed (nodeId: '{nodeIdInfo.NodeId}', published: '{nodeIdInfo.Published}')");
                    return false;
                }
                else
                {
                    nodeIdInfo.Published = true;
                    Logger.Verbose($"PublishOneNode succeeded (nodeId: '{nodeIdInfo.NodeId}', error: '{results[0].StatusCode}'");
                    return true;
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"Exception in PublishOneNode");
            }
            return false;
        }

        private async Task PublishNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Random random = new Random();
            int iteration = 0;

            while (true)
            {
                // publish all nodes.
                Logger.Information($"PublishNodesLoopAsync Iteration {iteration++} started");
                foreach (var node in _referenceServerNodeIds)
                {
                    PublishOneNode(node);
                }
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000));

                // publish nodes randomly selected.
                for (int i = 0; i < _referenceServerNodeIds.Count; i++)
                {
                    int nodeIndex = (int)(_referenceServerNodeIds.Count * random.NextDouble());
                    PublishOneNode(_referenceServerNodeIds[nodeIndex]);
                    await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000));
                }
                Logger.Information($"PublishNodesLoopAsync Iteration {iteration++} completed");

                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000));
                if (ct.IsCancellationRequested)
                {
                    return;
                }
            }
        }

        private bool UnpublishOneNode(NodeIdInfo nodeIdInfo, bool expectFailure = false)
        {
            try
            {
                VariantCollection inputArguments = new VariantCollection()
                {
                    nodeIdInfo.NodeId,
                    _testserverUrl
                };
                CallMethodRequestCollection requests = new CallMethodRequestCollection();
                CallMethodResultCollection results = null;
                DiagnosticInfoCollection diagnosticInfos = null;
                CallMethodRequest request = new CallMethodRequest();
                request.ObjectId = new NodeId("Methods", 2);
                request.MethodId = new NodeId("UnpublishNode", 2);
                request.InputArguments = inputArguments;
                requests.Add(request);
                ResponseHeader responseHeader = _session.Call(null, requests, out results, out diagnosticInfos);
                if (nodeIdInfo.Published && StatusCode.IsBad(results[0].StatusCode) && !expectFailure)
                {
                    Logger.Warning($"UnpublishOneNode failed (nodeId: '{nodeIdInfo.NodeId}', published: '{nodeIdInfo.Published}')");
                    return false;
                }
                else
                {
                    nodeIdInfo.Published = false;
                    Logger.Verbose($"UnpublishOneNode succeeded (nodeId: '{nodeIdInfo.NodeId}', error: '{results[0].StatusCode}'");
                    return true;
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"Exception in UnpublishOneNode");
            }
            return false;
        }

        private void UnpublishAllNodes()
        {
            // unpublish all nodes.
            UnpublishAllNodeIdNodes();
            UnpublishAllExpandedNodeIdNodes();
            Logger.Verbose($"UnpublishAllNodes completed");
        }

        private void UnpublishAllNodeIdNodes()
        {
            // unpublish all longhaul NodeId nodes.
            foreach (var node in _referenceServerNodeIds)
            {
                UnpublishOneNode(node, true);
            }
            Logger.Verbose($"UnpublishAllNodeIdNodes completed");
        }

        private void UnpublishAllExpandedNodeIdNodes()
        {
            // unpublish all longhaul ExpandedNodeId nodes.
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                UnpublishOneNode(node, true);
            }
            Logger.Verbose($"UnpublishAllExpandedNodeIdNodes completed");
        }

        private async Task UnpublishNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Random random = new Random();
            int iteration = 0;

            // Wait for some time to allow node publishing
            await Task.Delay(30000);
            while (true)
            {
                // unpublish nodes randomly selected.
                Logger.Information($"UnpublishNodes Iteration {iteration++} started");
                for (int i = 0; i < _referenceServerNodeIds.Count; i++)
                {
                    int nodeIndex = (int)(_referenceServerNodeIds.Count * random.NextDouble());
                    UnpublishOneNode(_referenceServerNodeIds[nodeIndex]);
                    await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000));
                }
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000));

                // unpublish all nodes.
                foreach (var node in _referenceServerNodeIds)
                {
                    UnpublishOneNode(node);
                }
                Logger.Information($"UnpublishNodes Iteration {iteration++} completed");

                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000));
                if (ct.IsCancellationRequested)
                {
                    return;
                }
            }
        }

        private async Task GetListOfPublishedNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Random random = new Random();
            int iteration = 0;

            VariantCollection inputArgumentsTestserver = new VariantCollection()
            {
                _testserverUrl
            };

            while (true)
            {
                Logger.Information($"GetListOfPublishedNodesLoopAsync Iteration {iteration++} started");
                for (int i = 0; i < _referenceServerNodeIds.Count; i++)
                {
                    try
                    {
                        CallMethodRequestCollection requests = new CallMethodRequestCollection();
                        CallMethodResultCollection results;
                        DiagnosticInfoCollection diagnosticInfos = null;
                        CallMethodRequest request = new CallMethodRequest
                        {
                            ObjectId = new NodeId("Methods", 2),
                            MethodId = new NodeId("GetPublishedNodes", 2),
                        };
                        request.InputArguments = inputArgumentsTestserver;
                        requests.Add(request);
                        ResponseHeader responseHeader = _session.Call(null, requests, out results, out diagnosticInfos);
                        if (StatusCode.IsBad(results[0].StatusCode))
                        {
                            Logger.Warning($"GetListOfPublishedNodesLoopAsync call was not successfull (status: '{results[0].StatusCode}'");
                        }
                        await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000));
                    }
                    catch (Exception e)
                    {
                        Logger.Fatal(e, $"Exception in GetListOfPublishedNodesLoopAsync");
                    }
                }
                Logger.Information($"GetListOfPublishedNodesLoopAsync Iteration {iteration++} completed");

                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000));
                if (ct.IsCancellationRequested)
                {
                    return;
                }
            }
        }

        private int GetNumberOfPublishedNodes()
        {
            Random random = new Random();
            int result = 0;

            VariantCollection inputArgumentsTestserver = new VariantCollection()
            {
                _testserverUrl
            };

            try
            {
                CallMethodRequestCollection requests = new CallMethodRequestCollection();
                CallMethodResultCollection results;
                DiagnosticInfoCollection diagnosticInfos = null;
                CallMethodRequest request = new CallMethodRequest
                {
                    ObjectId = new NodeId("Methods", 2),
                    MethodId = new NodeId("GetPublishedNodes", 2),
                };
                request.InputArguments = inputArgumentsTestserver;
                requests.Add(request);
                ResponseHeader responseHeader = _session.Call(null, requests, out results, out diagnosticInfos);
                if (StatusCode.IsBad(results[0].StatusCode))
                {
                    Logger.Warning($"GetNumberOfPublishedNodes call was not successfull (status: '{results[0].StatusCode}'");
                }
                else
                {
                    if (results?[0]?.OutputArguments.Count == 1)
                    {
                        string stringResult = results[0].OutputArguments[0].ToString();

                        int jsonStartIndex = stringResult.IndexOf("[");
                        int jsonEndIndex = stringResult.IndexOf("]");
                        PublishedNodesCollection nodelist = JsonConvert.DeserializeObject<PublishedNodesCollection>(stringResult.Substring(jsonStartIndex, jsonEndIndex - jsonStartIndex + 1));
                        result = nodelist.Count;
                    }
                    else
                    {
                        result = 0;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"Exception in GetNumberOfPublishedNodes");
            }
            return result;
        }

        private void Client_KeepAlive(Session sender, KeepAliveEventArgs e)
        {
            if (e.Status != null && ServiceResult.IsNotGood(e.Status))
            {
                Logger.Verbose("{0} {1}/{2}", e.Status, sender.OutstandingRequestCount, sender.DefunctRequestCount);

                if (_reconnectHandler == null)
                {
                    Logger.Verbose("--- RECONNECTING ---");
                    _reconnectHandler = new SessionReconnectHandler();
                    _reconnectHandler.BeginReconnect(sender, _reconnectPeriod * 1000, Client_ReconnectComplete);
                }
            }
        }

        private void Client_ReconnectComplete(object sender, EventArgs e)
        {
            // ignore callbacks from discarded objects.
            if (!Object.ReferenceEquals(sender, _reconnectHandler))
            {
                return;
            }

            _session = _reconnectHandler.Session;
            _reconnectHandler.Dispose();
            _reconnectHandler = null;

            Logger.Verbose("--- RECONNECTED ---");
        }

        private static void OnNotification(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            foreach (var value in item.DequeueValues())
            {
                if (_lastTimestamp != new DateTime(0) && value.ServerTimestamp - _lastTimestamp > TimeSpan.FromSeconds(_maxLongWaitSec * 1.3))
                {
                    Logger.Warning($"{item.DisplayName} publishing is too late: diff: {value.ServerTimestamp - _lastTimestamp}, max: {_maxLongWaitSec * 1.3}, serverTimestamp: {value.ServerTimestamp}, lastTimestamp: {_lastTimestamp}");
                }
                _lastTimestamp = value.ServerTimestamp;
            }
        }

        private static void CertificateValidator_CertificateValidation(CertificateValidator validator, CertificateValidationEventArgs e)
        {
            if (e.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                e.Accept = true;
                Logger.Verbose("All server certificates are trusted: {0}", e.Certificate.Subject);
            }
        }

        private const int MULTI_TAG_NUM = 100000;
        private const int TEST_TAG_NUM = 10000;
        private const int _maxLongWaitSec = 10;
        private const int _maxShortWaitSec = 5;
        private List<NodeIdInfo> _referenceServerNodeIds = null;
        private List<NodeIdInfo> _referenceServerExpandedNodeIds = null;
        private static DateTime _lastTimestamp;
        private const int _reconnectPeriod = 10;
        private Session _session;
        private SessionReconnectHandler _reconnectHandler;
        private string _publisherUrl;
        private string _testserverUrl;
    }
}
