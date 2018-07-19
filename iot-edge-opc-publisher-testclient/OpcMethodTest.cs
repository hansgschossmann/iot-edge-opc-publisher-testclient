using Newtonsoft.Json;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace NetCoreConsoleClient
{
    using OpcPublisher;
    using System.Linq;
    using static Program;

    public class OpcMethodTest : MethodTestBase
    {
        public static bool AutoAccept = false;
        public static string PublisherUrl = "opc.tcp://publisher:62222/UA/Publisher";

        public OpcMethodTest(string testserverUrl, int maxShortWaitSec, int maxLongWaitSec, CancellationToken ct) : base("OpcMethodTest", testserverUrl, maxShortWaitSec, maxLongWaitSec, ct)
        {
            string logPrefix = $"{_logClassPrefix}:OpcMethodTest:";
            Logger.Information($"{logPrefix} Publisher URL: {PublisherUrl}");
            //_testserverUrl = testserverUrl;

            _application = new ApplicationInstance
            {
                ApplicationName = "UA Core Sample Client",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "Opc.Ua.SampleClient"
            };

            // load the application configuration.
            _config = _application.LoadApplicationConfiguration(false).Result;

            // check the application certificate.
            bool haveAppCertificate = _application.CheckApplicationInstanceCertificate(false, 0).Result;
            if (!haveAppCertificate)
            {
                Logger.Fatal($"{logPrefix} Application instance certificate invalid!");
                throw new Exception("Application instance certificate invalid!");
            }

            if (haveAppCertificate)
            {
                _config.ApplicationUri = Utils.GetApplicationUriFromCertificate(_config.SecurityConfiguration.ApplicationCertificate.Certificate);
                if (_config.SecurityConfiguration.AutoAcceptUntrustedCertificates)
                {
                    AutoAccept = true;
                }
                _config.CertificateValidator.CertificateValidation += new CertificateValidationEventHandler(CertificateValidator_CertificateValidation);
            }
            else
            {
                Logger.Warning($"{logPrefix} Missing application certificate, using unsecure connection.");
            }

            Logger.Information($"{logPrefix} Discover endpoints of {PublisherUrl}.");
            _selectedEndpoint = CoreClientUtils.SelectEndpoint(PublisherUrl, haveAppCertificate, 15000);
            Logger.Information($"{logPrefix} Selected endpoint uses: {0}",
                _selectedEndpoint.SecurityPolicyUri.Substring(_selectedEndpoint.SecurityPolicyUri.LastIndexOf('#') + 1));

            Logger.Information($"{logPrefix} Create a session with OPC UA server.");
            _endpointConfiguration = EndpointConfiguration.Create(_config);
            _configuredEndpoint = new ConfiguredEndpoint(null, _selectedEndpoint, _endpointConfiguration);

            // create session
            _session = Session.Create(_config, _configuredEndpoint, false, "OPC UA Console Client", 60000, new UserIdentity(new AnonymousIdentityToken()), null).Result;
            _session.KeepAlive += Client_KeepAlive;

            Logger.Information($"{logPrefix} Browse the OPC UA server namespace.");
            ReferenceDescriptionCollection references;
            Byte[] continuationPoint;

            references = _session.FetchReferences(ObjectIds.ObjectsFolder);

            _session.Browse(
                null,
                null,
                ObjectIds.ObjectsFolder,
                0u,
                BrowseDirection.Forward,
                ReferenceTypeIds.HierarchicalReferences,
                true,
                (uint)NodeClass.Variable | (uint)NodeClass.Object | (uint)NodeClass.Method,
                out continuationPoint,
                out references);

            Logger.Information($"{logPrefix} DisplayName, BrowseName, NodeClass");
            foreach (var rd in references)
            {
                Logger.Information($"{logPrefix} {rd.DisplayName}, {rd.BrowseName}, {rd.NodeClass}");
                ReferenceDescriptionCollection nextRefs;
                byte[] nextCp;
                _session.Browse(
                    null,
                    null,
                    ExpandedNodeId.ToNodeId(rd.NodeId, _session.NamespaceUris),
                    0u,
                    BrowseDirection.Forward,
                    ReferenceTypeIds.HierarchicalReferences,
                    true,
                    (uint)NodeClass.Variable | (uint)NodeClass.Object | (uint)NodeClass.Method,
                    out nextCp,
                    out nextRefs);

                foreach (var nextRd in nextRefs)
                {
                    Logger.Information($"{logPrefix}    + {nextRd.DisplayName}, {nextRd.BrowseName}, {nextRd.NodeClass}");
                }

                _publishingIntervalSec = maxLongWaitSec;
                Logger.Information($"{logPrefix} Create a subscription with publishing interval of {_publishingIntervalSec} second.");
                var subscription = new Subscription(_session.DefaultSubscription) { PublishingInterval = _publishingIntervalSec * 1000 };

                Logger.Information($"{logPrefix} Add a list of items (server current time and status) to the subscription.");
                var list = new List<MonitoredItem> {
                new MonitoredItem(subscription.DefaultItem)
                {
                    DisplayName = "ServerStatusCurrentTime", StartNodeId = "i="+Variables.Server_ServerStatus_CurrentTime.ToString()
                }
            };
                _lastTimestamp = new DateTime(0);
                list.ForEach(i => i.Notification += OnNotification);
                subscription.AddItems(list);

                Logger.Information($"{logPrefix} Add the subscription to the session.");
                _session.AddSubscription(subscription);
                subscription.Create();

                //await Task.Delay(-1, ct);
                //subscription.Delete(true);
            }
        }

        //
        // Method to add adhoc tests running in exclusive mode.
        //
        protected override void AdhocTest()
        {
        }

        //private async Task PublishingIntervalAccuracyAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        //{
        //    string logPrefix = $"{_logClassPrefix}:PublishingIntervalAccuracyAsync:";
        //    _publishingIntervalSec = maxLongWaitSec;
        //    Logger.Information($"{logPrefix} Create a subscription with publishing interval of {_publishingIntervalSec} second.");
        //    var subscription = new Subscription(_session.DefaultSubscription) { PublishingInterval = _publishingIntervalSec * 1000 };

        //    Logger.Information("OpcMethod:Connection test: Add a list of items (server current time and status) to the subscription.");
        //    var list = new List<MonitoredItem> {
        //        new MonitoredItem(subscription.DefaultItem)
        //        {
        //            DisplayName = "ServerStatusCurrentTime", StartNodeId = "i="+Variables.Server_ServerStatus_CurrentTime.ToString()
        //        }
        //    };
        //    _lastTimestamp = new DateTime(0);
        //    list.ForEach(i => i.Notification += OnNotification);
        //    subscription.AddItems(list);

        //    Logger.Information($"{logPrefix} Add the subscription to the session.");
        //    _session.AddSubscription(subscription);
        //    subscription.Create();
        //    await Task.Delay(-1, ct);
        //    subscription.Delete(true);
        //}

        private async Task PublishNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            string logPrefix = $"{_logClassPrefix}:PublishNodesLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!ct.IsCancellationRequested)
            {
                // publish all nodes.
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                for (int i = 0; i < _testServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    PublishOneNode(_testServerNodeIds[i]);
                }
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);

                // publish nodes randomly selected.
                for (int i = 0; i < _testServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(_testServerNodeIds.Count * random.NextDouble());
                    PublishOneNode(_testServerNodeIds[nodeIndex]);
                }
                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task UnpublishNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishNodesLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!ct.IsCancellationRequested)
            {
                // unpublish nodes randomly selected
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                for (int i = 0; i < _testServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(_testServerNodeIds.Count * random.NextDouble());
                    UnpublishOneNode(_testServerNodeIds[nodeIndex]);
                }
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);

                // unpublish all nodes
                for (int i = 0; i < _testServerExpandedNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    UnpublishOneNode(_testServerExpandedNodeIds[i]);
                }

                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        //private async Task GetListOfPublishedNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        //{
        //    string logPrefix = $"{_logClassPrefix}:GetListOfPublishedNodesLoopAsync:";
        //    Random random = new Random();
        //    int iteration = 0;

        //    VariantCollection inputArgumentsTestserver = new VariantCollection()
        //    {
        //        _testserverUrl
        //    };

        //    while (!ct.IsCancellationRequested)
        //    {
        //        for (int i = 0; i < _testServerNodeIds.Count && !ct.IsCancellationRequested; i++)
        //        {
        //            Logger.Information($"{logPrefix} Iteration {iteration++} started");
        //            try
        //            {
        //                CallMethodRequestCollection requests = new CallMethodRequestCollection();
        //                CallMethodResultCollection results;
        //                DiagnosticInfoCollection diagnosticInfos = null;
        //                CallMethodRequest request = new CallMethodRequest
        //                {
        //                    ObjectId = new NodeId("Methods", 2),
        //                    MethodId = new NodeId("GetPublishedNodes", 2),
        //                };
        //                request.InputArguments = inputArgumentsTestserver;
        //                requests.Add(request);
        //                ResponseHeader responseHeader = _session.Call(null, requests, out results, out diagnosticInfos);
        //                if (StatusCode.IsBad(results[0].StatusCode))
        //                {
        //                    Logger.Warning($"{logPrefix} call was not successfull (status: '{results[0].StatusCode}'");
        //                }
        //                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
        //            }
        //            catch (Exception e)
        //            {
        //                Logger.Fatal(e, $"{logPrefix} Exception");
        //            }
        //            Logger.Information($"OpcMethod:GetListOfPublishedNodesLoopAsync Iteration {iteration++} completed");
        //        }

        //        // delay and check if we should stop.
        //        await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
        //    }
        //}

        //private bool PublishOneNode(NodeIdInfo nodeIdInfo, CancellationToken ct, string endpointUrl = null)
        //{
        //    return PublishOneNode(nodeIdInfo);
        //}

        protected override bool PublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl = null)
        {
            bool result = true;
            foreach (var nodeIdInfo in nodeIdInfos)
            {
                result &= PublishOneNode(nodeIdInfo);
            }
            return result;
        }

        private bool PublishOneNode(NodeIdInfo nodeIdInfo)
        {
            string logPrefix = $"{_logClassPrefix}:PublishOneNode:";
            try
            {
                int retryCount = 0;
                int maxRetries = 3;
                while (retryCount < maxRetries)
                {
                    VariantCollection inputArguments = new VariantCollection()
                {
                    nodeIdInfo.Id,
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

                    if (results[0].StatusCode == StatusCodes.BadSessionNotActivated)
                    {
                        retryCount++;
                        Logger.Warning($"{logPrefix} need to retry to publish node, since session is not yet activated (nodeId: '{nodeIdInfo.Id}', retry: '{retryCount}')");
                        Task.Delay(_maxShortWaitSec * 1000).Wait();
                        continue;
                    }
                    if (!nodeIdInfo.Published && StatusCode.IsBad(results[0].StatusCode))
                    {
                        Logger.Warning($"{logPrefix} failed (nodeId: '{nodeIdInfo.Id}', published: '{nodeIdInfo.Published}')");
                        return false;
                    }
                    else
                    {
                        nodeIdInfo.Published = true;
                        Logger.Verbose($"{logPrefix} succeeded (nodeId: '{nodeIdInfo.Id}', error: '{results[0].StatusCode}'");
                        return true;
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            Logger.Warning($"{logPrefix} failed (nodeId: '{nodeIdInfo.Id}', published: '{nodeIdInfo.Published}')");
            return false;
        }

        protected override bool UnpublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl)
        {
            bool result = true;
            foreach (var nodeInfo in nodeIdInfos)
            {
                result &= UnpublishOneNode(nodeInfo);
            }
            return result;
        }

        private bool UnpublishOneNode(NodeIdInfo nodeIdInfo, string endpointUrl = null)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishOneNode:";
            try
            {
                VariantCollection inputArguments = new VariantCollection()
                {
                    nodeIdInfo.Id,
                    endpointUrl ?? _testserverUrl
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
                if (nodeIdInfo.Published && StatusCode.IsBad(results[0].StatusCode))
                {
                    Logger.Warning($"{logPrefix} failed (nodeId: '{nodeIdInfo.Id}', published: '{nodeIdInfo.Published}')");
                    return false;
                }
                else
                {
                    nodeIdInfo.Published = false;
                    Logger.Verbose($"{logPrefix} succeeded (nodeId: '{nodeIdInfo.Id}', error: '{results[0].StatusCode}'");
                    return true;
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            return false;
        }

        protected override PublishedNodesCollection GetPublishedNodesLegacy(string endpointUrl, CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetPublishedNodesLegacy:";
            PublishedNodesCollection nodeList = null;

            VariantCollection inputArgumentsTestserver = new VariantCollection()
            {
                ""
            };

            try
            {
                CallMethodRequestCollection requests = new CallMethodRequestCollection();
                CallMethodResultCollection results = new CallMethodResultCollection();
                DiagnosticInfoCollection diagnosticInfos = null;
                CallMethodRequest request = new CallMethodRequest
                {
                    ObjectId = new NodeId("Methods", 2),
                    MethodId = new NodeId("GetPublishedNodes", 2),
                };
                request.InputArguments = inputArgumentsTestserver;
                requests.Add(request);
                try
                {
                    ResponseHeader responseHeader = _session.Call(null, requests, out results, out diagnosticInfos);
                }
                catch (Exception e)
                {
                    Logger.Fatal(e, $"{logPrefix} Exception");
                }
                if (StatusCode.IsBad(results[0].StatusCode))
                {
                    Logger.Warning($"{logPrefix} call was not successfull (status: '{results[0].StatusCode}'");
                }
                else
                {
                    if (results?[0]?.OutputArguments.Count == 1)
                    {
                        string stringResult = results[0].OutputArguments[0].ToString();
                        int jsonStartIndex = stringResult.IndexOf("[");
                        int jsonEndIndex = stringResult.LastIndexOf("]");
                        nodeList = JsonConvert.DeserializeObject<PublishedNodesCollection>(stringResult.Substring(jsonStartIndex, jsonEndIndex - jsonStartIndex + 1));
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            return nodeList;
        }

        protected override List<NodeModel> GetConfiguredNodesOnEndpoint(string endpointUrl, CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:GetConfiguredNodesOnEndpoint:";
            Random random = new Random();
            List<NodeModel> nodeList = new List<NodeModel>();

            try
            {
                PublishedNodesCollection publishedNodes = GetPublishedNodesLegacy(endpointUrl, ct);

                foreach (var publishedNode in publishedNodes)
                {
                    NodeModel node = new NodeModel(publishedNode.NodeID.ToString());
                    nodeList.Add(node);
                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            return nodeList;
        }
        protected override bool UnpublishAllConfiguredNodes(CancellationToken ct)
        {
            string logPrefix = $"{_logClassPrefix}:UnpublishAllConfiguredNodes:";
            List<PublisherConfigurationFileEntryLegacyModel> configFileEntries = null;
            bool result = true;

            VariantCollection inputArgumentsTestserver = new VariantCollection()
            {
                _testserverUrl
            };

            try
            {
                CallMethodRequestCollection requests = new CallMethodRequestCollection();
                CallMethodResultCollection results = new CallMethodResultCollection();
                DiagnosticInfoCollection diagnosticInfos = null;
                CallMethodRequest request = new CallMethodRequest
                {
                    ObjectId = new NodeId("Methods", 2),
                    MethodId = new NodeId("GetPublishedNodes", 2),
                };
                request.InputArguments = inputArgumentsTestserver;
                requests.Add(request);
                try
                {
                    ResponseHeader responseHeader = _session.Call(null, requests, out results, out diagnosticInfos);
                }
                catch (Exception e)
                {
                    Logger.Fatal(e, $"{logPrefix} Exception");
                }
                if (StatusCode.IsBad(results[0].StatusCode))
                {
                    Logger.Warning($"{logPrefix} call was not successfull (status: '{results[0].StatusCode}'");
                }
                else
                {
                    if (results?[0]?.OutputArguments.Count == 1)
                    {
                        string stringResult = results[0].OutputArguments[0].ToString();
                        int jsonStartIndex = stringResult.IndexOf("[");
                        int jsonEndIndex = stringResult.IndexOf("]");
                        //PublishedNodesCollection nodelist = JsonConvert.DeserializeObject<PublishedNodesCollection>(stringResult.Substring(jsonStartIndex, jsonEndIndex - jsonStartIndex + 1));
                        //foreach (NodeLookup node in nodelist)
                        //{
                        //    publishedNodes.Add(node.NodeID.ToString());
                        //}

                        configFileEntries = JsonConvert.DeserializeObject<List<PublisherConfigurationFileEntryLegacyModel>>(stringResult.Substring(jsonStartIndex, jsonEndIndex - jsonStartIndex + 1));

                        foreach (var configFileEntry in configFileEntries)
                        {
                            if (configFileEntry.OpcNodes == null)
                            {
                                result &= UnpublishOneNode(new NodeIdInfo(configFileEntry.NodeId.ToString()));
                            }
                            else
                            {
                                foreach (var node in configFileEntry.OpcNodes)
                                {
                                    result &= UnpublishOneNode(new NodeIdInfo(node.Id), configFileEntry.EndpointUrl.AbsoluteUri);
                                }
                            }
                        }
                    }

                }
            }
            catch (Exception e)
            {
                Logger.Fatal(e, $"{logPrefix} Exception");
            }
            return result;
        }

        private void Client_KeepAlive(Session sender, KeepAliveEventArgs e)
        {
            string logPrefix = $"{_logClassPrefix}:Client_KeepAlive:";
            if (e.Status != null && ServiceResult.IsNotGood(e.Status))
            {
                Logger.Verbose($"{logPrefix} {0} {1}/{2}", e.Status, sender.OutstandingRequestCount, sender.DefunctRequestCount);

                if (_reconnectHandler == null)
                {
                    Logger.Verbose("${logPrefix}--- RECONNECTING ---");
                    _reconnectHandler = new SessionReconnectHandler();
                    _reconnectHandler.BeginReconnect(sender, _reconnectPeriod * 1000, Client_ReconnectComplete);
                }
            }
        }

        private void Client_ReconnectComplete(object sender, EventArgs e)
        {
            string logPrefix = $"{_logClassPrefix}:Client_ReconnectComplete:";
            // ignore callbacks from discarded objects.
            if (!Object.ReferenceEquals(sender, _reconnectHandler))
            {
                return;
            }

            _session = _reconnectHandler.Session;
            _reconnectHandler.Dispose();
            _reconnectHandler = null;

            Logger.Verbose($"{logPrefix}--- RECONNECTED ---");
        }

        private static void OnNotification(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            string logPrefix = $"{_logClassPrefix}:SingleNodeIdTest:";
            foreach (var value in item.DequeueValues())
            {
                if (_lastTimestamp != new DateTime(0) && value.ServerTimestamp - _lastTimestamp > TimeSpan.FromSeconds(_publishingIntervalSec * 1.3))
                {
                    Logger.Warning($"{logPrefix}{item.DisplayName} publishing is too late: diff: {value.ServerTimestamp - _lastTimestamp}, max: {_publishingIntervalSec * 1.3}, serverTimestamp: {value.ServerTimestamp}, lastTimestamp: {_lastTimestamp}");
                }
                _lastTimestamp = value.ServerTimestamp;
            }
        }

        private static void OnMultiNotification(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            string logPrefix = $"{_logClassPrefix}:OnMultiNotification:";
            Logger.Information($"{logPrefix}");
        }

        private static void CertificateValidator_CertificateValidation(CertificateValidator validator, CertificateValidationEventArgs e)
        {
            string logPrefix = $"{_logClassPrefix}:CertificateValidator_CertificateValidation:";
            if (e.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                e.Accept = AutoAccept;
                if (AutoAccept)
                {
                    Logger.Verbose($"{logPrefix} Accepted Certificate: {0}", e.Certificate.Subject);
                }
                else
                {
                    Logger.Warning($"{logPrefix} Rejected Certificate: {0}", e.Certificate.Subject);
                }
            }
        }

        private static string _logClassPrefix = "OpcMethodTest";
        private static int _publishingIntervalSec;
        private static DateTime _lastTimestamp;
        private const int _reconnectPeriod = 10;
        private SessionReconnectHandler _reconnectHandler;
        private ApplicationInstance _application;
        private ApplicationConfiguration _config;
        private EndpointDescription _selectedEndpoint;
        private EndpointConfiguration _endpointConfiguration;
        private ConfiguredEndpoint _configuredEndpoint;
        private Session _session;
    }
}
