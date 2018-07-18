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
    using System.Linq;
    using static Program;

    public class OpcMethodTest
    {
        public static bool AutoAccept = false;
        public static string PublisherUrl = "opc.tcp://publisher:62222/UA/Publisher";

        public OpcMethodTest(string testserverUrl)
        {
            Logger.Information($"Publisher URL: {PublisherUrl}");
            _testserverUrl = testserverUrl;

            // create NodeId names for the test
            List<string> simulationNodePostfix = new List<string>
            {

                "Boolean",
                "Byte",
                "ByteString",
                "DateTime",
                "Double",
                "Duration",
                "Float",
                "Guid",
                "Int16",
                "Int32",
                "Int64",
                "Integer",
                "LocaleId",
                "LocalizedText",
                "NodeId",
                "Number",
                "QualifiedName",
                "SByte",
                "String",
                "Time",
                "UInt16",
                "UInt32",
                "UInt64",
                "UInteger",
                "UtcTime",
                "Variant",
                "XmlElement"
            };

            _testServerDataTypeExpandedNodeIds = new List<NodeIdInfo>();
            foreach (var postfix in simulationNodePostfix)
            {
                _testServerDataTypeExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_{postfix}"));
                _testServerDataTypeExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_Arrays_{postfix}"));
                _testServerDataTypeExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_Mass_{postfix}"));
            };

            _testServerMultiTagExpandedNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TEST_TAG_NUM; i++)
            {
                _testServerMultiTagExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=MultiTag_Integer{i:D7}"));
            }

            _testServerNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TEST_TAG_NUM; i++)
            {
                _testServerNodeIds.Add(new NodeIdInfo($"ns=2;s=OpcMethodTest_Integer{i:D7}"));
            }

            _testServerExpandedNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TEST_TAG_NUM; i++)
            {
                _testServerExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=OpcMethodTest_Integer{i:D7}"));
            }

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
                Logger.Fatal("Application instance certificate invalid!");
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
                Logger.Warning("Missing application certificate, using unsecure connection.");
            }

            Logger.Information($"Discover endpoints of {PublisherUrl}.");
            _selectedEndpoint = CoreClientUtils.SelectEndpoint(PublisherUrl, haveAppCertificate, 15000);
            Logger.Information("Selected endpoint uses: {0}",
                _selectedEndpoint.SecurityPolicyUri.Substring(_selectedEndpoint.SecurityPolicyUri.LastIndexOf('#') + 1));

            Logger.Information("Create a session with OPC UA server.");
            _endpointConfiguration = EndpointConfiguration.Create(_config);
            _configuredEndpoint = new ConfiguredEndpoint(null, _selectedEndpoint, _endpointConfiguration);

            // create session
            _session = Session.Create(_config, _configuredEndpoint, false, "OPC UA Console Client", 60000, new UserIdentity(new AnonymousIdentityToken()), null).Result;
            _session.KeepAlive += Client_KeepAlive;

            Logger.Information("Browse the OPC UA server namespace.");
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

            Logger.Information(" DisplayName, BrowseName, NodeClass");
            foreach (var rd in references)
            {
                Logger.Information($" {rd.DisplayName}, {rd.BrowseName}, {rd.NodeClass}");
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
                    Logger.Information($"   + {nextRd.DisplayName}, {nextRd.BrowseName}, {nextRd.NodeClass}");
                }
            }
        }

        public void RunExclusiveTests(CancellationToken ct)
        {
            DataTypeTest(ct, MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC);
//            MultiTagTest(ct, MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC);
            AddRemoveTestAsync(ct, MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC);

            return;
        }

        public async Task<List<Task>> RunTestsAsync(CancellationToken ct)
        {
            Logger.Information("Start OPC method tests");
            var publisherTests = new List<Task>
            {
                Task.Run(async () => await PublishingIntervalAccuracyAsync(ct, MAX_LONG_WAIT_SEC, MAX_LONG_WAIT_SEC)),
                Task.Run(async () => await PublishNodesLoopAsync(ct, MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC)),
                Task.Run(async () => await UnpublishNodesLoopAsync(ct, MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC)),
                Task.Run(async () => await GetListOfPublishedNodesLoopAsync(ct, MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC))
            };
            return publisherTests;
        }

        private void DataTypeTest(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Random random = new Random();

            // publish all nodes with different data types
            Logger.Information($"DataTypeTest started");
            for (int i = 0; i < _testServerDataTypeExpandedNodeIds.Count && !ct.IsCancellationRequested; i++)
            {
                PublishOneNode(_testServerDataTypeExpandedNodeIds[i]);
            }
            Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);

            // unpublish them
            for (int i = 0; i < _testServerDataTypeExpandedNodeIds.Count && !ct.IsCancellationRequested; i++)
            {
                UnpublishOneNode(_testServerDataTypeExpandedNodeIds[i]);
            }
            Logger.Information($"DataTypeTest completed");

            // delay and check if we should stop.
            Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
        }

        private void MultiTagTest(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Random random = new Random();

            // publish all nodes with different data types
            Logger.Information($"MultiTagTest started");
            DateTime startTime = DateTime.Now;
            for (int i = 0; i < _testServerMultiTagExpandedNodeIds.Count && !ct.IsCancellationRequested; i++)
            {
                PublishOneNode(_testServerMultiTagExpandedNodeIds[i]);
            }
            TimeSpan elapsedTime = DateTime.Now - startTime;
            Logger.Information($"MultiTagTest publishing {_testServerMultiTagExpandedNodeIds.Count} took {elapsedTime.TotalMilliseconds} ms");
            Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);

            // unpublish them
            for (int i = 0; i < _testServerMultiTagExpandedNodeIds.Count && !ct.IsCancellationRequested; i++)
            {
                UnpublishOneNode(_testServerMultiTagExpandedNodeIds[i]);
            }

            Logger.Information($"MultiTagTest completed");

            // delay and check if we should stop.
            Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
        }

        private async Task PublishingIntervalAccuracyAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            _publishingIntervalSec = maxLongWaitSec;
            Logger.Information($"Connection test: Create a subscription with publishing interval of {_publishingIntervalSec} second.");
            var subscription = new Subscription(_session.DefaultSubscription) { PublishingInterval = _publishingIntervalSec * 1000 };

            Logger.Information("Connection test: Add a list of items (server current time and status) to the subscription.");
            var list = new List<MonitoredItem> {
                new MonitoredItem(subscription.DefaultItem)
                {
                    DisplayName = "ServerStatusCurrentTime", StartNodeId = "i="+Variables.Server_ServerStatus_CurrentTime.ToString()
                }
            };
            _lastTimestamp = new DateTime(0);
            list.ForEach(i => i.Notification += OnNotification);
            subscription.AddItems(list);

            Logger.Information("Connection test: Add the subscription to the session.");
            _session.AddSubscription(subscription);
            subscription.Create();
            await Task.Delay(-1, ct);
            subscription.Delete(true);
        }

        private async Task PublishNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Random random = new Random();
            int iteration = 0;

            while (!ct.IsCancellationRequested)
            {
                // publish all nodes.
                Logger.Information($"PublishNodesLoopAsync Iteration {iteration++} started");
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
                    await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000), ct);
                }
                Logger.Information($"PublishNodesLoopAsync Iteration {iteration++} completed");

                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
            }
        }

        private async Task UnpublishNodesLoopAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Random random = new Random();
            int iteration = 0;

            while (!ct.IsCancellationRequested)
            {
                // unpublish nodes randomly selected
                Logger.Information($"UnpublishNodesLoopAsync Iteration {iteration++} started");
                for (int i = 0; i < _testServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(_testServerNodeIds.Count * random.NextDouble());
                    UnpublishOneNode(_testServerNodeIds[nodeIndex]);
                    await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000), ct);
                }
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);

                // unpublish all nodes
                for (int i = 0; i < _testServerExpandedNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    UnpublishOneNode(_testServerExpandedNodeIds[i]);
                }
                Logger.Information($"UnpublishNodesLoopAsync Iteration {iteration++} completed");

                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
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

            while (!ct.IsCancellationRequested)
            {
                Logger.Information($"GetListOfPublishedNodesLoopAsync Iteration {iteration++} started");
                for (int i = 0; i < _testServerNodeIds.Count && !ct.IsCancellationRequested; i++)
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
                        await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000), ct);
                    }
                    catch (Exception e)
                    {
                        Logger.Fatal(e, $"Exception in GetListOfPublishedNodesLoopAsync");
                    }
                }
                Logger.Information($"GetListOfPublishedNodesLoopAsync Iteration {iteration++} completed");

                // delay and check if we should stop.
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);
            }
        }


        private void AddRemoveTestAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
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
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // publish all nodes with NodeId and validate result
            Logger.Information($"Publish all nodes with NodeId");
            foreach (var node in _testServerNodeIds)
            {
                PublishOneNode(node);
            }
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _testServerNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_testServerNodeIds.Count} nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // publish nodes with NodeId one at a time and validate result
            Logger.Information($"Publish nodes with NodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _testServerNodeIds)
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
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _testServerNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_testServerNodeIds.Count} nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // unpublish nodes with NodeId one at a time and validate result
            Logger.Information($"Unpublish nodes with NodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _testServerNodeIds)
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
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // publish all nodes with ExpandedNodeId and validate result
            Logger.Information($"Publish all nodes with ExpandedNodeId");
            foreach (var node in _testServerExpandedNodeIds)
            {
                PublishOneNode(node);
            }
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _testServerExpandedNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_testServerExpandedNodeIds.Count} nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // publish nodes with ExpandedNodeId one at a time and validate result
            Logger.Information($"Publish nodes with ExpandedNodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _testServerExpandedNodeIds)
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
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _testServerExpandedNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_testServerExpandedNodeIds.Count} nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // unpublish nodes with ExpandedNodeId one at a time and validate result
            Logger.Information($"Unpublish nodes with ExpandedNodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _testServerExpandedNodeIds)
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
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and validate result
            Logger.Information($"Publish all nodes with NodeId (first) and ExpandedNodeId (second)");
            publishCount = unpublishCount = currentCount = 0;
            foreach (var node in _testServerNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            foreach (var node in _testServerExpandedNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _testServerNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_testServerNodeIds.Count} nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and validate result
            Logger.Information($"Publish nodes with NodeId (first) and ExpandedNodeId (second) one at a time and unpublish by NodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            for (int i = 0; i < _testServerNodeIds.Count; i++)
            {
                if (PublishOneNode(_testServerNodeIds[i]))
                {
                    publishCount++;
                }
                if (PublishOneNode(_testServerExpandedNodeIds[i]))
                {
                    publishCount++;
                }
            }
            Task.Delay(delay);
            for (int i = 0; i < _testServerNodeIds.Count; i++)
            {
                if (UnpublishOneNode(_testServerNodeIds[i]))
                {
                    unpublishCount++;
                }
            }
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and unpublish by ExpandedNodeId and validate result
            Logger.Information($"Publish nodes with NodeId (first) and ExpandedNodeId (second) one at a time and unpublish by ExtendedNodeId one at a time");
            publishCount = unpublishCount = currentCount = 0;
            for (int i = 0; i < _testServerNodeIds.Count; i++)
            {
                if (PublishOneNode(_testServerNodeIds[i]))
                {
                    publishCount++;
                }
                if (PublishOneNode(_testServerExpandedNodeIds[i]))
                {
                    publishCount++;
                }
            }
            Task.Delay(delay);
            for (int i = 0; i < _testServerExpandedNodeIds.Count; i++)
            {
                if (UnpublishOneNode(_testServerExpandedNodeIds[i]))
                {
                    unpublishCount++;
                }
            }
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // publish all nodes with NodeId (second) and ExpandedNodeId (first) and validate result
            Logger.Information($"Publish all nodes with NodeId (second) and ExpandedNodeId (first)");
            foreach (var node in _testServerExpandedNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            foreach (var node in _testServerNodeIds)
            {
                if (PublishOneNode(node))
                {
                    publishCount++;
                }
            }
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _testServerExpandedNodeIds.Count)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published! Expected {_testServerExpandedNodeIds.Count} nodes published");
            }
            if (ct.IsCancellationRequested)
            {
                return;
            }

            // cleanup published nodes and validate result
            Logger.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                Logger.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }
            Task.Delay(delay);
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
            foreach (var node in _testServerNodeIds)
            {
                UnpublishOneNode(node, true);
            }
            Logger.Verbose($"UnpublishAllNodeIdNodes completed");
        }

        private void UnpublishAllExpandedNodeIdNodes()
        {
            // unpublish all longhaul ExpandedNodeId nodes.
            foreach (var node in _testServerExpandedNodeIds)
            {
                UnpublishOneNode(node, true);
            }
            Logger.Verbose($"UnpublishAllExpandedNodeIdNodes completed");
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
                    Logger.Fatal(e, $"Exception in method call in GetNumberOfPublishedNodes");
                }
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
                        result = nodelist.Where( n => n.NodeID.Identifier.ToString().Contains("OpcMethodTest")).Count();
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
                if (_lastTimestamp != new DateTime(0) && value.ServerTimestamp - _lastTimestamp > TimeSpan.FromSeconds(_publishingIntervalSec * 1.3))
                {
                    Logger.Warning($"{item.DisplayName} publishing is too late: diff: {value.ServerTimestamp - _lastTimestamp}, max: {_publishingIntervalSec * 1.3}, serverTimestamp: {value.ServerTimestamp}, lastTimestamp: {_lastTimestamp}");
                }
                _lastTimestamp = value.ServerTimestamp;
            }
        }

        private static void OnMultiNotification(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            Logger.Information("OnMultiNotification build");
        }

        private static void CertificateValidator_CertificateValidation(CertificateValidator validator, CertificateValidationEventArgs e)
        {
            if (e.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                e.Accept = AutoAccept;
                if (AutoAccept)
                {
                    Logger.Verbose("Accepted Certificate: {0}", e.Certificate.Subject);
                }
                else
                {
                    Logger.Warning("Rejected Certificate: {0}", e.Certificate.Subject);
                }
            }
        }
        private List<NodeIdInfo> _testServerDataTypeExpandedNodeIds = null;
        private List<NodeIdInfo> _testServerMultiTagExpandedNodeIds = null;
        private List<NodeIdInfo> _testServerNodeIds = null;
        private List<NodeIdInfo> _testServerExpandedNodeIds = null;
        private static int _publishingIntervalSec;
        private static DateTime _lastTimestamp;
        private const int _reconnectPeriod = 10;
        private SessionReconnectHandler _reconnectHandler;
        private int _clientRunTime = Timeout.Infinite;
        private string _testserverUrl;
        private ApplicationInstance _application;
        private ApplicationConfiguration _config;
        private EndpointDescription _selectedEndpoint;
        private EndpointConfiguration _endpointConfiguration;
        private ConfiguredEndpoint _configuredEndpoint;
        private Session _session;
    }
}
