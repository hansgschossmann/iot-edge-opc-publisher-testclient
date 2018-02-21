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

using Microsoft.Extensions.Logging;
using Mono.Options;
using Newtonsoft.Json;
using Opc.Ua;
using Opc.Ua.Client;
using Opc.Ua.Configuration;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NetCoreConsoleClient
{

    public enum ExitCode : int
    {
        Ok = 0,
        ErrorCreateApplication = 0x11,
        ErrorDiscoverEndpoints = 0x12,
        ErrorCreateSession = 0x13,
        ErrorBrowseNamespace = 0x14,
        ErrorCreateSubscription = 0x15,
        ErrorMonitoredItem = 0x16,
        ErrorAddSubscription = 0x17,
        ErrorRunning = 0x18,
        ErrorNoKeepAlive = 0x30,
        ErrorInvalidCommandLine = 0x100
    };

    public class Program
    {
        public static int Main(string[] args)
        {
            _log = new LoggerConfiguration()
                .WriteTo.Console()
                .MinimumLevel.Debug()
                .CreateLogger();

            _log.Information($"{(Utils.IsRunningOnMono() ? "Mono" : ".Net Core")} OPC UA Console Client sample");

            // command line options
            bool showHelp = false;
            int stopTimeout = Timeout.Infinite;
            bool autoAccept = false;
            bool verboseLog = false;

            Mono.Options.OptionSet options = new Mono.Options.OptionSet {
                { "h|help", "show this message and exit", h => showHelp = h != null },
                { "a|autoaccept", "auto accept certificates (for testing only)", a => autoAccept = a != null },
                { "t|timeout=", "the number of seconds until the client stops.", (int t) => stopTimeout = t },
                { "v|verbose", "verbose output.", b => verboseLog = b != null }
            };

            IList<string> extraArgs = null;
            try
            {
                extraArgs = options.Parse(args);
                if (extraArgs.Count > 2)
                {
                    foreach (string extraArg in extraArgs)
                    {
                        _log.Error("Error: Unknown option: {0}", extraArg);
                        showHelp = true;
                    }
                }
            }
            catch (OptionException e)
            {
                _log.Fatal(e, $"Exception in Main");
                showHelp = true;
            }

            if (showHelp)
            {
                // show some app description message
                _log.Information(Utils.IsRunningOnMono() ?
                    "Usage: mono MonoConsoleClient.exe [OPTIONS] [PUBLISHERENDPOINTURL] [TESTSERVERENDPOINTURL]" :
                    "Usage: dotnet NetCoreConsoleClient.dll [OPTIONS] [PUBLISHERENDPOINTURL] [TESTSERVERENDPOINTURL]");
                _log.Information("");

                // output the options
                _log.Information("Options:");
                StringBuilder stringBuilder = new StringBuilder();
                StringWriter stringWriter = new StringWriter(stringBuilder);
                options.WriteOptionDescriptions(stringWriter);
                string[] helpLines = stringBuilder.ToString().Split("\r\n");
                foreach (var line in helpLines)
                {
                    _log.Information(line);
                }
                return (int)ExitCode.ErrorInvalidCommandLine;
            }

            // by default we are connecting to the OPC UA servers in the testbed 
            string publisherEndpointUrl = "opc.tcp://publisher:62222/UA/Publisher";
            string testserverEndpointUrl = "opc.tcp://testserver:62541/Quickstarts/ReferenceServer";
            switch (extraArgs.Count)
            {
                case 1:
                    publisherEndpointUrl = extraArgs[0];
                    break;
                case 2:
                    publisherEndpointUrl = extraArgs[0];
                    testserverEndpointUrl = extraArgs[1];
                    break;
            }
            _log.Information("Publisher endpoint URL: {0}", publisherEndpointUrl);
            _log.Information("Testserver endpoint URL: {0}", testserverEndpointUrl);

            MySampleClient client = new MySampleClient(_log, publisherEndpointUrl, testserverEndpointUrl, autoAccept, stopTimeout, verboseLog);
            client.Run();

            return (int)MySampleClient.ExitCode;
        }

        private static Serilog.Core.Logger _log = null;
    }

    public class MySampleClient
    {
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

        private enum TraceLevel
        {
            NoTrace = 0,
            Verbose,
            Info,
            Warning,
            Error
        };

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

        public MySampleClient(Serilog.Core.Logger log, string publisherEndpointURL, string testserverEndpointUrl, bool autoAccept, int stopTimeout, bool verboseLog)
        {
            _log = log;
            _publisherEndpointUrl = publisherEndpointURL;
            _testserverEndpointUrl = testserverEndpointUrl;
            _autoAccept = autoAccept;
            _clientRunTime = stopTimeout <= 0 ? Timeout.Infinite : stopTimeout * 1000;
            _verboseLog = verboseLog;

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
            _referenceServerNodeIds = new List<NodeIdInfo>();
            foreach (var postfix in simulationNodePostfix)
            {
                _referenceServerNodeIds.Add(new NodeIdInfo($"ns=2;s=Scalar_Simulation_{postfix}"));
                _referenceServerNodeIds.Add(new NodeIdInfo($"ns=2;s=Scalar_Simulation_Arrays_{postfix}"));
                _referenceServerNodeIds.Add(new NodeIdInfo($"ns=2;s=Scalar_Simulation_Mass_{postfix}"));
            };
            _referenceServerExpandedNodeIds = new List<NodeIdInfo>();
            foreach (var postfix in simulationNodePostfix)
            {
                _referenceServerExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_{postfix}"));
                _referenceServerExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_Arrays_{postfix}"));
                _referenceServerExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_Mass_{postfix}"));
            };
    }

    public void Run()
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken ct = cts.Token;

            ManualResetEvent quitEvent = new ManualResetEvent(false);
            try
            {
                Console.CancelKeyPress += (sender, eArgs) =>
                {
                    quitEvent.Set();
                    eArgs.Cancel = true;
                };
            }
            catch
            {
            }

            _log.Information($"Run testing {(_clientRunTime != -1 ? $"for {_clientRunTime / 1000} seconds or" : "till")} CTRL-C is pressed");
            Task client = Task.Run( async () => { await ConsoleSampleClientAsync(ct); });

            // wait for timeout or Ctrl-C
            quitEvent.WaitOne(_clientRunTime);
            cts.Cancel();

            // wait for test completion
            _log.Information($"Wait for test completion");
            client.Wait();

            // return error conditions
            if (_session.KeepAliveStopped)
            {
                _exitCode = ExitCode.ErrorNoKeepAlive;
                return;
            }

            _exitCode = ExitCode.Ok;
        }

        public static ExitCode ExitCode => _exitCode; 

        private async Task ConsoleSampleClientAsync(CancellationToken ct)
        {
            _log.Information("Create an Application Configuration.");
            _exitCode = ExitCode.ErrorCreateApplication;

            ApplicationInstance application = new ApplicationInstance
            {
                ApplicationName = "UA Core Sample Client",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = Utils.IsRunningOnMono() ? "Opc.Ua.MonoSampleClient" : "Opc.Ua.SampleClient"
            };

            // load the application configuration.
            ApplicationConfiguration config = await application.LoadApplicationConfiguration(false);

            // check the application certificate.
            bool haveAppCertificate = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!haveAppCertificate)
            {
                _log.Fatal("Application instance certificate invalid!");
                throw new Exception("Application instance certificate invalid!");
            }

            if (haveAppCertificate)
            {
                config.ApplicationUri = Utils.GetApplicationUriFromCertificate(config.SecurityConfiguration.ApplicationCertificate.Certificate);
                if (config.SecurityConfiguration.AutoAcceptUntrustedCertificates)
                {
                    _autoAccept = true;
                }
                config.CertificateValidator.CertificateValidation += new CertificateValidationEventHandler(CertificateValidator_CertificateValidation);
            }
            else
            {
                _log.Warning("Missing application certificate, using unsecure connection.");
            }

            _log.Information($"Discover endpoints of {_publisherEndpointUrl}.");
            _exitCode = ExitCode.ErrorDiscoverEndpoints;
            var selectedEndpoint = CoreClientUtils.SelectEndpoint(_publisherEndpointUrl, haveAppCertificate, 15000);
            _log.Information("Selected endpoint uses: {0}",
                selectedEndpoint.SecurityPolicyUri.Substring(selectedEndpoint.SecurityPolicyUri.LastIndexOf('#') + 1));

            _log.Information("Create a session with OPC UA server.");
            _exitCode = ExitCode.ErrorCreateSession;
            var endpointConfiguration = EndpointConfiguration.Create(config);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            _session = await Session.Create(config, endpoint, false, "OPC UA Console Client", 60000, new UserIdentity(new AnonymousIdentityToken()), null);

            // register keep alive handler
            _session.KeepAlive += Client_KeepAlive;

            _log.Information("Browse the OPC UA server namespace.");
            _exitCode = ExitCode.ErrorBrowseNamespace;
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

            _log.Information(" DisplayName, BrowseName, NodeClass");
            foreach (var rd in references)
            {
                _log.Information($" {rd.DisplayName}, {rd.BrowseName}, {rd.NodeClass}");
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
                    _log.Information($"   + {nextRd.DisplayName}, {nextRd.BrowseName}, {nextRd.NodeClass}");
                }
            }

            _log.Information("Run tests");
            // run add/remove test
            await OpcPublisherMethodsAddRemoveTestAsync();
            // run longhaul test
            var publisherTests = new List<Task>
            {
                Task.Run(async () => await OpcSubscriptionTestAsync(ct, _maxShortWaitSec, _maxLongWaitSec)),
                Task.Run(async () => await OpcPublisherMethodsLonghaulTestAsync(ct, _maxShortWaitSec, _maxLongWaitSec))
            };
            _exitCode = ExitCode.ErrorRunning;
            try
            {
                Task.WaitAll(publisherTests.ToArray());
            }
            catch
            {
            }
            _log.Information($"All tests completed");
        }

        private async Task OpcSubscriptionTestAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            _log.Information($"Subscription test: Create a subscription with publishing interval of {maxLongWaitSec} second.");
            _exitCode = ExitCode.ErrorCreateSubscription;
            var subscription = new Subscription(_session.DefaultSubscription) { PublishingInterval = maxLongWaitSec * 1000 };

            _log.Information("Subscription test: Add a list of items (server current time and status) to the subscription.");
            _exitCode = ExitCode.ErrorMonitoredItem;
            var list = new List<MonitoredItem> {
                new MonitoredItem(subscription.DefaultItem)
                {
                    DisplayName = "ServerStatusCurrentTime", StartNodeId = "i="+Variables.Server_ServerStatus_CurrentTime.ToString()
                }
            };
            _lastTimestamp = new DateTime(0);
            list.ForEach(i => i.Notification += OnNotification);
            subscription.AddItems(list);

            _log.Information("Subscription test: Add the subscription to the session.");
            _exitCode = ExitCode.ErrorAddSubscription;
            _session.AddSubscription(subscription);
            subscription.Create();
            await Task.Delay(-1, ct);
            subscription.Delete(true);
        }

        private async Task OpcPublisherMethodsLonghaulTestAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
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

        private async Task OpcPublisherMethodsAddRemoveTestAsync()
        {
            const int delay = 1000;
            int publishedNodesCount = 0;
            int unpublishCount = 0;
            int publishCount = 0;
            int currentCount = 0;

            publishedNodesCount = GetNumberOfPublishedNodes();
            _log.Information($"{_testserverEndpointUrl} is publishing {publishedNodesCount} node(s).");

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId and validate result
            _log.Information($"Publish all nodes with NodeId");
            foreach (var node in _referenceServerNodeIds)
            {
                PublishOneNode(node);
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerNodeIds.Count)
            {
                _log.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerNodeIds.Count} nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish nodes with NodeId one at a time and validate result
            _log.Information($"Publish nodes with NodeId one at a time");
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
                    _log.Error($"There are {currentCount} nodes published! Expected {publishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerNodeIds.Count)
            {
                _log.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerNodeIds.Count} nodes published");
            }

            // unpublish nodes with NodeId one at a time and validate result
            _log.Information($"Unpublish nodes with NodeId one at a time");
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
                    _log.Error($"There are {currentCount} nodes published! Expected {publishedNodesCount - unpublishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with ExpandedNodeId and validate result
            _log.Information($"Publish all nodes with ExpandedNodeId");
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                PublishOneNode(node);
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerExpandedNodeIds.Count)
            {
                _log.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerExpandedNodeIds.Count} nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }


            // publish nodes with ExpandedNodeId one at a time and validate result
            _log.Information($"Publish nodes with ExpandedNodeId one at a time");
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
                    _log.Error($"There are {currentCount} nodes published! Expected {publishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != _referenceServerExpandedNodeIds.Count)
            {
                _log.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerExpandedNodeIds.Count} nodes published");
            }

            // unpublish nodes with ExpandedNodeId one at a time and validate result
            _log.Information($"Unpublish nodes with ExpandedNodeId one at a time");
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
                    _log.Error($"There are {currentCount} nodes published! Expected {publishedNodesCount - unpublishCount} nodes published");
                }
            }
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and validate result
            _log.Information($"Publish all nodes with NodeId (first) and ExpandedNodeId (second)");
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
                _log.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerNodeIds.Count} nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and validate result
            _log.Information($"Publish nodes with NodeId (first) and ExpandedNodeId (second) one at a time and unpublish by NodeId one at a time");
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
                _log.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (first) and ExpandedNodeId (second) and unpublish by ExpandedNodeId and validate result
            _log.Information($"Publish nodes with NodeId (first) and ExpandedNodeId (second) one at a time and unpublish by NodeId one at a time");
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
                _log.Error($"There are {publishedNodesCount} nodes published! Expected no nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
            }

            // publish all nodes with NodeId (second) and ExpandedNodeId (first) and validate result
            _log.Information($"Publish all nodes with NodeId (second) and ExpandedNodeId (first)");
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
                _log.Error($"There are {publishedNodesCount} nodes published! Expected {_referenceServerExpandedNodeIds.Count} nodes published");
            }

            // cleanup published nodes and validate result
            _log.Information($"Unpublish all nodes");
            UnpublishAllNodes();
            await Task.Delay(delay);
            publishedNodesCount = GetNumberOfPublishedNodes();
            if (publishedNodesCount != 0)
            {
                _log.Error($"There are {publishedNodesCount} nodes published. Expected no nodes published");
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
                    _testserverEndpointUrl
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
                    _log.Warning($"PublishOneNode failed (nodeId: '{nodeIdInfo.NodeId}', published: '{nodeIdInfo.Published}')");
                    return false;
                }
                else
                {
                    nodeIdInfo.Published = true;
                    _log.Verbose($"PublishOneNode succeeded (nodeId: '{nodeIdInfo.NodeId}', error: '{results[0].StatusCode}'");
                    return true;
                }
            }
            catch (Exception e)
            {
                _log.Fatal(e, $"Exception in PublishOneNode");
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
                _log.Information($"PublishNodesLoopAsync Iteration {iteration++} started");
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
                _log.Information($"PublishNodesLoopAsync Iteration {iteration++} completed");

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
                    _testserverEndpointUrl
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
                    _log.Warning($"UnpublishOneNode failed (nodeId: '{nodeIdInfo.NodeId}', published: '{nodeIdInfo.Published}')");
                    return false;
                }
                else
                {
                    nodeIdInfo.Published = false;
                    _log.Verbose($"UnpublishOneNode succeeded (nodeId: '{nodeIdInfo.NodeId}', error: '{results[0].StatusCode}'");
                    return true;
                }
            }
            catch (Exception e)
            {
                _log.Fatal(e, $"Exception in UnpublishOneNode");
            }
            return false;
        }

        private void UnpublishAllNodes()
        {
            // unpublish all nodes.
            UnpublishAllNodeIdNodes();
            UnpublishAllExpandedNodeIdNodes();
            _log.Verbose($"UnpublishAllNodes completed");
        }

        private void UnpublishAllNodeIdNodes()
        {
            // unpublish all longhaul NodeId nodes.
            foreach (var node in _referenceServerNodeIds)
            {
                UnpublishOneNode(node, true);
            }
            _log.Verbose($"UnpublishAllNodeIdNodes completed");
        }

        private void UnpublishAllExpandedNodeIdNodes()
        {
            // unpublish all longhaul ExpandedNodeId nodes.
            foreach (var node in _referenceServerExpandedNodeIds)
            {
                UnpublishOneNode(node, true);
            }
            _log.Verbose($"UnpublishAllExpandedNodeIdNodes completed");
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
                _log.Information($"UnpublishNodes Iteration {iteration++} started");
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
                _log.Information($"UnpublishNodes Iteration {iteration++} completed");

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
                _testserverEndpointUrl
            };

            while (true)
            {
                _log.Information($"GetListOfPublishedNodesLoopAsync Iteration {iteration++} started");
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
                            _log.Warning($"GetListOfPublishedNodesLoopAsync call was not successfull (status: '{results[0].StatusCode}'");
                        }
                        await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000));
                    }
                    catch (Exception e)
                    {
                        _log.Fatal(e, $"Exception in GetListOfPublishedNodesLoopAsync");
                    }
                }
                _log.Information($"GetListOfPublishedNodesLoopAsync Iteration {iteration++} completed");

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
                _testserverEndpointUrl
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
                    _log.Warning($"GetNumberOfPublishedNodes call was not successfull (status: '{results[0].StatusCode}'");
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
                _log.Fatal(e, $"Exception in GetNumberOfPublishedNodes");
            }
            return result;
        }

        private void Client_KeepAlive(Session sender, KeepAliveEventArgs e)
        {
            if (e.Status != null && ServiceResult.IsNotGood(e.Status))
            {
                _log.Verbose("{0} {1}/{2}", e.Status, sender.OutstandingRequestCount, sender.DefunctRequestCount);

                if (_reconnectHandler == null)
                {
                    _log.Verbose("--- RECONNECTING ---");
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

            _log.Verbose("--- RECONNECTED ---");
        }

        private static void OnNotification(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            foreach (var value in item.DequeueValues())
            {
                if (_lastTimestamp != new DateTime(0) && value.ServerTimestamp - _lastTimestamp > TimeSpan.FromSeconds(_maxLongWaitSec * 1.3))
                {
                    _log.Warning($"{item.DisplayName} publishing is too late: diff: {value.ServerTimestamp - _lastTimestamp}, max: {_maxLongWaitSec * 1.3}, serverTimestamp: {value.ServerTimestamp}, lastTimestamp: {_lastTimestamp}");
                }
                _lastTimestamp = value.ServerTimestamp;
            }
        }

        private static void CertificateValidator_CertificateValidation(CertificateValidator validator, CertificateValidationEventArgs e)
        {
            if (e.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                e.Accept = _autoAccept;
                if (_autoAccept)
                {
                    _log.Verbose("Accepted Certificate: {0}", e.Certificate.Subject);
                }
                else
                {
                    _log.Warning("Rejected Certificate: {0}", e.Certificate.Subject);
                }
            }
        }

        private const int _maxLongWaitSec = 10;
        private const int _maxShortWaitSec = 5;
        private List<NodeIdInfo> _referenceServerNodeIds = null;
        private List<NodeIdInfo> _referenceServerExpandedNodeIds = null;
        private static DateTime _lastTimestamp;
        private const int _reconnectPeriod = 10;
        private Session _session;
        private SessionReconnectHandler _reconnectHandler;
        private string _publisherEndpointUrl;
        private string _testserverEndpointUrl;
        private int _clientRunTime = Timeout.Infinite;
        private static bool _autoAccept = false;
        private static ExitCode _exitCode;
        private static bool _verboseLog = false;
        private static Serilog.Core.Logger _log = null;
    }
}
