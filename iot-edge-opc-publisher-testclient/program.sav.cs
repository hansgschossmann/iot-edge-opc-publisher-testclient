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
    using System.Linq;
    using static Program;

    public class Program
    {
        public static Serilog.Core.Logger Logger = null;

        /// <summary>
        /// Synchronous main method of the app.
        /// </summary>
        public static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        /// <summary>
        /// Asynchronous part of the main method of the app.
        /// </summary>
        public async static Task MainAsync(string[] args)
        {
            Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .MinimumLevel.Debug()
                .CreateLogger();

            Logger.Information($"{(Utils.IsRunningOnMono() ? "Mono" : ".Net Core")} OPC UA Console Client sample");

            // command line options
            bool showHelp = false;
            int testTimeSec = Timeout.Infinite;
            bool autoAccept = false;
            string testserverUrl = "opc.tcp://testserver:62541/Quickstarts/ReferenceServer";
            string publisherUrl = "opc.tcp://publisher:62222/UA/Publisher";
            bool opcMethods = false;
            string iothubConnectionString = string.Empty;
            string iothubPublisherDeviceName = string.Empty;
            string iothubPublisherModuleName = string.Empty;
            bool iothubMethods = false;
            bool iothubMessages = false;

            Mono.Options.OptionSet options = new Mono.Options.OptionSet {
                { "h|help", "show this message and exit", h => showHelp = h != null },
                { "a|autoaccept", "auto accept certificates (for testing only)", a => autoAccept = a != null },
                { "tt|testtime=", "the number of seconds to run the different tests", (int t) => testTimeSec = t * 1000 },
                { "tu|testserverurl=", "URL of the OPC UA test server", (string s) => testserverUrl = s },
                { "pu|publisherurl=", "URL of the OPC Publisher (required when using OPC UA methods)", (string s) => publisherUrl = s },
                { "o1|opcmethods", "use the OPC UA methods calls to test",  b => opcMethods = b != null },
                { "ic|iothubconnectionstring=", "IoTHub owner connectionstring", (string s) => iothubConnectionString = s },
                { "id|iothubdevicename=", "IoTHub device name of the OPC Publisher (required when using IoT methods/messages)", (string s) => iothubPublisherDeviceName = s },
                { "im|iothubmodulename=", "IoTEdge module name of the OPC Publisher which runs in IoTEdge specified by im/iothubdevicename(required when using IoT methods/messages and IoTEdge)", (string s) => iothubPublisherModuleName = s },
                { "i1|iothubmethods", "use IoTHub direct methods calls to test", b => iothubMethods = b != null },
                { "i2|iothubmessages", "use IoTHub C2D messages to test", b => iothubMessages = b != null },
                { "lf|logfile=", $"the filename of the logfile to use.\nDefault: './{_logFileName}'", (string l) => _logFileName = l },
                { "ll|loglevel=", $"the loglevel to use (allowed: fatal, error, warn, info, debug, verbose).\nDefault: info", (string l) => {
                        List<string> logLevels = new List<string> {"fatal", "error", "warn", "info", "debug", "verbose"};
                        if (logLevels.Contains(l.ToLowerInvariant()))
                        {
                            _logLevel = l.ToLowerInvariant();
                        }
                        else
                        {
                            throw new OptionException("The loglevel must be one of: fatal, error, warn, info, debug, verbose", "loglevel");
                        }
                    }
                }
            };

            IList<string> extraArgs = null;
            try
            {
                extraArgs = options.Parse(args);
            }
            catch (OptionException e)
            {
                // initialize logging
                InitLogging();

                // show message
                Logger.Fatal(e, "Error in command line options");

                // show usage
                Usage(options, args);
                return;
            }

            // initialize logging
            InitLogging();

            // by default we are connecting to the OPC UA servers in the testbed 
            if (extraArgs.Count > 1)
            {
                for (int i = 1; i < extraArgs.Count; i++)
                {
                    Logger.Error("Error: Unknown option: {0}", extraArgs[i]);
                }
                Usage(options, args);
                return;
            }
            else
            {
                testserverUrl = extraArgs[0];
                Logger.Information($"Testserver endpoint URL: {testserverUrl}");
            }

            if (opcMethods == false && iothubMessages == false && iothubMethods == false)
            {
                Logger.Information($"No specific test area specified, enabling all.");
                opcMethods = iothubMethods = iothubMessages = true;
            }
            if (opcMethods)
            {
                Logger.Information($"Publisher URL: {publisherUrl}");

            }
            if (iothubMessages || iothubMethods)
            {
                if (string.IsNullOrEmpty(iothubConnectionString) || string.IsNullOrEmpty(iothubPublisherDeviceName))
                {
                    Logger.Fatal("For any tests via IoTHub communication an IoTHub connection string and the publisher devicename must be specified.");
                    return;
                }
                Logger.Information($"IoTHub connectionstring: {iothubConnectionString}");
                if (string.IsNullOrEmpty(iothubPublisherModuleName))
                {
                    Logger.Information($"Testing OPC Publisher device.");
                    Logger.Information($"IoTHub Publisher device name: {iothubPublisherDeviceName}");
                }
                else
                {
                    Logger.Information($"Testing OPC Publisher IoTEdge module.");
                    Logger.Information($"IoTEdge device name: {iothubPublisherDeviceName}");
                    Logger.Information($"IoTHub Publisher device name: {iothubPublisherModuleName}");
                }
            }

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

            List<Task> testTasks = new List<Task>();
            if (opcMethods)
            {
                OpcMethodTest opcMethodTest = new OpcMethodTest(publisherUrl, testserverUrl);
                testTasks.Add(opcMethodTest.RunAsync(ct));
            }

            if (iothubMethods)
            {
                IotHubMethodTest iotHubMethodTest = new IotHubMethodTest(iothubConnectionString, iothubPublisherDeviceName, iothubPublisherModuleName, testserverUrl);
                testTasks.Add(iotHubMethodTest.RunAsync(ct));
            }

            if (iothubMessages)
            {
                IotHubMessageTest iotHubMessageTest = new IotHubMessageTest(iothubConnectionString, iothubPublisherDeviceName, iothubPublisherModuleName, testserverUrl);
                testTasks.Add(iotHubMessageTest.RunAsync(ct));
            }

            // run all tests for the requiested time of till Ctrl-C is pressed
            Logger.Information($"Run tests {(testTimeSec != Timeout.Infinite ? $"for {testTimeSec} seconds or" : "till")} CTRL-C is pressed");
            quitEvent.WaitOne(testTimeSec * 1000);
            cts.Cancel();
            // wait till all tasks are completed
            Task.WaitAll(testTasks.ToArray());
            return;
        }

        /// <summary>
        /// Initialize logging.
        /// </summary>
        private static void InitLogging()
        {
            LoggerConfiguration loggerConfiguration = new LoggerConfiguration();

            // set the log level
            switch (_logLevel)
            {
                case "fatal":
                    loggerConfiguration.MinimumLevel.Fatal();
                    break;
                case "error":
                    loggerConfiguration.MinimumLevel.Error();
                    break;
                case "warn":
                    loggerConfiguration.MinimumLevel.Warning();
                    break;
                case "info":
                    loggerConfiguration.MinimumLevel.Information();
                    break;
                case "debug":
                    loggerConfiguration.MinimumLevel.Debug();
                    break;
                case "verbose":
                    loggerConfiguration.MinimumLevel.Verbose();
                    break;
            }

            // set logging sinks
            loggerConfiguration.WriteTo.Console();

            if (!string.IsNullOrEmpty(_logFileName))
            {
                // configure rolling file sink
                const int MAX_LOGFILE_SIZE = 1024 * 1024;
                const int MAX_RETAINED_LOGFILES = 2;
                loggerConfiguration.WriteTo.File(_logFileName, fileSizeLimitBytes: MAX_LOGFILE_SIZE, rollOnFileSizeLimit: true, retainedFileCountLimit: MAX_RETAINED_LOGFILES);
            }

            Logger = loggerConfiguration.CreateLogger();
            Logger.Information($"Current directory is: {System.IO.Directory.GetCurrentDirectory()}");
            Logger.Information($"Log file is: {_logFileName}");
            Logger.Information($"Log level is: {_logLevel}");
            return;
        }

        /// <summary>
        /// Usage message.
        /// </summary>
        private static void Usage(Mono.Options.OptionSet options, string[] args)
        {
            // show usage
            Logger.Information("");
            string commandLine = string.Empty;
            foreach (var arg in args)
            {
                commandLine = commandLine + " " + arg;
            }
            Logger.Information($"Command line: {commandLine}");
            Logger.Information("");
            Logger.Information("");
            Logger.Information("Usage: dotnet NetCoreConsoleClient.dll [OPTIONS] [TESTSERVERENDPOINTURL]");
            Logger.Information("");

            // output the options
            Logger.Information("Options:");
            StringBuilder stringBuilder = new StringBuilder();
            StringWriter stringWriter = new StringWriter(stringBuilder);
            options.WriteOptionDescriptions(stringWriter);
            string[] helpLines = stringBuilder.ToString().Split("\r\n");
            foreach (var line in helpLines)
            {
                Logger.Information(line);
            }
        }

        private static string _logFileName = $"{Utils.GetHostName()}-ìot-edge-opc-publisher-testclient.log";
        private static string _logLevel = "info";
    }

    public class OpcMethodTest
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

        public OpcMethodTest(string publisherUrl, string testserverUrl)
        {
            _publisherUrl = publisherUrl;
            _testserverUrl = testserverUrl;

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
            _referenceServerMultiTagIds = new List<NodeIdInfo>();
            for (var i = 0; i < MULTI_TAG_NUM; i++)
            {
                _referenceServerMultiTagIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=MultiTag_Integer{i:D7}"));
            }
        }

        public Task RunAsync(CancellationToken ct)
        {
            Task testTask = Task.Run(async () => { await RunAllTestsAsync(ct); });
            return testTask;
        }

        private async List<Task> RunAllTestsAsync(CancellationToken ct)
        {
            Logger.Information("Create an Application Configuration.");

            ApplicationInstance application = new ApplicationInstance
            {
                ApplicationName = "UA Core Sample Client",
                ApplicationType = ApplicationType.Client,
                ConfigSectionName = "Opc.Ua.SampleClient"
            };

            // load the application configuration.
            ApplicationConfiguration config = await application.LoadApplicationConfiguration(false);

            // check the application certificate.
            bool haveAppCertificate = await application.CheckApplicationInstanceCertificate(false, 0);
            if (!haveAppCertificate)
            {
                Logger.Fatal("Application instance certificate invalid!");
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
                Logger.Warning("Missing application certificate, using unsecure connection.");
            }

            Logger.Information($"Discover endpoints of {_publisherUrl}.");
            var selectedEndpoint = CoreClientUtils.SelectEndpoint(_publisherUrl, haveAppCertificate, 15000);
            Logger.Information("Selected endpoint uses: {0}",
                selectedEndpoint.SecurityPolicyUri.Substring(selectedEndpoint.SecurityPolicyUri.LastIndexOf('#') + 1));

            Logger.Information("Create a session with OPC UA server.");
            var endpointConfiguration = EndpointConfiguration.Create(config);
            var endpoint = new ConfiguredEndpoint(null, selectedEndpoint, endpointConfiguration);
            _session = await Session.Create(config, endpoint, false, "OPC UA Console Client", 60000, new UserIdentity(new AnonymousIdentityToken()), null);

            // register keep alive handler
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

            Logger.Information("Run tests using direct OPC UA client server communication");
            var publisherTests = new List<Task>
            {
                Task.Run(async () => await PublisherConnectionAsync(ct, _maxShortWaitSec, _maxLongWaitSec)),
                Task.Run(async () => await AddRemoveTestAsync(ct, _maxShortWaitSec, _maxLongWaitSec)),
                Task.Run(async () => await PublishNodesLoopAsync(ct, _maxShortWaitSec, _maxLongWaitSec)),
                Task.Run(async () => await UnpublishNodesLoopAsync(ct, _maxShortWaitSec, _maxLongWaitSec)),
                Task.Run(async () => await GetListOfPublishedNodesLoopAsync(ct, _maxShortWaitSec, _maxLongWaitSec))
            };
            return publisherTests;
        }

        private async Task PublisherConnectionAsync(CancellationToken ct, int maxShortWaitSec, int maxLongWaitSec)
        {
            Logger.Information($"Connection test: Create a subscription with publishing interval of {maxLongWaitSec} second.");
            var subscription = new Subscription(_session.DefaultSubscription) { PublishingInterval = maxLongWaitSec * 1000 };

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
                for (int i = 0; i < _referenceServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    PublishOneNode(_referenceServerNodeIds[i]);
                }
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);

                // publish nodes randomly selected.
                for (int i = 0; i < _referenceServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(_referenceServerNodeIds.Count * random.NextDouble());
                    PublishOneNode(_referenceServerNodeIds[nodeIndex]);
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

            // Wait for some time to allow all pending node publishing requests to be completed
            await Task.Delay(30000, ct);
            while (!ct.IsCancellationRequested)
            {
                // unpublish nodes randomly selected.
                Logger.Information($"UnpublishNodes Iteration {iteration++} started");
                for (int i = 0; i < _referenceServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(_referenceServerNodeIds.Count * random.NextDouble());
                    UnpublishOneNode(_referenceServerNodeIds[nodeIndex]);
                    await Task.Delay((int)(maxShortWaitSec * random.NextDouble() * 1000), ct);
                }
                await Task.Delay((int)(maxLongWaitSec * random.NextDouble() * 1000), ct);

                // unpublish all nodes.
                for (int i = 0; i < _referenceServerNodeIds.Count && !ct.IsCancellationRequested; i++)
                {
                    UnpublishOneNode(_referenceServerExpandedNodeIds[i]);
                }
                Logger.Information($"UnpublishNodes Iteration {iteration++} completed");

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
                for (int i = 0; i < _referenceServerNodeIds.Count && !ct.IsCancellationRequested; i++)
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


        private async Task AddRemoveTestAsync(CancellationToken ct)
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

        private static void OnMultiNotification(MonitoredItem item, MonitoredItemNotificationEventArgs e)
        {
            Logger.Information("OnMultiNotification build");
        }

        private static void CertificateValidator_CertificateValidation(CertificateValidator validator, CertificateValidationEventArgs e)
        {
            if (e.Error.StatusCode == StatusCodes.BadCertificateUntrusted)
            {
                e.Accept = _autoAccept;
                if (_autoAccept)
                {
                    Logger.Verbose("Accepted Certificate: {0}", e.Certificate.Subject);
                }
                else
                {
                    Logger.Warning("Rejected Certificate: {0}", e.Certificate.Subject);
                }
            }
        }

        private const int MULTI_TAG_NUM = 100000;
        private const int TEST_TAG_NUM = 10000;
        private const int MAX_SUBSCRIPTIONS = 500;
        private const int _maxLongWaitSec = 10;
        private const int _maxShortWaitSec = 5;
        private List<NodeIdInfo> _referenceServerNodeIds = null;
        private List<NodeIdInfo> _referenceServerExpandedNodeIds = null;
        private List<NodeIdInfo> _referenceServerMultiTagIds = null;
        private static DateTime _lastTimestamp;
        private const int _reconnectPeriod = 10;
        private Session _session;
        private SessionReconnectHandler _reconnectHandler;
        private string _publisherUrl;
        private string _testserverUrl;
        private int _clientRunTime = Timeout.Infinite;
        private static bool _autoAccept = false;
    }
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
                Task.Run(async () => await TestMultiSubscriptionsAsync(ct, )),
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
    public class IotHubMessageTest
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

        public IotHubMessageTest(string iothubConnectionString, string iothubPublisherDeviceName, string iothubPublisherModuleName, string testserverUrl)
        {
            _testserverUrl = testserverUrl;

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
            //Task.Run(async () => { await RunAllTestsAsync(ct); });
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

        private const int MULTI_TAG_NUM = 100000;
        private const int TEST_TAG_NUM = 10000;
        private const int _maxLongWaitSec = 10;
        private const int _maxShortWaitSec = 5;
        private List<NodeIdInfo> _referenceServerNodeIds = null;
        private List<NodeIdInfo> _referenceServerExpandedNodeIds = null;
        private string _testserverUrl;
    }
}
