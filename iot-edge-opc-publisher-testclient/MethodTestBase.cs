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

    public abstract class MethodTestBase
    {
        public MethodTestBase(string testSpecifier, string testserverUrl, int maxShortWaitSec, int maxLongWaitSec, CancellationToken ct)
        {
            _testSpecifier = testSpecifier;
            _testserverUrl = testserverUrl;
            _maxShortWaitSec = maxShortWaitSec;
            _maxLongWaitSec = maxLongWaitSec;
            _ct = ct;

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
                _testServerMultiTagExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=MultiTagTest_Integer{i:D7}"));
            }

            _testServerNodeIds = new List<NodeIdInfo>();
            _testServerNodeIds.Add(new NodeIdInfo("i=2258"));
            for (var i = 0; i < TEST_TAG_NUM; i++)
            {
                _testServerNodeIds.Add(new NodeIdInfo($"ns=2;s={testSpecifier}_Integer{i:D7}"));
            }

            _testServerExpandedNodeIds = new List<NodeIdInfo>();
            _testServerExpandedNodeIds.Add(new NodeIdInfo("nsu=http://opcfoundation.org/UA/;i=2258"));
            for (var i = 0; i < TEST_TAG_NUM; i++)
            {
                _testServerExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s={testSpecifier}_Integer{i:D7}"));
            }

            _testServerComplexNameNodeIds = new List<NodeIdInfo>();
            string specialChars = @"""!§$% &/ () =?`´\\+~*'#_-:.;,<>|@^°€µ{{[]}}";
            _testServerComplexNameNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s={testSpecifier}_{specialChars}"));

        }

        public void RunExclusiveTests(CancellationToken ct)
        {
            AdhocTest();
            SingleNodeIdTest();
            SingleExpandedNodeIdTest();
            ComplexNodeNameTest();
            PublishNodeExpandedNodeTest();
            // todo optimize publisher
            //DataTypeTest();
            //MultiTagTest();
            // todo: publish unknown endpoints, different publishing intervals
            return;
        }

        public List<Task> RunConcurrentTests(CancellationToken ct)
        {
            var publisherTests = new List<Task>
            {
                //Task.Run(async () => await PublishingIntervalAccuracyAsync()),
                Task.Run(async () => await PublishNodesLoopAsync()),
                Task.Run(async () => await UnpublishNodesLoopAsync()),
                Task.Run(async () => await GetPublishedNodesLegacyLoopAsync()),
                Task.Run(async () => await GetConfiguredNodesOnEndpointLoopAsync())
            };
            return publisherTests;
        }


        private void SingleNodeIdTest()
        {
            string logPrefix = $"{this.GetType().Name}:SingleNodeIdTest:";
            Random random = new Random();

            // publish a node using NodeId syntax
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = 1;
            List<NodeIdInfo> testNodes = new List<NodeIdInfo>();
            testNodes.Add(_testServerNodeIds[0]);
            PublishNodes(testNodes);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // todo verify that data is sent to IoTHub as expected, possibly measure turnouround

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes}) after publish");
            }

            // unpublish them
            UnpublishNodes(testNodes);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }
            Logger.Information($"{logPrefix} completed");
        }
        private void SingleExpandedNodeIdTest()
        {
            string logPrefix = $"{this.GetType().Name}:SingleExpandedNodeIdTest:";
            Random random = new Random();

            // publish a node using ExpandedNodeId syntax
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = 1;
            List<NodeIdInfo> testNodes = new List<NodeIdInfo>();
            testNodes.Add(_testServerExpandedNodeIds[0]);
            PublishNodes(testNodes);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes} after publish)");
            }

            // unpublish them
            UnpublishNodes(testNodes);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }
            Logger.Information($"{logPrefix} completed");
        }

        private void ComplexNodeNameTest()
        {
            string logPrefix = $"{this.GetType().Name}:ComplexNodeNameTest:";
            Random random = new Random();

            // publish nodes with complex names
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = _testServerComplexNameNodeIds.Count;
            PublishNodes(_testServerComplexNameNodeIds);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes} after publish)");
            }

            // unpublish them
            UnpublishNodes(_testServerComplexNameNodeIds);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }
            Logger.Information($"{logPrefix} completed");
        }

        private void PublishNodeExpandedNodeTest()
        {
            string logPrefix = $"{GetType().Name}:{System.Reflection.MethodBase.GetCurrentMethod().Name}:";
            Random random = new Random();
            List<NodeIdInfo> testNodesNodeId = new List<NodeIdInfo>();
            List<NodeIdInfo> testNodesExpandedNodeId = new List<NodeIdInfo>();
            testNodesNodeId.Add(_testServerNodeIds[0]);
            testNodesExpandedNodeId.Add(_testServerExpandedNodeIds[0]);

            Logger.Information($"{logPrefix} started");
            Logger.Information($"{logPrefix} publish same node twice using NodeId syntax and unpublish via NodeId syntax");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = 1;
            PublishNodes(testNodesNodeId);
            PublishNodes(testNodesNodeId);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes} after publish)");
            }

            // unpublish the node once should remove them all
            UnpublishNodes(testNodesExpandedNodeId);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }


            Logger.Information($"{logPrefix} publish same node twice using ExpandedNodeId syntax and unpublish via ExpandedNodeId syntax");
            UnpublishAllConfiguredNodes();
            PublishNodes(testNodesExpandedNodeId);
            PublishNodes(testNodesExpandedNodeId);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes} after publish)");
            }

            // unpublish them
            UnpublishNodes(testNodesExpandedNodeId);

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }


            Logger.Information($"{logPrefix} publish same node using NodeId (first) and ExpandedNodeId (second) syntax and unpublish it once using NodeId syntax");
            UnpublishAllConfiguredNodes();
            PublishNodes(testNodesNodeId);
            PublishNodes(testNodesExpandedNodeId);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes}) after publish");
            }

            // unpublish the node once using NodeId syntax
            UnpublishNodes(testNodesNodeId);

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }


            Logger.Information($"{logPrefix} publish same node using NodeId (first) and ExpandedNodeId (second) syntax and unpublish it once using ExpandedNodeId syntax");
            UnpublishAllConfiguredNodes();
            PublishNodes(testNodesNodeId);
            PublishNodes(testNodesExpandedNodeId);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes}) after publish");
            }

            // unpublish the node once using ExpandedNodeId syntax
            UnpublishNodes(testNodesExpandedNodeId);

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }


            Logger.Information($"{logPrefix} publish same node using NodeId (second) and ExpandedNodeId (first) syntax and unpublish it once using NodeId syntax");
            UnpublishAllConfiguredNodes();
            PublishNodes(testNodesExpandedNodeId);
            PublishNodes(testNodesNodeId);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes}) after publish");
            }

            // unpublish the node once using NodeId syntax
            UnpublishNodes(testNodesNodeId);

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }



            Logger.Information($"{logPrefix} publish same node using NodeId (second) and ExpandedNodeId (first) syntax and unpublish it once using ExpandedNodeId syntax");
            UnpublishAllConfiguredNodes();
            PublishNodes(testNodesExpandedNodeId);
            PublishNodes(testNodesNodeId);
            Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes}) after publish");
            }

            // unpublish the node once using ExpandedNodeId syntax
            UnpublishNodes(testNodesExpandedNodeId);

            // fetch the published nodes
            configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }


            Logger.Information($"{logPrefix} completed");
        }


        private void DataTypeTest()
        {
            string logPrefix = $"{this.GetType().Name}:DataTypeTest:";
            Random random = new Random();

            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();

            // publish nodes with different data types
            PublishNodes(_testServerDataTypeExpandedNodeIds);
            Task.Delay((int)(_maxLongWaitSec * random.NextDouble() * 1000), _ct).Wait();

            int configuredNodesCount = GetConfiguredNodesOnEndpointCount("Scalar_Simulation");
            if (configuredNodesCount != _testServerDataTypeExpandedNodeIds.Count)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {_testServerDataTypeExpandedNodeIds.Count}) after publish");
            }

            // unpublish them
            UnpublishNodes(_testServerDataTypeExpandedNodeIds, _ct, _testserverUrl);

            configuredNodesCount = GetConfiguredNodesOnEndpointCount(_testserverUrl, "Scalar_Simulation");
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }

            Logger.Information($"{logPrefix} completed");
        }


        private void MultiTagTest()
        {
            string logPrefix = $"{this.GetType().Name}:MultiTagTest:";
            Random random = new Random();

            // publish all nodes with different data types
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();

            DateTime startTime = DateTime.Now;
            PublishNodes(_testServerMultiTagExpandedNodeIds, _ct);
            TimeSpan elapsedTime = DateTime.Now - startTime;
            Logger.Information($"{logPrefix} configuration of {_testServerMultiTagExpandedNodeIds.Count} nodes took {elapsedTime.TotalMilliseconds} ms");
            Task.Delay((int)(_maxLongWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // unpublish them
            UnpublishNodes(_testServerMultiTagExpandedNodeIds, _ct, _testserverUrl);

            Logger.Information($"{logPrefix} completed");

            // delay and check if we should stop.
            Task.Delay((int)(_maxLongWaitSec * random.NextDouble() * 1000), _ct).Wait();
        }

        private async Task PublishNodesLoopAsync()
        {
            string logPrefix = $"{this.GetType().Name}:PublishNodesLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                // publish all nodes.
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                PublishNodes(_testServerNodeIds);
                await Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct);

                // publish nodes randomly selected.
                for (int i = 0; i < _testServerNodeIds.Count && !_ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(_testServerNodeIds.Count * random.NextDouble());
                    PublishNodes(_testServerNodeIds.GetRange(nodeIndex, 1));
                }

                // delay and check if we should stop.
                await Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task UnpublishNodesLoopAsync()
        {
            string logPrefix = $"{this.GetType().Name}:UnpublishNodesLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                // unpublish nodes randomly selected
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                for (int i = 0; i < _testServerNodeIds.Count && !_ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(_testServerNodeIds.Count * random.NextDouble());
                    UnpublishNodes(_testServerNodeIds.GetRange(nodeIndex, 1));
                }
                await Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct);

                // unpublish all nodes
                UnpublishNodes(_testServerExpandedNodeIds);

                // delay and check if we should stop.
                await Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetConfiguredNodesOnEndpointLoopAsync()
        {
            string logPrefix = $"{this.GetType().Name}:GetConfiguredNodesOnEndpointLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                for (int i = 0; i < _testServerNodeIds.Count && !_ct.IsCancellationRequested; i++)
                {
                    try
                    {
                        GetConfiguredNodesOnEndpointCount();

                        await Task.Delay((int)(_maxShortWaitSec * random.NextDouble() * 1000), _ct);
                    }
                    catch (Exception e)
                    {
                        Logger.Fatal(e, $"{logPrefix} Exception");
                    }
                }

                // delay and check if we should stop.
                await Task.Delay((int)(_maxLongWaitSec * random.NextDouble() * 1000), _ct);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetPublishedNodesLegacyLoopAsync()
        {
            string logPrefix = $"{this.GetType().Name}:GetPublishedNodesLegacyLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                GetPublishedNodesLegacy(_testserverUrl, _ct);
                // delay and check if we should stop.
                await Task.Delay((int)(_maxLongWaitSec * random.NextDouble() * 1000), _ct);
                Logger.Information($"{logPrefix} Iteration {iteration} completed");
            }
        }

        private int GetConfiguredNodesOnEndpointCount(string idSpecifier = null)
        {
            return GetConfiguredNodesOnEndpointCount(_testserverUrl, idSpecifier ?? _testSpecifier);
        }

        private int GetConfiguredNodesOnEndpointCount(string endpointUrl, string idSpecifier = null)
        {
            int result = 0;
            List<NodeModel> nodeList = null;
            nodeList = GetConfiguredNodesOnEndpoint(endpointUrl ?? _testserverUrl, _ct);
            result = nodeList.Count(n => n.Id.Contains(idSpecifier ?? _testSpecifier));
            return result;
        }

        private bool UnpublishNodes(List<NodeIdInfo> nodeIdInfos)
        {

            return UnpublishNodes(nodeIdInfos, _ct, _testserverUrl);
        }

        private bool PublishNodes(List<NodeIdInfo> nodeIdInfos)
        {

            return PublishNodes(nodeIdInfos, _ct, _testserverUrl);
        }

        private bool UnpublishAllConfiguredNodes()
        {

            return UnpublishAllConfiguredNodes(_ct);
        }

        protected abstract void AdhocTest();
        protected abstract bool PublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl = null);
        protected abstract bool UnpublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl = null);
        protected abstract PublishedNodesCollection GetPublishedNodesLegacy(string endpointUrl, CancellationToken ct);
        protected abstract List<NodeModel> GetConfiguredNodesOnEndpoint(string endpointUrl, CancellationToken ct);
        protected abstract bool UnpublishAllConfiguredNodes(CancellationToken ct);

        protected string _testSpecifier;
        protected List<NodeIdInfo> _testServerDataTypeExpandedNodeIds = null;
        protected List<NodeIdInfo> _testServerMultiTagExpandedNodeIds = null;
        protected List<NodeIdInfo> _testServerNodeIds = null;
        protected List<NodeIdInfo> _testServerExpandedNodeIds = null;
        protected List<NodeIdInfo> _testServerComplexNameNodeIds = null;
        protected string _testserverUrl;
        protected int _maxShortWaitSec;
        protected int _maxLongWaitSec;
        protected CancellationToken _ct;
    }
}
