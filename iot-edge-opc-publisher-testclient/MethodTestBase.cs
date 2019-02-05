using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace OpcPublisherTestClient
{
    using OpcPublisher;
    using System.Linq;
    using static Program;

    public abstract class MethodTestBase
    {
        public MethodTestBase(string testSpecifier, string testserverUrl, int maxShortWaitSec, int maxLongWaitSec, CancellationToken ct)
        {
            TestSpecifier = testSpecifier;
            TestserverUrl = testserverUrl;
            MaxShortWaitSec = maxShortWaitSec;
            MaxLongWaitSec = maxLongWaitSec;
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

            TestserverDataTypeExpandedNodeIds = new List<NodeIdInfo>();
            foreach (var postfix in simulationNodePostfix)
            {
                TestserverDataTypeExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_{postfix}"));
                TestserverDataTypeExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_Arrays_{postfix}"));
                TestserverDataTypeExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=Scalar_Simulation_Mass_{postfix}"));
            };

            TestserverMultiTagExpandedNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TestTagNum; i++)
            {
                TestserverMultiTagExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s=MultiTagTest_Integer{i:D7}"));
            }

            TestserverNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TestTagNum; i++)
            {
                TestserverNodeIds.Add(new NodeIdInfo($"ns=2;s={testSpecifier}_Integer{i:D7}"));
            }

            TestserverExpandedNodeIds = new List<NodeIdInfo>();
            for (var i = 0; i < TestTagNum; i++)
            {
                TestserverExpandedNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s={testSpecifier}_Integer{i:D7}"));
            }

            TestserverComplexNameNodeIds = new List<NodeIdInfo>();
            string specialChars = "\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]}";
            TestserverComplexNameNodeIds.Add(new NodeIdInfo($"nsu=http://opcfoundation.org/Quickstarts/ReferenceApplications;s={testSpecifier}_{specialChars}"));
        }

        public void RunExclusiveTests(CancellationToken ct = default(CancellationToken))
        {
            AdhocTest();
            SingleNodeIdTest();
            SingleExpandedNodeIdTest();
            ComplexNodeNameTest();
            PublishNodeExpandedNodeTest();
            // todo optimize publisher
            DataTypeTest();
            MultiTagTest();
            // todo: publish unknown endpoints, different publishing intervals
            return;
        }

        public List<Task> RunConcurrentTests(CancellationToken ct)
        {
            var publisherTests = new List<Task>
            {
                //Task.Run(async () => await PublishingIntervalAccuracyAsync()),
                Task.Run(async () => await PublishNodesLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await UnpublishNodesLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await GetPublishedNodesLegacyLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await GetConfiguredNodesOnEndpointLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await GetInfoLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await GetDiagnosticInfoLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await GetDiagnosticStartupLogLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await GetDiagnosticLogLoopAsync().ConfigureAwait(false)),
                Task.Run(async () => await CallUnknownMethodLoopAsync().ConfigureAwait(false)),
            };
            return publisherTests;
        }


        private void SingleNodeIdTest()
        {
            string logPrefix = $"{GetType().Name}:SingleNodeIdTest:";
            Random random = new Random();

            // publish a node using NodeId syntax
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = 1;
            List<NodeIdInfo> testNodes = new List<NodeIdInfo>();
            testNodes.Add(TestserverNodeIds[0]);
            PublishNodes(testNodes);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // todo verify that data is sent to IoTHub as expected, possibly measure turnouround

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes}) after publish");
            }

            // unpublish them
            UnpublishNodes(testNodes);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

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
            string logPrefix = $"{GetType().Name}:SingleExpandedNodeIdTest:";
            Random random = new Random();

            // publish a node using ExpandedNodeId syntax
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = 1;
            List<NodeIdInfo> testNodes = new List<NodeIdInfo>();
            testNodes.Add(TestserverExpandedNodeIds[0]);
            PublishNodes(testNodes);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes} after publish)");
            }

            // unpublish them
            UnpublishNodes(testNodes);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

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
            string logPrefix = $"{GetType().Name}:ComplexNodeNameTest:";
            Random random = new Random();

            // publish nodes with complex names
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = TestserverComplexNameNodeIds.Count;
            PublishNodes(TestserverComplexNameNodeIds);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes} after publish)");
            }

            // unpublish them
            UnpublishNodes(TestserverComplexNameNodeIds);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

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
            testNodesNodeId.Add(TestserverNodeIds[0]);
            testNodesExpandedNodeId.Add(TestserverExpandedNodeIds[0]);

            Logger.Information($"{logPrefix} started");
            Logger.Information($"{logPrefix} publish same node twice using NodeId syntax and unpublish via NodeId syntax");
            UnpublishAllConfiguredNodes();
            int numberOfTestNodes = 1;
            PublishNodes(testNodesNodeId);
            PublishNodes(testNodesNodeId);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // fetch the published nodes
            int configuredNodesCount = GetConfiguredNodesOnEndpointCount();
            if (configuredNodesCount != numberOfTestNodes)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {numberOfTestNodes} after publish)");
            }

            // unpublish the node once should remove them all
            UnpublishNodes(testNodesExpandedNodeId);
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

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
            Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).Wait();

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
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();
            PublishNodes(testNodesExpandedNodeId);
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();

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
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();
            PublishNodes(testNodesExpandedNodeId);
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();

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
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();
            PublishNodes(testNodesNodeId);
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();

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
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();
            PublishNodes(testNodesNodeId);
            Task.Delay((int)(Math.Max(MaxShortWaitSec, MaxLongWaitSec * random.NextDouble()) * 1000), _ct).Wait();

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
            string logPrefix = $"{GetType().Name}:DataTypeTest:";
            Random random = new Random();

            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();

            // publish nodes with different data types
            PublishNodes(TestserverDataTypeExpandedNodeIds);
            Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).Wait();

            int configuredNodesCount = GetConfiguredNodesOnEndpointCount("Scalar_Simulation");
            if (configuredNodesCount != TestserverDataTypeExpandedNodeIds.Count)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: {TestserverDataTypeExpandedNodeIds.Count}) after publish");
            }

            // unpublish them
            UnpublishNodes(TestserverDataTypeExpandedNodeIds, _ct, TestserverUrl);

            configuredNodesCount = GetConfiguredNodesOnEndpointCount(TestserverUrl, "Scalar_Simulation");
            if (configuredNodesCount != 0)
            {
                Logger.Error($"{logPrefix} There is(are) {configuredNodesCount} node(s) configured (expected: 0) after unpublish");
            }

            Logger.Information($"{logPrefix} completed");
        }


        private void MultiTagTest()
        {
            string logPrefix = $"{GetType().Name}:MultiTagTest:";
            Random random = new Random();

            // publish all nodes with different data types
            Logger.Information($"{logPrefix} started");
            UnpublishAllConfiguredNodes();

            DateTime startTime = DateTime.Now;
            PublishNodes(TestserverMultiTagExpandedNodeIds, _ct);
            TimeSpan elapsedTime = DateTime.Now - startTime;
            Logger.Information($"{logPrefix} configuration of {TestserverMultiTagExpandedNodeIds.Count} nodes took {elapsedTime.TotalMilliseconds} ms");
            Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).Wait();

            // unpublish them
            UnpublishNodes(TestserverMultiTagExpandedNodeIds, _ct, TestserverUrl);

            Logger.Information($"{logPrefix} completed");

            // delay and check if we should stop.
            Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).Wait();
        }

        private async Task PublishNodesLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:PublishNodesLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                // publish all nodes.
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                PublishNodes(TestserverNodeIds);
                await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);

                // publish nodes randomly selected.
                for (int i = 0; i < TestserverNodeIds.Count && !_ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(TestserverNodeIds.Count * random.NextDouble());
                    PublishNodes(TestserverNodeIds.GetRange(nodeIndex, 1));
                }

                // delay and check if we should stop.
                await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task UnpublishNodesLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:UnpublishNodesLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                // unpublish nodes randomly selected
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                for (int i = 0; i < TestserverNodeIds.Count && !_ct.IsCancellationRequested; i++)
                {
                    int nodeIndex = (int)(TestserverNodeIds.Count * random.NextDouble());
                    UnpublishNodes(TestserverNodeIds.GetRange(nodeIndex, 1));
                }
                await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);

                // unpublish all nodes
                UnpublishNodes(TestserverExpandedNodeIds);

                // delay and check if we should stop.
                await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetConfiguredNodesOnEndpointLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:GetConfiguredNodesOnEndpointLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                for (int i = 0; i < TestserverNodeIds.Count && !_ct.IsCancellationRequested; i++)
                {
                    try
                    {
                        GetConfiguredNodesOnEndpointCount();

                        await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        Logger.Fatal(e, $"{logPrefix} Exception");
                    }
                }

                // delay and check if we should stop.
                await Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetInfoLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:GetInfoLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                try
                {
                    GetInfo(_ct);

                    await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Fatal(e, $"{logPrefix} Exception");
                }

                // delay and check if we should stop.
                await Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetDiagnosticInfoLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:GetDiagnosticInfoLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                try
                {
                    GetDiagnosticInfo(_ct);

                    await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Fatal(e, $"{logPrefix} Exception");
                }

                // delay and check if we should stop.
                await Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetDiagnosticLogLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:GetDiagnosticLogLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                try
                {
                    GetDiagnosticLog(_ct);

                    await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Fatal(e, $"{logPrefix} Exception");
                }

                // delay and check if we should stop.
                await Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetDiagnosticStartupLogLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:GetDiagnosticStartupLogLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                try
                {
                    GetDiagnosticStartupLog(_ct);

                    await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Fatal(e, $"{logPrefix} Exception");
                }

                // delay and check if we should stop.
                await Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task CallUnknownMethodLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:GetUnknownMethodLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                try
                {
                    CallUnknownMethod(_ct);

                    await Task.Delay((int)(MaxShortWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    Logger.Fatal(e, $"{logPrefix} Exception");
                }

                // delay and check if we should stop.
                await Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration++} completed");
            }
        }

        private async Task GetPublishedNodesLegacyLoopAsync()
        {
            string logPrefix = $"{GetType().Name}:GetPublishedNodesLegacyLoopAsync:";
            Random random = new Random();
            int iteration = 0;

            while (!_ct.IsCancellationRequested)
            {
                Logger.Information($"{logPrefix} Iteration {iteration++} started");
                GetPublishedNodesLegacy(TestserverUrl, _ct);
                // delay and check if we should stop.
                await Task.Delay((int)(MaxLongWaitSec * random.NextDouble() * 1000), _ct).ConfigureAwait(false);
                Logger.Information($"{logPrefix} Iteration {iteration} completed");
            }
        }

        private int GetConfiguredNodesOnEndpointCount(string idSpecifier = null)
        {
            return GetConfiguredNodesOnEndpointCount(TestserverUrl, idSpecifier ?? TestSpecifier);
        }

        private int GetConfiguredNodesOnEndpointCount(string endpointUrl, string idSpecifier = null)
        {
            int result = 0;
            List<OpcNodeOnEndpointModel> nodeList = null;
            nodeList = GetConfiguredNodesOnEndpoint(endpointUrl ?? TestserverUrl, _ct);
            result = nodeList.Count(n => n.Id.Contains(idSpecifier ?? TestSpecifier, StringComparison.InvariantCulture));
            return result;
        }

        private bool UnpublishNodes(List<NodeIdInfo> nodeIdInfos)
        {

            return UnpublishNodes(nodeIdInfos, _ct, TestserverUrl);
        }

        private bool PublishNodes(List<NodeIdInfo> nodeIdInfos)
        {

            return PublishNodes(nodeIdInfos, _ct, TestserverUrl);
        }

        private bool UnpublishAllConfiguredNodes()
        {

            return UnpublishAllConfiguredNodes(_ct);
        }

        protected abstract void AdhocTest();
        protected abstract bool PublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl = null);
        protected abstract bool UnpublishNodes(List<NodeIdInfo> nodeIdInfos, CancellationToken ct, string endpointUrl = null);
        protected abstract PublishedNodesCollection GetPublishedNodesLegacy(string endpointUrl, CancellationToken ct);
        protected abstract List<OpcNodeOnEndpointModel> GetConfiguredNodesOnEndpoint(string endpointUrl, CancellationToken ct);
        protected abstract bool UnpublishAllConfiguredNodes(CancellationToken ct);
        protected abstract void GetInfo(CancellationToken ct);
        protected abstract void GetDiagnosticInfo(CancellationToken ct);
        protected abstract void GetDiagnosticLog(CancellationToken ct);
        protected abstract void GetDiagnosticStartupLog(CancellationToken ct);
        protected abstract void CallUnknownMethod(CancellationToken ct);


        protected string TestSpecifier { get; set; }
        protected List<NodeIdInfo> TestserverDataTypeExpandedNodeIds { get; } = null;
        protected List<NodeIdInfo> TestserverMultiTagExpandedNodeIds { get; } = null;
        protected List<NodeIdInfo> TestserverNodeIds { get; } = null;
        protected List<NodeIdInfo> TestserverExpandedNodeIds { get; } = null;
        protected List<NodeIdInfo> TestserverComplexNameNodeIds { get; } = null;
        protected string TestserverUrl { get; set; }
        protected int MaxShortWaitSec { get; set; }
        protected int MaxLongWaitSec { get; set; }
#pragma warning disable CA1707 // Identifiers should not contain underscores
        protected CancellationToken _ct { get; set; }
#pragma warning restore CA1707 // Identifiers should not contain underscores
    }
}
