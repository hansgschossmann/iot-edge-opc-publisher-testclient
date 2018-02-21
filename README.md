# iot-edge-opc-publisher-testclient
This OPC UA client is based on the source of the Console client source in the [OPC UA .NET Standard github repository](https://github.com/OPCFoundation/UA-.NETStandard) of the OPC Foundation.
The original Console client could be found in the subdirectory ./SampleApplications/Samples/NetCoreConsoleClient of the repository.
There is a docker container of this repository available as hansgschossmann/iot-edge-opc-publisher-testclient.

The test client does connect to opc.tcp://publisher:62222 and then run tests on the OPC UA exposed methodes of the OPC UA server integrated in the OPC Publisher. Those are:
- PublishNode
- UnpublishNode
- GetPublishedNodes

The test include:
- Run publish/unpublish sequeces using NodeId and ExpandedNodeId syntax
- Run longhaul test with random publish/unpublish calls of nodes

This [repository](https://github.com/hansgschossmann/iot-edge-opc-publisher-testserver.git) contains the implementation of an OPC UA server, which implements the nodes, which are used for all publishing/unpublishing operations and must run before the tests are started.

This [repository](https://github.com/hansgschossmann/iot-edge-opc-publisher-test.git) contains a docker compose configuration to start up the testbed automatically.
