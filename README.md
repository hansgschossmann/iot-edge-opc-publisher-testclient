# iot-edge-opc-publisher-testclient
This OPC UA client is based on the source of the Console client source in the [OPC UA .NET Standard github repository](https://github.com/OPCFoundation/UA-.NETStandard) of the OPC Foundation.
The original Console client could be found in the subdirectory ./SampleApplications/Samples/NetCoreConsoleClient of the repository.

A container is available as [hansgschossmann\iot-edge-opc-publisher-testclient](https://hub.docker.com/r/hansgschossmann/iot-edge-opc-publisher-testclient/) on Docker Hub.

Command line is:

         Usage: iot-edge-opc-publisher-testclient.exe [<options>]
         OPC Publisher test client. Requires iot-edge-opc-publisher-testclient and OPC Publisher.
         To exit the application, just press CTRL-C while it is running.
         Options:
           -h, --help                 show this message and exit
           -a, --autoaccept           auto accept certificates (for testing only)
               --ne, --noexclusive    do not execute any exclusive tests
               --tt, --testtime=VALUE the number of seconds to run the different tests
               --tu, --testserverurl=VALUE
                                      URL of the OPC UA test server
               --pu, --publisherurl=VALUE
                                      URL of the OPC Publisher (required when using OPC
                                        UA methods)
               --o1, --opcmethods     use the OPC UA methods calls to test
               --ic, --iotHubConnectionString=VALUE
                                      IoTHub owner connectionstring
               --id, --iothubdevicename=VALUE
                                      IoTHub device name of the OPC Publisher (required
                                        when using IoT methods)
               --im, --iothubmodulename=VALUE
                                      IoTEdge module name of the OPC Publisher which
                                        runs in IoTEdge specified by im/iothubdevicename(
                                        required when using IoT methods and IoTEdge)
               --i1, --iotHubMethods  use IoTHub direct methods calls to test
               --lf, --logfile=VALUE  the filename of the logfile to use.
                                        Default: './acfbb604c946-ìot-edge-opc-publisher-
                                        testclient.log'
               --ll, --loglevel=VALUE the loglevel to use (allowed: fatal, error, warn,
                                        info, debug, verbose).
                                        Default: info

The test include:
- Run publish/unpublish sequences using NodeId and ExpandedNodeId syntax
- Run longhaul test with random publish/unpublish calls of nodes

This [repository](https://github.com/hansgschossmann/iot-edge-opc-publisher-testserver.git) contains the implementation of an OPC UA server, which implements the nodes, which are used for all publishing/unpublishing operations and must run before the tests are started.

This [repository](https://github.com/hansgschossmann/iot-edge-opc-publisher-testbed.git) contains a docker compose configuration to start up the testbed automatically.
