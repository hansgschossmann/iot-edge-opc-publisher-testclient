using Mono.Options;
using Opc.Ua;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OpcPublisherTestClient
{
    using System.Reflection;
    using static OpcMethodTest;

    public class Program
    {
        public static Serilog.Core.Logger Logger = null;

        public static bool RunExclusiveTests = true;

        // number of tags for testing huge number of tags
        public const int MULTI_TAG_NUM = 100000;
        // number of tags for regular tests
        public const int TEST_TAG_NUM = 500;
        // number of subscriptions for testing huge number of subscriptions
        public const int MAX_SUBSCRIPTIONS = 500;
        // number of endpoints URLs we test
        public const int MAX_SERVER_ENDPOINTS = 50;
        // long wait time
        public const int MAX_LONG_WAIT_SEC = 10;
        // short wait time
        public const int MAX_SHORT_WAIT_SEC = 5;

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

            Logger.Information($"OPC Publisher testclient");

            // command line options
            bool showHelp = false;
            int testTimeMillisec = Timeout.Infinite;
            bool opcMethods = false;
            bool iotHubMethods = false;
            string iotHubConnectionString = string.Empty;
            string iotHubPublisherDeviceName = string.Empty;
            string iotHubPublisherModuleName = string.Empty;
            int initialWait = 10000;

            Mono.Options.OptionSet options = new Mono.Options.OptionSet {
                { "h|help", "show this message and exit", h => showHelp = h != null },
                { "ip|initialpause=", $"initial wait in sec to allow other services to start up.\nDefault: {initialWait/1000}", (int i) => initialWait = i * 1000 },
                { "aa|autoaccept", "auto accept certificates (for testing only)", a => AutoAccept = a != null },
                { "ne|noexclusive", "do not execute any exclusive tests", ne => RunExclusiveTests = ne == null },
                { "tt|testtime=", "the number of seconds to run the different tests", (int t) => testTimeMillisec = t * 1000 },
                { "tu|testserverurl=", "URL of the OPC UA test server", (string s) => _testserverUrl = s },
                { "pu|publisherurl=", "URL of the OPC Publisher (required when using OPC UA methods)", (string s) => PublisherUrl = s },
                { "o1|opcmethods", "use the OPC UA methods calls to test",  b => opcMethods = b != null },
                { "ic|iothubconnectionstring=", "IoTHub owner connectionstring", (string s) => iotHubConnectionString = s },
                { "id|iothubdevicename=", "IoTHub device name of the OPC Publisher (required when using IoT methods)", (string s) => iotHubPublisherDeviceName = s },
                { "im|iothubmodulename=", "IoTEdge module name of the OPC Publisher which runs in IoTEdge specified by im/iothubdevicename(required when using IoT methods and IoTEdge)", (string s) => iotHubPublisherModuleName = s },
                { "i1|iothubmethods", "use IoTHub direct methods calls to test", b => iotHubMethods = b != null },
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

            // show usage if requested
            if (showHelp)
            {
                Usage(options);
                return;
            }

            // by default we are connecting to the OPC UA servers in the testbed 
            if (extraArgs.Count > 0)
            {
                for (int i = 1; i < extraArgs.Count; i++)
                {
                    Logger.Error("Error: Unknown option: {0}", extraArgs[i]);
                }
                Usage(options, args);
                return;
            }

            // initial wait
            Logger.Information($"Waiting for {initialWait/1000} secondes...");
            Thread.Sleep(initialWait);

            // sanity check parameters
            if (opcMethods == false && iotHubMethods == false)
            {
                Logger.Information($"No specific test area specified, enabling all.");
                opcMethods = iotHubMethods = true;
            }
            if (opcMethods)
            {
                Logger.Information($"Publisher URL: {PublisherUrl}");

            }
            if (iotHubMethods)
            {
                if (string.IsNullOrEmpty(iotHubConnectionString) || string.IsNullOrEmpty(iotHubPublisherDeviceName))
                {
                    Logger.Fatal("For any tests via IoTHub communication an IoTHub connection string and the publisher devicename (and modulename) must be specified.");
                    return;
                }
                Logger.Information($"IoTHub connectionstring: {iotHubConnectionString}");
                if (string.IsNullOrEmpty(iotHubPublisherModuleName))
                {
                    Logger.Information($"Testing OPC Publisher device.");
                    Logger.Information($"IoTHub Publisher device name: {iotHubPublisherDeviceName}");
                }
                else
                {
                    Logger.Information($"Testing OPC Publisher IoTEdge module.");
                    Logger.Information($"IoTEdge device name: {iotHubPublisherDeviceName}");
                    Logger.Information($"IoTHub Publisher device name: {iotHubPublisherModuleName}");
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

            // instantiate test objectes
            OpcMethodTest opcMethodTest = null;
            IotHubMethodTest iotHubMethodTest = null;
            if (opcMethods)
            {
                opcMethodTest = new OpcMethodTest(_testserverUrl, MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC, ct);
            }

            if (iotHubMethods)
            {
                iotHubMethodTest = new IotHubMethodTest(iotHubConnectionString, iotHubPublisherDeviceName, iotHubPublisherModuleName, _testserverUrl,
                    MAX_SHORT_WAIT_SEC, MAX_LONG_WAIT_SEC, ct);
            }

            // run all tests with need exclusive access to the server
            if (RunExclusiveTests && opcMethods)
            {
                opcMethodTest.RunExclusiveTests(ct);
            }

            if (RunExclusiveTests && iotHubMethods)
            {
                iotHubMethodTest.RunExclusiveTests(ct);
            }

            // run all tests which can be executed concurrently
            List<Task> testTasks = new List<Task>();
            if (opcMethods)
            {
                testTasks.AddRange(opcMethodTest.RunConcurrentTests(ct));
            }

            if (iotHubMethods)
            {
                testTasks.AddRange(iotHubMethodTest.RunConcurrentTests(ct));
            }

            // run all tests for the specified time or Ctrl-C is pressed
            Logger.Information($"Run tests {(testTimeMillisec != Timeout.Infinite ? $"for {testTimeMillisec/1000} seconds or" : "till")} CTRL-C is pressed");
            quitEvent.WaitOne(testTimeMillisec);
            Logger.Information($"Signal cancellation and wait will everything is completed.");
            cts.Cancel();
            // wait till all tasks are completed
            Task.WaitAll(testTasks.ToArray());
            Logger.Information($"Exiting....");
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
            Logger.Information("Usage: dotnet NetCoreConsoleClient.dll [OPTIONS]");
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

        /// <summary>
        /// Usage message.
        /// </summary>
        private static void Usage(Mono.Options.OptionSet options)
        {

            // show usage
            Logger.Information("");
            Logger.Information("Usage: {0}.exe [<options>]", Assembly.GetEntryAssembly().GetName().Name);
            Logger.Information("");
            Logger.Information("OPC Publisher test client. Requires iot-edge-opc-publisher-testclient and OPC Publisher.");
            Logger.Information("To exit the application, just press CTRL-C while it is running.");
            Logger.Information("");

            // output the options
            Logger.Information("Options:");
            StringBuilder stringBuilder = new StringBuilder();
            StringWriter stringWriter = new StringWriter(stringBuilder);
            options.WriteOptionDescriptions(stringWriter);
            string[] helpLines = stringBuilder.ToString().Split("\n");
            foreach (var line in helpLines)
            {
                Logger.Information(line);
            }
        }

        private static string _logFileName = $"{Utils.GetHostName()}-ìot-edge-opc-publisher-testclient.log";
        private static string _logLevel = "info";
        private static string _testserverUrl = "opc.tcp://testserver:62541/Quickstarts/ReferenceServer";
    }
}
