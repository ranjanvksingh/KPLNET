using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Aws.Kinesis.Protobuf;
using Google.Protobuf;

using KPLNETInterface;

namespace KPLNETClientInterface
{
    enum ProcessFailureBehavior { Shutdown, AutoRestart }
    class UserRecordFailedException : Exception
    {
        private static readonly long serialVersionUID = 3168271192277927600L;
        private UserRecordResult result;

        public UserRecordFailedException(UserRecordResult result)
        {
            this.result = result;
        }

        public UserRecordResult getResult()
        {
            return result;
        }
    }
    class IrrecoverableError : Exception
    {
        private static readonly long serialVersionUID = 2657526423645068574L;

        public IrrecoverableError(String message) : base(message) { }

        public IrrecoverableError(Exception t) : this("IrrecoverableError", t) { }

        public IrrecoverableError(String message, Exception t) : base(message, t) { }
    }
    class DaemonException : Exception
    {
        private static readonly long serialVersionUID = -1161618354800585162L;

        public DaemonException(String message) : base(message) { }
    }
    class FileAgeManager
    {
        private static readonly FileAgeManager instance = new FileAgeManager();
        private readonly HashSet<string> watchedFiles;
        public static FileAgeManager GetInstance()
        {
            return instance;
        }

        private FileAgeManager()
        {
            this.watchedFiles = new HashSet<string>();
            new Timer((obj) => run(), null, 1000, 1000);
        }

        public void registerFiles(List<string> toRegister)
        {
            foreach (string f in toRegister)
                watchedFiles.Add(f);
        }

        public void run()
        {
            foreach (string file in watchedFiles)
                if (File.Exists(file))
                    File.SetLastWriteTime(file, DateTime.Now);
        }
    }

    public class Daemon : IKPLRep
    {
        private long shutdown = 0;
        private Process process = null;
        private byte[] rcvBuf = new byte[8 * 1024 * 1024];
        private Queue<Message> outgoingMessages = new Queue<Message>();
        private Queue<Message> incomingMessages = new Queue<Message>();

        private string inPipe;
        private string outPipe;
        private StreamBuffer inSB;
        private StreamBuffer outSB;
        private LogInputStreamReader stdOutReader;
        private LogInputStreamReader stdErrReader;
        private NamedPipeServerStream inPipeServer;
        private NamedPipeServerStream outPipeServer;

        private readonly ILogger log;
        private readonly String workingDir;
        private readonly String pathToExecutable;
        private readonly KPLNETConfiguration config;
        private readonly Action<Message> messageHandler;
        private readonly Action<Exception> exceptionHandler;
        private readonly Dictionary<String, String> environmentVariables;

        public const Int32 SERVER_IN_BUFFER_SIZE = 4096;
        public const Int32 SERVER_OUT_BUFFER_SIZE = 4096;

        public static Daemon GetTestDaemon(string inPipe, string outPipe)
        {
            return new Daemon(inPipe, outPipe, true);
        }

        private Daemon(string inPipe, string outPipe, bool forTesting)
        {
            if (!forTesting)
                throw new Exception("This constructor is for unit testing only.");

            this.inPipe = inPipe;
            this.outPipe = outPipe;
            StartNamedPipeServerStreams();
            new Thread(() =>
            {
                Parallel.Invoke(new Action[]
                {
                    () => {while (Interlocked.Read(ref shutdown) == 0) sendMessage();},
                    () => {while (Interlocked.Read(ref shutdown) == 0) receiveMessage();}
                });
            }).Start();
        }

        /**
         * Starts up the child process, connects to it, and beings sending and
         * receiving messages.
         * 
         * @param pathToExecutable
         *            Path to the binary that we will use to start up the child.
         * @param handler
         *            Message handler that handles messages received from the child.
         * @param workingDir
         *            Working directory. The KPL creates FIFO files; it will do so
         *            in this directory.
         * @param config
         *            KPL configuration.
         */
        public Daemon(String pathToExecutable, Action<Message> messageHandler, Action<Exception> exceptionHandler, String workingDir, KPLNETConfiguration config, Dictionary<String, String> environmentVariables, ILogger log)
        {
            this.environmentVariables = environmentVariables;
            this.pathToExecutable = pathToExecutable;
            this.exceptionHandler = exceptionHandler;
            this.messageHandler = messageHandler;
            this.workingDir = workingDir;
            this.config = config;
            this.log = log;

            Task.Run(() =>
            {
                try
                {
                    startChildProcess();
                }
                catch (Exception e)
                {
                    fatalError("Error running child process", e);
                }
            });
        }

        /**
        * Enqueue a message to be sent to the child process.
        * 
        * @param m
        */
        public void add(Message m)
        {
            if (Interlocked.Read(ref shutdown) == 1)
                fatalError("The child process has been shutdown and can no longer accept messages.");

            try
            {
                lock (outgoingMessages)
                    outgoingMessages.Enqueue(m);
            }
            catch (ThreadInterruptedException e)
            {
                fatalError("Unexpected error", e);
            }
        }

        /**
         * Immediately kills the child process and shuts down the threads in this
         * Daemon.
         */
        public void destroy()
        {
            fatalError("Destroy is called", false);
        }

        public string InPipeAbsolutePath()
        {
            return @"\\.\pipe\" + inPipe;
        }

        public string OutPipeAbsolutePath()
        {
            return @"\\.\pipe\" + outPipe;
        }

        public String getPathToExecutable()
        {
            return pathToExecutable;
        }

        public String getWorkingDir()
        {
            return workingDir;
        }

        public int getQueueSize()
        {
            return outgoingMessages.Count;
        }

        /**
         * Send a message to the child process. If there are no messages available
         * in the queue, this method blocks until there is one.
         */
        private void sendMessage()
        {
            try
            {
                if (!outPipeServer.IsConnected)
                    outPipeServer.WaitForConnection();

                Message m = null;
                lock (outgoingMessages)
                {
                    if (outgoingMessages.Count() > 0)
                        m = outgoingMessages.Dequeue();
                }
                if(m == null)
                {
                    Thread.Sleep(5);
                    return;
                }
                if (outSB == null)
                    outSB = new StreamBuffer(outPipeServer);

                outSB.WriteBuffer(m.ToByteArray());
            }
            catch (IOException e)
            {
                fatalError("Error writing message to daemon", e);
            }
            catch (ThreadInterruptedException e)
            {
                fatalError("Error writing message to daemon", e);
            }
        }

        /**
         * Read a message from the child process off the wire. If there are no bytes
         * available on the socket, or there if there are not enough bytes to form a
         * complete message, this method blocks until there is.
         */
        private void receiveMessage()
        {
            try
            {
                if (!inPipeServer.IsConnected)
                    inPipeServer.WaitForConnection();

                if (inSB == null)
                    inSB = new StreamBuffer(inPipeServer);

                rcvBuf = inSB.ReadBuffer();
                var bs = ByteString.CopyFrom(rcvBuf);
                if (bs != null)
                {
                    var m = Message.Parser.ParseFrom(bs);
                    lock(incomingMessages)
                        incomingMessages.Enqueue(m);
                }
                else
                {
                    Thread.Sleep(5);
                }
            }
            catch (IOException e)
            {
                fatalError("Error reading message from daemon", e);
            }
            catch (ThreadInterruptedException e)
            {
                fatalError("Error reading message from daemon", e);
            }
        }

        public bool Try_Take(out Message m, bool forTesting)
        {
            m = null;
            if (!forTesting)
                throw new Exception("This is only for testing.");

            lock (incomingMessages)
                if (incomingMessages.Count > 0)
                        m = incomingMessages.Dequeue();

            return m != null;
        }

        /**
         * Invokes the message handler, giving it a message received from the child
         * process.
         */
        private void returnMessage()
        {
            try
            {
                Message m = null;
                lock (incomingMessages)
                {
                    if (incomingMessages.Count > 0)
                    {
                        m = incomingMessages.Dequeue();
                    }
                }
                if(m != null)
                {
                    if (messageHandler != null)
                    {
                        try
                        {
                            messageHandler(m);
                        }
                        catch (Exception e)
                        {
                            log.error("Error in message handler", e);
                        }
                    }
                }
                else
                    Thread.Sleep(5);                
            }
            catch (ThreadInterruptedException e)
            {
                fatalError("Unexpected error", e);
            }
        }

        /**
         * Send an update credentail message to the child process.
         */
        private void updateCredentials()
        {
            try
            {
                var cred = config.AWSCredentials;
                if (cred != null && KPLRepMessageBuilder.CheckAndUpdateCredentialIfChanged(cred))
                {
                    var m = KPLRepMessageBuilder.makeSetCredentialsMessage(cred.Akid, cred.SecretKey, cred.SessionToken, false);

                    lock (outgoingMessages)
                        outgoingMessages.Enqueue(m);

                    //var metricCred = config.AWSMetricsCredentials ?? config.AWSCredentials;
                    //if (metricCred == null)
                    //    outgoingMessages.Add(makeSetCredentialsMessage(metricCred.Akid, metricCred.SecretKey, metricCred.SessionToken, true));
                }
                else
                {

                }

                Thread.Sleep((int)config.credentialsRefreshDelay);
            }
            catch (ThreadInterruptedException e)
            {
                log.warn("Exception during updateCredentials", e);
            }
        }

        /**
         * Start up the loops that continuously send and receive messages to and
         * from the child process.
         */
        private void StartSendReceiveMessageLoops()
        {
            Parallel.Invoke(new Action[]
                {
                    () => {while (Interlocked.Read(ref shutdown) == 0) sendMessage();},
                    () => {while (Interlocked.Read(ref shutdown) == 0) returnMessage();},
                    () => {while (Interlocked.Read(ref shutdown) == 0) receiveMessage();},
                    () => {while (Interlocked.Read(ref shutdown) == 0) updateCredentials();}
                });
        }

        private void StartNamedPipeServerStreams()
        {
            DateTime start = DateTime.Now;
            while (true)
            {
                try
                {
                    inPipeServer = new NamedPipeServerStream(inPipe, PipeDirection.InOut);
                    outPipeServer = new NamedPipeServerStream(outPipe, PipeDirection.InOut);
                    break;
                }
                catch (IOException e)
                {
                    if (inPipeServer != null && inPipeServer.IsConnected)
                        inPipeServer.Close();
                    if (outPipeServer != null && outPipeServer.IsConnected)
                        outPipeServer.Close();

                    try
                    {
                        Thread.Sleep(100);
                    }
                    catch (ThreadInterruptedException) { }

                    if (DateTime.Now > start.AddSeconds(2))
                        throw e;
                }
            }
        }

        private string uuid8Chars()
        {
            return Guid.NewGuid().ToString().Substring(0, 8);
        }

        private string protobufToHex(Message msg)
        {
            return msg.SerializeAsString();
        }

        private void startChildProcess()
        {
            var credentials = config.AWSCredentials;
            var metricsCreds = config.AWSMetricsCredentials;
            if (metricsCreds == null)
                metricsCreds = credentials;

            var startInfo = new ProcessStartInfo(pathToExecutable);
            string c = protobufToHex(config.toProtobufMessage());
            string k = protobufToHex(KPLRepMessageBuilder.makeSetCredentialsMessage(credentials.Akid, credentials.SecretKey, credentials.SessionToken, false));
            string w = protobufToHex(KPLRepMessageBuilder.makeSetCredentialsMessage(metricsCreds.Akid, metricsCreds.SecretKey, metricsCreds.SessionToken, true));

            do { inPipe = "amz-aws-kpl-in-pipe-" + uuid8Chars(); } while (File.Exists(@"\\.\pipe\" + inPipe));
            do { outPipe = "amz-aws-kpl-out-pipe-" + uuid8Chars(); } while (File.Exists(@"\\.\pipe\" + outPipe));

            startInfo.Arguments = " -o \"" + inPipe + "\" -i \"" + outPipe + "\" -c \"" + c + "\" -k \"" + k + "\" -w \"" + w;

            startInfo.UseShellExecute = false;
            foreach (var ev in environmentVariables)
                startInfo.EnvironmentVariables[ev.Key] = ev.Value;

            new Thread(() =>
                {
                    try
                    {
                        StartNamedPipeServerStreams();
                        StartSendReceiveMessageLoops();
                    }
                    catch (IOException e)
                    {
                        fatalError("Unexpected error connecting to child process", e, false);
                    }
                }).Start();

            try
            {
                startInfo.RedirectStandardOutput = true;
                startInfo.RedirectStandardError = true;
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                fatalError("Error starting child process", e, false);
            }

            stdOutReader = new LogInputStreamReader(process.StandardOutput, "StdOut", (logger, message) => { logger.info(message); }, log);
            stdErrReader = new LogInputStreamReader(process.StandardError, "StdErr", (logger, message) => { logger.warn(message); }, log);

            Task.Run(() => stdOutReader.Run());
            Task.Run(() => stdErrReader.Run());

            process.WaitForExit();

            stdOutReader.shutdown();
            stdErrReader.shutdown();

            try
            {
                if (inPipeServer != null)
                    inPipeServer.Close();
                if (outPipeServer != null)
                    outPipeServer.Close();
            }
            catch (Exception)
            {
            }

            fatalError("Child process exited.");
        }

        private void fatalError(String message)
        {
            fatalError(message, true);
        }

        private void fatalError(String message, bool retryable)
        {
            fatalError(message, null, retryable);
        }

        private void fatalError(String message, Exception t)
        {
            fatalError(message, t, true);
        }

        private void fatalError(String message, Exception t, bool retryable)
        {
            if (Interlocked.CompareExchange(ref shutdown, 1, 0) == 0)
            {
                if (process != null)
                {
                    if (stdErrReader != null)
                        stdErrReader.prepareForShutdown();
                    if (stdOutReader != null)
                        stdOutReader.prepareForShutdown();

                    process.Close();
                }
                try
                {
                    Thread.Sleep(1000);
                }
                catch (ThreadInterruptedException) { }

                if (exceptionHandler != null)
                {
                    if (retryable)
                        exceptionHandler(t != null ? new Exception(message, t) : new Exception(message));
                    else
                        exceptionHandler(t != null ? new IrrecoverableError(message, t) : new IrrecoverableError(message));
                }
            }
        }
    }

}
