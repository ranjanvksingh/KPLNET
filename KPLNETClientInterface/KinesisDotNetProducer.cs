using System;
using System.Collections.Generic;
using System.Text;
using System.Numerics;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;
using System.Collections;
using System.Security;
using System.Threading.Tasks;
using System.Threading;

using Aws.Kinesis.Protobuf;

using KPLNETInterface;

namespace KPLNETClientInterface
{
    /**
    * An interface to the native KPL daemon. This class handles the creation,
    * destruction and use of the child process.
    * 
    * <p>
    * <b>Use a single instance within the application whenever possible:</b>
    * <p>
    * <ul>
    * <li>One child process is spawned per instance of KinesisProducer. Additional
    * instances introduce overhead and reduce aggregation efficiency.</li>
    * <li>All streams within a region that can be accessed with the same
    * credentials can share the same KinesisProducer instance.</li>
    * <li>The {@link #addUserRecord} methods are thread safe, and be called
    * concurrently.</li>
    * <li>Therefore, unless you need to put to multiple regions, or need to use
    * different credentials for different streams, you should avoid creating
    * multiple instances of KinesisProducer.</li>
    * </ul>
    * <p>
    */
    public class KinesisDotNetProducer
    {
        private static readonly BigInteger UINT_128_MAX = BigInteger.Pow(UInt64.MaxValue, 2);
        private static readonly Object EXTRACT_BIN_MUTEX = new Object();
        private readonly ILogger log = null;
        private string pathToExecutable;
        private string pathToLibDir;
        private string pathToTmpDir;
        private long messageNumber = 1;
        private DateTime lastChild = DateTime.Now;
        private KPLNETConfiguration config;
        private Dictionary<string, string> env;
        private ConcurrentDictionary<long, TaskCompletionSource<KPLDotNetResult>> futures = new ConcurrentDictionary<long, TaskCompletionSource<KPLDotNetResult>>();
        private IKPLRep child;
        private bool destroyed = false;
        private ProcessFailureBehavior processFailureBehavior = ProcessFailureBehavior.AutoRestart;
        public const string AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
        public const string AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";

        /**
        * Start up a KinesisProducer instance.
        * 
        * <p>
        * Since this creates a child process, it is fairly expensive. Avoid
        * creating more than one per application, unless putting to multiple
        * regions at the same time. All streams in the same region can share the
        * same instance.
        * 
        * <p>
        * All methods in KinesisProducer are thread-safe.
        * 
        * @param config
        *            Configuration for the KinesisProducer. See the docs for that
        *            class for details.
        * 
        * @see KinesisProducerConfiguration
        */
        public KinesisDotNetProducer(ILogger log, KPLNETConfiguration config)
        {
            this.config = config;
            this.log = log;
            if(config.runAsDaemon)
                extractBinaries();

            if(config.AWSCredentials == null)
            {
                string akid = Environment.GetEnvironmentVariable(AWS_ACCESS_KEY_ID);
                string sKey = Environment.GetEnvironmentVariable(AWS_SECRET_ACCESS_KEY);
                if (akid == null || sKey == null)
                    throw new ArgumentNullException("AWSCredentials is null. AWS_ACCESS_KEY_ID or AWS_SECRET_KEY is not available in Environment variable. Either set the environment variables or pass AWSCredentials in the config object.");
                
                config.AWSCredentials = new AWSCredentials() { Akid = akid, SecretKey = sKey };
            }

            env = new Dictionary<string, string>()
                    {
                        {"LD_LIBRARY_PATH", pathToLibDir},
                        {"DYLD_LIBRARY_PATH", pathToLibDir},
                        {"CA_DIR", pathToTmpDir}
                    };

            child = KPLRepBuilder.GetKPLRep(pathToExecutable, OnMessage, OnError, pathToTmpDir, config, env, log);
        }

        /**
        * Start up a KinesisProducer instance.
        * 
        * <p>
        * Since this creates a child process, it is fairly expensive. Avoid
        * creating more than one per application, unless putting to multiple
        * regions at the same time. All streams in the same region can share the
        * same instance.
        * 
        * <p>
        * The KPL will use a set of default configurations. You can set custom
        * configuration using the constructor that takes a {@link KinesisProducerConfiguration}
        * object.
        * 
        * <p>
        * All methods in KinesisProducer are thread-safe.
        */
        public KinesisDotNetProducer(ILogger log) : this(log, new KPLNETConfiguration()) { }

        /**
        * Put a record asynchronously. A {@link ListenableFuture} is returned that
        * can be used to retrieve the result, either by polling or by registering a
        * callback.
        * 
        * <p>
        * The return value can be disregarded if you do not wish to process the
        * result. Under the covers, the KPL will automatically re-attempt puts in
        * case of transient errors (including throttling). A failed result is
        * generally returned only if an irrecoverable error is detected (e.g.
        * trying to put to a stream that doesn't exist), or if the record expires.
        *
        * <p>
        * <b>Thread safe.</b>
        * 
        * <p>
        * To add a listener to the future:
        * <p>
        * <code>
        * ListenableFuture&lt;PutRecordResult&gt; f = myKinesisProducer.addUserRecord(...);
        * com.google.common.util.concurrent.Futures.addCallback(f, callback, executor);
        * </code>
        * <p>
        * where <code>callback</code> is an instance of
        * {@link com.google.common.util.concurrent.FutureCallback} and
        * <code>executor</code> is an instance of
        * {@link java.util.concurrent.Executor}.
        * <p>
        * <b>Important:</b>
        * <p>
        * If long-running tasks are performed in the callbacks, it is recommended
        * that a custom executor be provided when registering callbacks to ensure
        * that there are enough threads to achieve the desired level of
        * parallelism. By default, the KPL will use an internal thread pool to
        * execute callbacks, but this pool may not have a sufficient number of
        * threads if a large number is desired.
        * <p>
        * Another option would be to hand the result off to a different component
        * for processing and keep the callback routine fast.
        * 
        * @param stream
        *            Stream to put to.
        * @param partitionKey
        *            Partition key. Length must be at least one, and at most 256
        *            (inclusive).
        * @param data
        *            Binary data of the record. Maximum size 1MiB.
        * @return A future for the result of the put.
        * @throws IllegalArgumentException
        *             if input does not meet stated constraints
        * @throws DaemonException
        *             if the child process is dead
        * @see ListenableFuture
        * @see UserRecordResult
        * @see KinesisProducerConfiguration#setRecordTtl(long)
        * @see UserRecordFailedException
        */
        public TaskCompletionSource<KPLDotNetResult> AddUserRecord(string stream, string partitionKey, MemoryStream data)
        {
            return AddUserRecord(stream, partitionKey, null, data);
        }

        /**
        * Put a record asynchronously. A {@link ListenableFuture} is returned that
        * can be used to retrieve the result, either by polling or by registering a
        * callback.
        * 
        * <p>            * The return value can be disregarded if you do not wish to process the
        * result. Under the covers, the KPL will automatically reattempt puts in
        * case of transient errors (including throttling). A failed result is
        * generally returned only if an irrecoverable error is detected (e.g.
        * trying to put to a stream that doesn't exist), or if the record expires.
        *
        * <p>
        * <b>Thread safe.</b>
        * 
        * <p>
        * To add a listener to the future:
        * <p>
        * <code>
        * ListenableFuture&lt;PutRecordResult&gt; f = myKinesisProducer.addUserRecord(...);
        * com.google.common.util.concurrent.Futures.addCallback(f, callback, executor);
        * </code>
        * <p>
        * where <code>callback</code> is an instance of
        * {@link com.google.common.util.concurrent.FutureCallback} and
        * <code>executor</code> is an instance of
        * {@link java.util.concurrent.Executor}.
        * <p>
        * <b>Important:</b>
        * <p>
        * If long-running tasks are performed in the callbacks, it is recommended
        * that a custom executor be provided when registering callbacks to ensure
        * that there are enough threads to achieve the desired level of
        * parallelism. By default, the KPL will use an internal thread pool to
        * execute callbacks, but this pool may not have a sufficient number of
        * threads if a large number is desired.
        * <p>
        * Another option would be to hand the result off to a different component
        * for processing and keep the callback routine fast.
        * 
        * @param stream
        *            Stream to put to.
        * @param partitionKey
        *            Partition key. Length must be at least one, and at most 256
        *            (inclusive).
        * @param explicitHashKey
        *            The hash value used to explicitly determine the shard the data
        *            record is assigned to by overriding the partition key hash.
        *            Must be a valid string representation of a positive integer
        *            with value between 0 and <tt>2^128 - 1</tt> (inclusive).
        * @param data
        *            Binary data of the record. Maximum size 1MiB.
        * @return A future for the result of the put.
        * @throws IllegalArgumentException
        *             if input does not meet stated constraints
        * @throws DaemonException
        *             if the child process is dead
        * @see ListenableFuture
        * @see UserRecordResult
        * @see KinesisProducerConfiguration#setRecordTtl(long)
        * @see UserRecordFailedException
        */
        public TaskCompletionSource<KPLDotNetResult> AddUserRecord(string stream, string partitionKey, string explicitHashKey, MemoryStream data)
        {
            if (stream == null)
                throw new ArgumentException("Stream name cannot be null");

            stream = stream.Trim();

            if (stream.Length == 0)
                throw new ArgumentException("Stream name cannot be empty");

            if (partitionKey == null)
                throw new ArgumentException("partitionKey cannot be null");

            if (partitionKey.Length < 1 || partitionKey.Length > 256)
                throw new ArgumentException("Invalid parition key. Length must be at least 1 and at most 256, got " + partitionKey.Length);

            try
            {
                UTF8Encoding.UTF8.GetBytes(partitionKey);
            }
            catch (Exception e)
            {
                throw new ArgumentException("Partition key must be valid UTF-8");
            }

            BigInteger b = new BigInteger(0);
            if (explicitHashKey != null)
            {
                explicitHashKey = explicitHashKey.Trim();
                try
                {
                    b = new BigInteger(UTF8Encoding.UTF8.GetBytes(explicitHashKey));
                }
                catch (FormatException e)
                {
                    throw new ArgumentException("Invalid explicitHashKey, must be an integer, got " + explicitHashKey);
                }

                if (b != null && b.CompareTo(UINT_128_MAX) > 0 || b.CompareTo(BigInteger.Zero) < 0)
                        throw new ArgumentException("Invalid explicitHashKey, must be greater or equal to zero and less than or equal to (2^128 - 1), got " + explicitHashKey);
            }

            if (data != null && data.Length > 1024 * 1024)
                throw new ArgumentException("Data must be less than or equal to 1MB in size, got " + data.Length + " bytes");

            long id = Interlocked.Increment(ref messageNumber) - 1;
            var f = new TaskCompletionSource<KPLDotNetResult>();
            futures.TryAdd(id, f);

            PutRecord pr = new PutRecord() { StreamName = stream, PartitionKey = partitionKey, Data = data != null ? Google.Protobuf.ByteString.CopyFrom(data.ToArray()) : Google.Protobuf.ByteString.Empty };
            if (!b.IsZero)
                pr.ExplicitHashKey = b.ToString();

            child.add(new Message() { Id = (ulong)id, PutRecord = pr });

            return f;
        }

        /**
        * Get the number of unfinished records currently being processed. The
        * records could either be waiting to be sent to the child process, or have
        * reached the child process and are being worked on.
        * 
        * <p>
        * This is equal to the number of futures returned from {@link #addUserRecord}
        * that have not finished.
        * 
        * @return The number of unfinished records currently being processed.
        */
        public int getOutstandingRecordsCount() { return futures.Count; }

        /**
        * Get metrics from the KPL.
        * 
        * <p>
        * The KPL computes and buffers useful metrics. Use this method to retrieve
        * them. The same metrics are also uploaded to CloudWatch (unless disabled).
        * 
        * <p>
        * Multiple metrics exist for the same name, each with a different list of
        * dimensions (e.g. stream name). This method will fetch all metrics with
        * the provided name.
        * 
        * <p>
        * See the stand-alone metrics documentation for details about individual
        * metrics.
        * 
        * <p>
        * This method is synchronous and will block while the data is being
        * retrieved.
        * 
        * @param metricName
        *            Name of the metrics to fetch.
        * @param windowSeconds
        *            Fetch data from the last N seconds. The KPL maintains data at
        *            per second granularity for the past minute. To get total
        *            cumulative data since the start of the program, use the
        *            overloads that do not take this argument.
        * @return A list of metrics with the provided name.
        * @throws ExecutionException
        *             If an error occurred while fetching metrics from the child
        *             process.
        * @throws InterruptedException
        *             If the thread is interrupted while waiting for the response
        *             from the child process.
        * @see Metric
        */
        public TaskCompletionSource<KPLDotNetResult> GetMetrics(string metricName, int windowSeconds)
        {
            MetricsRequest mrb = new MetricsRequest();
            if (!string.IsNullOrEmpty(metricName))
                mrb.Name = metricName;

            if (windowSeconds > 0)
                mrb.Seconds = (ulong)windowSeconds;

            long id = Interlocked.Increment(ref messageNumber) - 1;
            var f = new TaskCompletionSource<KPLDotNetResult>();
            futures.TryAdd(id, f);

            child.add(new Message() { Id = (ulong)id, MetricsRequest = mrb });

            return f;
        }

        /**
        * Get metrics from the KPL.
        * 
        * <p>
        * The KPL computes and buffers useful metrics. Use this method to retrieve
        * them. The same metrics are also uploaded to CloudWatch (unless disabled).
        * 
        * <p>
        * Multiple metrics exist for the same name, each with a different list of
        * dimensions (e.g. stream name). This method will fetch all metrics with
        * the provided name.
        * 
        * <p>
        * The retrieved data represents cumulative statistics since the start of
        * the program. To get data from a smaller window, use
        * {@link #getMetrics(string, int)}.
        * 
        * <p>
        * See the stand-alone metrics documentation for details about individual
        * metrics.
        * 
        * <p>
        * This method is synchronous and will block while the data is being
        * retrieved.
        * 
        * @param metricName
        *            Name of the metrics to fetch.
        * @return A list of metrics with the provided name.
        * @throws ExecutionException
        *             If an error occurred while fetching metrics from the child
        *             process.
        * @throws InterruptedException
        *             If the thread is interrupted while waiting for the response
        *             from the child process.
        * @see Metric
        */
        public TaskCompletionSource<KPLDotNetResult> GetMetrics(string metricName)
        {
            return GetMetrics(metricName, -1);
        }

        /**
        * Get metrics from the KPL.
        * 
        * <p>
        * The KPL computes and buffers useful metrics. Use this method to retrieve
        * them. The same metrics are also uploaded to CloudWatch (unless disabled).
        * 
        * <p>
        * This method fetches all metrics available. To fetch only metrics with a
        * given name, use {@link #getMetrics(string)}.
        * 
        * <p>
        * The retrieved data represents cumulative statistics since the start of
        * the program. To get data from a smaller window, use
        * {@link #getMetrics(int)}.
        * 
        * <p>
        * See the stand-alone metrics documentation for details about individual
        * metrics.
        * 
        * <p>
        * This method is synchronous and will block while the data is being
        * retrieved.
        * 
        * @return A list of all of the metrics maintained by the KPL.
        * @throws ExecutionException
        *             If an error occurred while fetching metrics from the child
        *             process.
        * @throws InterruptedException
        *             If the thread is interrupted while waiting for the response
        *             from the child process.
        * @see Metric
        */
        public TaskCompletionSource<KPLDotNetResult> GetMetrics()
        {
            return GetMetrics(null);
        }

        /**
        * Get metrics from the KPL.
        * 
        * <p>
        * The KPL computes and buffers useful metrics. Use this method to retrieve
        * them. The same metrics are also uploaded to CloudWatch (unless disabled).
        * 
        * <p>
        * This method fetches all metrics available. To fetch only metrics with a
        * given name, use {@link #getMetrics(string, int)}.
        * 
        * <p>
        * See the stand-alone metrics documentation for details about individual
        * metrics.
        * 
        * <p>
        * This method is synchronous and will block while the data is being
        * retrieved.
        * 
        * @param windowSeconds
        *            Fetch data from the last N seconds. The KPL maintains data at
        *            per second granularity for the past minute. To get total
        *            cumulative data since the start of the program, use the
        *            overloads that do not take this argument.
        * @return A list of metrics with the provided name.
        * @throws ExecutionException
        *             If an error occurred while fetching metrics from the child
        *             process.
        * @throws InterruptedException
        *             If the thread is interrupted while waiting for the response
        *             from the child process.
        * @see Metric
        */
        public TaskCompletionSource<KPLDotNetResult> GetMetrics(int windowSeconds)
        {
            return GetMetrics(null, windowSeconds);
        }

        /**
        * Immediately kill the child process. This will cause all outstanding
        * futures to fail immediately.
        * 
        * <p>
        * To perform a graceful shutdown instead, there are several options:
        * 
        * <ul>
        * <li>Call {@link #flush()} and wait (perhaps with a time limit) for all
        * futures. If you were sending a very high volume of data you may need to
        * call flush multiple times to clear all buffers.</li>
        * <li>Poll {@link #getOutstandingRecordsCount()} until it returns 0.</li>
        * <li>Call {@link #flushSync()}, which blocks until completion.</li>
        * </ul>
        * 
        * Once all records are confirmed with one of the above, call destroy to
        * terminate the child process. If you are terminating the JVM then calling
        * destroy is unnecessary since it will be done automatically.
        */
        public void Destroy()
        {
            destroyed = true;
            child.destroy();
        }

        /**
        * Instruct the child process to perform a flush, sending some of the
        * records it has buffered for the specified stream.
        * 
        * <p>
        * This does not guarantee that all buffered records will be sent, only that
        * most of them will; to flush all records and wait for completion, use
        * {@link #flushSync}.
        * 
        * <p>
        * This method returns immediately without blocking.
        * 
        * @param stream
        *            Stream to flush
        * @throws DaemonException
        *             if the child process is dead
        */
        public void Flush(string stream)
        {
            Aws.Kinesis.Protobuf.Flush f = new Aws.Kinesis.Protobuf.Flush();
            if (stream != null) f.StreamName = stream;
            Message m = new Message() { Id = (ulong)(Interlocked.Increment(ref messageNumber) - 1), Flush = f };
            child.add(m);
        }

        /**
        * Instruct the child process to perform a flush, sending some of the
        * records it has buffered. Applies to all streams.
        * 
        * <p>
        * This does not guarantee that all buffered records will be sent, only that
        * most of them will; to flush all records and wait for completion, use
        * {@link #flushSync}.
        * 
        * <p>
        * This method returns immediately without blocking.
        * 
        * @throws DaemonException
        *             if the child process is dead
        */
        public void Flush()
        {
            Flush(null);
        }

        /**
        * Instructs the child process to flush all records and waits until all
        * records are complete (either succeeding or failing).
        * 
        * <p>
        * The wait includes any retries that need to be performed. Depending on
        * your configuration of record TTL and request timeout, this can
        * potentially take a long time if the library is having trouble delivering
        * records to the backend, for example due to network problems. 
        * 
        * <p>
        * This is useful if you need to shutdown your application and want to make
        * sure all records are delivered before doing so.
        * 
        * @throws DaemonException
        *             if the child process is dead
        * 
        * @see KinesisProducerConfiguration#setRecordTtl(long)
        * @see KinesisProducerConfiguration#setRequestTimeout(long)
        */
        public void FlushSync()
        {
            while (getOutstandingRecordsCount() > 0)
            {
                Flush();
                try { Thread.Sleep(500); } catch (ThreadInterruptedException e) { }
            }
        }

        private void OnMessage(Message m)
        {
            Task.Run(() =>
            {
                if (m != null && m.PutRecordResult != null)
                    onPutRecordResult(m);
                else if (m != null && m.MetricsResponse != null)
                    onMetricsResponse(m);
                else
                    log.error("Unexpected message type from child process");
            });
        }

        private void OnError(Exception t)
        {
            // Don't log error if the user called destroy
            if (!destroyed)
            {
                log.error("Error in child process", t);
            }

            // Fail all outstanding futures
            foreach (var entry in futures)
            {
                Task.Run(() =>
                {
                    entry.Value.SetException(t);
                });
            }
            futures.Clear();

            if (processFailureBehavior == ProcessFailureBehavior.AutoRestart && !destroyed)
            {
                log.info("Restarting native producer process.");
                child = KPLRepBuilder.GetKPLRep(pathToExecutable, OnMessage, OnError, pathToTmpDir, config, env, log);
            }
            else
            {
                // Only restart child if it's not an irrecoverable error, and if
                // there has been some time (3 seconds) between the last child
                // creation. If the child process crashes almost immediately, we're
                // going to abort to avoid going into a loop.
                if (!(t is IrrecoverableError) && DateTime.Now > lastChild.AddSeconds(3))
                {
                    lastChild = DateTime.Now;
                    child = KPLRepBuilder.GetKPLRep(pathToExecutable, OnMessage, OnError, pathToTmpDir, config, env, log);
                }
            }
        }

        private void extractBinaries()
        {
            lock (EXTRACT_BIN_MUTEX)
            {
                List<string> watchFiles = new List<string>(2);
                string root = "amazon-kinesis-producer-native-binaries";
                string tmpDir = config.tempDirectory;
                if (tmpDir.Trim().Length == 0)
                    tmpDir = Path.GetTempPath();

                tmpDir = tmpDir + @"\" + root;
                pathToTmpDir = tmpDir;

                string binPath = config.nativeExecutable;
                if (string.IsNullOrEmpty(binPath))
                {
                    log.info("Extracting binaries to " + tmpDir);
                    try
                    {
                        if (!Directory.Exists(tmpDir) && Directory.CreateDirectory(tmpDir) != null)
                            throw new IOException("Could not create tmp dir " + tmpDir);

                        string executableName = "kinesis_producer.exe";
                        byte[] bin = GetResourceFile(root + "/windows/" + executableName);
                        string mdHex = HashPassword(bin);

                        pathToExecutable = pathToTmpDir + @"\kinesis_producer_" + mdHex + ".exe";
                        watchFiles.Add(pathToExecutable);
                        if (File.Exists(pathToExecutable))
                        {
                            bool contentEqual = false;
                            if (new FileInfo(pathToExecutable).Length == bin.Length)
                            {
                                byte[] existingBin = File.ReadAllBytes(pathToExecutable);
                                contentEqual = StructuralComparisons.StructuralEqualityComparer.Equals(bin, existingBin);
                            }
                            if (!contentEqual)
                            {
                                throw new SecurityException("The contents of the binary " + Path.GetFullPath(pathToExecutable) + " is not what it's expected to be.");
                            }
                        }
                        else
                        {
                            File.WriteAllBytes(pathToExecutable, bin);
                        }

                        string certFileName = "b204d74a.0";
                        string certFile = pathToTmpDir + @"\" + certFileName;
                        if (!File.Exists(certFile))
                        {
                            byte[] certs = GetResourceFile("cacerts/" + certFileName);
                            File.WriteAllBytes(certFile, certs);
                        }

                        watchFiles.Add(certFile);
                        pathToLibDir = pathToTmpDir;
                        FileAgeManager.GetInstance().registerFiles(watchFiles);
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Could not copy native binaries to temp directory " + tmpDir, e);
                    }

                }
                else
                {
                    pathToExecutable = binPath.Trim();
                    log.warn("Using non-default native binary at " + pathToExecutable);
                    pathToLibDir = "";
                }

            }
        }

        /**
        * Matches up the incoming PutRecordResult to an outstanding future, and
        * completes that future with the appropriate data.
        * 
        * <p>
        * We adapt the protobuf PutRecordResult to another simpler
        * PutRecordResult class to hide the protobuf stuff from the user.
        * 
        * @param msg
        */
        private void onPutRecordResult(Message msg)
        {
            TaskCompletionSource<KPLDotNetResult> f = getFuture(msg);
            UserRecordResult result = UserRecordResult.fromProtobufMessage(msg.PutRecordResult);
            
            if (result.isSuccessful())
                f.SetResult(new KPLDotNetResult() { KPLResultType = KPLDotNetResultType.UserRecordResult, UserResult = result });
            else
                f.SetException(new UserRecordFailedException(result));
        }

        private void onMetricsResponse(Message msg)
        {
            TaskCompletionSource<KPLDotNetResult> f = getFuture(msg);
            List<Metric> userMetrics = new List<Metric>();
            MetricsResponse res = msg.MetricsResponse;
            foreach (Aws.Kinesis.Protobuf.Metric metric in res.Metrics)
                userMetrics.Add(new Metric(metric));

            f.SetResult(new KPLDotNetResult() { KPLResultType = KPLDotNetResultType.Metrics, Metrics = userMetrics });
        }

        private TaskCompletionSource<KPLDotNetResult> getFuture(Message msg)
        {
            ulong id = msg.SourceId;
            TaskCompletionSource<KPLDotNetResult> f = null;
            if (!futures.TryRemove((long)id, out f) || f == null)
                throw new Exception("Future for message id " + id + " not found");

            return f;
        }

        private byte[] GetResourceFile(string resourceName)
        {
            return null;
        }

        private string HashPassword(byte[] inputBytes)
        {
            var sha1 = SHA1Managed.Create();
            byte[] outputBytes = sha1.ComputeHash(inputBytes);
            return BitConverter.ToString(outputBytes).Replace("-", "").ToLower();
        }
    }
}
