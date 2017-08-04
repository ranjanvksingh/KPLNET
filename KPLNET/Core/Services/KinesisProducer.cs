using System;
using System.Linq;
using System.Threading;
using System.Collections.Concurrent;
using System.Collections.Generic;

using Aws.Kinesis.Protobuf;
using Google.Protobuf;

using KPLNETInterface;
using KPLNET.Auth;
using KPLNET.Core.Clients.KinesisClient;
using KPLNET.Http;
using KPLNET.Metrics;
using KPLNET.Utils;

namespace KPLNET.Kinesis.Core
{
    class KinesisProducer
    {
        private const int kMessageDrainMinBackoff = 100; //ms
        private const int kMessageDrainMaxBackoff = 200;//10000;
        private const int kMessageMaxBatchSize = 16;
        private string region_;
        private KPLNETConfiguration config_;
        private AwsCredentialsProviderChain creds_chain_;
        private AwsCredentialsProviderChain metrics_creds_chain_;
        private Executor executor_;
        private AwsHttpClient http_client_;
        private AwsKinesisClient kinesis_client_;
        private IMessageManager ipc_manager_;
        private MetricsManager metrics_manager_;
        private ConcurrentDictionary<string, Pipeline> pipelines_;
        private bool shutdown_;
        private Thread message_drainer_;
        private ScheduledCallback report_outstanding_;
        private ISocketFactory socket_factory_;

        public KinesisProducer(IMessageManager ipc_manager, ISocketFactory socket_factory, string region, KPLNETConfiguration config, AwsCredentialsProvider creds_provider, AwsCredentialsProvider metrics_creds_provider, Executor executor)
        {
            region_ = region;
            config_ = config;
            socket_factory_ = socket_factory;
            executor_ = executor;
            ipc_manager_ = ipc_manager;
            creds_chain_ = AwsCredentialsProviderChain.create(new List<AwsCredentialsProvider> { creds_provider });
            metrics_creds_chain_ = metrics_creds_provider == creds_provider ? creds_chain_ : AwsCredentialsProviderChain.create(new List<AwsCredentialsProvider> { metrics_creds_provider });
            pipelines_ = new ConcurrentDictionary<string, Pipeline>();
            shutdown_ = false;

            create_metrics_manager();

            if (config.clientType == KPLNETInterface.ClientType.SocketClient)
                create_http_client();
            else
                create_kinesis_client();

            report_outstanding();

            message_drainer_ = new Thread(() => { drain_messages(); });
            message_drainer_.Start();
        }

        void create_http_client()
        {
            var endpoint = config_.kinesisEndpoint;
            var port = (int)config_.kinesisPort;

            if (string.IsNullOrEmpty(endpoint))
            {
                endpoint = "kinesis." + region_ + ".amazonaws.com";
                port = 443;
            }

            http_client_ = new AwsHttpClient(
                      executor_,
                      socket_factory_,
                      endpoint,
                      port,
                      true,
                      config_.verifyCertificate,
                      config_.minConnections,
                      config_.maxConnections,
                      config_.connectTimeout,
                      config_.requestTimeout);
        }

        void create_kinesis_client()
        {
            kinesis_client_ = new AwsKinesisClient(
                        executor_,
                        config_.AWSCredentials,
                        config_.region,
                        config_.minConnections,
                        config_.maxConnections,
                        config_.connectTimeout,
                        config_.requestTimeout);
        }

        Pipeline create_pipeline(string stream)
        {
            StdErrorOut.Instance.StdOut(LogLevel.info, string.Format("Created pipeline for stream \"{0}\"", stream));
            return new Pipeline
            (
                region_,
                stream,
                config_,
                executor_,
                http_client_,
                kinesis_client_,
                creds_chain_,
                metrics_manager_,
                (ur) => ipc_manager_.put(ur.to_put_record_result())
            );
        }

        void drain_messages()
        {
            int backoff = kMessageDrainMinBackoff;

            while (!shutdown_)
            {
                Message m;
                List<Message> buf = new List<Message>();
                // The checks must be in this order because try_take has a side effect
                while (buf.Count < kMessageMaxBatchSize && ipc_manager_.try_take(out m) && m != null)
                    lock (buf)
                        buf.Add(m);

                StdErrorOut.Instance.StdOut(LogLevel.debug, string.Format("buf.Count [ = {0}] != 0", buf.Count));
                if (buf.Count != 0)
                {
                    Message[] batch;
                    lock (buf)
                    {
                        batch = new Message[buf.Count];
                        buf.CopyTo(batch, 0);
                    }

                    buf = null;
                    StdErrorOut.Instance.StdOut(LogLevel.debug, "at time of submit batch.Count() = " + batch.Count());
                    executor_.Submit
                        (
                            () =>
                            {
                                try
                                {
                                    StdErrorOut.Instance.StdOut(LogLevel.debug, "at time of execution batch.Count() = " + batch.Count());
                                    for (int i = 0; i < batch.Count(); i++)
                                    {
                                        var v = batch[i];
                                        if (v != null)
                                        {
                                            StdErrorOut.Instance.StdOut(LogLevel.debug, "before calling on_ipc_message(v)");
                                            on_ipc_message(v);
                                        }
                                        else
                                        {
                                            StdErrorOut.Instance.StdOut(LogLevel.debug, "var v = batch2[i] is null for i = " + i);
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    StdErrorOut.Instance.StdOut(LogLevel.error, string.Format("Error while calling on_ipc_message(v). Exception = {0}", e.ToString()));
                                }
                            }
                        );

                backoff = kMessageDrainMinBackoff;
            }
                else
                {
                Thread.Sleep(backoff);
                backoff = Math.Min(backoff * 2, kMessageDrainMaxBackoff);
                StdErrorOut.Instance.StdOut(LogLevel.debug, "drain message backoff = " + backoff);
            }
        }
    }

    void report_outstanding()
    {
        foreach (var kv in pipelines_)
        {
            metrics_manager_.finder()
                .set_name(Names.UserRecordsPending)
                .set_stream(kv.Key)
                .find()
                .Put(kv.Value.outstanding_user_records());
        }

        var delay = 200;
        if (null == report_outstanding_)
            report_outstanding_ = executor_.Schedule(() => { report_outstanding(); }, delay);
        else
            report_outstanding_.reschedule(delay);
    }

    void on_ipc_message(Message m)
    {
        StdErrorOut.Instance.StdOut(LogLevel.debug, "void on_ipc_message(Message m)");
        if (m.PutRecord != null)
        {
            on_put_record(m);
        }
        else if (m.Flush != null)
        {
            on_flush(m.Flush);
        }
        else if (m.SetCredentials != null)
        {
            on_set_credentials(m.SetCredentials);
        }
        else if (m.MetricsRequest != null)
        {
            on_metrics_request(m);
        }
        else
            StdErrorOut.Instance.StdError("Received unknown message type");
    }

    void on_put_record(Message m)
    {
        var ur = new UserRecord(m);
        ur.set_deadline_from_now((long)config_.recordMaxBufferedTime);
        ur.set_expiration_from_now((long)config_.recordTtl);

        var stream = ur.Stream();
        var pipeline = GetPipeline(stream);
        pipeline.put(ur);
    }

    Pipeline GetPipeline(string streamName)
    {
        if (!pipelines_.ContainsKey(streamName))
            pipelines_[streamName] = create_pipeline(streamName);

        return pipelines_[streamName];
    }

    void on_flush(Flush flush_msg)
    {
        if (!string.IsNullOrEmpty(flush_msg.StreamName))
            GetPipeline(flush_msg.StreamName).flush();
        else
            foreach (var kv in pipelines_)
                kv.Value.flush();
    }

    void on_set_credentials(SetCredentials set_creds)
    {
        // If the metrics_creds_chain_ is pointing to the same instance as the regular creds_chain_, and we receive a new set of creds just for metrics, we need to create a new instance of AwsCredentialsProviderChain for metrics_creds_chain_ to point to. Otherwise we'll wrongly override the regular credentials when setting the metrics credentials.
        if (set_creds.ForMetrics && metrics_creds_chain_ == creds_chain_)
            metrics_creds_chain_ = new AwsCredentialsProviderChain();

        AwsCredentialsProviderChain chain = set_creds.ForMetrics ? metrics_creds_chain_ : creds_chain_;
        chain.reset
          (
             new List<AwsCredentialsProvider> {
                    new BasicAwsCredentialsProvider
                    (
                        set_creds.Credentials.Akid,
                        set_creds.Credentials.SecretKey,
                        set_creds.Credentials.Token
                    )
            }
          );
    }

    public void Join()
    {
        executor_.Join();
    }

    void create_metrics_manager()
    {
        Level level;
        Enum.TryParse(config_.metricsLevel, out level);
        Granularity granularity;
        Enum.TryParse(config_.metricsGranularity, out granularity);

        List<Tuple<string, string, Granularity>> extra_dims = new List<Tuple<string, string, Granularity>>();
        foreach (var t in config_.AdditionalMetricsDimension())
            extra_dims.Add(new Tuple<string, string, Granularity>(t.Key, t.Value, (Granularity)Enum.Parse(typeof(Granularity), t.Granularity)));

        metrics_manager_ = new MetricsManager(
                executor_,
                socket_factory_,
                metrics_creds_chain_,
				config_,
				region_,
                config_.metricsNamespace,
                level,
                granularity,
                extra_dims,
                config_.kinesisEndpoint,
                config_.kinesisPort,
                config_.metricsUploadDelay);
    }

    void on_metrics_request(Message m)
    {
        var req = m.MetricsRequest;
        List<Metrics.Metric> metrics = new List<Metrics.Metric>();

        // filter by name, if necessary
        if (string.IsNullOrEmpty(req.Name))
        {
            foreach (var metric in metrics_manager_.all_metrics())
            {
                var dims = metric.All_dimensions();
                if (dims.Count == 0 || dims[0].Key != "MetricName")
                    return;

                if (dims[0].Value == req.Name)
                    metrics.Add(metric);
            }
        }
        else
            metrics = metrics_manager_.all_metrics();

        // convert the data into protobuf
        Message reply = new Message();
        reply.Id = (ulong)new Random().Next();
        reply.SourceId = m.Id;
        var res = reply.MetricsResponse;

        foreach (var metric in metrics)
        {
            var dims = metric.All_dimensions();

            if (dims.Count == 0 || dims[0].Key == "MetricName")
                return;

            var pm = new Aws.Kinesis.Protobuf.Metric();
            pm.Name = dims[0].Value;
            res.Metrics.Add(pm);

            for (int i = 1; i < dims.Count; i++)
            {
                var d = new Aws.Kinesis.Protobuf.Dimension();
                d.Key = dims[i].Key;
                d.Value = dims[i].Value;
                pm.Dimensions.Add(d);
            }

            var accum = metric.Accumulator();
            var stats = pm.Stats;

            if (req.Seconds != 0)
			{
				int s = (int)req.Seconds;
                stats.Count = accum.count(s);
                stats.Sum = accum.sum(s);
                stats.Min = accum.min(s);
                stats.Max = accum.max(s);
                stats.Mean = accum.mean(s);

				pm.Seconds = (ulong)s;
			}
            else
            {
                stats.Count = accum.count();
                stats.Sum = accum.sum();
                stats.Min = accum.min();
                stats.Max = accum.max();
                stats.Mean = accum.mean();
                pm.Seconds = (ulong)accum.elapsedSeconds();
            }
        }

        ipc_manager_.put(reply);
    }

}
}
