using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;

using KPLNET.Auth;
using KPLNET.Http;
using KPLNET.Utils;

namespace KPLNET.Metrics
{
    using Dimension = KeyValuePair<string, string>;
    using Dimensions = List<KeyValuePair<string, string>>;
    using ExtraDimensions = List<Tuple<string, string, Granularity>>;
    using ExtraDimMap = Dictionary<Granularity, List<KeyValuePair<string, string>>>;
    using Amazon.CloudWatch.Model;
    using System.Threading;

    public enum Granularity
    {
        Global = 0,
        Stream = 100,
        Shard = 200
    }
    public enum State
    {
        EMPTY,
        HAS_NAME,
        HAS_ERR_CODE,
        HAS_STREAM,
        HAS_SHARD
    }
    public enum Level
    {
        None = 0,
        Summary = 100,
        Detailed = 200
    }
    public struct Names
    {
        public static string Test;

        public static string UserRecordsReceived;
        public static string UserRecordsPending;
        public static string UserRecordsPut;
        public static string UserRecordsDataPut;

        public static string KinesisRecordsPut;
        public static string KinesisRecordsDataPut;

        public static string ErrorsByCode;
        public static string AllErrors;
        public static string RetriesPerRecord;
        public static string UserRecordExpired;

        public static string BufferingTime;
        public static string RequestTime;

        public static string UserRecordsPerKinesisRecord;
        public static string KinesisRecordsPerPutRecordsRequest;
        public static string UserRecordsPerPutRecordsRequest;
    }
    public struct Units
    {
        public static string Count;
        public static string Milliseconds;
        public static string Bytes;
        public static string None;
    }
    public struct DimensionNames
    {
        public static string MetricName;
        public static string StreamName;
        public static string ShardId;
        public static string ErrorCode;
    }
    public class MetricsConstant
    {
        public static bool filter(List<KeyValuePair<string, string>> dimensions, Level max_level, Granularity max_granularity)
        {
            var level_map = new Dictionary<string, Level>
                    {
					      {Names.Test, Level.Detailed },
					      {Names.UserRecordsReceived, Level.Detailed },
					      {Names.UserRecordsPending, Level.Detailed },
					      {Names.UserRecordsPut, Level.Summary },
					      {Names.UserRecordsDataPut, Level.Detailed },
					      {Names.KinesisRecordsPut, Level.Summary },
					      {Names.KinesisRecordsDataPut, Level.Detailed },
					      {Names.ErrorsByCode, Level.Summary },
					      {Names.AllErrors, Level.Summary },
					      {Names.RetriesPerRecord, Level.Detailed },
					      {Names.UserRecordExpired, Level.Summary },
					      {Names.BufferingTime, Level.Summary },
					      {Names.RequestTime, Level.Detailed },
					      {Names.UserRecordsPerKinesisRecord, Level.Detailed },
					      {Names.KinesisRecordsPerPutRecordsRequest, Level.Detailed },
					      {Names.UserRecordsPerPutRecordsRequest, Level.Detailed }
                    };

            if (max_level == Level.None)
                return false;

            if (dimensions.Count == 0 || dimensions[0].Key != "MetricName")
                return false;

            var level = Level.Detailed;
            if (level_map.ContainsKey(dimensions[0].Value))
                level = level_map[dimensions[0].Value];
            else
                StdErrorOut.Instance.StdOut(LogLevel.warn, string.Format("Unknown metric {0}, did you forget to add it to MetricsConstant.filter()?", dimensions[0].Value));

            Granularity granularity = Granularity.Global;
            foreach (var p in dimensions)
            {
                if (p.Key == "StreamName")
                    granularity = Granularity.Stream;
                else if (p.Key == "ShardId")
                    granularity = Granularity.Shard;
            }

            if (level > max_level || granularity > max_granularity)
                return false;

            return true;
        }

        public static Level level(string s)
        {
            if (s == "none")
                return Level.None;
            else if (s == "summary")
                return Level.Summary;
            else if (s == "detailed")
                return Level.Detailed;
            else
                throw new Exception("Unknown metric level " + s);
        }

        public static Granularity granularity(string s)
        {
            if (s == "global")
                return Granularity.Global;
            else if (s == "stream")
                return Granularity.Stream;
            else if (s == "shard")
                return Granularity.Shard;
            else
                throw new Exception("Unknown metric granularity " + s);
        }

        public static string unit(string name)
        {
            var unit_map = new Dictionary<string, string>
                    {
					  {Names.Test, Units.Count },

					  {Names.UserRecordsReceived, Units.Count },
					  {Names.UserRecordsPending, Units.Count },
					  {Names.UserRecordsPut, Units.Count },
					  {Names.UserRecordsDataPut, Units.Bytes },

					  {Names.KinesisRecordsPut, Units.Count },
					  {Names.KinesisRecordsDataPut, Units.Bytes },

					  {Names.ErrorsByCode, Units.Count },
					  {Names.AllErrors, Units.Count },
					  {Names.RetriesPerRecord, Units.Count },
					  {Names.UserRecordExpired, Units.Count },

					  {Names.BufferingTime, Units.Milliseconds },
					  {Names.RequestTime, Units.Milliseconds },

					  {Names.UserRecordsPerKinesisRecord, Units.Count },
					  {Names.KinesisRecordsPerPutRecordsRequest, Units.Count },
					  {Names.UserRecordsPerPutRecordsRequest, Units.Count }
                    };

            if (unit_map.ContainsKey(name))
                return unit_map[name];


            StdErrorOut.Instance.StdOut(LogLevel.warn, string.Format("Unknown metric {0}, did you forget to add it to MetricsConstant.filter()?", name));
            return Units.None;
        }
    }

    public class MetricsManager
    {
        protected Executor executor;
        protected string endpoint;
        protected AwsHttpClient http_client;
        protected AwsCredentialsProvider creds;
        protected string region;
        protected string cw_namespace;
        protected Level level;
        protected Granularity granularity;
        protected ExtraDimMap extra_dimensionsMap = new ExtraDimMap();
        protected double upload_frequency;
        protected double retry_frequency;
        protected MetricsIndex metrics_index;
        protected ScheduledCallback scheduled_upload;
        protected ScheduledCallback scheduled_retry;
        protected List<string> retryable_requests;
        protected ISocketFactory socket_factory_;
        int retry_delay_ = 10 * 1000;

        protected MetricsManager() { }
        public MetricsManager(Executor executor, ISocketFactory socket_factory, AwsCredentialsProvider creds, string region, string cw_namespace = "KinesisProducerLib",
             Level level = Level.Detailed,
             Granularity granularity = Granularity.Shard,
             ExtraDimensions extra_dimensions = null,
             string custom_endpoint = "",
             ulong port = 443,
             double upload_frequency = 1 * 60 * 1000,
             double retry_frequency = 10* 1000)
        {
            if (extra_dimensions == null)
                extra_dimensions = new ExtraDimensions();
            this.creds = creds;
            this.region = region;
            this.cw_namespace = cw_namespace;
            this.socket_factory_ = socket_factory;
            this.level = level;
            this.granularity = granularity;
            this.upload_frequency = upload_frequency;
            this.retry_frequency = retry_frequency;
            this.executor = executor;
            this.endpoint = string.IsNullOrEmpty(custom_endpoint) ? "monitoring." + region + ".amazonaws.com" : custom_endpoint;
            this.http_client = new AwsHttpClient(executor, socket_factory_, endpoint, (int)port, true, string.IsNullOrEmpty(custom_endpoint));
            retryable_requests = new List<string>();
            if (level != Level.None)
                StdErrorOut.Instance.StdOut(LogLevel.warn, string.Format("Uploading metrics to {0}:{1}", endpoint, port));

            extra_dimensionsMap[Granularity.Global] = new Dimensions();
            extra_dimensionsMap[Granularity.Stream] = new Dimensions();
            extra_dimensionsMap[Granularity.Shard] = new Dimensions();

            foreach (var t in extra_dimensions)
                extra_dimensionsMap[t.Item3].Add(new KeyValuePair<string, string>(t.Item1, t.Item2));

            scheduled_upload =
                executor.Schedule(
                    () =>
                    {
                        scheduled_upload.reschedule(upload_frequency);
                        upload();
                    },
                    upload_frequency);

            scheduled_retry =
                executor.Schedule(
                    () =>
                    {
                        scheduled_retry.reschedule(retry_frequency);
                        retry_uploads();
                    },
                    retry_frequency);
        }

        public virtual MetricsFinderBuilder finder()
        {
            return new MetricsFinderBuilder(this, extra_dimensionsMap);
        }

        public virtual Metric get_metric(MetricsFinder finder)
        {
            return metrics_index.get_metric(finder);
        }

        public virtual List<Metric> all_metrics()
        {
            return metrics_index.get_all();
        }

        public virtual void stop()
        {
            scheduled_upload.Cancel();
            scheduled_retry.Cancel();
        }

        protected static int kNumBuckets = 60;

        protected static string escape(string s)
        {
            return KPLNETInterface.Utils.url_encode(s);
        }

        protected static string escape(double d)
        {
            return escape(d.ToString());
        }

        string generate_query_args(Metric m, int idx, DateTime tp)
        {
            string prefix = "MetricData.member.";
            prefix += idx;
            prefix += ".";
            string metric_name = "";
            StringBuilder ss = new StringBuilder();

            {
                int i = 1;
                foreach (var p in m.All_dimensions())
                {
                    if (p.Key != "MetricName")
                    {
                        string dim_prefix = "Dimensions.member.";
                        dim_prefix += i++;
                        dim_prefix += ".";
                        ss.Append(prefix).Append(dim_prefix).Append("Name=").Append(escape(p.Key)).Append("&")
                          .Append(prefix).Append(dim_prefix).Append("Value=").Append(escape(p.Value)).Append("&");
                    }
                    else
                    {
                        ss.Append(prefix).Append("MetricName=").Append(p.Value).Append("&");
                        metric_name = p.Value;
                    }
                }
            }

            {
                var a = m.Accumulator();
                var count = a.count((ulong)kNumBuckets);
                if (count == 0)
                    return null;

                string stat_prefix = prefix;
                stat_prefix += "StatisticValues.";
                ss.Append(stat_prefix).Append("Maximum=").Append(escape(a.max((ulong)kNumBuckets))).Append("&")
                  .Append(stat_prefix).Append("Minimum=").Append(escape(a.min((ulong)kNumBuckets))).Append("&")
                  .Append(stat_prefix).Append("Sum=").Append(escape(a.sum((ulong)kNumBuckets))).Append("&")
                  .Append(stat_prefix).Append("SampleCount=").Append(escape(count)).Append("&");
            }

            ss.Append(prefix).Append("Unit=").Append(MetricsConstant.unit(metric_name)).Append("&");
            ss.Append(prefix).Append("Timestamp=").Append(escape(KPLNETInterface.Utils.format_ptime(tp))).Append("&");

            return ss.ToString();
        }


        MetricDatum to_sdk_metric_datum(Metric m, int numBuckets)
        {
            var a = m.Accumulator();
            StatisticSet ss = new StatisticSet();
            ss.Sum = a.sum((ulong)numBuckets);
            ss.Minimum = a.min((ulong)numBuckets);
            ss.Maximum = a.max((ulong)numBuckets);
            ss.SampleCount = a.count((ulong)numBuckets);

            MetricDatum d = new MetricDatum();
            d.StatisticValues = ss;

            foreach (var p in m.All_dimensions())
            {
                if (p.Key == "MetricName")
                    d.MetricName = p.Value;
                else
                    d.Dimensions.Add(new Amazon.CloudWatch.Model.Dimension() { Name = p.Key, Value = p.Value });
            }

            d.Unit = MetricsConstant.unit(d.MetricName);
            d.Timestamp = DateTime.Now;
            return d;
        }

        void uploadClient(List<Metric> uploads)
        {
            for (int i = 1; i <= uploads.Count; i++)
            {
                int step = uploads.Count / i;
                if (step > 20)
                    continue;

                List<PutMetricDataRequest> batches = new List<PutMetricDataRequest>();

                for (int j = 0; j < uploads.Count; j += step)
                {
                    int k = Math.Min(j + step, uploads.Count);
                    PutMetricDataRequest req = new PutMetricDataRequest();
                    req.MetricData = new List<MetricDatum>();
                    req.Namespace = cw_namespace;
                    for (int z = j; z < k; z++)
                        req.MetricData.Add(to_sdk_metric_datum(uploads[z], kNumBuckets));

                    batches.Add(req);
                }

                bool small_enough = true;
                foreach (var pmdr in batches)
                    if (pmdr.MetricData.Count > 38 * 1024)
                    {
                        small_enough = false;
                        break;
                    }

                if (small_enough)
                    foreach (var pmdr in batches)
                        upload_one(pmdr, new UploadContext(DateTime.Now));

                break;
            }
        }

        struct UploadContext
        {
            public UploadContext(DateTime created)
            {
                this.created = created;
                attempts = 0;
            }
            public DateTime created;
            public int attempts;
        }

        void upload_one(PutMetricDataRequest pmdr, UploadContext ctx) 
        {
            var cw_client_ = new Amazon.CloudWatch.AmazonCloudWatchClient();
            CancellationTokenSource cts = new CancellationTokenSource();
            var token = cts.Token;
            cw_client_.PutMetricDataAsync(pmdr, token).ContinueWith((pmdresp) =>
                    {
                        if (pmdresp.Result.HttpStatusCode != System.Net.HttpStatusCode.OK)
                        {
                            ctx.attempts++;
                            var msg = pmdresp.Exception.Message;
                            if (ctx.created > DateTime.Now.AddMinutes(-30))
                                throw new Exception("Metrics upload failed. Error Mesg : " + msg);
                            else
                            {
                                if (ctx.attempts == 5)
                                {
                                    //LOG(error) << "Metrics upload failed. | Code: " << code  << " | Message: " << msg << " | Request was: " << req.SerializePayload();
                                }
                                executor.Schedule(() => { upload_one(pmdr, ctx); }, retry_delay_);
                            }
                        }
                    });
        }

        void upload()
        {
            List<Metric> uploads = new List<Metric>();

            foreach (var m in metrics_index.get_all())
                if (MetricsConstant.filter(m.All_dimensions(), level, granularity))
                    uploads.Add(m);

            // Sort in reverse order to create a stack we can pop in unreversed order.
            uploads.Sort
                (
                    (a, b) =>
                    {
                        var v1 = a.All_dimensions();
                        var v2 = b.All_dimensions();

                        if (v1.Count != v2.Count)
                            return v1.Count < v2.Count ? -1 : 1;

                        for (int i = 0; i < v1.Count; i++)
                            if (v1[i].Key != v2[i].Key || v1[i].Value != v2[i].Value)
                                return v1[i].Key.CompareTo(v2[i].Key);

                        return 1;
                    }
                );
            bool http = false;
            if (http)
                uploadHTTP(uploads);
            else
                uploadClient(uploads);
        }

        void uploadHTTP(List<Metric> uploads)
        {

            var tp = DateTime.UtcNow;
            string query_str = "";
            int j = 1;

            while (uploads.Count != 0)
            {
                var m = uploads[uploads.Count - 1];
                uploads.RemoveAt(uploads.Count - 1);

                var q = generate_query_args(m, j, tp);

                // If the query args came back as none, that means the metric was empty; just drop it and move on.
                if (string.IsNullOrEmpty(q))
                    continue;

                // If we're still under the limit with the new addition, go ahead and add it. We can drop the metric now that we've used it.
                if (q.Length >= 39 * 1024)
                    return;

                if (query_str.Length + q.Length < 39 * 1024 && j <= 20)
                {
                    query_str += q;
                    j++;
                    // Otherwise, flush, and reinsert the metric back into the stack because
                    // we need to change its index to 1 and re-generate the query args.
                }
                else
                {
                    upload_one_batch(query_str);
                    query_str = "";
                    j = 1;
                    uploads.Add(m);
                }
            }

            // Flush any remaining data.
            if (query_str.Length > 0)
                upload_one_batch(query_str);
        }

        void upload_one_batch(string query_str)
        {
            string s = "/doc/2010-08-01/?" + query_str + "Action=PutMetricData" + "&Version=2010-08-01" + "&Namespace=" + cw_namespace;
            upload_with_path(s);
        }

        void upload_with_path(string path)
        {
            AwsHttpRequest req = new AwsHttpRequest("POST", path);
            req.add_header("Host", endpoint);
            req.add_header("Content-Length", "0");
            SigV4Context ctx = new SigV4Context(region, "monitoring", creds);

            try
            {
                RequestSigner.sign_v4(req, ctx);
                http_client.put(req, (r) => { handle_result(r); }, path, DateTime.Now, DateTime.Now);
            }
            catch (Exception e)
            {
                handle_result(new AwsHttpResult(e.ToString(), path, DateTime.Now, DateTime.Now));
            }
        }

        void handle_result(AwsHttpResult result)
        {
            bool retry = false;
            bool failed = false;
            string err_msg = "";

            if (!result.successful())
            {
                failed = true;
                err_msg = result.error();
                retry = true;
            }
            else if (result.status_code() != 200)
            {
                failed = true;
                err_msg = result.response_body();
                retry = result.status_code() >= 500;
            }

            if (failed)
            {
                StdErrorOut.Instance.StdError(string.Format("Metrics upload failed: \n{0}\nRequest was: \n {1}", err_msg, result.context<string>()));
            }

            if (retry)
            {
                lock (mutex_)
                    retryable_requests.Add(result.context<string>());
            }
        }

        void retry_uploads()
        {
            if (retryable_requests.Count() == 0)
                return;

            List<string> tmp = new List<string>();
            {
                lock (mutex_)
                {
                    tmp.AddRange(retryable_requests);
                    retryable_requests = new List<string>();
                }
            }

            while (tmp.Count != 0)
            {
                var p = tmp[0];
                tmp.RemoveAt(0);
                upload_with_path(p);
            }
        }
        object mutex_ = new object();
    }

    public class NullMetricsManager : MetricsManager
    {
        public NullMetricsManager()
        {
            this.dummy = new Metric(new Metric(), new KeyValuePair<string, string>());
            extra_dimensionsMap[Granularity.Global] = new Dimensions();
            extra_dimensionsMap[Granularity.Stream] = new Dimensions();
            extra_dimensionsMap[Granularity.Shard] = new Dimensions();
        }

        public override Metric get_metric(MetricsFinder finder)
        {
            return dummy;
        }

        public override List<Metric> all_metrics()
        {
            List<Metric> v = new List<Metric>();
            v.Add(dummy);
            return v;
        }

        public override void stop() { }

        private Metric dummy;
    }

    public class MetricsFinderBuilder
    {
        public MetricsFinderBuilder(MetricsManager manager, ExtraDimMap extra_dims)
        {
            this.state = State.EMPTY;
            this.manager = manager;
            this.extra_dims = extra_dims;
        }

        public MetricsFinderBuilder set_name(string name)
        {
            if (state != State.EMPTY)
                return this;

            state = State.HAS_NAME;
            mf.PushDimension(DimensionNames.MetricName, name);
            foreach (var p in extra_dims[Granularity.Global])
            {
                mf.PushDimension(p.Key, p.Value);
            }
            return this;
        }

        public MetricsFinderBuilder set_stream(string stream)
        {
            if (state != State.HAS_NAME && state != State.HAS_ERR_CODE)
                return this;

            state = State.HAS_STREAM;
            mf.PushDimension(DimensionNames.StreamName, stream);
            foreach (var p in extra_dims[Granularity.Stream])
            {
                mf.PushDimension(p.Key, p.Value);
            }
            return this;
        }

        public MetricsFinderBuilder set_shard(string shard)
        {
            if (state != State.HAS_STREAM)
                return this;

            state = State.HAS_SHARD;
            mf.PushDimension(DimensionNames.ShardId, shard);
            foreach (var p in extra_dims[Granularity.Shard])
            {
                mf.PushDimension(p.Key, p.Value);
            }
            return this;
        }

        public MetricsFinderBuilder set_error_code(string error_code)
        {
            if (state != State.HAS_NAME)
                return this;

            state = State.HAS_ERR_CODE;
            mf.PushDimension(DimensionNames.ErrorCode, error_code);
            return this;
        }

        public Metric find()
        {
            return manager.get_metric(mf);
        }

        private State state;
        private MetricsManager manager;
        private ExtraDimMap extra_dims;
        private MetricsFinder mf = new MetricsFinder();
    }
}
