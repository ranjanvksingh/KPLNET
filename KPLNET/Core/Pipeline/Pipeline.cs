using KPLNET.Auth;
using KPLNET.Core.Clients.KinesisClient;
using KPLNET.Http;
using KPLNET.Metrics;
using KPLNET.Utils;
using System;

using KPLNETInterface;

namespace KPLNET.Kinesis.Core
{
    public class Pipeline
    {
        private string region_;
        private KPLNETConfiguration config_;
        private Executor executor_;
        private AwsHttpClient http_client_;
        private AwsKinesisClient kinesis_client_;
        private MetricsManager metrics_manager_;
        private Action<UserRecord> finish_user_record_cb_;
        private SigV4Context sig_v4_ctx_;
        private ShardMap shard_map_;
        private Aggregator aggregator_;
        private Limiter limiter_;
        private Collector collector_;
        private Retrier retrier_;
        private Metric user_records_rcvd_metric_;
        private ulong outstanding_user_records_;

        public Pipeline(string region, string stream, KPLNETConfiguration config, Executor executor, AwsHttpClient http_client, AwsKinesisClient kinesis_client, AwsCredentialsProvider creds_provider, MetricsManager metrics_manager, Action<UserRecord> finish_user_record_cb)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "started pipeline creation.");
            try
            {
                this.region_ = region;
                this.config_ = config;
                this.executor_ = executor;
                this.http_client_ = http_client;
                this.kinesis_client_ = kinesis_client;
                this.metrics_manager_ = metrics_manager;
                this.finish_user_record_cb_ = finish_user_record_cb;
                this.sig_v4_ctx_ = new SigV4Context(this.region_, "kinesis", creds_provider);
                this.shard_map_ = new ShardMap(executor, config, http_client, kinesis_client, creds_provider, this.region_, stream, this.metrics_manager_, 1000, 30000);
                StdErrorOut.Instance.StdOut(LogLevel.debug, "after shard_map_ pipeline creation.");
                this.aggregator_ = new Aggregator(this.executor_, this.shard_map_, (Action<KinesisRecord>)(kr => this.limiter_put(kr)), this.config_, this.metrics_manager_);
                this.limiter_ = new Limiter(this.executor_, (Action<KinesisRecord>)(kr => this.collector_put(kr)), (Action<KinesisRecord>)(kr => this.retrier_put_kr(kr)), this.config_);
                this.collector_ = new Collector(this.executor_, (Action<PutRecordsRequest>)(prr => this.send_put_records_request(prr)), this.config_, this.metrics_manager_);
                this.retrier_ = new Retrier(this.config_, (Action<UserRecord>)(ur => this.finish_user_record(ur)), (Action<UserRecord>)(ur => this.aggregator_put(ur)), (Action<DateTime>)(tp => this.shard_map_.invalidate(tp)), (Action<string, string>)((code, msg) => this.limiter_.add_error(code, msg)), this.metrics_manager_);
                this.outstanding_user_records_ = 0;
                StdErrorOut.Instance.StdOut(LogLevel.debug, "done pipeline creation.");
            }
            catch(Exception ex)
            {
                StdErrorOut.Instance.StdOut(LogLevel.error, ex.ToString());
            }
        }

        public void put(UserRecord ur)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.put called.");
            try
            {
                this.outstanding_user_records_ += 1;
                this.aggregator_put(ur);
            }
            catch(Exception ex)
            {
                StdErrorOut.Instance.StdError("pipeline put failed.", ex);
            }
        }

        private void aggregator_put(UserRecord ur)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.aggregator_put. aggregator_ NULL = " + (this.aggregator_ == null ? "true" : "false"));
            KinesisRecord kr = this.aggregator_.put(ur);
            if (kr != null)
                this.limiter_put(kr);
        }

        private void limiter_put(KinesisRecord kr)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.limiter_put");
            this.limiter_.put(kr);
        }

        public void collector_put(KinesisRecord kr)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.collector_put");
            PutRecordsRequest prr = this.collector_.put(kr);
            if (null != prr)
                this.send_put_records_request(prr);
        }

        private void send_put_records_request(PutRecordsRequest prr)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.send_put_records_request called.");
            if (config_.clientType == KPLNETInterface.ClientType.SocketClient)
                HttpClientSendPutRecordsRequest(prr);
            else
                KinesisClientSendPutRecordsRequest(prr);
        }

        private void KinesisClientSendPutRecordsRequest(PutRecordsRequest prr)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.send_put_records_request");
            try
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "before RequestSigner.sign_v4(request, sig_v4_ctx_)");
                this.kinesis_client_.PutRecordsRequest(prr, (result) => this.retrier_put(result), (object)prr, prr.Deadline(), prr.Expiration());
                StdErrorOut.Instance.StdOut(LogLevel.debug, "http_client_.put");
            }
            catch (Exception ex)
            {
                this.retrier_.put(new AwsHttpResult(ex.ToString(), (object)prr, DateTime.Now, DateTime.Now));
            }
        }

        private void HttpClientSendPutRecordsRequest(PutRecordsRequest prr)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.send_put_records_request");
            AwsHttpRequest kinesisRequest = AwsHttp.create_kinesis_request(this.region_, "PutRecords", prr.serialize());
            try
            {
                RequestSigner.sign_v4(kinesisRequest, this.sig_v4_ctx_);
                StdErrorOut.Instance.StdOut(LogLevel.debug, "after RequestSigner.sign_v4(request, sig_v4_ctx_)");
                this.http_client_.put(kinesisRequest, (Action<AwsHttpResult>)(result => this.retrier_put(result)), (object)prr, prr.Deadline(), prr.Expiration());
                StdErrorOut.Instance.StdOut(LogLevel.debug, "http_client_.put");
            }
            catch (Exception ex)
            {
                this.retrier_.put(new AwsHttpResult(ex.ToString(), (object)prr, DateTime.Now, DateTime.Now));
            }
        }

        private void retrier_put(AwsKinesisResult result)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.retrier_put");
            this.executor_.Submit((Action)(() => { try { this.retrier_.put(result); } catch (Exception e) { StdErrorOut.Instance.StdError("pipeline retrier_put_kr failed", e); } }));
        }

        private void retrier_put(AwsHttpResult result)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.retrier_put");
            this.executor_.Submit((Action)(() => { try { this.retrier_.put(result); } catch (Exception e) { StdErrorOut.Instance.StdError("pipeline retrier_put_kr failed", e); } }));
        }

        private void retrier_put_kr(KinesisRecord kr)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.retrier_put_kr");
            this.executor_.Submit((Action)(() => { try { this.retrier_.put(kr, "Expired", "Expiration reached while waiting in limiter"); } catch (Exception e) { StdErrorOut.Instance.StdError("pipeline retrier_put_kr failed", e); } }));
        }

        public void finish_user_record(UserRecord ur)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.finish_user_record");
            this.finish_user_record_cb_(ur);
            --this.outstanding_user_records_;
        }

        public void flush()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Pipeline.flush");
            this.aggregator_.flush();
            this.executor_.Schedule((Action)(() => this.collector_.flush()), 80.0);
        }

        public ulong outstanding_user_records()
        {
            return this.outstanding_user_records_;
        }
    }
}
