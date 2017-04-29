using System;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Collections.Generic;

using KPLNETInterface;
using KPLNET.Auth;
using KPLNET.Http;
using KPLNET.Metrics;
using KPLNET.Utils;
using KPLNET.Core;
using KPLNET.Core.Clients.KinesisClient;


namespace KPLNET.Kinesis.Core
{
    public class ShardMap
    {
        private enum State { INVALID, UPDATING, READY };
        private Executor executor;
        private KPLNETConfiguration config;
        private AwsHttpClient http_client;
        protected AwsKinesisClient kinesis_client;
        private AwsCredentialsProvider creds_provider;
        private string region;
        private string stream;
        private MetricsManager metrics_manager;
        private State state;
        private List<KeyValuePair<BigInteger, ulong>> end_hash_key_to_shard_id = new List<KeyValuePair<BigInteger, ulong>>();
        private DateTime updated_at;
        private int min_backoff;
        private int max_backoff;
        private int backoff;
        private ScheduledCallback scheduled_callback;
        private const int kMinBackoff = 1000; //ms
        private const int kMaxBackoff = 30000; //ms
        object mutex_ = new object();
        
        public ShardMap(
            Executor executor,
            KPLNETConfiguration config,
            AwsHttpClient http_client,
            AwsKinesisClient kinesis_client,
            AwsCredentialsProvider creds,
            string region,
            string stream,
            MetricsManager metrics_manager,
            int min_backoff = kMinBackoff,
            int max_backoff = kMaxBackoff)
        {
            this.executor = executor;
            this.http_client = http_client;
            this.kinesis_client = kinesis_client;
            this.creds_provider = creds;
            this.config = config;
            this.region = region;
            this.stream = stream;
            this.metrics_manager = metrics_manager;
            this.state = State.INVALID;
            this.min_backoff = min_backoff;
            this.max_backoff = max_backoff;
            this.backoff = min_backoff;
            update();
        }

        protected ShardMap() { }

        public virtual long Shard_id(BigInteger hash_key)
        {
            long retVal = -1;
            if (state == State.READY && Monitor.TryEnter(mutex_))
            {
                try
                {
                    var first = end_hash_key_to_shard_id.FirstOrDefault(x => x.Key >= hash_key);
                    retVal = (long)first.Value;
                }
                catch (Exception)
                {
                    StdErrorOut.Instance.StdError(string.Format("Could not map hash key to shard id. Something's wrong  with the shard map. Hash key = {0}", hash_key));
                }
                finally
                {
                    Monitor.Exit(mutex_);
                }
            }
            return retVal;
        }

        public void invalidate(DateTime seen_at)
        {
            lock (mutex_)
                if (seen_at > updated_at && state == State.READY)
                    update();
        }

        private void update(string start_shard_id = "")
        {
            if (string.IsNullOrEmpty(start_shard_id) && state == State.UPDATING)
                return;

            if (state != State.UPDATING)
            {
                state = State.UPDATING;
                StdErrorOut.Instance.StdOut(LogLevel.info, string.Format("Updating shard map for stream \"{0}\"", stream));
                end_hash_key_to_shard_id.Clear();
                if (scheduled_callback != null)
                    scheduled_callback.Cancel();
            }

            if (config.clientType == KPLNETInterface.ClientType.SocketClient)
                HttpClientUpdate(start_shard_id);
            else
                KinesisClientUpdate(start_shard_id);
        }

        private void HttpClientUpdate(string start_shard_id)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{\"StreamName\":\"").Append(stream).Append("\"");
            if (!string.IsNullOrEmpty(start_shard_id))
                sb.Append(",\"ExclusiveStartShardId\":\"").Append(start_shard_id).Append("\"");
            sb.Append("}");

            var req = AwsHttp.create_kinesis_request(region, "DescribeStream", sb.ToString());
            var ctx = new SigV4Context(region, "kinesis", creds_provider);

            try
            {
                RequestSigner.sign_v4(req, ctx);
                http_client.put(req, (r) => { update_callback(r); }, null, DateTime.Now, DateTime.Now);
            }
            catch (Exception e)
            {
                update_callback(new AwsKinesisResult(e.ToString(), null, DateTime.Now, DateTime.Now));
            }
        }

        protected virtual void KinesisClientUpdate(string start_shard_id)
        {
            try
            {
                var dsr = new DescribeStreamRequest() { Stream = stream, Limit = 100 };
                if(!string.IsNullOrEmpty(start_shard_id))
                    dsr.ExclusiveStartShardId = start_shard_id;

                kinesis_client.DescribeStreamRequest(dsr, (r) => { update_callback(r); }, null, DateTime.Now, DateTime.Now.AddDays(1));
            }
            catch (Exception e)
            {
                update_callback(new AwsKinesisResult(e.ToString(), null, DateTime.Now, DateTime.Now));
            }
        }

        public static long shard_id_from_str(string shard_id)
        {
            var parts = shard_id.Split("-".ToArray());
            return long.Parse(parts[1]);
        }

        public static string shard_id_to_str(ulong id)
        {
            var i = id.ToString();
            var p = new string('0', 12 - i.Length);
            return "shardId-" + p + i;
        }

        void update_fail(string error)
        {
            StdErrorOut.Instance.StdError(string.Format("Shard map update for stream \"{0}\" failed: {1}; retrying in {2} ms", stream, error, backoff));
            lock (mutex_)
            {
                state = State.INVALID;

                if (null == scheduled_callback)
                    scheduled_callback = executor.Schedule(() => { update(); }, backoff);
                else
                    scheduled_callback.reschedule(backoff);

                backoff = Math.Min(backoff * 3 / 2, max_backoff);
            }
        }
        
        void update_callback(AwsHttpResult result)
        {
            if (!result.successful())
            {
                update_fail(result.error());
                return;
            }

            if (result.status_code() != 200)
            {
                update_fail(result.response_body());
                return;
            }

            try
            {
                dynamic json = System.Web.Helpers.Json.Decode(result.response_body());
                var stream_description = json["StreamDescription"];

                string stream_status = stream_description["StreamStatus"];
                if (stream_status != "ACTIVE" && stream_status != "UPDATING")
                {
                    string ss = "Stream status is " + stream_status;
                    throw new Exception(ss);
                }

                List<dynamic> shards = stream_description["Shards"];
                for (int i = 0; i < shards.Count; i++)
                {
                    // Check if the shard is closed, if so, do not use it.
                    if (shards[i]["SequenceNumberRange"]["EndingSequenceNumber"])
                        continue;

                    end_hash_key_to_shard_id.Add(new KeyValuePair<BigInteger, ulong>(BigInteger.Parse(shards[i]["HashKeyRange"]["EndingHashKey"]), shard_id_from_str(shards[i]["ShardId"])));
                }

                backoff = min_backoff;

                if (stream_description["HasMoreShards"])
                {
                    update(shards[shards.Count - 1]["ShardId"]);
                    return;
                }

                end_hash_key_to_shard_id.Sort();

                lock (mutex_)
                {
                    state = State.READY;
                    updated_at = DateTime.Now;
                }

                StdErrorOut.Instance.StdOut(LogLevel.info, string.Format("Successfully updated shard map for stream \"{0}\" found {1} shards", stream, end_hash_key_to_shard_id.Count));
            }
            catch (Exception ex)
            {
                update_fail(ex.ToString());
            }
        }

        protected void update_callback(AwsKinesisResult result)
        {
            if (!result.successful())
            {
                update_fail(result.error());
                return;
            }

            StdErrorOut.Instance.StdOut(LogLevel.debug, "ShardMap Update_callback result.status_code() = " + (int)result.status_code());
            if ((int)result.status_code() != 200)
            {
                update_fail(result.response().DescribeStreamResponse.ToString());
                return;
            }

            try
            {
                var response = result.response().DescribeStreamResponse;
                var stream_description = response.StreamDescription;
                string stream_status = stream_description.StreamStatus;
                StdErrorOut.Instance.StdOut(LogLevel.debug, string.Format("response = {0}, stream_status = {1}", response, stream_status));
                if (stream_status != "ACTIVE" && stream_status != "UPDATING")
                {
                    string ss = string.Format("Stream status is {0}", stream_status);
                    throw new Exception(ss);
                }

                List<Amazon.Kinesis.Model.Shard> shards = stream_description.Shards;
                StdErrorOut.Instance.StdOut(LogLevel.debug, string.Format("shards.Count = {0}", shards.Count));
                for (int i = 0; i < shards.Count; i++)
                {
                    // Check if the shard is closed, if so, do not use it.
                    if (shards[i].SequenceNumberRange.EndingSequenceNumber != null)
                        continue;

                    end_hash_key_to_shard_id.Add(new KeyValuePair<BigInteger, ulong>(BigInteger.Parse(shards[i].HashKeyRange.EndingHashKey), (ulong)shard_id_from_str(shards[i].ShardId)));
                }

                backoff = min_backoff;

                if (stream_description.HasMoreShards)
                {
                    update(shards[shards.Count - 1].ShardId);
                    return;
                }

                end_hash_key_to_shard_id.Sort((a, b) => { return a.Key < b.Key ? -1 : 1; });

                lock (mutex_)
                {
                    state = State.READY;
                    updated_at = DateTime.Now;
                }

                StdErrorOut.Instance.StdOut(LogLevel.info, string.Format("Successfully updated shard map for stream \"{0}\" found {1} shards", stream, end_hash_key_to_shard_id.Count));
            }
            catch (Exception ex)
            {
                update_fail(ex.ToString());
            }
        }
    }
}
