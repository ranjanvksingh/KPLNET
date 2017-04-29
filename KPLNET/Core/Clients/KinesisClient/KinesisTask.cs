using System;
using System.Text;
using System.Threading;
using System.Collections.Generic;

using Amazon.Kinesis;

using KPLNET.Utils;
using KPLNET.Kinesis.Core;

namespace KPLNET.Core.Clients.KinesisClient
{
    using ResponseCallback = Action<AwsKinesisResult>;
    using KinesisClientReturn = Action<AmazonKinesisClient>;

    public enum KinesisRquestType { PutRecordsRequest, DescribeStream, Ec2Metadata, PutMetricData }
    class KinesisTask : TimeSensitive
    {
        private Executor executor_;
        private AmazonKinesisClient kinesisClient_;
        private object context_;
        private ResponseCallback response_cb_;
        private int timeout_;
        private DateTime end_deadline_;
        private DateTime start_;
        private bool finished_;
        private bool submitted_ = false;
        private byte[] buffer_ = new byte[64 * 1024];
        private bool started_ = false;
        private KinesisRquestType reqType_;
        private PutRecordsRequest prRequest_;
        private AwsKinesisResponse response_;
        private DescribeStreamRequest dsRequest_;

        public KinesisTask(Executor executor, PutRecordsRequest request, DescribeStreamRequest dsRequest, KinesisRquestType reqType, object context, ResponseCallback response_cb, KinesisClientReturn kinesisClientReturn, DateTime deadline, DateTime expiration, int timeout)
            : base(deadline, expiration)
        {
            executor_ = executor;
            context_ = context;
            response_cb_ = response_cb;
            dsRequest_ = dsRequest;
            timeout_ = timeout;
            finished_ = false;
            reqType_ = reqType;
            prRequest_ = request;
        }

        public void run(ClientWithLoad client)
        {
            try
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "Kinesistask.run");
                if (!started_)
                {
                    started_ = true;
                    kinesisClient_ = client.ActualClient;
                    start_ = DateTime.Now;
                    end_deadline_ = start_.AddMilliseconds(timeout_);
                    StdErrorOut.Instance.StdOut(LogLevel.debug, "Kinesistask.run before Execute();");
                    Execute();
                    Interlocked.Decrement(ref client.Load);
                }
                else
                {
                    Interlocked.Decrement(ref client.Load);
                    throw new Exception("The same task cannot be run more than once");
                }
            }
            catch(Exception e)
            {

                StdErrorOut.Instance.StdError("Error in Kinesis Task run", e);
            }
        }

        void Execute()
        {
            if (reqType_ == KinesisRquestType.PutRecordsRequest)
                ProcessPutRecordsRequest();
            if (reqType_ == KinesisRquestType.DescribeStream)
                ProcessDescribeStream();
        }

        private void ProcessPutRecordsRequest()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Kinesis.Execute before build PutRecordsRequest");
            var req = new Amazon.Kinesis.Model.PutRecordsRequest()
            {
                StreamName = prRequest_.stream(),
                Records = BuildPutRecordsRequestEntries(prRequest_)
            };

            StdErrorOut.Instance.StdOut(LogLevel.debug, "Kinesis.Execute after build PutRecordsRequest");
            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken ct = cts.Token;
            var responseTask = kinesisClient_.PutRecordsAsync(req, ct);
            responseTask.Wait(timeout_, ct);
            if (responseTask.IsCompleted)
            {
                response_ = new AwsKinesisResponse(responseTask.Result);
                succeed();
            }
            if (!responseTask.IsCompleted)
            {
                cts.Cancel();
                fail("Failed to put records tp the Kinesis Stream");
            }
        }

        private void ProcessDescribeStream()
        {
            var req = new Amazon.Kinesis.Model.DescribeStreamRequest()
            {
                StreamName = dsRequest_.Stream,
                ExclusiveStartShardId = dsRequest_.ExclusiveStartShardId,
                Limit = dsRequest_.Limit
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            CancellationToken ct = cts.Token;
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Kinesis.Execute before kinesisClient_.DescribeStreamAsync(req, ct)");
            var responseTask = kinesisClient_.DescribeStreamAsync(req, ct);
            responseTask.Wait(timeout_, ct);
            if (responseTask.IsCompleted)
                {
                    response_ = new AwsKinesisResponse(responseTask.Result);
                    succeed();
                }
                if (!responseTask.IsCompleted)
                {
                cts.Cancel();
                fail("Failed to describe Kinesis Stream");
                }
        }

        private List<Amazon.Kinesis.Model.PutRecordsRequestEntry> BuildPutRecordsRequestEntries(PutRecordsRequest request_)
        {
            if (request_ == null)
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "NULL PutRecordsRequest");
                return null;
            }
            else
                StdErrorOut.Instance.StdOut(LogLevel.debug, "request_.serialize() = " + request_.serialize());

            var retVal = new List<Amazon.Kinesis.Model.PutRecordsRequestEntry>();
            foreach (var item in request_.Items())
            {
                var reqEntry = new Amazon.Kinesis.Model.PutRecordsRequestEntry();
                reqEntry.PartitionKey = item.partition_key();
                reqEntry.ExplicitHashKey = item.explicit_hash_key();
				if (item.is_aggregated)
					reqEntry.Data = new System.IO.MemoryStream(item.SerializedAggregatedRecord);
				else
					reqEntry.Data = new System.IO.MemoryStream(Encoding.UTF8.GetBytes(item.serialize()));
				retVal.Add(reqEntry);
            }
            return retVal;
        }

        public void fail(string reason)
        {
            finish(new AwsKinesisResult(reason, context_, start_, DateTime.Now));
        }

        void succeed()
        {
            finish(new AwsKinesisResult(response_, context_, start_, DateTime.Now));
        }

        void finish(AwsKinesisResult result)
        {
            started_ = true;
            if (submitted_)
                return;

            submitted_ = true;
            executor_.Submit
                (
                    () =>
                    {
                        try
                        {
                            response_cb_(result);
                            finished_ = true;
                        }
                        catch(Exception e)
                        {
                            StdErrorOut.Instance.StdError("KinesisTask response_cb_ failed.", e);
                        }
                    }
                );
        }

        public bool finished()
        {
            return finished_;
        }
    }
}
