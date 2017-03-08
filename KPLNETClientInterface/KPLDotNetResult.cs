using System;
using System.Collections.Generic;

namespace KPLNETClientInterface
{
    public enum KPLDotNetResultType { UserRecordResult, Metrics }
    public class KPLDotNetResult
    {
        public KPLDotNetResultType KPLResultType { get; set; }

        public List<Metric> Metrics { get; set; }
        public UserRecordResult UserResult { get; set; }
    }

    public class Attempt
    {
        private uint delay;
        private uint duration;
        private String errorMessage;
        private String errorCode;
        private bool success;

        private Attempt(uint delay, uint duration, String errorMessage, String errorCode, bool success)
        {
            this.delay = delay;
            this.duration = duration;
            this.errorMessage = errorMessage;
            this.errorCode = errorCode;
            this.success = success;
        }

        /**
         * 
         * @return Delay in milliseconds between the start of this attempt and the
         *         previous attempt. If this is the first attempt, then returns the
         *         delay between the message reaching the daemon and the first
         *         attempt.
         */
        public uint getDelay()
        {
            return delay;
        }

        /**
         * @return Duration of the attempt. Mainly consists of network and server
         *         latency, but also includes processing overhead within the daemon.
         */
        public uint getDuration()
        {
            return duration;
        }

        /**
         * 
         * @return Error message associated with this attempt. Null if no error
         *         (i.e. successful).
         */
        public String getErrorMessage()
        {
            return errorMessage;
        }

        /**
         * 
         * @return Error code associated with this attempt. Null if no error
         *         (i.e. successful).
         */
        public String getErrorCode()
        {
            return errorCode;
        }

        /**
         * 
         * @return Whether the attempt was successful. If true, then the record has
         *         been confirmed by the backend.
         */
        public bool isSuccessful()
        {
            return success;
        }

        public static Attempt fromProtobufMessage(Aws.Kinesis.Protobuf.Attempt a)
        {
            return new Attempt(
                    a.Delay,
                    a.Duration,
                    a.ErrorMessage,
                    a.ErrorCode,
                    a.Success);
        }
    }
    public class UserRecordResult
    {
        private List<Attempt> attempts;
        private String sequenceNumber;
        private String shardId;
        private bool successful;

        private UserRecordResult(List<Attempt> attempts, String sequenceNumber, String shardId, bool successful)
        {
            this.attempts = attempts;
            this.sequenceNumber = sequenceNumber;
            this.shardId = shardId;
            this.successful = successful;
        }

        /**
         *
         * @return List of {@link Attempt}s, in the order they were made.
         */
        public List<Attempt> getAttempts()
        {
            return attempts;
        }

        /**
         * 
         * @return The sequence number assigned by the backend to this record.
         *         Multiple records may have the same sequence number if aggregation
         *         is enabled. Will be null if the put failed.
         */
        public String getSequenceNumber()
        {
            return sequenceNumber;
        }

        /**
         * 
         * @return Shard ID returned by the backend. The record was written to this
         *         shard. Will be null if the put failed.
         */
        public String getShardId()
        {
            return shardId;
        }

        /**
         * 
         * @return Whether the record put was successful. If true, then the record
         *         has been confirmed by the backend.
         */
        public bool isSuccessful()
        {
            return successful;
        }

        public static UserRecordResult fromProtobufMessage(Aws.Kinesis.Protobuf.PutRecordResult r)
        {
            List<Attempt> attempts = new List<Attempt>(r.Attempts.Count);
            foreach (Aws.Kinesis.Protobuf.Attempt a in r.Attempts)
                attempts.Add(Attempt.fromProtobufMessage(a));

            return new UserRecordResult(
                    new List<Attempt>(attempts),
                    r.SequenceNumber,
                    r.ShardId,
                    r.Success);
        }
    }

    public class Metric
    {
        private String name;
        private ulong duration;
        private Dictionary<String, String> dimensions;
        private double sum;
        private double mean;
        private double sampleCount;
        private double min;
        private double max;

        /**
         * Gets the dimensions of this metric. The returned map has appropriate
         * iteration order and is immutable.
         * 
         * @return Immutable map containing the dimensions.
         */
        public Dictionary<String, String> getDimensions()
        {
            return dimensions;
        }

        public double getSum()
        {
            return sum;
        }

        public double getMean()
        {
            return mean;
        }

        public double getSampleCount()
        {
            return sampleCount;
        }

        public double getMin()
        {
            return min;
        }

        public double getMax()
        {
            return max;
        }

        public String getName()
        {
            return name;
        }

        /**
         * @return The number of seconds over which the statistics in this Metric
         *         instance was accumulated. For example, a duration of 10 means
         *         that the statistics in this Metric instance represents 10 seconds
         *         worth of samples.
         */
        public ulong getDuration()
        {
            return duration;
        }

        public Metric(Aws.Kinesis.Protobuf.Metric m)
        {
            this.name = m.Name;
            this.duration = m.Seconds;

            dimensions = new Dictionary<String, String>();
            foreach (Aws.Kinesis.Protobuf.Dimension d in m.Dimensions)
                dimensions.Add(d.Key, d.Value);

            Aws.Kinesis.Protobuf.Stats s = m.Stats;
            this.max = s.Max;
            this.mean = s.Mean;
            this.min = s.Min;
            this.sum = s.Sum;
            this.sampleCount = s.Count;
        }

        public override string ToString()
        {
            return "Metric [name=" + name + ", duration=" + duration + ", dimensions=" + dimensions + ", sum=" + sum
                    + ", mean=" + mean + ", sampleCount=" + sampleCount + ", min=" + min + ", max=" + max + "]";
        }
    }
}
