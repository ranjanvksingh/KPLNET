using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Aws.Kinesis.Protobuf;

namespace KPLNETInterface
{
    public class AWSCredentials
    {
        public string Akid { get; set; }
        public string SecretKey { get; set; }
        public string SessionToken { get; set; }
    }

    public enum AWSRegions
    {
        Unknown,
        APNortheast1,
        APNortheast2,
        APSouth1,
        APSoutheast1,
        APSoutheast2,
        CACentral1,
        CNNorth1,
        EUCentral1,
        EUWest1,
        EUWest2,
        SAEast1,
        USEast1,
        USEast2,
        USGovCloudWest1,
        USWest1,
        USWest2,
    }

    public class KPLNETConfiguration
    {
        public bool runAsDaemon = true;
        public bool aggregationEnabled = true;
        public ulong aggregationMaxCount = 4294967295L;
        public ulong aggregationMaxSize = 51200L;
        public String cloudwatchEndpoint = "";
        public ulong cloudwatchPort = 443L;
        public ulong collectionMaxCount = 500L;
        public ulong collectionMaxSize = 5242880L;
        public ulong connectTimeout = 6000L;
        public ulong credentialsRefreshDelay = 5000L;
        public bool enableCoreDumps = false;
        public bool failIfThrottled = false;
        public String kinesisEndpoint = "";
        public ulong kinesisPort = 443L;
        public String logLevel = "info";
        public ulong maxConnections = 24L;
        public String metricsGranularity = "shard";
        public String metricsLevel = "detailed";
        public String metricsNamespace = "KinesisProducerLibrary";
        public ulong metricsUploadDelay = 60000L;
        public ulong minConnections = 1L;
        public String nativeExecutable = "";
        public ulong rateLimit = 150L;
        public ulong recordMaxBufferedTime = 100L;
        public ulong recordTtl = 30000L;
        public AWSRegions region = AWSRegions.Unknown;
        public ulong requestTimeout = 6000L;
        public String tempDirectory = "";
        public bool verifyCertificate = true;
        public AWSCredentials AWSCredentials = null;
        public AWSCredentials AWSMetricsCredentials = null;
        public ClientType clientType = ClientType.KinesisClient;
        private List<AdditionalDimension> additionalDims = new List<AdditionalDimension>();

        public List<AdditionalDimension> AdditionalMetricsDimension()
        {
            return additionalDims;
        }

        public void addAdditionalMetricsDimension(String key, String value, String granularity)
        {
            additionalDims.Add(new AdditionalDimension() { Key = key, Value = value, Granularity = granularity });
        }

        public Message toProtobufMessage()
        {
            Aws.Kinesis.Protobuf.Configuration c = new Aws.Kinesis.Protobuf.Configuration()
            {
                AggregationEnabled = aggregationEnabled,
                AggregationMaxCount = aggregationMaxCount,
                AggregationMaxSize = aggregationMaxSize,
                CloudwatchEndpoint = cloudwatchEndpoint,
                CloudwatchPort = cloudwatchPort,
                CollectionMaxCount = collectionMaxCount,
                CollectionMaxSize = collectionMaxSize,
                ConnectTimeout = connectTimeout,
                EnableCoreDumps = enableCoreDumps,
                FailIfThrottled = failIfThrottled,
                KinesisEndpoint = kinesisEndpoint,
                KinesisPort = kinesisPort,
                LogLevel = logLevel,
                MaxConnections = maxConnections,
                MetricsGranularity = metricsGranularity,
                MetricsLevel = metricsLevel,
                MetricsNamespace = metricsNamespace,
                MetricsUploadDelay = metricsUploadDelay,
                MinConnections = minConnections,
                RateLimit = rateLimit,
                RecordMaxBufferedTime = recordMaxBufferedTime,
                RecordTtl = recordTtl,
                Region = region.ToString(),
                RequestTimeout = requestTimeout,
                VerifyCertificate = verifyCertificate
            };

            return new Aws.Kinesis.Protobuf.Message() { Configuration = c, Id = 0 };
        }

       public void transfer_from_protobuf_msg(Message m) 
	  {
		if (m.Configuration == null) 
		  throw new Exception("Not a configuration message");

		var c = m.Configuration;
		aggregationEnabled= c.AggregationEnabled;
		aggregationMaxCount= c.AggregationMaxCount;
		aggregationMaxSize= c.AggregationMaxSize;
		collectionMaxCount= c.CollectionMaxCount;
		collectionMaxSize= c.CollectionMaxSize;
		connectTimeout= c.ConnectTimeout;
		failIfThrottled= c.FailIfThrottled;
		logLevel= c.LogLevel;
		maxConnections= c.MaxConnections;
		metricsGranularity= c.MetricsGranularity;
		metricsLevel= c.MetricsLevel;
		metricsNamespace= c.MetricsNamespace;
		metricsUploadDelay= c.MetricsUploadDelay;
		minConnections= c.MinConnections;
		kinesisPort= c.KinesisPort;
		kinesisEndpoint= c.KinesisEndpoint;
		rateLimit= c.RateLimit;
		recordMaxBufferedTime= c.RecordMaxBufferedTime;
		recordTtl= c.RecordTtl;
        region = (AWSRegions)Enum.Parse(typeof(AWSRegions), c.Region);
		requestTimeout= c.RequestTimeout;
		verifyCertificate= c.VerifyCertificate;

		for (var i = 0; i < c.AdditionalMetricDims.Count; i++) 
		{
		  var ad = c.AdditionalMetricDims[i];
		  additionalDims.Add(new AdditionalDimension{Key = ad.Key, Value = ad.Value, Granularity = ad.Granularity});
		}
	  }
    }
}
