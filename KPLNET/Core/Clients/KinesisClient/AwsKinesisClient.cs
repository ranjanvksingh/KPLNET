using System;
using System.Linq;
using System.Threading;

using Amazon.Kinesis;

using KPLNET.Utils;
using KPLNETInterface;
using KPLNET.Kinesis.Core;

namespace KPLNET.Core.Clients.KinesisClient
{
    using ResponseCallback = Action<AwsKinesisResult>;

    public class ClientWithLoad : IComparable
    {
        public AmazonKinesisClient ActualClient { get; set; }
        public int Load;

        public int CompareTo(object obj)
        {
            var comparerWith = obj as ClientWithLoad;
            return comparerWith == null ? -1 : Load.CompareTo(comparerWith.Load);
        }
    }

    public class AwsKinesisClient
    {
        protected Executor executor_;
        private ulong requestTimeout;
        object mutex_ = new object();
        AWSCredentials cred_;
        ClientWithLoad[] clients;
        Amazon.RegionEndpoint region_ = Amazon.RegionEndpoint.EUWest1;
        
        public AwsKinesisClient(Utils.Executor executor, AWSCredentials cred, AWSRegions region, ulong minConnections = 1, ulong maxConnections = 6, ulong connectTimeout = 10000, ulong requestTimeout = 10000, bool single_use_sockets = false)
        {
            this.executor_ = executor;
            this.requestTimeout = requestTimeout;
            this.cred_ = cred;
            this.region_ = MapRegion(region);
            CreateClients((int)maxConnections);
        }

        public static Amazon.RegionEndpoint MapRegion(AWSRegions region)
        {
            switch (region)
            {
                case AWSRegions.APNortheast1:
                    return Amazon.RegionEndpoint.APNortheast1;
                case AWSRegions.APNortheast2:
                    return Amazon.RegionEndpoint.APNortheast2;
                case AWSRegions.APSouth1:
                    return Amazon.RegionEndpoint.APSouth1;
                case AWSRegions.APSoutheast1:
                    return Amazon.RegionEndpoint.APSoutheast1;
                case AWSRegions.APSoutheast2:
                    return Amazon.RegionEndpoint.APSoutheast2;
                case AWSRegions.CACentral1:
                    return Amazon.RegionEndpoint.CACentral1;
                case AWSRegions.CNNorth1:
                    return Amazon.RegionEndpoint.CNNorth1;
                case AWSRegions.EUCentral1:
                    return Amazon.RegionEndpoint.EUCentral1;
                case AWSRegions.EUWest1:
                    return Amazon.RegionEndpoint.EUWest1;
                case AWSRegions.EUWest2:
                    return Amazon.RegionEndpoint.EUWest2;
                case AWSRegions.SAEast1:
                    return Amazon.RegionEndpoint.SAEast1;
                case AWSRegions.USEast1:
                    return Amazon.RegionEndpoint.USEast1;
                case AWSRegions.USEast2:
                    return Amazon.RegionEndpoint.USEast2;
                case AWSRegions.USGovCloudWest1:
                    return Amazon.RegionEndpoint.USGovCloudWest1;
                case AWSRegions.USWest1:
                    return Amazon.RegionEndpoint.USWest1;
                case AWSRegions.USWest2:
                    return Amazon.RegionEndpoint.USWest2;
                case AWSRegions.Unknown:
                default:
                    return null;
            }
        }

        public ClientWithLoad GetNextLeastUsedClient()
        {
            ClientWithLoad retVal = null;
            lock (clients)
                retVal = clients.Min((clientWL) => clientWL);

            Interlocked.Increment(ref retVal.Load);
            return retVal;
        }

        public void PutRecordsRequest(PutRecordsRequest request, ResponseCallback cb, object context, DateTime deadline, DateTime expiration)
        {
            var task = new KinesisTask(executor_, request, null, KinesisRquestType.PutRecordsRequest, context, cb, (client) => { }, deadline, expiration, (int)requestTimeout);
            new Thread(() => task.run(GetNextLeastUsedClient())).Start();
        }

        public void DescribeStreamRequest(DescribeStreamRequest request, ResponseCallback cb, object context, DateTime deadline, DateTime expiration)
        {
            var task = new KinesisTask(executor_, null, request, KinesisRquestType.DescribeStream, context, cb, (client) => { }, deadline, expiration, (int)requestTimeout);
            new Thread(()=>task.run(GetNextLeastUsedClient())).Start();
        }

        void CreateClients(int maxConnections)
        {
            try
            {
                clients = new ClientWithLoad[maxConnections];
                for (int i = 0; i < maxConnections; i++)
                    clients[i] = new ClientWithLoad() 
                    {
                        ActualClient = string.IsNullOrEmpty(cred_.SessionToken) ? new AmazonKinesisClient(cred_.Akid, cred_.SecretKey, region_) : new AmazonKinesisClient(cred_.Akid, cred_.SecretKey, cred_.SessionToken, region_)
                    };
            }
            catch (Exception ex)
            {
                StdErrorOut.Instance.StdOut(LogLevel.error, ex.ToString());
                throw ex;
            }

        }
    }
}
