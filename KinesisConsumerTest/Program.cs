using System;
using System.Threading.Tasks;

using Amazon.Kinesis;
using Amazon.Kinesis.Model;

using KPLNETInterface;

namespace KinesisConsumerTest
{
    class Program
    {
        public const string AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
        public const string AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";

        static AmazonKinesisClient c = null;
        public static KPLNETInterface.Log4NetLogger log = null;

        static Action<Task<GetRecordsResponse>> continuationAction = (Action<Task<GetRecordsResponse>>)(a =>
        {
            string data = null;
            if (a.Result.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                string errorMsg;
                if (a.Result.Records.Count > 0)
                {
                    var userrecords = UserRecord.deaggregate(a.Result.Records, out errorMsg);
                    foreach (var rec in userrecords)
                    {
                        byte[] bytes = new byte[rec.Data.Length];
                        rec.Data.Read(bytes, 0, (int)rec.Data.Length);
                        data = System.Text.Encoding.UTF8.GetString(bytes);
                        log.info(String.Format("Retrieved record: partition key = {0} sequence number = {1}, subsequence number = {2}, data = {3}", rec.PartitionKey, rec.SequenceNumber, rec.getSubSequenceNumber(), data));
                    }
                }

                var next = a.Result.NextShardIterator;
                if (next != null)
                    c.GetRecordsAsync(new GetRecordsRequest() { Limit = 1000, ShardIterator = next }).ContinueWith(continuationAction);
            }
            else
            {
                Console.Error.WriteLine("GetRecordsRequest failed with Error code: " + a.Result.HttpStatusCode.ToString());
            }
        });

        static void Main(string[] args)
        {
            try
            {
                log = new KPLNETInterface.Log4NetLogger(typeof(Program));
                string akid = Environment.GetEnvironmentVariable(AWS_ACCESS_KEY_ID);
                string sKey = Environment.GetEnvironmentVariable(AWS_SECRET_ACCESS_KEY);
                if (akid == null || sKey == null)
                    return;

                c = new AmazonKinesisClient(akid, sKey, Amazon.RegionEndpoint.USWest2);
                c.GetShardIteratorAsync(new GetShardIteratorRequest() { ShardId = "shardId-000000000000", StreamName = "KPLNETTest", ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER, StartingSequenceNumber = "49572131548342384703435782055007292317434442354572394498" }).
                    ContinueWith((b) =>
                    {
                        if (b.Result.HttpStatusCode == System.Net.HttpStatusCode.OK)
                        {
                            string it = b.Result.ShardIterator;
                            if (it != null)
                                c.GetRecordsAsync(new GetRecordsRequest() { Limit = 1000, ShardIterator = it }).ContinueWith(continuationAction);
                            else
                                Console.WriteLine("Done iterating all items in the shard");
                        }
                        else
                        {
                            Console.Error.WriteLine("GetShardIteratorRequest failed with Error code: " + b.Result.HttpStatusCode.ToString());
                        }
                    });
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }
            finally
            {
                Console.ReadLine();
            }
        }
    }
}
