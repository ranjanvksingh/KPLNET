using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using KPLNETInterface;
using KPLNETClientInterface;

namespace KPLNETTest
{
    internal class ProducerTestCode
    {
        string firstSequenceNumber;
        Log4NetLogger log = null;
        public ProducerTestCode()
        {
            log = new Log4NetLogger(typeof(ProducerTestCode));
        }

        public static void Main(string[] args)
        {
            new ProducerTestCode().AppStart();
        }

        void AppStart()
        {
            KPLNETConfiguration config = new KPLNETConfiguration()
                                {
                                    runAsDaemon = false,
                                    nativeExecutable = Properties.Settings.Default.DeamonAppPath,
                                    region = AWSRegions.USWest1,
                                    aggregationMaxCount = 100,
                                    collectionMaxCount = 100,
                                    logLevel = log.LogLevel.ToString().ToLower()
                                };

            KinesisDotNetProducer kinesisDotNetProducer = new KinesisDotNetProducer(log, config);
            
            string streamName = "KPLNETTest";
            while (true)
            {
                Console.WriteLine("Enter Number Of messages to add. Enter X to exit.");
                string uEntry = Console.ReadLine();
                if (uEntry.ToLower() == "x")
                    break;

                int batchesCount = 0;
                if (int.TryParse(uEntry, out batchesCount))
                    RunForNumberOfBatches(batchesCount, streamName, kinesisDotNetProducer);
                else
                    Console.WriteLine("Invalid entry");
            }            
        }

        int succcessCount = 0;
        int errorCount = 0;

        void LogCount(Task<KPLDotNetResult> a)
        {
            if (a.IsCompleted)
            {
                if (Interlocked.Increment(ref succcessCount) % 10000 == 0)
                    log.info(string.Format("Success Count = {0} and Error Count = {1}", succcessCount, errorCount));

                if (string.IsNullOrEmpty(firstSequenceNumber))
                {
                    firstSequenceNumber = a.Result.UserResult.getSequenceNumber();
                    log.info("firstSequenceNumber = " + firstSequenceNumber);
                }
            }
            else
            {
                if (Interlocked.Increment(ref errorCount) % 100 == 0)
                    log.info(string.Format("Success Count = {0} and Error Count = {1}", succcessCount, errorCount));
            }
        }

        void LogResult(Task<KPLDotNetResult> a)
        {
            string lInfo = "";
            string lerror = "";
            if (a.IsCompleted)
                lInfo = "SequenceNumber: " + a.Result.UserResult.getSequenceNumber() + " ShardId: " + a.Result.UserResult.getShardId() + " Success: " + (object)a.Result.UserResult.isSuccessful();
            else
                lerror = "Add User record failed";

            if (!string.IsNullOrEmpty(lInfo))
                log.info(lInfo);
            else
                log.error(lerror);
        }

        void RunForNumberOfBatches(int numberOfBatches, string streamName, KinesisDotNetProducer kinesisDotNetProducer)
        {
            for (int i = 0; i < numberOfBatches; i++)
            {
                if(i % 10000 == 0)
                    log.info("started Sending user records with id = " + i);

                kinesisDotNetProducer.AddUserRecord(streamName, "Partition" + i % 2, new MemoryStream(Encoding.UTF8.GetBytes(string.Format("This message # {0} is for Kinesis", i)))).Task.ContinueWith(LogCount);
                if (i % 40 == 0)
                    Thread.Sleep(1);
            }
        }
    }
}
