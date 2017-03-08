using System;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using KPLNET.Kinesis.Core;

namespace KPLNETUnitTests
{
    using Callback = Action<KPLNET.Kinesis.Core.KinesisRecord>;

    [TestClass]
    public class LimterUnitTest
    {
        KinesisRecord make_kinesis_record(DateTime deadline, DateTime expiration)
        {
            var ur = TestUtil.make_user_record();
            ur.set_deadline(deadline);
            ur.set_expiration(expiration);
            ur.Predicted_shard(0);
            var kr = new KinesisRecord();
            kr.add(ur);
            return kr;
        }

        Limiter make_limiter(Callback cb, Callback expired_cb = null)
        {
            if (expired_cb == null)
                expired_cb = (kr) => { };

            var executor = new KPLNET.Utils.IoServiceExecutor(2);
            var config = new KPLNETInterface.KPLNETConfiguration();
            config.rateLimit = 100;
            return new Limiter(
                executor,
                cb,
                expired_cb,
                config);
        }


        // Test that the limiter emits records ordered by deadline, closest first
        [TestMethod]
        public void LimterUnitTest_Ordering()
        {
            List<KinesisRecord> original = new List<KinesisRecord>();
            List<KinesisRecord> result = new List<KinesisRecord>();

            var limiter = make_limiter((kr) => { result.Add(kr); });

            // We make every deadline unique since there's no guarantee on the relative
            // order of two records with the same deadline.
            var first_deadline = DateTime.Now;

            // First, put a bunch of records to drain the token bucket and fill up the
            // limiter's queue
            int initial_batch_size = 2000;
            for (int i = 0; i < initial_batch_size; i++)
            {
                var kr = make_kinesis_record(first_deadline.AddMilliseconds(i), DateTime.Now.AddDays(1));
                original.Add(kr);
                limiter.put(kr);
            }

            // Then put records with random deadlines
            var rand = new Random(1337);
            List<int> used = new List<int>();
            for (int i = 0; i < 2000; i++)
            {
                int delta;
                do
                {
                    delta = rand.Next() % 100000;
                }
                while (used.Exists((a) => a == delta));

                used.Add(delta);

                var kr = make_kinesis_record(first_deadline.AddMilliseconds(initial_batch_size + delta), DateTime.Now.AddDays(1));
                original.Add(kr);
                limiter.put(kr);
            }

            while (result.Count < original.Count)
            {
                Thread.Sleep(1000);
                //LOG(info) << result.size() << " / " << original.size();
            }

            // Check that the records were emitted in the order of their deadlines
            Assert.AreEqual(original.Count, result.Count);
            original.Sort((a, b) => { return (a.Deadline() < b.Deadline()) ? -1 : ((a.Deadline() == b.Deadline()) ? 0 : 1); });

            for (int i = 0; i < original.Count; i++)
            {
                if (original[i] != result[i])
                {
                    StringBuilder ss = new StringBuilder();
                    ss.Append("failed (").Append(i).Append(") ")
                      .Append("original: ").Append(original[i].Deadline().ToString())
                      .Append(", result: ").Append(result[i].Deadline().ToString());
                    Assert.Fail(ss.ToString());
                }
            }
        }

        // Test that the limiter removes records that have expired while sitting in the
        // queue
        [TestMethod]
        public void LimterUnitTest_Expiration()
        {
            List<KinesisRecord> expect_sent = new List<KinesisRecord>();
            List<KinesisRecord> expect_expired = new List<KinesisRecord>();
            List<KinesisRecord> sent = new List<KinesisRecord>();
            List<KinesisRecord> expired = new List<KinesisRecord>();

            var limiter = make_limiter((kr) => { sent.Add(kr); },
                                        (kr) => { expired.Add(kr); });

            // First, put a bunch of records to drain the token bucket and fill up the
            // limiter's queue. These records won't ever expire.
            for (int i = 0; i < 3000; i++)
            {
                var kr = make_kinesis_record(DateTime.Now, DateTime.Now.AddDays(1));
                expect_sent.Add(kr);
                limiter.put(kr);
            }

            var rand = new Random(1337);

            // Put some records with expiration in the past
            for (int i = 0; i < 1000; i++)
            {
                var kr =
                    make_kinesis_record(
                        DateTime.Now.AddMilliseconds(1),
                        DateTime.Now.AddMilliseconds(-1 * (rand.Next() % 1000000)));
                expect_expired.Add(kr);
                limiter.put(kr);
            }

            // Put some records that are not expired, but will expire while waiting in the
            // queue
            for (int i = 0; i < 1000; i++)
            {
                var kr =
                    make_kinesis_record(
                        DateTime.Now.AddMilliseconds(1),
                        DateTime.Now.AddMilliseconds((1000 + rand.Next() % 100)));
                Assert.IsTrue(!kr.expired());
                expect_expired.Add(kr);
                limiter.put(kr);
            }

            while (sent.Count < expect_sent.Count || expired.Count < expect_expired.Count)
            {
                Thread.Sleep(1000);
                //LOG(info) << sent.size() << " / " << expect_sent.size() << "; "
                //          << expired.size() << " / " << expect_expired.size();
            }

            Assert.AreEqual(expect_sent.Count, sent.Count);
            Assert.AreEqual(expect_expired.Count, expired.Count);
        }
    }
}
