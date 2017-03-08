using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using KPLNET.Kinesis.Core;

namespace KPLNETUnitTests
{
    [TestClass]
    public class KinesisRecordUnitTest
    {
        public KinesisRecordUnitTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }

        // Single records should not be turned into AggregatedRecord's
        [TestMethod]
        public void KinesisRecordUnitTest_SingleRecordTestMethod()
        {
            // No explicit hash key (ehk)
            {
                KinesisRecord r = new KinesisRecord();
                var ur = TestUtil.make_user_record();
                r.add(ur);
                TestUtil.verify_unaggregated(ur, r);
            }

            // With ehk
            {
                KinesisRecord r = new KinesisRecord();
                var ur = TestUtil.make_user_record("a", "a", "123");
                r.add(ur);
                TestUtil.verify_unaggregated(ur, r);
            }
        }

        // In the next 6 test cases we add a bunch of UserRecords to a KinesisRecord,
        // then we remove some. Each case has a different distribution of partition and
        // explicit hash keys. These tests exercise the add and remove methods of
        // KinesisRecord. In practice we typically add records until the KinesisRecord
        // has an estimated size that's over some threshold, then we remove records
        // until the accurate size is at or below that threshold.

        // All the records have the same partition key
        [TestMethod]
        public void KinesisRecordUnitTest_SameParitionKey()
        {
            int N = 1000;
            int M = 100;

            List<UserRecord> user_records = new List<UserRecord>();
            KinesisRecord r = new KinesisRecord();
            for (int i = 0; i < N; i++)
            {
                var ur = TestUtil.make_user_record();
                user_records.Add(ur);
                r.add(ur);
            }
            for (int i = 0; i < M; i++)
            {
                r.remove_last();
                user_records.RemoveAt(user_records.Count - 1);
            }

            TestUtil.verify(user_records, r);
        }

        // Every record has a different key
        [TestMethod]
        public void KinesisRecordUnitTest_DifferentParitionKey()
        {
            int N = 1000;
            int M = 100;

            List<UserRecord> user_records = new List<UserRecord>();
            KinesisRecord r = new KinesisRecord();
            for (int i = 0; i < N; i++)
            {
                var ur = TestUtil.make_user_record(i.ToString());
                user_records.Add(ur);
                r.add(ur);
            }
            for (int i = 0; i < M; i++)
            {
                r.remove_last();
                user_records.RemoveAt(user_records.Count - 1);
            }

            TestUtil.verify(user_records, r);
        }

        // Mix of single and duplicate keys, some contiguous, some not
        [TestMethod]
        public void KinesisRecordUnitTest_MixedParitionKey()
        {
            List<string> keys = new List<string>() { "a", "b", "b", "a", "a", "c", "b", "a", "d", "e", "e", "d", "d", "d" };

            List<UserRecord> user_records = new List<UserRecord>();
            KinesisRecord r = new KinesisRecord();
            for (int i = 0; i < keys.Count; i++)
            {
                var ur = TestUtil.make_user_record(keys[i]);
                user_records.Add(ur);
                r.add(ur);
            }
            for (int i = 0; i < 3; i++)
            {
                r.remove_last();
                user_records.RemoveAt(user_records.Count - 1);
            }
            TestUtil.verify(user_records, r);
        }

        [TestMethod]
        public void KinesisRecordUnitTest_SameEHK()
        {
            int N = 1000;
            int M = 100;

            List<UserRecord> user_records = new List<UserRecord>();
            KinesisRecord r = new KinesisRecord();
            for (int i = 0; i < N; i++)
            {
                var ur = TestUtil.make_user_record("pk", "data", "123");
                user_records.Add(ur);
                r.add(ur);
            }
            for (int i = 0; i < M; i++)
            {
                r.remove_last();
                user_records.RemoveAt(user_records.Count - 1);
            }

            TestUtil.verify(user_records, r);
        }



        [TestMethod]
        public void KinesisRecordUnitTest_DifferentEHK()
        {
            int N = 1000;
            int M = 100;

            List<UserRecord> user_records = new List<UserRecord>();
            KinesisRecord r = new KinesisRecord();
            for (int i = 0; i < N; i++)
            {
                var ur =
                    TestUtil.make_user_record("pk", "data", i.ToString());
                user_records.Add(ur);
                r.add(ur);
            }
            for (int i = 0; i < M; i++)
            {
                r.remove_last();
                user_records.RemoveAt(user_records.Count - 1);
            }

            TestUtil.verify(user_records, r);
        }

        [TestMethod]
        public void KinesisRecordUnitTest_MixedEHK()
        {
            List<string> keys = new List<string>() { "1", "2", "2", "1", "1", "3", "2", "1", "4", "5", "5", "4", "4", "4" };

            List<UserRecord> user_records = new List<UserRecord>();
            KinesisRecord r = new KinesisRecord();
            for (int i = 0; i < keys.Count; i++)
            {
                var ur = TestUtil.make_user_record("pk", "data", keys[i]);
                user_records.Add(ur);
                r.add(ur);
            }
            for (int i = 0; i < 3; i++)
            {
                r.remove_last();
                user_records.RemoveAt(user_records.Count - 1);
            }

            TestUtil.verify(user_records, r);
        }

        // TestUtil.verify accurate_size. Since this is probabilistic, it's not
        // guaranteed to catch all errors. It should however detect any obvious
        // mistakes.
        [TestMethod]
        public void KinesisRecordUnitTest_AccurateSize()
        {
            for (int i = 0; i < 100; i++)
            {
                KinesisRecord r = new KinesisRecord();
                long num_records = new Random().Next(1, 512);
                for (long j = 0; j < num_records; j++)
                {
                    int key_size = new Random().Next(1, 256);
                    int data_size = new Random().Next(1, 64 * 1024);

                    var ur = TestUtil.make_user_record(new string('a', key_size),
                                               new string('a', data_size),
                                               new Random().Next() % 2 == 0 ? "" : "123");
                    r.add(ur);
                }
                int predicted = (int)r.accurate_size();
                string serialized = r.serialize();

                if(r.Items().Count > 1)
                    Assert.AreEqual(predicted, serialized.Length-16);
                else
                    Assert.AreEqual(predicted, serialized.Length);
            }
        }

        // Check that the estimated_size() gives a sane estimate
        [TestMethod]
        public void KinesisRecordUnitTest_EstimatedSize()
        {
            for (int i = 0; i < 50; i++)
            {
                KinesisRecord r = new KinesisRecord();
                int num_records = new Random().Next(2, 512);

                // non repeated partition keys
                for (int j = 0; j < num_records; j++)
                {
                    int key_size = new Random().Next(1, 256);
                    int data_size = new Random().Next(1, 64 * 1024);
                    var ur = TestUtil.make_user_record(new string('a', key_size),
                                               new string('a', data_size),
                                               new Random().Next() % 2 == 0 ? "" : "123");
                    r.add(ur);
                }

                // repeated partition keys
                int key_size1 = new Random().Next(1, 256);
                for (long j = 0; j < num_records; j++)
                {
                    int data_size = new Random().Next(1, 64 * 1024);
                    var ur = TestUtil.make_user_record(new string('a', key_size1),
                                               new string('a', data_size),
                                               new Random().Next() % 2 == 0 ? "" : "123");
                    r.add(ur);
                }

                // small keys small data
                for (long j = 0; j < num_records; j++)
                {
                    var ur = TestUtil.make_user_record(new string('a', 2),
                                               new string('a', 2),
                                               new Random().Next() % 2 == 0 ? "" : "123");
                    r.add(ur);
                }

                int estimated = (int)r.Estimated_size();
                string serialized = r.serialize();

                double diff = (double)serialized.Length - estimated;
                double percentage_diff = diff / serialized.Length * 100;
                percentage_diff *= percentage_diff < 0 ? -1 : 1;

                StringBuilder ss = new StringBuilder();
                ss.Append("Estimated size should be within 1 percent or 32 bytes of actual ")
                   .Append("size, estimate was ").Append(estimated).Append(", actual size was ")
                   .Append(serialized.Length).Append(" (").Append(percentage_diff).Append("% difference)");
                //BOOST_CHECK_MESSAGE(percentage_diff < 1 || diff < 32, ss.ToString());
            }
        }

        // TestUtil.verify correct behavior when container is empty
        [TestMethod]
        public void KinesisRecordUnitTest_Empty()
        {
            KinesisRecord r = new KinesisRecord();
            Assert.AreEqual(0, (int)r.accurate_size());
            Assert.AreEqual(0, (int)r.Estimated_size());

            try
            {
                r.serialize();
                Assert.Fail("Calling serialize on empty KinesisRecord should cause exception");
            }
            catch (Exception e)
            {
                // expected
            }
        }

        // Test that clearing works correctly
        [TestMethod]
        public void KinesisRecordUnitTest_Clearing()
        {
            int N = 10;

            List<UserRecord> user_records = new List<UserRecord>();
            KinesisRecord r = new KinesisRecord();
            for (int i = 0; i < N; i++)
            {
                var ur = TestUtil.make_user_record();
                user_records.Add(ur);
                r.add(ur);
            }

            TestUtil.verify(user_records, r);

            for (int i = 0; i < 5; i++)
            {
                r.clear();
                user_records.Clear();

                for (int j = 0; j < N; j++)
                {
                    var ur = TestUtil.make_user_record();
                    user_records.Add(ur);
                    r.add(ur);
                }

                TestUtil.verify(user_records, r);
            }
        }

        // Test that deadlines are correctly set
        [TestMethod]
        public void KinesisRecordUnitTest_Deadlines()
        {
            // The nearest deadline should always be kept
            {
                KinesisRecord r = new KinesisRecord();
                var start = DateTime.Now;
                for (int i = 0; i < 10; i++)
                {
                    var ur = TestUtil.make_user_record();
                    ur.set_deadline(start.AddMilliseconds(i * 100));
                    ur.set_expiration(start.AddMilliseconds(i * 100));
                    r.add(ur);
                }
                Assert.IsTrue(r.Deadline() == start);
                Assert.IsTrue(r.Expiration() == start);
            }

            // If a nearer deadline comes in, it should override the previous
            {
                KinesisRecord r = new KinesisRecord();
                var earlier = DateTime.Now;
                var later = earlier.AddMilliseconds(500);
                {
                    var ur = TestUtil.make_user_record();
                    ur.set_deadline(later);
                    r.add(ur);
                    Assert.IsTrue(r.Deadline() == later);
                }
                {
                    var ur = TestUtil.make_user_record();
                    ur.set_deadline(earlier);
                    r.add(ur);
                    Assert.IsTrue(r.Deadline() == earlier);
                }
                // Removing the last added record should restore the previous deadline
                r.remove_last();
                Assert.IsTrue(r.Deadline() == later);
            }
        }

        // The throughput of calling add() and estimated_size() on each UserRecord.
        // 1.6 M/s on mac, at time of writing.
        //[TestMethod]
        //public void KinesisRecordUnitTest_AddAndEstimateThroughput()
        //{
        //    int n = 0;

        //    int N = 1000000;

        //    List<UserRecord> v = new List<UserRecord>();
        //    for (int i = 0; i < N; i++)
        //    {
        //        v.Add(TestUtil.make_user_record("pk", "data", "123"));
        //    }

        //    KinesisRecord r = new KinesisRecord();

        //    var start = DateTime.Now;

        //    for (int i = 0; i < N; i++)
        //    {
        //        r.add(v[i]);
        //        if (r.Estimated_size() < 100000)
        //        {
        //            n++;
        //        }
        //    }

        //    var nanos = (DateTime.Now - start).TotalMilliseconds * 1000;
        //    double seconds = nanos / 1e9;
        //    //LOG(info) << "KinesisRecord add and estimate rate: " << N / seconds << " rps";
        //}
    }
}
