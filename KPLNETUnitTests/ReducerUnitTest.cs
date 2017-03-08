using System;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using KPLNET.Utils;
using KPLNET.Kinesis.Core;

namespace KPLNETUnitTests
{
    using ReducerURKR = Reducer<UserRecord, KinesisRecord>;
    using FlushCallback = Action<KinesisRecord>;


    [TestClass]
    public class ReducerUnitTest
    {
        ReducerURKR make_reducer(int size_limit = 256 * 1024, int count_limit = 1000, FlushCallback cb = null)
        {
            if (cb == null)
                cb = (kr) => { };

            var executor = new IoServiceExecutor(8);
            return new ReducerURKR(executor, cb, (ulong)size_limit, (ulong)count_limit);
        }

        [TestMethod]
        public void ReducerUnitTest_CountLimit()
        {
            int limit = 100;
            var reducer = make_reducer(int.MaxValue, limit);
            List<UserRecord> v = new List<UserRecord>();
            for (int j = 0; j < 3; j++)
            {
                v.Clear();
                for (int i = 0; i < limit; i++)
                {
                    var ur = TestUtil.make_user_record();
                    v.Add(ur);
                    KinesisRecord result = reducer.add(ur) as KinesisRecord;
                    if (i < limit - 1)
                    {
                        Assert.IsNull(result);
                    }
                    else
                    {
                        Assert.IsNotNull(result);

                        TestUtil.verify(v, result);
                        Assert.AreEqual((int)reducer.size(), 0);
                    }
                }
            }
        }

        [TestMethod]
        public void ReducerUnitTest_SizeLimit()
        {
            int limit = 10000;
            var reducer = make_reducer(limit, int.MaxValue);
            List<UserRecord> v = new List<UserRecord>();

            int k = 0;
            for (int j = 0; j < 250; j++)
            {
                // Put records until flush happens.
                KinesisRecord kr;
                do
                {
                    var ur = TestUtil.make_user_record(
                            "pk",
                            new Random().Next().ToString(),
                            "123",
                            10000 + k++);
                    v.Add(ur);
                    kr = reducer.add(ur) as KinesisRecord;
                }
                while (kr == null);

                // Check that the count of flushed and remaining records add up to what was
                // put.
                Assert.AreEqual((int)kr.Size() + (int)reducer.size(), v.Count);

                // Move the copies of those records that weren't flushed into another vector
                // so we can compare the flushed result against the original vector.
                List<UserRecord> tmp = new List<UserRecord>();
                for (int i = v.Count - 1; i >= kr.Items().Count; i--)
                {
                    tmp.Insert(0, v[i]);
                    v.RemoveAt(i);
                }

                // Check data integrity
                TestUtil.verify(v, kr);

                // Also check we didn't exceed the limit
                var serialized_len = kr.serialize().Length;
                Assert.IsTrue(serialized_len <= limit);

                // Move the unflushed records back so they're accounted for in the next
                // iteration.
                v.Clear();
                v.AddRange(tmp);
            }
        }

        [TestMethod]
        public void ReducerUnitTest_Deadline()
        {
            KinesisRecord kr = null;
            var reducer = make_reducer(
                int.MaxValue,
                int.MaxValue,
                (result) => { kr = result; });

            List<UserRecord> v = new List<UserRecord>();
            for (int i = 0; i < 100; i++)
            {
                var ur = TestUtil.make_user_record(
                        "pk",
                        new Random().Next().ToString(),
                        "123",
                        5000 + i);
                v.Add(ur);
                reducer.add(ur);
            }

            // Should not flush after 1 second because we've set the deadlines to be 5
            Thread.Sleep(1000);
            Assert.AreEqual((int)reducer.size(), 100);

            // Now we're going to put a record with a deadline that's now to trigger the
            // flush.
            var ur1 = TestUtil.make_user_record(
                    "pk",
                    new Random().Next().ToString(),
                    "123",
                    0);
            reducer.add(ur1);
            // This record should be moved to the front when flushed because of its
            // deadline
            v.Insert(0, ur1);

            Thread.Sleep(300);
            Assert.AreEqual((int)reducer.size(), 0);
            Assert.IsNotNull(kr);
            TestUtil.verify(v, kr);
        }

        // Test that a flush due to limits cancels a deadline trigger. Flushes always
        // remove records with closer deadlines first. Once they are removed, the
        // Reducer's timeout should be reset to the min deadline of the records that
        // remain.
        [TestMethod]
        public void ReducerUnitTest_ResetTimeout()
        {
            KinesisRecord kr = null;
            var reducer = make_reducer(
                300,
                int.MaxValue,
                (result) =>
                {
                    kr = result;
                });

            List<UserRecord> v = new List<UserRecord>();
            int k = 0;

            for (int i = 0; i < 10; i++)
            {
                var ur = TestUtil.make_user_record(
                        "pk",
                      new Random().Next().ToString(),
                        "123",
                        2000 + k++);
                v.Add(ur);
                reducer.add(ur);
            }

            // This record has a much closer deadline
            {
                var ur = TestUtil.make_user_record(
                        "pk",
                        new Random().Next().ToString(),
                        "123",
                        100);
                v.Insert(0, ur);
                reducer.add(ur);
            }

            // Put records until flush happens
            KinesisRecord kr2;
            {
                do
                {
                    // The other records have a further deadline.
                    var ur = TestUtil.make_user_record(
                            "pk",
                          new Random().Next().ToString(),
                            "123",
                            2000 + k++);
                    v.Add(ur);
                    kr2 = reducer.add(ur) as KinesisRecord;
                } while (kr2 == null);
            }

            // Put a few more to make sure the buffer is not empty
            for (int i = 0; i < 10; i++)
            {
                var ur = TestUtil.make_user_record(
                        "pk",
                        new Random().Next().ToString(),
                        "123",
                        2000 + k++);
                v.Add(ur);
                reducer.add(ur);
            }

            // No flush should happen in the next second even though we had a record with
            // a 100ms deadline - that record should've gotten flushed by the size limit,
            // and its deadline should no longer apply.
            Thread.Sleep(1000);
            Assert.IsNull(kr);

            // The timer should now be set to the min deadline of the remaining records.
            // It should go off after another second or so
            Thread.Sleep(1500);
            Assert.IsNotNull(kr);

            // Check data integrity
            Assert.AreEqual(kr.Size() + kr2.Size() + reducer.size(), (ulong)v.Count);
            for (int i = v.Count - 1; i >= (int)kr.Size() + (int)kr2.Size(); i--)
                v.RemoveAt(i);

            List<UserRecord> v2 = new List<UserRecord>();
            for (int i = (int)kr2.Size() - 1; i >= 0; i--)
            {
                v2.Insert(0, v[i]);
                v.RemoveAt(i);
            }
            TestUtil.verify(v, kr);
            TestUtil.verify(v2, kr2);
        }

        // A KinesisRecord should be produced even if a UserRecord is over the size
        // limit all by itself
        [TestMethod]
        public void ReducerUnitTest_GiantRecord()
        {
            var reducer = make_reducer(10000, int.MaxValue);
            var ur = TestUtil.make_user_record("pk", new string('a', 11000));
            var kr = reducer.add(ur) as KinesisRecord;
            Assert.IsNotNull(kr);
            TestUtil.verify_unaggregated(ur, kr);
        }

        // Manual flush should not produce empty KinesisRecords
        [TestMethod]
        public void ReducerUnitTest_NonEmpty()
        {
            KinesisRecord kr = null;
            var reducer = make_reducer(
                int.MaxValue,
                int.MaxValue,
                (result) =>
                {
                    kr = result;
                });
            reducer.Flush();
            Thread.Sleep(100);
            Assert.IsNull(kr); //"Flush should not have produced a KinesisRecord because the Reducer is empty"
        }

        [TestMethod]
        public void ReducerUnitTest_Concurrency()
        {
            int counter = 0;
            //LOG(info) << "Starting concurrency test. If this doesn't finish in 30 " << "seconds or so it probably means there's a deadlock.";
            ConcurrentBag<UserRecord> put = new ConcurrentBag<UserRecord>();
            ConcurrentBag<KinesisRecord> results = new ConcurrentBag<KinesisRecord>();

            var reducer = make_reducer(5000, int.MaxValue, (result) => { results.Add(result); });
            List<Thread> threads = new List<Thread>();
            for (int i = 0; i < 16; i++)
            {
                var t = new Thread(() =>
                  {
                      for (int j = 0; j < 32; j++)
                      {
                          KinesisRecord kr = null;
                          do
                          {
                              int count = Interlocked.Increment(ref counter);
                              var ur = TestUtil.make_user_record("pk", count.ToString(), "123", count);
                              put.Add(ur);
                              kr = reducer.add(ur) as KinesisRecord;
                          } while (kr == null);

                          results.Add(kr);

                          // Call flush sometimes too to mix things up further
                          if (i % 8 == 0 && i == j)
                              reducer.Flush();
                      }
                  });
                t.Start();
                threads.Add(t);
            }

            foreach (var t in threads)
                t.Join();

            while (reducer.size() > 0)
            {
                reducer.Flush();
                Thread.Sleep(1000);
            }

            Assert.AreEqual((int)reducer.size(), 0);

            //LOG(info) << "Finished putting data, " << counter << " records put. "
            //          << "Analyzing results...";

            // Check that all records made it out. Order is no longer guaranteed with
            // many workers, but we've put a monotonically increasing number in the
            // record data, so that will allow us to sort and match records up.
            List<UserRecord> put_v = new List<UserRecord>(put.ToArray());

            List<UserRecord> result_v = new List<UserRecord>();
            var resultsArray = results.ToArray();
            for (int i = 0; i < results.Count; i++)
            {
                result_v.AddRange(resultsArray[i].Items());
            }
            Comparison<UserRecord> ckr = (a, b) => { return string.Compare(a.Data().ToStringUtf8(), b.Data().ToStringUtf8()); };
            put_v.Sort(ckr);
            result_v.Sort(ckr);

            Assert.AreEqual(put_v.Count, result_v.Count);
            for (int i = 0; i < put_v.Count; i++)
            {
                Assert.AreEqual(put_v[i], result_v[i]);
            }
        }
    }
}
