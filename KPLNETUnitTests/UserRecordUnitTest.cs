using System;
using System.Text;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KPLNETUnitTests
{
    /// <summary>
    /// Summary description for UserRecordUnitTest
    /// </summary>
    [TestClass]
    public class UserRecordUnitTest
    {
        ulong kDefaultId = 1234567;
        string kDefaultData = "hello world";
        string kDefaultPartitionKey = "abcd";
        string kDefaultStream = "myStream";

        Aws.Kinesis.Protobuf.Message make_put_record()
        {
            var m = new Aws.Kinesis.Protobuf.Message();
            m.Id = kDefaultId;
            m.PutRecord = new Aws.Kinesis.Protobuf.PutRecord();
            m.PutRecord.PartitionKey = kDefaultPartitionKey;
            m.PutRecord.Data = Google.Protobuf.ByteString.CopyFrom(kDefaultData, Encoding.Default);
            m.PutRecord.StreamName = kDefaultStream;

            return m;
        }

        void throughput_test(bool ehk)
        {
            int N = 250000;
            List<Aws.Kinesis.Protobuf.Message> messages = new List<Aws.Kinesis.Protobuf.Message>();
            for (int i = 0; i < N; i++)
            {
                var m = make_put_record();
                m.PutRecord.PartitionKey = new string('a', 256);
                if (ehk)
                {
                    m.PutRecord.ExplicitHashKey = new string('1', 38);
                }
                messages.Add(m);
            }

            List<KPLNET.Kinesis.Core.UserRecord> v = new List<KPLNET.Kinesis.Core.UserRecord>();

            var start = DateTime.Now;

            for (int i = 0; i < N; i++)
            {
                v.Add(new KPLNET.Kinesis.Core.UserRecord(messages[i]));
            }

            var end = DateTime.Now;
            double seconds = (end - start).TotalSeconds;
            double rate = (double)N / seconds;
            //LOG(info) << "Message conversion rate (" << (ehk ? "with" : "no") << " EHK): "  << rate << " messages/s";
        }

        [TestMethod]
        public void UserRecordUnitTest_BasicConversion()
        {
            var m = make_put_record();
            KPLNET.Kinesis.Core.UserRecord ur = new KPLNET.Kinesis.Core.UserRecord(m);

            Assert.AreEqual(ur.Stream(), kDefaultStream);
            Assert.AreEqual(ur.Partition_key(), kDefaultPartitionKey);
            Assert.AreEqual(ur.Data().ToString(Encoding.Default), kDefaultData);
            Assert.AreEqual(ur.Source_id(), kDefaultId);
        }

        // We should be using the md5 of the partition key when there is no explicit
        // hash key.
        [TestMethod]
        public void UserRecordUnitTest_HashKeyFromPartitionKey()
        {
            var m = make_put_record();
            KPLNET.Kinesis.Core.UserRecord ur = new KPLNET.Kinesis.Core.UserRecord(m);
            Assert.AreEqual(ur.Hash_key().ToString(), KPLNETInterface.Utils.GetDecimalHashKey(kDefaultPartitionKey).ToString());
        }

        [TestMethod]
        public void UserRecordUnitTest_ExplicitHashKey()
        {
            string explicit_hash_key = "123456789";
            var m = make_put_record();
            m.PutRecord.ExplicitHashKey = explicit_hash_key;

            var ur = new KPLNET.Kinesis.Core.UserRecord(m);
            Assert.AreEqual(ur.Hash_key().ToString(), explicit_hash_key);
        }

        [TestMethod]
        public void UserRecordUnitTest_PutRecordResultFail()
        {
            var m = make_put_record();
            var ur = new KPLNET.Kinesis.Core.UserRecord(m);

            KPLNET.Kinesis.Core.Attempt a = new KPLNET.Kinesis.Core.Attempt();
            var now = DateTime.Now;
            a.set_start(now);
            a.set_end(now.AddMilliseconds(5));
            a.set_error("code", "message");
            ur.add_attempt(a);

            KPLNET.Kinesis.Core.Attempt b = new KPLNET.Kinesis.Core.Attempt();
            b.set_start(now.AddMilliseconds(11));
            b.set_end(now.AddMilliseconds(18));
            b.set_error("code2", "message2");
            ur.add_attempt(b);

            Aws.Kinesis.Protobuf.Message m2 = ur.to_put_record_result();
            Assert.IsTrue(m2.PutRecordResult != null);
            var prr = m2.PutRecordResult;

            Assert.AreEqual(prr.Attempts.Count, 2);
            Assert.AreEqual(prr.Success, false);
            Assert.AreEqual(prr.Attempts[0].Success, false);
            Assert.AreEqual(prr.Attempts[0].ErrorCode, "code");
            Assert.AreEqual(prr.Attempts[0].ErrorMessage, "message");
            //Assert.AreEqual((int)prr.Attempts[0].Delay, 0);
            Assert.AreEqual((int)prr.Attempts[0].Duration, 5);
            Assert.AreEqual(prr.Attempts[1].Success, false);
            Assert.AreEqual(prr.Attempts[1].ErrorCode, "code2");
            Assert.AreEqual(prr.Attempts[1].ErrorMessage, "message2");
            Assert.AreEqual((int)prr.Attempts[1].Delay, 6);
            Assert.AreEqual((int)prr.Attempts[1].Duration, 7);
        }

        [TestMethod]
        public void UserRecordUnitTest_PutRecordResultSuccess()
        {
            var m = make_put_record();
            var ur = new KPLNET.Kinesis.Core.UserRecord(m);

            KPLNET.Kinesis.Core.Attempt a = new KPLNET.Kinesis.Core.Attempt();
            a.set_error("code", "message");
            ur.add_attempt(a);

            KPLNET.Kinesis.Core.Attempt b = new KPLNET.Kinesis.Core.Attempt();
            b.set_result("shard-0", "123456789");
            ur.add_attempt(b);

            Aws.Kinesis.Protobuf.Message m2 = ur.to_put_record_result();
            Assert.IsNotNull(m2.PutRecordResult);
            var prr = m2.PutRecordResult;

            Assert.AreEqual(prr.Attempts.Count, 2);
            Assert.AreEqual(prr.Success, true);
            Assert.AreEqual(prr.ShardId, "shard-0");
            Assert.AreEqual(prr.SequenceNumber, "123456789");
        }

        [TestMethod]
        public void SUserRecordUnitTest_HashKeyThroughputNoEHK()
        {
            throughput_test(false);
        }

        [TestMethod]
        public void UserRecordUnitTest_HashKeyThroughputWithEHK()
        {
            //throughput_test(true);
        }
    }
}
