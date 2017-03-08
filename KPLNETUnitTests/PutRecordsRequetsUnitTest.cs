using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using KPLNET.Kinesis.Core;

namespace KPLNETUnitTests
{

    [TestClass]
    public class PutRecordsRequetsUnitTest
    {


        KinesisRecord make_kinesis_record(int min_serialized_size = 0)
        {
            var kr = new KinesisRecord();
            int num_user_records = 1 + (new Random().Next() % 100);

            Func<UserRecord> make_ur = () =>
            {
                return TestUtil.make_user_record(
                    new Random().Next().ToString(),
                    new Random().Next().ToString(),
                    new Random().Next().ToString(),
                    10000,
                    "myStream");
            };

            for (int i = 0; i < num_user_records; i++)
            {
                kr.add(make_ur());
            }

            if (min_serialized_size > 0)
            {
                while ((int)kr.accurate_size() < min_serialized_size)
                {
                    kr.add(make_ur());
                }
            }

            return kr;
        }


        [TestMethod]
        public void PutRecordsRequetsUnitTest_SizePrediction()
        {
            PutRecordsRequest prr = new PutRecordsRequest();
            Assert.AreEqual((int)prr.accurate_size(), 0);
            Assert.AreEqual(prr.Estimated_size(), prr.accurate_size());

            Stack<int> sizes = new Stack<int>();
            sizes.Push(0);
            int N = 100;

            for (int i = 0; i < N; i++)
            {
                var kr = make_kinesis_record();
                prr.add(kr);

                int expected_growth = kr.serialize().Length + kr.partition_key().Length;
                int expected_size = sizes.Peek() + expected_growth;
                Assert.AreEqual((int)prr.accurate_size(), (expected_size - 16));
                Assert.AreEqual(prr.Estimated_size(), prr.accurate_size());

                sizes.Push(expected_size-16);
            }

            for (int i = 0; i < N; i++)
            {
                Assert.AreEqual((int)prr.accurate_size(), sizes.Peek());
                Assert.AreEqual(prr.Estimated_size(), prr.accurate_size());

                sizes.Pop();
                prr.remove_last();
            }

            Assert.AreEqual((int)prr.accurate_size(), 0);
            Assert.AreEqual(prr.Estimated_size(), prr.accurate_size());
        }

        [TestMethod]
        public void PutRecordsRequetsUnitTest_StreamName()
        {
            PutRecordsRequest prr = new PutRecordsRequest();
            var kr = make_kinesis_record();
            prr.add(kr);
            Assert.AreEqual(prr.stream(), "myStream");
        }
    }
}
