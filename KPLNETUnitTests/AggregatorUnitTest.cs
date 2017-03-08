using System;
using System.Numerics;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using KPLNET.Utils;
using KPLNET.Kinesis.Core;

namespace KPLNETUnitTests
{
    using FlushCallback = Action<KPLNET.Kinesis.Core.KinesisRecord>;

    [TestClass]
    public class AggregatorUnitTest
    {
        public long kCountLimit = 100;
        public AggregatorUnitTest()
        {
        }
        public Aggregator make_aggregator(bool shard_map_down = false, FlushCallback cb = null, KPLNETInterface.KPLNETConfiguration config = null)
        {
            if (cb == null)
                cb = (kr) => { };

            if (config == null)
            {
                config = new KPLNETInterface.KPLNETConfiguration();
                config.aggregationMaxCount = (ulong)kCountLimit;
            }

            return new Aggregator(new IoServiceExecutor(4), new MockShardMap(shard_map_down), cb, config);
        }        

        class MockShardMap : ShardMap
        {

            bool down_;
            List<long> shard_ids_ = new List<long>();
            List<BigInteger> limits_ = new List<BigInteger>();
            public MockShardMap(bool down = false)
            {
                down_ = down;
                limits_.Add(BigInteger.Parse("85070591730234615865843651857942052862"));
                shard_ids_.Add(2);

                limits_.Add(BigInteger.Parse("170141183460469231731687303715884105727"));
                shard_ids_.Add(3);

                limits_.Add(BigInteger.Parse("340282366920938463463374607431768211455"));
                shard_ids_.Add(1);
            }

            public override long Shard_id(BigInteger hash_key)
            {
                if (down_)
                {
                    return -1;
                }

                for (int i = 0; i < shard_ids_.Count; i++)
                {
                    if (hash_key <= limits_[i])
                    {
                        return shard_ids_[i];
                    }

                }

                return -1;
            }
        }

        

        [TestMethod]
        public void AggregatorUnitTest_BasicTestMethod()
        {
            var aggregator = make_aggregator();

            for (int shard_id = 1; shard_id <= 3; shard_id++)
            {
                List<UserRecord> v = new List<UserRecord>();
                KinesisRecord kr = null;

                for (int i = 0; i < kCountLimit; i++)
                {
                    var ur = TestUtil.make_user_record("pk", TestUtil.random_string(new Random().Next(100)), TestUtil.get_hash_key(shard_id).ToString(), 10000 + i, "MyStream", 0);
                    kr = aggregator.put(ur);
                    v.Add(ur);

                    Assert.IsFalse(ur.Predicted_shard() == -1);
                    Assert.AreEqual(ur.Predicted_shard(), shard_id);
                }

                Assert.IsNotNull(kr);
                TestUtil.verify(v, kr);
            }
        }

        [TestMethod]
        public void AggregatorUnitTest_ShardMapDownTestMethod()
        {
            var aggregator = make_aggregator(true);

            for (int i = 0; i < 100; i++)
            {
                var ur = TestUtil.make_user_record("pk", TestUtil.random_string(new Random().Next(100)), TestUtil.random_BigInt(new Random().Next(10)), 100000, "MyStream", 0);

                var kr = aggregator.put(ur);
                Assert.IsNotNull(kr);
                TestUtil.verify_unaggregated(ur, kr);
            }
        }

        [TestMethod]
        public void AggregatorUnitTest_AggregationDisabled()
        {
            var config = new KPLNETInterface.KPLNETConfiguration();
            config.aggregationEnabled = false;
            var aggregator = make_aggregator(false, (kr) => { }, config);

            for (int i = 0; i < 100; i++)
            {
                var ur = TestUtil.make_user_record("pk", TestUtil.random_string(new Random().Next(100)), TestUtil.random_BigInt(new Random().Next(10)), 100000, "MyStream", 0);
                
                var kr = aggregator.put(ur);
                Assert.IsNotNull(kr);
                TestUtil.verify_unaggregated(ur, kr);
            }
        }
    }
}
