using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Text;
using System.Numerics;

using Aws.Kinesis.Protobuf;

using KPLNET.Kinesis.Core;

namespace KPLNETUnitTests
{
    using FlushCallback = Action<KPLNET.Kinesis.Core.KinesisRecord>;
    class TestUtil
    {

        public static BigInteger get_hash_key(long shard_id)
        {
            Dictionary<long, BigInteger> shard_id_to_hash_key = new Dictionary<long, BigInteger>();
            shard_id_to_hash_key.Add(1, BigInteger.Parse("170141183460469231731687303715884105728"));
            shard_id_to_hash_key.Add(2, BigInteger.Parse("0"));
            shard_id_to_hash_key.Add(3, BigInteger.Parse("85070591730234615865843651857942052863"));
            return shard_id_to_hash_key[shard_id];
        }

        public static void verify_unaggregated(UserRecord ur, KinesisRecord kr)
        {
            var serialized = kr.serialize();
            Assert.AreEqual(ur.Data().ToString(Encoding.Default), serialized);
            Assert.AreEqual(ur.Partition_key(), kr.partition_key());
            if (ur.explicit_hash_key().ToString() != "-1")
            {
                Assert.AreEqual(ur.explicit_hash_key().ToString(), kr.explicit_hash_key());
            }
            else
            {
                Assert.AreEqual(KPLNETInterface.Utils.GetDecimalHashKey(ur.Partition_key()).ToString(), kr.explicit_hash_key());
            }
        }

        public static UserRecord make_user_record(
                        string partition_key = "abcd",
                        string data = "1234",
                        string explicit_hash_key = "",
                        long deadlineMS = 100000,
                        string stream = "myStream",
                        ulong source_id = 0)
        {
            Aws.Kinesis.Protobuf.Message m = new Aws.Kinesis.Protobuf.Message();
            m.Id = source_id;
            m.PutRecord = new Aws.Kinesis.Protobuf.PutRecord();
            m.PutRecord.PartitionKey = partition_key;
            m.PutRecord.StreamName = stream;
            if (!string.IsNullOrEmpty(explicit_hash_key))
            {
                m.PutRecord.ExplicitHashKey = explicit_hash_key;
            }

            m.PutRecord.Data = Google.Protobuf.ByteString.CopyFrom(data, Encoding.Default);
            var r = new UserRecord(m);
            r.set_deadline_from_now(deadlineMS);
            r.set_expiration_from_now(deadlineMS * 2);
            return r;
        }

        public static string random_string(int len)
        {
            Random rnd = new Random();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++)
            {
                sb.Append((char)(33 + (rnd.Next() % 93)));
            }
            return sb.ToString();
        }

        public static string random_BigInt(int len)
        {
            Random rnd = new Random();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++)
            {
                sb.Append((char)(48 + (rnd.Next() % 10)));
            }
            return sb.ToString();
        }

        public static void verify(List<UserRecord> original, KinesisRecord kr)
        {
            AggregatedRecord ar = null;
            verify_format(original, kr, ref ar);
            verify_content(original, ar);
        }

        public static void verify_format(List<UserRecord> original, KinesisRecord kr, ref AggregatedRecord container)
        {
            container = null;
            string serialized = kr.serialize();

            // verify magic number
            string expected_magic = KinesisRecord.kMagic;
            int magic_len = expected_magic.Length;
            string magic = serialized.Substring(0, magic_len);
            Assert.AreEqual(expected_magic, magic);

            // verify protobuf payload
            string payload = serialized.Substring(expected_magic.Length, serialized.Length - 32 - magic_len);
            container = AggregatedRecord.Parser.ParseFrom(Google.Protobuf.ByteString.CopyFrom(payload, Encoding.Default));
            Assert.IsNotNull(container);

            // verify md5 checksum
            Assert.AreEqual(KPLNETInterface.Utils.CreateMD5(payload), serialized.Substring(serialized.Length - 32, 32));

            // verify the explicit hash key set on the Kinesis record
            List<string> acceptable_hash_keys = new List<string>();
            foreach (var ur in original)
            {
                if (ur.explicit_hash_key() > -1)
                {
                    acceptable_hash_keys.Add(ur.explicit_hash_key().ToString());
                }
                else
                {
                    acceptable_hash_keys.Add(KPLNETInterface.Utils.GetDecimalHashKey(ur.Partition_key()).ToString());
                }
            }

            Assert.IsTrue(acceptable_hash_keys.Exists((i) => i == kr.explicit_hash_key()));
        }

        public static void verify_content(List<UserRecord> original, AggregatedRecord result)
        {
            // verify record count
            Assert.AreEqual(original.Count, result.Records.Count);

            for (int i = 0; i < result.Records.Count; i++)
            {
                var r = result.Records[i];
                // verify partition key
                Assert.AreEqual(original[i].Partition_key(), result.PartitionKeyTable[(int)r.PartitionKeyIndex]);

                // verify explicit hash key
                if (original[i].explicit_hash_key().ToString() != "-1")
                {
                    Assert.AreEqual(original[i].explicit_hash_key().ToString(), result.ExplicitHashKeyTable[(int)r.ExplicitHashKeyIndex]);
                }

                // verify data
                Assert.AreEqual(original[i].Data(), r.Data);
            }
        }
    }
}
