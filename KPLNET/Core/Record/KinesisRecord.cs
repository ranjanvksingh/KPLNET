using System;
using System.Text;
using System.Collections.Generic;

using Aws.Kinesis.Protobuf;
using Google.Protobuf;

using KPLNET.Utils;
using KPLNETInterface;

namespace KPLNET.Kinesis.Core
{
    class KeySet
    {
        public KeyValuePair<bool, uint> Add(string s)
        {
            lock (lookup)
            {
                try
                {
                    if (lookup.ContainsKey(s))
                    {
                        counts[s]++;
                        return new KeyValuePair<bool, uint>(false, lookup[s]);
                    }
                    else
                    {
                        uint lk = (uint)keys.Count;
                        lookup.Add(s, lk);
                        counts.Add(s, 1);
                        keys.Add(s);
                        return new KeyValuePair<bool, uint>(true, lk);
                    }
                }
                catch(Exception ex)
                {
                    return new KeyValuePair<bool, uint>(false, 0);
                }
            }
        }

        public KeySet()
        {
            keys = new List<string>();
            lookup = new Dictionary<string, uint>();
            counts = new Dictionary<string, uint>();
        }

        public bool Empty()
        {
            return keys.Count == 0;
        }

        public void Clear()
        {
            keys.Clear();
            lookup.Clear();
            counts.Clear();
        }

        public KeyValuePair<bool, uint> Remove_one(string s)
        {
            var cnt = counts[s];
            if (cnt > 1)
            {
                counts[s] = cnt - 1;
                return new KeyValuePair<bool, uint>(false, 0);
            }
            else
            {
                counts.Remove(s);
                var idx = lookup[s];
                keys.RemoveAt((int)idx);
                lookup.Remove(s);
                foreach (KeyValuePair<string, uint> p in lookup)
                    if (p.Value > idx)
                        lookup[p.Key] -= 1;

                return new KeyValuePair<bool, uint>(true, idx);
            }
        }

        public string First()
        {
            return keys[0];
        }

        private List<string> keys;
        private Dictionary<string, uint> lookup;
        private Dictionary<string, uint> counts;
    }

    public class KinesisRecord : SerializableContainer<UserRecord>
    {
        private static int kFixedOverhead = 4 + 16;
        private AggregatedRecord aggregated_record;
        private KeySet explicit_hash_keys;
        private KeySet partition_keys;
        private int estimated_size;
        private int cached_accurate_size;
        private bool cached_accurate_size_valid;
        public byte[] kMagic = null;

        public KinesisRecord()
        {
			unchecked
			{
				kMagic = new byte[] { (byte)-13, (byte)-119, (byte)-102, (byte)-62 };
			}
            estimated_size = 0;
            cached_accurate_size = 0;
            cached_accurate_size_valid = false;
            explicit_hash_keys = new KeySet();
            partition_keys = new KeySet();
            aggregated_record = new AggregatedRecord();
        }

        public override ulong accurate_size()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "KinesisRecord.accurate_size");
            if (items.Count == 0)
            {
                return 0;
            }
            else if (items.Count == 1)
            {
                return (ulong)items[0].Data().Length;
            }
            else
            {
                if (!cached_accurate_size_valid)
                {
                    cached_accurate_size_valid = true;
                    cached_accurate_size = kFixedOverhead + aggregated_record.CalculateSize();
                }
                return (ulong)cached_accurate_size;
            }
        }

        public override ulong Estimated_size()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "KinesisRecord.Estimated_size");
            if (items.Count < 2)
                return accurate_size();
            else
                return (ulong)estimated_size;
        }

        public override string serialize()
        {
            if (items.Count == 0)
                throw new Exception("Cannot serialize empty container");

            if (items.Count == 1)
            {
                return items[0].Data().ToString(Encoding.Default);
            }

            string s = aggregated_record.SerializeAsString();

            return new StringBuilder().Append(new string(new char[4] { '\xF3', '\x89', '\x9A', '\xC2' })).Append(s).Append(KPLNETInterface.Utils.CreateMD5(s)).ToString();
        }

		public byte[] SerializedAggregatedRecord 
		{ 
			get 
			{ 
				byte[] s = aggregated_record.ToByteArray();
				byte[] md5 = KPLNETInterface.Utils.GetMD5(s);
				return KPLNETInterface.Utils.Combine(KPLNETInterface.Utils.Combine(kMagic, s), md5);
			}
		}

		public bool is_aggregated { get { return items.Count > 1; } }

        public string partition_key()
        {
            if (items.Count == 0)
                throw new Exception("Cannot compute partition_key for empty container");

            if (items.Count == 1)
                return items[0].Partition_key();

            // We will always set an explicit hash key if we created an aggregated record.
            // We therefore have no need to set a partition key since the records within
            // the container have their own parition keys anyway. We will therefore use a
            // single byte to save space.
            return "a";
        }

        public string explicit_hash_key()
        {
            if (items.Count == 0)
                throw new Exception("Cannot compute explicit_hash_key for empty container");

            return items[0].Hash_key_decimal_str().ToString();
        }

        protected override void after_clear()
        {
            cached_accurate_size_valid = false;
            explicit_hash_keys.Clear();
            partition_keys.Clear();
            aggregated_record.Records.Clear();
            aggregated_record.ExplicitHashKeyTable.Clear();
            aggregated_record.PartitionKeyTable.Clear();
            estimated_size = 0;
        }

        protected override void after_add(UserRecord ur)
        {
            if (ur == null)
                return;

            cached_accurate_size_valid = false;
            var new_record = new Record();
            aggregated_record.Records.Add(new_record);
            new_record.Data = ur.Data();
            estimated_size += ur.Data().Length + 3;

			//PKIndex = 0 is causing issue for deaggregator so making sure PKIndex starts with 1
			if (aggregated_record.PartitionKeyTable.Count == 0)
			{
				var pk = " ";
				var add_result = partition_keys.Add(pk);
				if (add_result.Key)
				{
					aggregated_record.PartitionKeyTable.Add(pk);
					estimated_size += pk.Length + 3;
				}
				estimated_size += 2;
			}

			{
                var pk = ur.Partition_key();
                var add_result = partition_keys.Add(pk);
                if (add_result.Key)
                {
                    aggregated_record.PartitionKeyTable.Add(pk);
                    estimated_size += pk.Length + 3;
                }
                new_record.PartitionKeyIndex = add_result.Value;
                estimated_size += 2;
            }

			//EHKIndex = 0 is causing issue for deaggregator so making sure EHKIndex starts with 1
			if (aggregated_record.ExplicitHashKeyTable.Count == 0)
			{
				var fehk = " ";
				var add_result = explicit_hash_keys.Add(fehk.ToString());
				if (add_result.Key)
				{
					aggregated_record.ExplicitHashKeyTable.Add(fehk.ToString());
					estimated_size += fehk.ToString().Length + 3;
				}
				estimated_size += 2;
			}

			var ehk = ur.explicit_hash_key();
            if (ehk != null && ehk != -1)
            {
                var add_result = explicit_hash_keys.Add(ehk.ToString());
                if (add_result.Key)
                {
                    aggregated_record.ExplicitHashKeyTable.Add(ehk.ToString());
                    estimated_size += ehk.ToString().Length + 3;
                }
                new_record.ExplicitHashKeyIndex = add_result.Value;
                estimated_size += 2;
            }
        }

        protected override void after_remove(UserRecord ur)
        {
            cached_accurate_size_valid = false;
            aggregated_record.Records.RemoveAt(aggregated_record.Records.Count - 1);
            estimated_size -= (ur.Data().Length + 3);

            {
                var pk_rm_result = partition_keys.Remove_one(ur.Partition_key());
                if (pk_rm_result.Key)
                {
                    aggregated_record.PartitionKeyTable.RemoveAt(aggregated_record.PartitionKeyTable.Count - 1);
                    for (int i = 0; i < aggregated_record.Records.Count; i++)
                    {
                        var r = aggregated_record.Records[i];
                        if (r.PartitionKeyIndex > pk_rm_result.Value)
                        {
                            r.PartitionKeyIndex = (r.PartitionKeyIndex - 1);
                        }
                    }
                    estimated_size -= (ur.Partition_key().Length + 3);
                }
                estimated_size -= 2;
            }

            var ehk = ur.explicit_hash_key();
            if (ehk != null && ehk != -1)
            {
                var ehk_rm_result = explicit_hash_keys.Remove_one(ehk.ToString());
                if (ehk_rm_result.Key)
                {
                    aggregated_record.ExplicitHashKeyTable.RemoveAt(aggregated_record.ExplicitHashKeyTable.Count - 1);
                    for (int i = 0; i < aggregated_record.Records.Count; i++)
                    {
                        var r = aggregated_record.Records[i];
                        if (r.ExplicitHashKeyIndex > ehk_rm_result.Value)
                        {
                            r.ExplicitHashKeyIndex = r.ExplicitHashKeyIndex - 1;
                        }
                    }
                    estimated_size -= (ehk.ToString().Length + 3);
                }
                estimated_size -= 2;
            }
        }
    }
}
