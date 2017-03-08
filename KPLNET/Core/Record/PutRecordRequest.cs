using System;
using System.Text;

using KPLNET.Utils;

namespace KPLNET.Kinesis.Core
{
    public class PutRecordsRequest : SerializableContainer<KinesisRecord>
    {
        private ulong total_size;
        public PutRecordsRequest()
        {
            total_size = 0;
        }

        public override ulong accurate_size() { return total_size; }

        public override string serialize()
        {
            if (items.Count == 0)
                throw new Exception("Cannot serialize an empty PutRecordsRequest");

            var sb = new StringBuilder();
            // The data will expand due to Base64 and json overhead
            sb.Append("{\"StreamName\":\"")
            .Append(stream())
            .Append("\",\"Records\":[");
            foreach (var kr in items)
            {
                sb.Append("{\"Data\":\"")
                .Append(Convert.ToBase64String(Encoding.UTF8.GetBytes(kr.serialize())))
                .Append("\",\"PartitionKey\":\"")
                .Append(kr.partition_key())
                .Append("\",\"ExplicitHashKey\":\"")
                .Append(kr.explicit_hash_key())
                .Append( "\"},");
            }
            sb.Append(']')
            .Append('}');

            StdErrorOut.Instance.StdOut(LogLevel.debug, " prr.serialize() s = " + sb.ToString());
            return sb.ToString();
        }

        public string stream()
        {
            if (items.Count == 0)
                throw new Exception("Cannot get stream name of an empty PutRecordsRequest");

            return items[0].Items()[0].Stream();
        }

        protected override void after_add(KinesisRecord ur)
        {
            // This is how the backend counts towards the current 5MB limit
            total_size += (ulong)ur.partition_key().Length + ur.accurate_size();
        }

        protected override void after_remove(KinesisRecord ur)
        {
            total_size -= (ulong)ur.partition_key().Length + ur.accurate_size();
        }

        protected override void after_clear()
        {
            total_size = 0;
        }
    }
}
