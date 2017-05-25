using System;
using System.Collections.Generic;
using System.Numerics;

using Aws.Kinesis.Protobuf;
using Google.Protobuf;

using KPLNET.Utils;


namespace KPLNET.Kinesis.Core
{
    public class UserRecord : TimeSensitive
    {
        private UInt64 source_id;
        private string stream;
        private string partition_key;
        private BigInteger hash_key;
        private ByteString data;
        private List<Attempt> attempts;
        private long predicted_shard = -1;
        private bool has_explicit_hash_key;
        private bool finished;

        public UserRecord(Message m)
        {
            attempts = new List<Attempt>();
            finished = false;
            if (m.PutRecord == null)
                throw new Exception("Message is not a PutRecord");

            source_id = m.Id;
            var put_record = m.PutRecord;
            stream = put_record.StreamName;
            partition_key = put_record.PartitionKey;
            data = put_record.Data;
            has_explicit_hash_key = !string.IsNullOrEmpty(put_record.ExplicitHashKey);

            if (has_explicit_hash_key)
                hash_key = BigInteger.Parse(put_record.ExplicitHashKey);
            else
                hash_key = KPLNETInterface.Utils.GetDecimalHashKey(partition_key);

            StdErrorOut.Instance.StdOut(LogLevel.debug, "hash_key = " + hash_key + " partition_key = " + partition_key);
        }


        public Message to_put_record_result()
        {
            Message m = new Message();
            m.SourceId = source_id;
            m.Id = (ulong)new KPLNETInterface.RandomGenerator().GetNextInt64(0, long.MaxValue);

            var prr = new PutRecordResult();
            m.PutRecordResult = prr;
            try
            {
                prr.Success = false;
            }
            catch (Exception ex) { StdErrorOut.Instance.StdOut(LogLevel.debug,ex.ToString()); throw ex; }
            
            for (int i = 0; i < attempts.Count; i++)
            {
                var a = new Aws.Kinesis.Protobuf.Attempt();
                prr.Attempts.Add(a);
                var delay = (i == 0) ? attempts[i].Start() - Arrival() : attempts[i].Start() - attempts[i - 1].End();
                a.Delay = (uint)delay.TotalMilliseconds;
                a.Duration = (uint)attempts[i].duration();
                a.Success = attempts[i].Success();

                StdErrorOut.Instance.StdOut(LogLevel.debug, "UserRecord.to_put_record_result a.Success = " + a.Success);
                if (a.Success)
                {
                    prr.ShardId = attempts[i].Shard_id();
                    prr.SequenceNumber = attempts[i].Sequence_number();
                    prr.Success = true;
                }
                else
                {
                    a.ErrorCode = attempts[i].Error_code();
                    a.ErrorMessage = attempts[i].Error_message();
                }
            }

            finished = true;
            return m;
        }

        public void add_attempt(Attempt a)
        {
            attempts.Add(a);
        }

        public ulong Source_id()
        {
            return source_id;
        }

        public string Stream()
        {
            return stream;
        }

        public string Partition_key()
        {
            return partition_key;
        }

        public BigInteger Hash_key()
        {
            return hash_key;
        }

        public ByteString Data()
        {
            return data;
        }

        public List<Attempt> Attempts()
        {
            return attempts;
        }

        public bool Finished()
        {
            return finished;
        }

        public BigInteger Hash_key_decimal_str()
        {
            return hash_key;
        }

        public BigInteger explicit_hash_key()
        {
            if (has_explicit_hash_key)
                return Hash_key_decimal_str();
            else
                return -1;
        }

        public void Predicted_shard(long sid)
        {
            predicted_shard = sid;
        }

        public long Predicted_shard()
        {
            return predicted_shard;
        }
    }

    public class Attempt
    {

        private DateTime start;
        private DateTime end;
        private bool error;
        private string error_code;
        private string error_message;
        private string sequence_number;
        private string shard_id;

        public Attempt set_start(DateTime tp)
        {
            start = tp;
            return this;
        }

        public Attempt set_end(DateTime tp)
        {
            end = tp;
            return this;
        }

        public DateTime Start()
        {
            return start;
        }

        public DateTime End()
        {
            return end;
        }

        public double duration()
        {
            return (end - start).TotalMilliseconds;
        }

        public Attempt set_error(string code, string msg)
        {
            error = true;
            error_code = code;
            error_message = msg;
            return this;
        }

        public Attempt set_result(string shard_id, string sequence_number)
        {
            error = false;
            this.shard_id = shard_id;
            this.sequence_number = sequence_number;
            return this;
        }

        public string Shard_id()
        {
            return shard_id;
        }

        public string Sequence_number()
        {
            return sequence_number;
        }

        public string Error_code()
        {
            return error_code;
        }

        public string Error_message()
        {
            return error_message;
        }

        public bool Success()
        {
            return !error;
        }
    }
}
