
/**
 * This class represents a KPL user record.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Google.Protobuf;

namespace KPLNETInterface
{
    public class UserRecord : Amazon.Kinesis.Model.Record
    {
        private static readonly int DIGEST_SIZE = 16;
        private readonly long subSequenceNumber;
        private readonly string explicitHashKey;
        private readonly bool aggregated;

        /**
         * Create a User Record from a Kinesis Record.
         *
         * @param record Kinesis record
         */
        public UserRecord(Amazon.Kinesis.Model.Record record)
            : this(false, record, 0, null)
        {
        }

        /**
         * Create a User Record.
         * 
         * @param aggregated whether the record is aggregated
         * @param record Kinesis record
         * @param subSequenceNumber subsequence number
         * @param explicitHashKey explicit hash key
         */
        protected UserRecord(bool aggregated, Amazon.Kinesis.Model.Record record, long subSequenceNumber, string explicitHashKey)
        {
            if (subSequenceNumber < 0)
                throw new Amazon.Kinesis.Model.InvalidArgumentException("Cannot have an invalid, negative subsequence number");

            this.aggregated = aggregated;
            this.subSequenceNumber = subSequenceNumber;
            this.explicitHashKey = explicitHashKey;

            this.SequenceNumber = record.SequenceNumber;
            this.Data = record.Data;
            this.PartitionKey = record.PartitionKey;
            this.ApproximateArrivalTimestamp = record.ApproximateArrivalTimestamp;
        }

        /**
         * @return subSequenceNumber of this UserRecord.
         */
        public long getSubSequenceNumber()
        {
            return subSequenceNumber;
        }

        /**
         * @return explicitHashKey of this UserRecord.
         */
        public string getExplicitHashKey()
        {
            return explicitHashKey;
        }

        /**
         * @return a boolean indicating whether this UserRecord is aggregated.
         */
        public bool isAggregated()
        {
            return aggregated;
        }

        /**
         * @return the String representation of this UserRecord.
         */
        public override string ToString()
        {
            return "UserRecord [subSequenceNumber=" + subSequenceNumber + ", explicitHashKey=" + explicitHashKey
                    + ", aggregated=" + aggregated + ", getSequenceNumber()=" + SequenceNumber + ", getData()="
                    + Data + ", getPartitionKey()=" + PartitionKey + "]";
        }

        /**
         * {@inheritDoc}
         */
        public override int GetHashCode()
        {
            int prime = 31;
            int result = base.GetHashCode();
            result = prime * result + (aggregated ? 1231 : 1237);
            result = prime * result + ((explicitHashKey == null) ? 0 : explicitHashKey.GetHashCode());
            result = prime * result + (int)(subSequenceNumber ^ (subSequenceNumber >> 32));
            return result;
        }

        /**
         * {@inheritDoc}
         */
        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!base.Equals(obj))
            {
                return false;
            }
            if (GetType() != obj.GetType())
            {
                return false;
            }

            UserRecord other = (UserRecord)obj;
            if (aggregated != other.aggregated)
            {
                return false;
            }
            if (explicitHashKey == null)
            {
                if (other.explicitHashKey != null)
                    return false;
            }
            else if (!explicitHashKey.Equals(other.explicitHashKey))
            {
                return false;
            }
            if (subSequenceNumber != other.subSequenceNumber)
            {
                return false;
            }

            return true;
        }

        /**
         * This method deaggregates the given list of Amazon Kinesis records into a
         * list of KPL user records. This method will then return the resulting list
         * of KPL user records.
         * 
         * @param records  A list of Amazon Kinesis records, each possibly aggregated.
         * @return A resulting list of deaggregated KPL user records.
         */
        public static List<UserRecord> deaggregate(List<Amazon.Kinesis.Model.Record> records, out string errorMesaage)
        {
            errorMesaage = "";
            List<UserRecord> result = new List<UserRecord>();
			byte[] kMagic = null;
			unchecked
			{
				kMagic = new byte[] { (byte)-13, (byte)-119, (byte)-102, (byte)-62 };
			}

            foreach (var record in records)
            {
                bool aggregated = true;
                long subSeqNum = 0;

                MemoryStream bb = record.Data;
                byte[] bytes = new byte[bb.Length];
                bb.Read(bytes, 0, bytes.Length);
                record.Data = new MemoryStream(bytes);//setting data in case of aggregated = false;
				
				if (bytes.Length < kMagic.Length)
					aggregated = false;
				else if (!Utils.AreArrayEqual(kMagic, bytes.Take(kMagic.Length).ToArray()))
						aggregated = false;

                if (bytes.Length < DIGEST_SIZE + kMagic.Length)
                    aggregated = false;

                if (aggregated)
                {
                    byte[] serializedData = bytes.Skip(kMagic.Length).Take(bytes.Length - DIGEST_SIZE - kMagic.Length).ToArray();
					byte[] md5Hash = bytes.Skip(bytes.Length - DIGEST_SIZE).Take(DIGEST_SIZE).ToArray();
					byte[] calcMD5 = Utils.GetMD5(serializedData);
                    if (!Utils.AreArrayEqual(md5Hash, calcMD5))
                        aggregated = false;
                    else
                    {
                        ByteString bytestr = ByteString.CopyFrom(serializedData);
                        try
                        {
                            Aws.Kinesis.Protobuf.AggregatedRecord ar = Aws.Kinesis.Protobuf.AggregatedRecord.Parser.ParseFrom(bytestr);
                            aggregated = true;
                            var pks = ar.PartitionKeyTable;
                            var ehks = ar.ExplicitHashKeyTable;
                            var aat = record.ApproximateArrivalTimestamp;
                            try
                            {
                                int recordsInCurrRecord = 0;
                                for (int i = 0; i < ar.Records.Count; i++)
                                {
                                    Aws.Kinesis.Protobuf.Record mr = ar.Records[i];
                                    string partitionKey = pks[(int)mr.PartitionKeyIndex];
                                    string explicitHashKey = ehks[(int)mr.ExplicitHashKeyIndex];

                                    //long effectiveHashKey = explicitHashKey != "-1" ? int.Parse(explicitHashKey) : new BigInteger(1, md5(partitionKey.getBytes("UTF-8")));
                                    //if (effectiveHashKey < startingHashKey || effectiveHashKey > endingHashKey  )
                                    //{
                                    //    for (int toRemove = 0; toRemove < recordsInCurrRecord; ++toRemove)
                                    //        result.RemoveAt(result.Count - 1);
                                    //    break;
                                    //}

                                    ++recordsInCurrRecord;
                                    Amazon.Kinesis.Model.Record recd = new Amazon.Kinesis.Model.Record()
                                            {
                                                Data = new MemoryStream(mr.Data.ToByteArray()),
                                                PartitionKey = partitionKey,
                                                SequenceNumber = record.SequenceNumber,
                                                ApproximateArrivalTimestamp = aat
                                            };

                                    result.Add(new UserRecord(true, recd, subSeqNum++, explicitHashKey));
                                }
                            }
                            catch (Exception e)
                            {
                                StringBuilder sb = new StringBuilder();
                                sb.Append("Unexpected exception during deaggregation, record was:\n");
                                sb.Append("PKS:\n");

                                foreach (string s in pks)
                                {
                                    sb.Append(s).Append("\n");
                                }

                                sb.Append("EHKS: \n");

                                foreach (string s in ehks)
                                {
                                    sb.Append(s).Append("\n");
                                }

                                foreach (Aws.Kinesis.Protobuf.Record mr in ar.Records)
                                {
                                    sb.Append("Record: [hasEhk=").Append(mr.ExplicitHashKeyIndex >= 0 ? "0" : "1").Append(", ")
                                        .Append("ehkIdx=").Append(mr.ExplicitHashKeyIndex).Append(", ")
                                        .Append("pkIdx=").Append(mr.PartitionKeyIndex).Append(", ")
                                        .Append("dataLen=").Append(mr.Data.ToByteArray().Length).Append("]\n");
                                }
                                sb.Append("Sequence number: ").Append(record.SequenceNumber).Append("\n")
                                    .Append("Raw data: ")
                                    .Append(serializedData).Append("\n");

                                errorMesaage += ("\n" + new Exception(sb.ToString(), e).ToString());

                            }
                        }
                        catch (InvalidProtocolBufferException e)
                        { 
                            aggregated = false; 
                        }
                    }
                }

                if (!aggregated)
                {
                    result.Add(new UserRecord(record));
                }
            }

            return result;
        }
    }
}