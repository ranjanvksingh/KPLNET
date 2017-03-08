using System;
using System.Collections.Generic;

namespace KPLNET.Metrics
{
    using Accumulator = AccumulatorImpl<double, double, int>;

    //ToDo: Find a statistical library for C# to implement Accumulator min, max, sum, mean, count values
    public class AccumulatorImpl<ValType, BucketSize, NumBuckets>
    {
        private DateTime start_time;
        public AccumulatorImpl()
        {
            this.start_time = DateTime.Now;
        }

        public void put(ValType val)
        {
        }

        public ValType count(UInt64 buckets = UInt64.MaxValue)
        {
            return default(ValType); 
        }

        public ValType mean(UInt64 buckets = UInt64.MaxValue)
        {
        return default(ValType); 
        }

        public ValType min(UInt64 buckets = UInt64.MaxValue)
        {
            return default(ValType); 
        }

        public ValType max(UInt64 buckets = UInt64.MaxValue)
        {
            return default(ValType); 
        }

        public ValType sum(UInt64 buckets = UInt64.MaxValue)
        {
            return default(ValType); 
        }

        public DateTime Start_time()
        {
            return start_time;
        }

        public UInt64 elapsedSeconds() 
        {
          return 0;
        }

        public ValType Get<Stat>(UInt64 buckets)
        {
            if (buckets != UInt64.MaxValue)
                return default(ValType); 
            else
                return default(ValType); 
        }

    }

    public class ConcurrentAccumulator<AccumType>
    {
        public void GetAccessor<V>(V val)
        {
        }
    };

    public class AccumulatorList<ValType, AccumType, BucketSize, NumBuckets>
    {
        public ValType Get<Stat>(int buckets)
        {
            if (buckets == 0)
                return default(ValType);

            return default(ValType);
        }

        private List<KeyValuePair<DateTime, ConcurrentAccumulator<AccumType>>> accums = null;
    }
}
