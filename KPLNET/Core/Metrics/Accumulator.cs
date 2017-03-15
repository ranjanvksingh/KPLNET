using System;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace KPLNET.Metrics
{
	public class AccumulatorImpl
	{
		enum Tag { Min, Max, Sum, Mean, Count }

		class MetricAccumulatorSet
		{
			object mutex_ = new object();
			double Sum, Max, Min, Count;

			public double GetStat(Tag tag)
			{
				switch (tag)
				{
					case Tag.Count:
						return Count;
					case Tag.Max:
						return Max;
					case Tag.Min:
						return Min;
					case Tag.Sum:
						return Sum;
					case Tag.Mean:
						return Sum / Count;
					default:
						return 0;
				}
			}

			public void SetStat(Tag tag, double val)
			{
				lock (mutex_)
				{
					switch (tag)
					{
						case Tag.Count:
							Count = Count + 1;
							break;

						case Tag.Sum:
							Sum = Sum + val;
							break;

						case Tag.Max:
							if (Max < val)
								Max = val;
							break;

						case Tag.Min:
							if (Min > val)
								Min = val;
							break;
					}
				}
			}

			public void SetStat(double val)
			{
				lock (mutex_)
				{
					Count = Count + 1;
					Sum = Sum + val;

					if (Max < val)
						Max = val;

					if (Min > val)
						Min = val;

				}
			}
		}

		class ConcurrentAccumulator
		{
			public double GetStat(Tag tag)
			{
				lock (mutex_)
					return accum_.GetStat(tag);
			}

			public void SetStat(double val)
			{
				lock (mutex_)
					accum_.SetStat(val);
			}

			object mutex_ = new object();
			MetricAccumulatorSet accum_ = new MetricAccumulatorSet();
		};

		class AccumulatorList
		{
			public double Get(Tag tag, int buckets)
			{
				if (buckets == 0)
					return 0;

				DateTime cutoff = DateTime.Now.AddSeconds(-1 * buckets);

				lock (mutex_)
				{
					if (tag == Tag.Count || tag == Tag.Sum)
						return sum(tag, cutoff);
					else if (tag == Tag.Mean)
						return sum(Tag.Sum, cutoff) / sum(Tag.Count, cutoff);
					else
						return combine(tag, cutoff).GetStat(tag);
				}
			}

			public void Set(double val)
			{
				DateTime d = DateTime.Now;
				DateTime dts = new DateTime(d.Year, d.Month, d.Day, d.Hour, d.Minute, d.Second);

				lock (mutex_)
					if (accums_.Count == 0 || !accums_.ContainsKey(dts))
						accums_[dts] = new ConcurrentAccumulator();

				accums_[dts].SetStat(val);
			}

			double sum(Tag tag, DateTime cutoff)
			{
				return combine(tag, cutoff).GetStat(tag);
			}

			double buckets_between(DateTime a, DateTime b)
			{
				var d = (b > a) ? (b - a) : (a - b);
				return d.TotalSeconds;
			}

			MetricAccumulatorSet combine(Tag tag, DateTime cutoff)
			{
				MetricAccumulatorSet a = new MetricAccumulatorSet();
				foreach (var p in accums_)
				{
					if (p.Key > cutoff)
						a.SetStat(tag, p.Value.GetStat(tag));
				}
				return a;
			}

			object mutex_ = new object();
			ConcurrentDictionary<DateTime, ConcurrentAccumulator> accums_ = new ConcurrentDictionary<DateTime, ConcurrentAccumulator>();
		}

		AccumulatorList accums_ = new AccumulatorList();
		ConcurrentAccumulator overall_ = new ConcurrentAccumulator();		
		DateTime start_time;
        public AccumulatorImpl()
        {
            start_time = DateTime.Now;
        }

        public void put(double val)
		{
			accums_.Set(val);
			overall_.SetStat(val);
		}

        public double count(int buckets = int.MaxValue)
		{
			return Get(Tag.Count, buckets);
        }

        public double mean(int buckets = int.MaxValue)
		{
			return Get(Tag.Mean, buckets);
        }

        public double min(int buckets = int.MaxValue)
		{
			return Get(Tag.Min, buckets);
		}

        public double max(int buckets = int.MaxValue)
		{
			return Get(Tag.Max, buckets);
		}

        public double sum(int buckets = int.MaxValue)
		{
			return Get(Tag.Sum, buckets);
		}

        public DateTime Start_time()
        {
            return start_time;
        }

        public double elapsedSeconds() 
        {
			return (DateTime.Now - start_time).TotalSeconds;
        }

        double Get(Tag tag, int buckets)
        {
            if (buckets != int.MaxValue)
                return accums_.Get(tag, buckets); 
            else
                return overall_.GetStat(tag); 
        }

	}
}
