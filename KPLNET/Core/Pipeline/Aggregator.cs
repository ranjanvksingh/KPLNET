using KPLNET.Metrics;
using KPLNET.Utils;
using System;
using System.Collections.Concurrent;

using KPLNETInterface;

namespace KPLNET.Kinesis.Core
{
    using System.Numerics;
    using DeadlineCallback = Action<KinesisRecord>;
    using ReducerMap = ConcurrentDictionary<long, Reducer<UserRecord, KinesisRecord>>;
    public class Aggregator
    {
        private Executor executor_;
        private ShardMap shard_map_;
        private DeadlineCallback deadline_callback_;
        private KPLNETConfiguration config_;
        private MetricsManager metrics_manager_;
        private ReducerMap reducers_;

        public Aggregator
          (
            Executor executor,
            ShardMap shard_map,
            Action<KinesisRecord> deadline_callback,
            KPLNETConfiguration config,
            MetricsManager metrics_manager = null
          )
        {
            executor_ = executor;
            shard_map_ = shard_map;
            deadline_callback_ = deadline_callback;
            config_ = config;
            metrics_manager_ = metrics_manager ?? new NullMetricsManager();
            reducers_ = new ReducerMap();
        }

        public KinesisRecord put(UserRecord ur)
        {
            // If shard map is not available, or aggregation is disabled, just send the record by itself, and do not attempt to aggrgegate.
            long shard_id = -1;
            BigInteger hk = 0;
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Aggregator.put -> Called"); 
            if (config_.aggregationEnabled && shard_map_ != null)
            {
                hk = ur.Hash_key();
                shard_id = shard_map_.Shard_id(hk);

                StdErrorOut.Instance.StdOut(LogLevel.debug, "hk = " + hk + "    shard_id= " + shard_id);
            }
            if (-1 == shard_id)
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "if (-1 == shard_id)");
                var kr = new KinesisRecord();
                kr.add(ur);
                return kr;
            }
            else
            {
                ur.Predicted_shard(shard_id);
                return GetReducer(shard_id).add(ur) as KinesisRecord;
            }
        }

        Reducer<UserRecord, KinesisRecord> GetReducer(long shardId)
        {
            Reducer<UserRecord, KinesisRecord> retVal = null;
            if (!reducers_.ContainsKey(shardId))
                    reducers_[shardId] = create_reducer();

            return reducers_[shardId];
        }

        public void flush()
        {
            foreach (var r in reducers_)
                    r.Value.Flush();
        }

        private Reducer<UserRecord, KinesisRecord> create_reducer()
        {
            return new Reducer<UserRecord, KinesisRecord>(executor_, deadline_callback_, config_.aggregationMaxSize, config_.aggregationMaxCount);
        }
    }
}
