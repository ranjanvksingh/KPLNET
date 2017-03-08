using System;

using KPLNET.Metrics;
using KPLNET.Utils;
using System.Collections.Concurrent;

using KPLNETInterface;

namespace KPLNET.Kinesis.Core
{
    using FlushCallback = Action<PutRecordsRequest>;
    public class Collector
    {
        private FlushCallback flush_callback_;
        private MetricsManager metrics_manager_;
        private Reducer<KinesisRecord, PutRecordsRequest> reducer_;
        private ConcurrentDictionary<long, ulong> buffered_data_;

        public Collector(Executor executor, FlushCallback flush_callback, KPLNETConfiguration config, MetricsManager metrics_manager = null)
        {
            flush_callback_ = flush_callback;
            reducer_ = new Reducer<KinesisRecord, PutRecordsRequest>(executor, (prr) => handle_flush(prr), config.collectionMaxSize, config.collectionMaxCount, (kr) => { return should_flush(kr); });
            buffered_data_ = new ConcurrentDictionary<long, ulong>();
            metrics_manager_ = metrics_manager ?? new NullMetricsManager();
        }

        public PutRecordsRequest put(KinesisRecord kr)
        {
            var prr = reducer_.add(kr) as PutRecordsRequest;
            decrease_buffered_data(prr);
            return prr;
        }

        public void flush()
        {
            reducer_.Flush();
        }

        // We don't want any individual shard to accumulate too much data
        // because that makes traffic to that shard bursty, and might cause
        // throttling, so we flush whenever a shard reaches a certain limit.
        private bool should_flush(KinesisRecord kr)
        {
            var shard_id = kr.Items()[0].Predicted_shard();
            if (shard_id != 0)
            {
                var d = (buffered_data_[shard_id] += kr.accurate_size());
                if (d >= 256 * 1024)
                    return true;
            }
            return false;
        }

        private void decrease_buffered_data(PutRecordsRequest prr)
        {
            if (prr == null)
                return;

            foreach (var kr in prr.Items())
            {
                var shard_id = kr.Items()[0].Predicted_shard();
                if (shard_id != 0)
                    buffered_data_[shard_id] -= kr.accurate_size();
            }
        }

        private void handle_flush(PutRecordsRequest prr)
        {
            if (prr == null)
                return;
            
            decrease_buffered_data(prr);
            flush_callback_(prr);
        }
    }
}
