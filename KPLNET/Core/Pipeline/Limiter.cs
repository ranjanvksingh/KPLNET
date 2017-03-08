using System;
using System.Collections.Generic;
using System.Collections.Concurrent;

using KPLNET.Utils;
using KPLNETInterface;

namespace KPLNET.Kinesis.Core
{
    using Callback = Action<KinesisRecord>;
    using System.Threading;

    public class Limiter
    {
        private Executor executor_;
        private Callback callback_;
        private Callback expired_callback_;
        private ConcurrentDictionary<long, ShardLimiter> limiters_;
        private ScheduledCallback scheduled_poll_;
        private static int kDrainDelayMillis = 25;
        private KPLNETConfiguration config_;

        public Limiter(Executor executor,
                 Callback callback,
                 Callback expired_callback,
                 KPLNETConfiguration config)
        {
            executor_ = executor;
            callback_ = callback;
            config_ = config;
            expired_callback_ = expired_callback;
            limiters_ = new ConcurrentDictionary<long, ShardLimiter>();
            poll();
        }

        ShardLimiter GetShardLimiter(long shardId)
        {
            if (!limiters_.ContainsKey(shardId))
                limiters_[shardId] = new ShardLimiter((double)config_.rateLimit / 100.0);

            return limiters_[shardId];
        }

        public void add_error(string code, string msg)
        {
            // TODO react to throttling errors
        }

        public void put(KinesisRecord kr)
        {
            var shard_id = kr.Items()[kr.Items().Count - 1].Predicted_shard();
            // Limiter doesn't work if we don't know which shard the record is going to
            if (shard_id == -1)
                callback_(kr);
            else
                GetShardLimiter(shard_id).put(kr, callback_, expired_callback_);
        }

        private void poll()
        {
            foreach (var kv in limiters_)
                kv.Value.drain(callback_, expired_callback_);

            var delay = kDrainDelayMillis;
            if (null == scheduled_poll_)
                scheduled_poll_ = executor_.Schedule(() => { poll(); }, delay);
            else
                scheduled_poll_.reschedule(delay);
        }
    }

    // The limiter in its current form is not going to eliminate throttling errors,
    // but it will put a cap on how many can occur per unit time. The number of
    // throttling errors will scale linearly with the number of producer instances
    // if every instance is putting as fast as it can and there isn't enough
    // capacity in the stream to absorb the traffic.
    public class ShardLimiter
    {
        // The bucket and internal queue are synchronized with the draining_ flag, only one thread can be performing drain at a time.
        private static int draining_ = 0;//ATOMIC_FLAG_INIT;
        private TokenBucket token_bucket_ = new TokenBucket();
        private TimeSensitiveQueue<KinesisRecord> internal_queue_ = new TimeSensitiveQueue<KinesisRecord>();
        private ConcurrentQueue<KinesisRecord> queue_ = new ConcurrentQueue<KinesisRecord>();
        private const ulong kRecordsPerSecLimit = 1000; // Each shard supports up to 1,000 records per second for writes
        private const ulong kBytesPerSecLimit = 1024 * 1024; //Each shard supports up to a maximum total data write rate of 1 MB per second

        public ShardLimiter(double token_growth_multiplier = 1.0)
        {
            token_bucket_.add_token_stream(kRecordsPerSecLimit, token_growth_multiplier * kRecordsPerSecLimit);
            token_bucket_.add_token_stream(kBytesPerSecLimit, token_growth_multiplier * kBytesPerSecLimit);
        }

        public void put(KinesisRecord incoming, Callback callback, Callback expired_callback)
        {
            queue_.Enqueue(incoming);
            drain(callback, expired_callback);
        }

        public void drain(Callback callback, Callback expired_callback)
        {
            if (0 == Interlocked.Exchange(ref draining_, 1))
            {
                try
                {
                    KinesisRecord kr;
                    while (queue_.TryDequeue(out kr))
                        internal_queue_.insert(kr);

                    internal_queue_.consume_expired(expired_callback);

                    internal_queue_.consume_by_deadline
                        (
                            (r) =>
                            {
                                double bytes = r.accurate_size();
                                if (token_bucket_.try_take(new List<double> { 1, bytes }))
                                {
                                    callback(r);
                                    return true;
                                }
                                return false;
                            }
                        );
                }
                catch (Exception ex)
                {
                    StdErrorOut.Instance.StdError("Shard Limiter drain failed", ex);
                }
                finally
                {
                    //Release the lock
                    Interlocked.Exchange(ref draining_, 0);
                }
            }
        }
    }
}
