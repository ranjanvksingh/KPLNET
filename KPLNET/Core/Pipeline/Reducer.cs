using System;
using System.Collections.Generic;

using KPLNET.Utils;

namespace KPLNET.Kinesis.Core
{
    public class Reducer<T, U> where T : TimeSensitive
                                where U : SerializableContainer<T>, new()
    {
        protected Executor executor;
        protected Action<U> flush_callback;
        protected ulong size_limit;
        protected ulong count_limit;
        protected Func<T, bool> flush_predicate;
        protected SerializableContainer<T> container;
        protected ScheduledCallback scheduled_callback;
        protected object mutex = new object();

        public Reducer(
             Executor executor,
             Action<U> flush_callback,
             ulong size_limit,
             ulong count_limit,
             Func<T, bool> flush_predicate = null)
        {
            this.executor = executor;
            this.flush_callback = flush_callback;
            this.size_limit = size_limit;
            this.count_limit = count_limit;
            this.flush_predicate = flush_predicate ?? ((a) => { return false; });
            this.container = new U();
            this.scheduled_callback = executor.Schedule(() => { deadline_reached(); }, DateTime.Now.AddDays(1));
        }

        //Put a record. If this triggers a flush, an instance of SerializableContainer<T> will be returned, otherwise null will be returned.
        public SerializableContainer<T> add(T input)
        {
            lock(mutex)
                container.add(input);

            var size = container.Size();
            var estSize = container.Estimated_size();

            if (size >= count_limit || (estSize + 30) >= size_limit || flush_predicate(input))
            {
                var output = flush(mutex);
                if (null != output && output.Size() > 0)
                    return output;
            }

            set_deadline();
            return null;
        }

        // Manually trigger a flush, as though a deadline has been reached
        public void Flush()
        {
            deadline_reached();
        }

        // Records in the process of being flushed won't be counted
        public ulong size()
        {
            return container.Size();
        }

        DateTime Deadline()
        {
            if (!scheduled_callback.Completed())
                return scheduled_callback.Expiration();
            else
                return DateTime.Now.AddDays(30);
        }

        U flush(object mutex)
        {

            List<T> records = null;
            lock (mutex)
            {
                scheduled_callback.Cancel();
                records = container.Items();
                container = new U();
            }

            records.Sort((a, b) => { if(a == null || b == null) return -1; return a.Deadline() < b.Deadline() ? -1 : a.Deadline() > b.Deadline() ? 1 : 0; });
            var flush_container = new U();

            foreach (var r in records)
                flush_container.add(r);

            records.Clear();

            while (((ulong)flush_container.Size() > count_limit || flush_container.accurate_size() + 30 > size_limit) && flush_container.Size() > 1)
                records.Add(flush_container.remove_last());

            lock (mutex)
            {
                foreach (var r in records)
                    container.add(r);

                set_deadline();
            }

            return flush_container;
        }

        void set_deadline()
        {
            if (container.empty())
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "Reducer.set_deadline -> if (container.empty())");
                scheduled_callback.Cancel();
                return;
            }

            if (scheduled_callback.Completed() || container.Deadline() < scheduled_callback.Expiration())
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, string.Format("if (scheduled_callback.Completed() [= {0}]|| container.Deadline() [{1}] < scheduled_callback.Expiration() [= {2}])", scheduled_callback.Completed(), container.Deadline(), scheduled_callback.Expiration()));
                scheduled_callback.Reschedule(container.Deadline());
            }
        }

        void deadline_reached()
        {
            var r = flush(mutex);
            
            if (null != r && r.Size() > 0)
                flush_callback(r);
        }
    }
}
