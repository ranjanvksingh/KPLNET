using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using KPLNET.Utils;

namespace KPLNET.Kinesis.Core
{
    public class SerializableContainer<T> : TimeSensitive where T : TimeSensitive
    {
        protected virtual void after_add(T new_item) { }
        protected virtual void after_remove(T item) { }
        protected virtual void after_clear() { }

        protected List<T> items = new List<T>();
        protected Stack<KeyValuePair<DateTime, DateTime>> deadlines_and_expirations = new Stack<KeyValuePair<DateTime,DateTime>>();

        public virtual void add(T new_item)
        {
            if (new_item == null)
                return;

            deadlines_and_expirations.Push(new KeyValuePair<DateTime, DateTime>(Deadline(), Expiration()));
            inherit_deadline_and_expiration(new_item);
            items.Add(new_item);
            after_add(new_item);
        }

        public virtual T remove_last()
        {
            if (items.Count == 0)
                return default(T);

            lock (deadlines_and_expirations)
            {
                if (deadlines_and_expirations.Count > 0)
                {
                    var prev_times = deadlines_and_expirations.Peek();
                    set_deadline(prev_times.Key);
                    set_expiration(prev_times.Value);
                    deadlines_and_expirations.Pop();
                }
            }

            var i = items[items.Count - 1];
            items.RemoveAt(items.Count - 1);
            after_remove(i);
            return i;
        }

        public virtual ulong Estimated_size() { return accurate_size(); }

        public virtual ulong accurate_size() { throw new NotImplementedException(); }

        public virtual void clear()
        {
            set_deadline_from_now(-1);
            set_expiration_from_now(-1);
            while (deadlines_and_expirations.Count != 0)
                deadlines_and_expirations.Pop();

            items.Clear();
            after_clear();
        }

        public virtual string serialize() { throw new NotImplementedException(); }

        public List<T> Items() { return items; }

        public ulong Size() { return (ulong)items.Count; }

        public bool empty() { return items.Count == 0; }


    }
}
