using System;
using System.Collections.Generic;
using System.Linq;


namespace KPLNET.Utils
{
    public class TimeSensitive
    {
        public TimeSensitive()
        {
            arrival = DateTime.Now;
            expiration = DateTime.Now.AddDays(10);
        }

        public TimeSensitive(DateTime deadline, DateTime expiration)
        {
            arrival = DateTime.Now;
            this.expiration = expiration;
            set_deadline(deadline);
        }

        public DateTime Arrival() { return arrival; }

        public DateTime Deadline() { return deadline; }

        public DateTime Expiration() { return expiration; }

        public void set_deadline(DateTime tp)
        {
            deadline = tp;
            if (expiration > epochTime && expiration < deadline)
                deadline = expiration;
        }

        public void set_expiration(DateTime tp)
        {
            expiration = tp;
        }

        public void set_deadline_from_now(long ms)
        {
            set_deadline(DateTime.Now.AddMilliseconds(ms));
        }

        public void set_expiration_from_now(long ms)
        {
            expiration = DateTime.Now.AddMilliseconds(ms);
        }

        public bool expired()
        {
            return DateTime.Now > expiration;
        }

        public void inherit_deadline_and_expiration(TimeSensitive other)
        {
            if (other == null)
                return;

            if (this.Deadline() == epochTime || other.Deadline() < this.Deadline())
                set_deadline(other.Deadline());

            if (Expiration() == epochTime || other.Expiration() < Expiration())
                set_expiration(other.Expiration());
        }

        private DateTime epochTime = new DateTime(1, 1, 1);
        private DateTime arrival;
        private DateTime deadline;
        private DateTime expiration;
    }
    
    public class MultiIndexList<T> where T : TimeSensitive
    {
        SortedDictionary<DateTime, List<T>> expirationSortedDictionary = new SortedDictionary<DateTime, List<T>>();
        SortedDictionary<DateTime, List<T>> deadlineSortedDictionary = new SortedDictionary<DateTime, List<T>>();

        int size = 0;

        public DateTime[] ExpirationQueue { get { DateTime[] retVal = new DateTime[expirationSortedDictionary.Keys.Count]; expirationSortedDictionary.Keys.CopyTo(retVal, 0); return retVal; } }
        public DateTime[] DeadlineQueue { get { DateTime[] retVal = new DateTime[deadlineSortedDictionary.Keys.Count]; deadlineSortedDictionary.Keys.CopyTo(retVal, 0); return retVal; } }

        public T GetFirstInDeadlineQueuue()
        {
            if (expirationSortedDictionary.Keys.Count > 0)
                return expirationSortedDictionary[expirationSortedDictionary.Keys.First()][0];
            return null;
        }
        public void AddItem(T item)
        {
            lock (expirationSortedDictionary)
            {
                if (!expirationSortedDictionary.ContainsKey(item.Expiration()))
                    expirationSortedDictionary[item.Expiration()] = new List<T> { item };
                else
                    expirationSortedDictionary[item.Expiration()].Add(item);
            }

            lock (deadlineSortedDictionary)
            {
                if (!deadlineSortedDictionary.ContainsKey(item.Deadline()))
                    deadlineSortedDictionary[item.Deadline()] = new List<T> { item };
                else
                    deadlineSortedDictionary[item.Deadline()].Add(item);
            }
            size++;
        }

        public List<T> GetExpirationQueueMember(DateTime dt)
        {
            if (expirationSortedDictionary.ContainsKey(dt))
                return expirationSortedDictionary[dt];

            return null;
        }

        public List<T> GetDeadlineQueueMember(DateTime dt)
        {
            if (deadlineSortedDictionary.ContainsKey(dt))
                return deadlineSortedDictionary[dt];

            return null;
        }

        public void RemoveItem(T item)
        {
            lock (expirationSortedDictionary)
            {
                if (expirationSortedDictionary.ContainsKey(item.Expiration()))
                {
                    expirationSortedDictionary[item.Expiration()].Remove(item);
                    if (expirationSortedDictionary[item.Expiration()].Count == 0)
                        expirationSortedDictionary.Remove(item.Expiration());
                }
            }

            lock (deadlineSortedDictionary)
            {
                if (deadlineSortedDictionary.ContainsKey(item.Deadline()))
                {
                    deadlineSortedDictionary[item.Deadline()].Remove(item);
                    if (deadlineSortedDictionary[item.Deadline()].Count == 0)
                        deadlineSortedDictionary.Remove(item.Deadline());
                }
            }
            size--;
        }

        public int Size()
        {
            return size;
        }

    }

    public class TimeSensitiveQueue<T> where T : TimeSensitive
    {
        MultiIndexList<T> container = new MultiIndexList<T>();

        public void consume_expired(Action<T> f)
        {
            var idx = container.ExpirationQueue;
            for (int it = 0; it < idx.Length && Consume((p) =>
            {
                if (p.expired())
                {
                    StdErrorOut.Instance.StdOut(LogLevel.debug, string.Format("WhyExpired? Expiration = {0}, Deadline = {1}, Arrival = {2}", p.Expiration(), p.Deadline(), p.Arrival()));
                    f(p);
                    return true;
                }
                return false;
            }, container.GetExpirationQueueMember(idx[it])); it++) { }
        }

        public bool Try_Dequeue_From_deadline_Queue(out T retVal)
        {
            retVal = null;
            try
            {
                retVal  = container.GetFirstInDeadlineQueuue();
                if (retVal != null)
                {
                    container.RemoveItem(retVal);
                    return true;
                }
                return false;
            }
            catch (Exception) { return false; }

        }

        public void consume_by_deadline(Func<T, bool> f)
        {
            var idx = container.DeadlineQueue;
            for (int it = 0; it < idx.Length && Consume((p) =>
            {
                return f(p);
            }, container.GetDeadlineQueueMember(idx[it])); it++) { }
        }

        public bool Consume(Func<T, bool> f, List<T> items)
        {
            var copied = items.ToArray();
            foreach (var item in copied)
                if (!f(item))
                {
                    return false;
                }
                else
                {
                    container.RemoveItem(item);
                }

            return true;
        }

        public void insert(T item)
        {
            container.AddItem(item);
        }

        public int size()
        {
            return container.Size();
        }
    }
}
