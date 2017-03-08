using System;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace KPLNET.Utils
{
    public abstract class ScheduledCallback
    {
        public virtual void Cancel() { throw new NotImplementedException(); }
        public virtual bool Completed() { throw new NotImplementedException(); }
        public virtual void Reschedule(DateTime at) { throw new NotImplementedException(); }
        public virtual DateTime Expiration() { throw new NotImplementedException(); }

        public void reschedule(double from_now)
        {
            Reschedule(DateTime.Now.AddMilliseconds(from_now));
        }

        public double TimeLeft()
        {
            return (Expiration() > DateTime.Now) ? (Expiration() - DateTime.Now).TotalMilliseconds : 0;
        }
    }

    public class TimerScheduledCallback : ScheduledCallback
    {
        public TimerScheduledCallback(Action f, DateTime at)
        {
            f_ = f;
            Reschedule(at);
        }

        public override void Cancel()
        {
            Dispose();
        }

        void Dispose()
        {
            lock (this)
            {
                completed_ = true;
                if (timer_ == null)
                    return;

                timer_.Stop();
                timer_.Close();
                timer_.Dispose();
            }
        }

        public override bool Completed()
        {
            return completed_;
        }

        void CreateTimer(DateTime at)
        {
            lock (this)
            {
                completed_ = false;
                timer_ = new System.Timers.Timer();
                timer_.Interval = at > DateTime.Now ? (at - DateTime.Now).TotalMilliseconds : 1;
                timer_.AutoReset = false;
                timer_.Elapsed += (a,b) => { f_(); };
                timer_.Disposed += Timer__Disposed;
                timer_.Start();
            }
        }

        bool timerAlive = true;

        private void Timer__Disposed(object sender, EventArgs e)
        {
            timerAlive = false;
        }

        public void RescheduleNew(DateTime at)
        {
            completed_ = false;
            if (timerAlive)
            {
                lock (this)
                {
                    timer_.Interval = at > DateTime.Now ? (at - DateTime.Now).TotalMilliseconds : 1;
                    timer_.AutoReset = false;
                    timer_.Enabled = true;
                }
            }
            else
                CreateTimer(at);
        }

        public override void Reschedule(DateTime at)
        {
            Dispose();
            lock (this)
            {
                completed_ = false;
                timer_ = new System.Timers.Timer();
                timer_.Interval = at > DateTime.Now ? (at - DateTime.Now).TotalMilliseconds : 1;
                timer_.Elapsed += (sender, e) => { f_(); };
                timer_.AutoReset = false;
                timer_.Start();
            }
        }

        public override DateTime Expiration()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "timer_.Interval = " + timer_.Interval);
            return DateTime.Now.AddMilliseconds(timer_.Interval);
        }

        private bool completed_;
        private Action f_;
        private System.Timers.Timer timer_ = null;
    }

    public abstract class Executor
    {
        public virtual void Submit(Action f) { throw new NotImplementedException(); }
        public virtual int NumOfThreads() { throw new NotImplementedException(); }
        public virtual void Join() { throw new NotImplementedException(); }
        public virtual ScheduledCallback Schedule(Action f, DateTime at) { throw new NotImplementedException(); }

        public virtual ScheduledCallback Schedule(Action f, double from_now)
        {
            return Schedule(f, DateTime.Now.AddMilliseconds(from_now));
        }
    }

    public class IoServiceExecutor : Executor
    {
        public IoServiceExecutor(long num_threads)
        {
            clean_up_cb_ = new TimerScheduledCallback(() => { clean_up(); }, DateTime.Now.AddSeconds(1));

            for (var i = 0; i < num_threads; i++)
            {
                var t = new Thread(ProcessQueue);
                threads.Add(t.ManagedThreadId, t);
                t.Start();
            }
        }

        private void ProcessQueue()
        {
            while (waitingThreads.WaitOne())
            {
                Action f;
                try
                {
                    if (processQueue.TryDequeue(out f) && f != null)
                    {
                        StdErrorOut.Instance.StdOut(LogLevel.debug, "Executor.ProcessQueue dequeued.");
                        f();
                    }
                }
                catch (Exception e )
                {
                    StdErrorOut.Instance.StdError("ProcessQueue failed", e); 
                }
            }
        }

        public override void Submit(Action f)
        {
            processQueue.Enqueue(f);
            waitingThreads.Release();
        }

        public override ScheduledCallback Schedule(Action f, DateTime at)
        {
            var cb = new TimerScheduledCallback(f, at);
            lock (clean_up_mutex_)
                callbacks_.Add(cb);
            return cb;
        }

        public override int NumOfThreads()
        {
            return threads.Count;
        }

        public override void Join()
        {
            if (!threads.ContainsKey(Thread.CurrentThread.ManagedThreadId))
                threads.Add(Thread.CurrentThread.ManagedThreadId, Thread.CurrentThread);

            ProcessQueue();
        }

        private void clean_up()
        {
            lock (clean_up_mutex_)
            {
                try
                {
                    if (callbacks_.Count > 0)
                    {
                        TimerScheduledCallback[] temp = new TimerScheduledCallback[callbacks_.Count];
                        callbacks_.CopyTo(temp);

                        foreach (var item in temp)
                            callbacks_.Remove(item);
                    }
                }
                catch(Exception e)
                {
                    StdErrorOut.Instance.StdError("Executor.clean_up failed", e);
                }
            }
            clean_up_cb_.Reschedule(DateTime.Now.AddSeconds(1));
        }

        object clean_up_mutex_ = new object();
        ConcurrentQueue<Action> processQueue = new ConcurrentQueue<Action>();
        Dictionary<int, Thread> threads = new Dictionary<int, Thread>();
        List<TimerScheduledCallback> callbacks_ = new List<TimerScheduledCallback>();
        TimerScheduledCallback clean_up_cb_;
        Semaphore waitingThreads = new Semaphore(0, int.MaxValue);
    }
}
