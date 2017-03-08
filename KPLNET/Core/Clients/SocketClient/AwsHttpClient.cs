using System;
using System.Collections.Generic;

using KPLNET.Utils;

namespace KPLNET.Http
{
    using ResponseCallback = Action<AwsHttpResult>;
    using SocketReturn = Action<ISocket>;
    public class AwsHttpClient
    {
        private Utils.Executor executor_;
        private string endpoint;
        private int port;
        private bool verifyCertificate;
        private bool secure;
        private ulong minConnections;
        private ulong maxConnections;
        private ulong connectTimeout;
        private ulong requestTimeout;
        private ScheduledCallback scheduled_callback;
        private List<SocketTask> tasks_in_progress_ = new List<SocketTask>();
        private List<ISocket> available_ = new List<ISocket>();
        private int pending_;
        private TimeSensitiveQueue<SocketTask> task_queue_ = new TimeSensitiveQueue<SocketTask>();
        private bool single_use_sockets_ = false;
        private ISocketFactory socketFactory_;
        object mutex_ = new object();

        public AwsHttpClient(Utils.Executor executor, ISocketFactory socketFactory, string endpoint, int port, bool verifyCertificate, bool secure, ulong minConnections = 1, ulong maxConnections = 6, ulong connectTimeout = 10000, ulong requestTimeout = 10000, bool single_use_sockets = false)
        {
            int poll_delay = 3000;
            this.executor_ = executor;
            this.socketFactory_ = socketFactory;
            this.endpoint = endpoint;
            this.port = port;
            this.verifyCertificate = verifyCertificate;
            this.secure = secure;
            this.minConnections = minConnections;
            this.maxConnections = maxConnections;
            this.connectTimeout = connectTimeout;
            this.requestTimeout = requestTimeout;
            this.scheduled_callback = executor_.Schedule
                        (
                            () =>
                            {
                                run_tasks();
                                lock (mutex_)
                                    scheduled_callback.reschedule(poll_delay);
                            },
                            poll_delay
                        );
        }

        public void put(AwsHttpRequest request, ResponseCallback cb, object context, DateTime deadline, DateTime expiration)
        {
            lock (mutex_)
            {
                task_queue_.insert(new SocketTask(
                                            executor_,
                                            request,
                                            context,
                                            cb,
                                            (socket) => { reuse_socket(socket); },
                                            deadline,
                                            expiration,
                                            (int)requestTimeout));
            }
            run_tasks();
            StdErrorOut.Instance.StdOut(LogLevel.debug, "before run_tasks");
        }

        void open_connection()
        {
            pending_++;
            var s = socketFactory_.create(endpoint, port, secure, verifyCertificate);
            StdErrorOut.Instance.StdOut(LogLevel.debug, "AwsHttpClient.open_connection");
            s.open
              (
                (success, reason) =>
                {
                    StdErrorOut.Instance.StdOut(LogLevel.debug, "(success,  reason) = " + success);
                    pending_--;
                    if (success)
                    {
                        lock (mutex_)
                            available_.Add(s);

                        StdErrorOut.Instance.StdOut(LogLevel.debug, "success -> available_.Add(s) ->");
                        run_tasks();
                    }
                    else if (endpoint != "169.254.169.254")
                        StdErrorOut.Instance.StdError(string.Format("Failed to open connection to {0}:{1} : {2}", endpoint, port, reason));

                    // We need to explicitly reset because there is a circular reference between s and this callback.
                    s.reset();
                },
                connectTimeout
              );
        }

        void open_connections(int n)
        {
            for (int i = 0; i < n && total_connections() < (int)maxConnections; i++)
                open_connection();
        }

        void reuse_socket(ISocket socket)
        {
            lock (mutex_)
            {
                pending_--;
                if (!single_use_sockets_ && socket.good())
                    available_.Add(socket);
                else
                {
                    socket.close();
                    if (total_connections() < (int)minConnections)
                        open_connections((int)minConnections - total_connections());
                }
            }
            run_tasks();
        }

        void run_tasks()
        {
            lock (mutex_)
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "before run_tasks");
                while (tasks_in_progress_.Count != 0 && tasks_in_progress_[0].finished())
                    tasks_in_progress_.RemoveAt(0);
                while (0 != available_.Count && !available_[0].good())
                    available_.RemoveAt(0);

                task_queue_.consume_expired
                  (
                      (t) =>
                      {
                          tasks_in_progress_.Add(t);
                          t.fail("Expired while waiting in HttpClient queue");
                      }
                  );

                task_queue_.consume_by_deadline
                (
                  (t) =>
                  {
                      if (available_.Count == 0)
                          return false;

                      pending_++;
                      var socket = available_[0];
                      available_.RemoveAt(0);
                      tasks_in_progress_.Add(t);
                      t.run(socket);
                      return true;
                  }
                );

                if (task_queue_.size() > total_connections())
                {
                    open_connections(1);
                }
            }
        }

        int available_connections()
        {
            return available_.Count;
        }

        int total_connections()
        {
            return available_.Count + pending_;
        }
    }
}
