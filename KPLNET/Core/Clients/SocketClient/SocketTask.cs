using System;

using KPLNET.Utils;

namespace KPLNET.Http
{
    using ResponseCallback = Action<AwsHttpResult>;
    using SocketReturn = Action<ISocket>;
    class SocketTask : TimeSensitive
    {
        private Executor executor_;
        private string raw_request_;
        private ISocket socket_;
        private object context_;
        private ResponseCallback response_cb_;
        private SocketReturn socketReturn_;
        private int timeout_;
        private AwsHttpResponse response_;
        private DateTime end_deadline_;
        private DateTime start_;
        private bool finished_;
        private bool submitted_ = false;
        private byte[] buffer_ = new byte[64 * 1024];
        private bool started_ = false;

        public SocketTask(Executor executor,
           AwsHttpRequest request,
           object context,
            ResponseCallback response_cb,
            SocketReturn socketReturn,
           DateTime deadline,
           DateTime expiration,
          int timeout)
            : base(deadline, expiration)
        {
            executor_ = executor;
            raw_request_ = request.to_string();
            context_ = context;
            response_cb_ = response_cb;
            socketReturn_ = socketReturn;
            timeout_ = timeout;
            response_ = new AwsHttpResponse();
            finished_ = false;
        }

        public void run(ISocket socket)
        {
            if (!started_)
            {
                started_ = true;
                socket_ = socket;
                start_ = DateTime.Now;
                end_deadline_ = start_.AddMilliseconds(timeout_);
                write();
            }
            else
            {
                throw new Exception("The same task cannot be run more than once");
            }
        }

        void write()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Socket write task.write()");
            socket_.write
                (
                    raw_request_,
                    raw_request_.Length,
                    (success, reason) =>
                    {
                        StdErrorOut.Instance.StdOut(LogLevel.debug, "Socket write task.write() -> (success,  reason) = " + success);
                        if (success)
                            read();
                        else
                            fail(reason);
                    },
                    timeout_
                );
        }

        void read()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Socket write task.read() ");

            socket_.read
                      (
                        buffer_,
                        buffer_.Length,
                        (num_read, reason) =>
                        {
                            StdErrorOut.Instance.StdOut(LogLevel.debug, "Socket write task.read() (num_read,  reason) = " + num_read);
                            if (num_read < 0)
                            {
                                fail(reason);
                                return;
                            }

                            response_.update(buffer_, num_read);

                            if (response_.complete())
                                succeed();
                            else
                                read();
                        },
                        (end_deadline_ - DateTime.Now).TotalMilliseconds
                    );
        }

        public void fail(string reason)
        {
            finish
              (
                new AwsHttpResult(reason, context_, start_, DateTime.Now),
                true
              );
        }

        void succeed()
        {
            bool close_socket = false;
            foreach (var h in response_.headers())
            {
                if (h.Key.ToLower() == "connection" && h.Value.ToLower() == "close")
                {
                    close_socket = true;
                    break;
                }
            }

            finish
              (
                new AwsHttpResult(response_, context_, start_, DateTime.Now),
                close_socket
              );
        }

        void finish(AwsHttpResult result, bool close_socket = false)
        {
            started_ = true;
            if (!submitted_)
            {
                submitted_ = true;
                executor_.Submit
                    (
                        () =>
                        {
                            try
                            {
                                response_cb_(result);
                                if (socket_ != null && socket_.good())
                                {
                                    if (close_socket)
                                    {
                                        socket_.close();
                                    }
                                    socketReturn_(socket_);
                                }
                                finished_ = true;
                            }
                            catch(Exception e)
                            {
                                StdErrorOut.Instance.StdError("Socket Task response_cb_ failed.", e);
                            }
                        }
                    );
            }
        }

        public bool finished()
        {
            return finished_;
        }
    }
}
