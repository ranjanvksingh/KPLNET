using System;
using System.Text;
using System.Threading;
using System.IO.Pipes;
using System.Collections.Concurrent;

using Aws.Kinesis.Protobuf;
using Google.Protobuf;

using KPLNET.Utils;
using KPLNETInterface;

namespace KPLNET.Kinesis.Core
{
    using IpcMessageQueue = ConcurrentQueue<Aws.Kinesis.Protobuf.Message>;
    public class IpcChannel
    {
        private string in_file_;
        private string out_file_;
        private NamedPipeClientStream inPipeClientStream;
        private NamedPipeClientStream outPipeClientStream;
        private StreamBuffer inSB;
        private StreamBuffer outSB;

        public IpcChannel(string in_file, string out_file)
        {
            in_file_ = in_file;
            out_file_ = out_file;
        }

        public byte[] read()
        {
            if (inPipeClientStream == null)
                return null;

            //return new StreamBuffer(inPipeClientStream).ReadBuffer();
            if (inSB == null)
                inSB = new StreamBuffer(inPipeClientStream);

            return inSB.ReadBuffer();
        }

        public long write(byte[] buf)
        {
            if (outPipeClientStream == null)
                return -1;

            if (!outPipeClientStream.IsConnected)
                outPipeClientStream.Connect();

            StdErrorOut.Instance.StdOut(LogLevel.debug, "IpcManager.write");
            //return new StreamBuffer(outPipeClientStream).WriteBuffer(buf);

            if (outSB == null)
                outSB = new StreamBuffer(outPipeClientStream);

            return outSB.WriteBuffer(buf);
        }

        public bool open_read_channel()
        {
            if (null == in_file_)
                return false;

            inPipeClientStream = new NamedPipeClientStream(".", in_file_, PipeDirection.InOut, PipeOptions.Asynchronous | PipeOptions.WriteThrough);
            
            if (!inPipeClientStream.IsConnected)
                inPipeClientStream.Connect();

            return true;
        }

        public bool open_write_channel()
        {
            if (null == out_file_)
                return false;

            outPipeClientStream = new NamedPipeClientStream(".", out_file_, PipeDirection.InOut, PipeOptions.Asynchronous | PipeOptions.WriteThrough);

            if (!outPipeClientStream.IsConnected)
                outPipeClientStream.Connect();

            return true;
        }

        public void close_read_channel()
        {
            if (inPipeClientStream != null)
            {
                inPipeClientStream.Close();
                inPipeClientStream = null;
            }
        }

        public void close_write_channel()
        {
            if (outPipeClientStream != null)
            {
                outPipeClientStream.Close();
                outPipeClientStream = null;
            }
        }

    }

    public class IpcWorker
    {
        protected const int kMaxMessageSize = 2 * 1024 * 1024;
        protected const int kBufferSize = 2 * kMaxMessageSize;
        protected IpcChannel channel_;
        protected IpcMessageQueue queue_;
        protected bool shutdown_;

        public IpcWorker(IpcChannel channel, IpcMessageQueue queue)
        {
            channel_ = channel;
            queue_ = queue;
            shutdown_ = false;
        }

        public virtual void Shutdown()
        {
            shutdown_ = true;
        }

        public virtual void start() { }

    }

    public class IpcReader : IpcWorker
    {
        public IpcReader(IpcChannel channel, IpcMessageQueue queue) : base(channel, queue) { }

        public override void start()
        {
            if (channel_.open_read_channel())
            {
                while (!shutdown_)
                {
                    try
                    {
                        Byte[] bytes = channel_.read();
                        var bs = ByteString.CopyFrom(bytes);
                        if (bs != null)
                        {
                            queue_.Enqueue(Message.Parser.ParseFrom(bs));
                        }
                        else
                        {
                            Thread.Sleep(5); 
                        }
                    }
                    catch (Exception e)
                    {
                        StdErrorOut.Instance.StdError("IpcReader.Start failed with error: " + e.ToString());
                    }
                }
            }
        }
    }

    public class IpcWriter : IpcWorker
    {
        public IpcWriter(IpcChannel channel, IpcMessageQueue queue) : base(channel, queue) { }

        public override void start()
        {
            if (channel_.open_write_channel())
            {
                Message m = null;
                while (!shutdown_)
                {
                    try
                    {
                        if (!queue_.TryDequeue(out m))
                        {
                            Thread.Sleep(5);  
                            continue;
                        }

                        channel_.write(m.ToByteArray());
                    }
                    catch (Exception e)
                    {
                        StdErrorOut.Instance.StdError("IpcWriter.Start failed with error: " + e.ToString());
                    }
                }
            }
        }
    }

    public class IpcManager : IMessageManager
    {
        private IpcMessageQueue in_queue_;
        private IpcMessageQueue out_queue_;
        private IoServiceExecutor executor_;
        private IpcChannel channel_;
        private IpcWorker reader_;
        private IpcWorker writer_;
        private const int kMaxMessageSize = 2 * 1024 * 1024;

        public IpcManager(IpcChannel channel)
        {
            in_queue_ = new IpcMessageQueue();
            out_queue_ = new IpcMessageQueue();
            executor_ = new IoServiceExecutor(2);
            channel_ = channel;
            reader_ = new IpcReader(channel_, in_queue_);
            writer_ = new IpcWriter(channel_, out_queue_);
            executor_.Submit(() => { try { reader_.start(); } catch (Exception e) { StdErrorOut.Instance.StdError("IpcManager.reader_.start(); failed.", e); } });
            executor_.Submit(() => { try { writer_.start(); } catch (Exception e) { StdErrorOut.Instance.StdError("IpcManager.writer_.start(); failed.", e); } });
        }

        public void put(Message data)
        {            
            StdErrorOut.Instance.StdOut(LogLevel.debug, "IpcManager.put");
            int size = data.CalculateSize();
            StdErrorOut.Instance.StdOut(LogLevel.debug, "IpcManager.put after data.CalculateSize");
            if (size > kMaxMessageSize)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("Outgoing message too large, was ").Append(size).Append(" bytes, max allowed is ").Append(kMaxMessageSize).Append(" bytes");
                StdErrorOut.Instance.StdOut(LogLevel.debug,sb.ToString());
                throw new Exception(sb.ToString());
            }

            StdErrorOut.Instance.StdOut(LogLevel.debug, "before out_queue_.Enqueue(data)");
            out_queue_.Enqueue(data);
            StdErrorOut.Instance.StdOut(LogLevel.debug, "after out_queue_.Enqueue(data)");
        }

        public bool try_take(out Message dest)
        {
            return in_queue_.TryDequeue(out dest);
        }

        public void shutdown()
        {
            reader_.Shutdown();
            writer_.Shutdown();
        }

        public void close_write_channel()
        {
            channel_.close_write_channel();
        }

        public void close_read_channel()
        {
            channel_.close_read_channel();
        }
    }
}
