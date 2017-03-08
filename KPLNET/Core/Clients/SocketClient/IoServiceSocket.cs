using System;
using System.Text;
using System.Net.Sockets;
using System.Net.Security;

using KPLNET.Utils;

namespace KPLNET.Http
{
    using ConnectCallback = Action<bool, string>;
    using WriteCallback = Action<bool, string>;
    using ReadCallback = Action<int, string>;
    
    using System.IO;
    using System.Threading;
    public interface ISocket
    {
        void open(ConnectCallback cb, double timeoutInMS);
        void write(string data, int len, WriteCallback cb, double timeoutInMS);
        void read(byte[] dest, int max_len, ReadCallback cb, double timeoutInMS);
        bool good();
        void close();
        void reset();
    }

    public interface ISocketFactory
    {
        ISocket create(string endpoint, int port, bool secure, bool verify_cert);
    }

    public class IoServiceSocketFactory : ISocketFactory
    {
        public ISocket create(string endpoint, int port, bool secure, bool verify_cert)
        {
            return new IoServiceSocket(endpoint, port, secure, verify_cert);
        }
    }

    public class IoServiceSocket : ISocket
    {
        SslStream ssl_socket_;
        TcpClient tcp_socket_;
        ConnectCallback connect_cb_;
        DateTime last_use_;
        string endpoint_;
        int port_;
        bool secure_;
        bool verify_cert_;
        static double kIdleTimeoutSeconds = 8;
        NetworkStream netStream_;
        
        //boost::asio::steady_timer timer_;
        //boost::asio::ssl::context ssl_ctx_;
        //std::shared_ptr<boost::asio::ip::tcp::resolver> resolver_;
        
        public IoServiceSocket(string endpoint, int port, bool secure, bool verify_cert)
        {
            //ssl_ctx_(boost::asio::ssl::context::tlsv1_client),

            endpoint_ = endpoint;
            port_ = port;
            secure_ = secure;
            verify_cert_ = verify_cert;
        }

        public void open(ConnectCallback cb, double timeout)
        {
            connect_cb_ = cb;
            
            //if (secure_) 
            //{
            //    ssl_ctx_.add_certificate_authority( boost::asio::buffer(std::string(aws::auth::VeriSignClass3PublicPrimaryG5)));
            //    ssl_ctx_.add_certificate_authority( boost::asio::buffer(std::string(aws::auth::VeriSignClass3PublicPrimary)));
            //    ssl_socket_ = std::make_shared<boost::asio::ssl::stream<TcpSocket>>(*io_service_,ssl_ctx_);
            //    if (verify_cert_) 
            //    {
            //    ssl_socket_->set_verify_mode(boost::asio::ssl::verify_peer);
            //    ssl_socket_->set_verify_callback(boost::asio::ssl::rfc2818_verification(endpoint_));
            //    }
            //}
            //else
            //{
            //    tcp_socket_ = new TcpClient(endpoint_, port_);
            //}

            tcp_socket_ = new TcpClient();
            var result = tcp_socket_.BeginConnect(endpoint_, port_, null, null);
            last_use_ = DateTime.Now;
            var success = result.AsyncWaitHandle.WaitOne(TimeSpan.FromMilliseconds(timeout));
            connect_cb_(success, !success ? "" : "Failed to connect.");

            //    resolver_ = std::make_shared<boost::asio::ip::tcp::resolver>(*io_service_);
            //    boost::asio::ip::tcp::resolver::query query(endpoint_, std::to_string(port_));
            //    resolver_->async_resolve(query, [this](auto ec, auto ep) {this->on_resolve(ec, ep);});           
            
        }

        public void write(string data, int len, WriteCallback cb, double timeout)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write");
            UpdateStream();
            StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write after UpdateStream");
            
            //if (ssl_socket_ != null) 
            //{
            //  write_impl(ssl_socket_, data, len, cb, timeout);
            //} 
            //else 

            if (netStream_ != null)
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write before write_implTcpSocket(data, len, cb, timeout);");
                write_implTcpSocket(data, len, cb, timeout);
            } 
            else 
            {
                throw new Exception("Socket not initialized");
            }

            last_use_ = DateTime.Now;
        }

        private void UpdateStream()
        {
            if (netStream_ == null)
                netStream_ = tcp_socket_.GetStream();
        }

        public void read(byte[] dest, int max_len, ReadCallback cb, double timeout)
        {
            UpdateStream();

            //if (secure_) 
            //{
            //    read_impl(ssl_socket_, dest, max_len, cb, timeout);
            //} 
            //else 

            if (netStream_ != null) 
            {
                new Thread(() => read_implTcpSocket(dest, max_len, cb, timeout)).Start();
            } 
            else 
            {
                throw new Exception("Socket stream not connected.");
            }
        }


        private void write_implTcpSocket(string data, int len, WriteCallback cb, double timeout)
        {
            bool success = false;
            if (netStream_.CanWrite)
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write_implTcpSocket after netStream_.CanWrite"); 
                var cts = new CancellationTokenSource();
                var token = cts.Token;
                StdErrorOut.Instance.StdOut(LogLevel.debug, "data = " + data);
                var task = netStream_.WriteAsync(Encoding.Default.GetBytes(data), 0, len, token);
                
                success = task.Wait((int)timeout, token);
                StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write_implTcpSocket after success = task.Wait((int)timeout, token)");
                if (!task.IsCompleted && token.CanBeCanceled)
                {
                    StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write_implTcpSocket before cts.Cancel()");
                    cts.Cancel();
                }
            }
            if (!success)
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write_implTcpSocket after if (!success)");
                close();
                StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.write_implTcpSocket after close()");
            }

            cb(success, !success ? "" : "Failed to connect.");
        }

        private void read_implTcpSocket(byte[] dest, int max_len, ReadCallback cb, double timeout)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket");
            bool success = true;
            int lengthread = 0;
            if (netStream_.CanRead)
            {
                DateTime dtNow = DateTime.Now;
                byte[] data = new byte[1024];
                using (MemoryStream ms = new MemoryStream())
                {

                    int numBytesRead;
                    StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket before while ((numBytesRead = netStream_.Read(data, 0, data.Length)) > 0)");
                    while ((numBytesRead = netStream_.Read(data, 0, data.Length)) > 0)
                    {
                        StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket before ms.Write(data, 0, numBytesRead);");
                        ms.Write(data, 0, numBytesRead);
                        if (DateTime.Now > dtNow.AddMilliseconds(1000 * 60 * 1))
                        {
                            StdErrorOut.Instance.StdOut(LogLevel.debug, "Cant wait for more than a minute.");
                            success = false;
                            break;
                        }
                    }
                    if (success)
                    {
                        StdErrorOut.Instance.StdOut(LogLevel.debug, "Finished reading all data.");
                        lengthread = (int)ms.Length;
                        StdErrorOut.Instance.StdOut(LogLevel.debug, "Finished reading all data. lengthread = " + lengthread);
                        ms.ToArray().CopyTo(dest, 0);
                        StdErrorOut.Instance.StdOut(LogLevel.debug, "Finished reading all data. ms.ToArray().CopyTo(dest, 0); done.");
                    }
                }
                
                //StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket after if (netStream_.CanRead)");
                //var cts = new CancellationTokenSource();
                //var token = cts.Token;
                //StdErrorOut.Instance.StdOut(LogLevel.debug,DateTime.Now + " calling netStream_.ReadAsync with timeout = " + timeout);
                //var task = netStream_.ReadAsync(dest, 0, max_len, token);
                //last_use_ = DateTime.Now;
                //StdErrorOut.Instance.StdOut(LogLevel.debug,DateTime.Now + " calling  task.Wait((int)timeout, token) before Thread.Sleep(25000)");
                //Thread.Sleep(25000);
                //StdErrorOut.Instance.StdOut(LogLevel.debug,DateTime.Now + " calling  task.Wait((int)timeout, token) after Thread.Sleep(25000)");
                //success = task.Wait((int)timeout, token);
                //StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket after success = task.Wait((int)timeout, token);");
                //if (!task.IsCompleted && token.CanBeCanceled)
                //{
                //    StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket after if (!task.IsCompleted && token.CanBeCanceled)");
                //    cts.Cancel();
                //    StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket after cts.Cancel()");
                //}

                if (success)
                {
                    StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket before cb(lengthread, )");
                    cb(lengthread, "");
                }
            }
            if (!success)
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket after if (!success)");
                close();
                StdErrorOut.Instance.StdOut(LogLevel.debug, "IoSocket.read_implTcpSocket after close();");
                cb(-1, "Failed to read data from the socket server.");
            }
        }

        public void reset()
        {
            connect_cb_ = null; // We need to explicitly reset because there is a circular reference between s and this callback.
        }

        public bool good()
        {
            bool is_open = false;
            //if (secure_) 
            //{
            //    //is_open = ssl_socket_.lowest_layer().is_open();
            //} 
            //else 
            if (tcp_socket_ != null) 
            {
                is_open = tcp_socket_.Connected;
            }

            bool idle_timed_out_ = (DateTime.Now - last_use_).TotalSeconds > kIdleTimeoutSeconds;
            return is_open & !idle_timed_out_;
        }

        public void close()
        {
            try 
            {
                //if (secure_) 
                //{
                //ssl_socket_->lowest_layer().close();
                //} 
                //else 
                
                if (tcp_socket_ != null) 
                {
                    netStream_.Close();
                    netStream_ = null;
                    tcp_socket_.Close();
                }
            }
            catch (Exception ex) {}
        }

        //private void on_resolve(string ec, string endpoint_it)
        //{
        //    if (ec) 
        //    {
        //        on_connect_finish(ec);
        //        return;
        //    }

        //    if (secure_) 
        //    {
        //        boost::asio::async_connect
        //        (
        //            ssl_socket_->lowest_layer(),
        //            endpoint_it,
        //            [this](autoec, auto ep) 
        //            { 
        //                this->on_ssl_connect(ec); 
        //            }
        //        );
        //    } 
        //    else 
        //    {
        //        boost::asio::async_connect
        //        (
        //            *tcp_socket_,
        //            endpoint_it,
        //            [this](autoec, auto ep) 
        //            { 
        //                this->on_connect_finish(ec); 
        //            }
        //        );
        //    }
        //}

        //private void on_ssl_connect(string ec)
        //{
        //    if (ec) 
        //    {
        //        on_connect_finish(ec);
        //        return;
        //    }

        //    ssl_socket_->async_handshake
        //        (
        //            boost::asio::ssl::stream_base::client,
        //            [this](autoec) 
        //                { 
        //                    this->on_connect_finish(ec); 
        //                }
        //        );
        //}

        //private void on_connect_finish(string ec)
        //{
        //    timer_.cancel();
        //    if (ec)
        //    {
        //        connect_cb_(false, ec.message());
        //    }
        //    else
        //    {
        //        connect_cb_(true, "");
        //    }
        //}
    }
}
