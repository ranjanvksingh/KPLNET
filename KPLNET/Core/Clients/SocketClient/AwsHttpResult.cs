using System;

using System.Collections.Generic;

namespace KPLNET.Http
{
    public class AwsHttpResult
    {
        private string error_;
        private AwsHttpResponse response_;
        private object context_;
        DateTime start_time_;
        DateTime end_time_;
        private string p;

        public AwsHttpResult(AwsHttpResponse response, object context, DateTime start_time, DateTime end_time)
        {
            response_ = response;
            context_ = context;
            start_time_ = start_time;
            end_time_ = end_time;
        }

        public AwsHttpResult(string error, object context, DateTime start_time, DateTime end_time)
        {
            error_ = error;
            context_ = context;
            start_time_ = start_time;
            end_time_ = end_time;
        }

        public AwsHttpResult(string p)
        {
            // TODO: Complete member initialization
            this.p = p;
        }

        public bool successful() { return string.IsNullOrEmpty(error_); }

        public string error()
        {
            return error_;
        }

        public string response_body()
        {
            return response_.data();
        }

        public int status_code()
        {
            return (response_ != null ) ? response_.Status_code() : -1;
        }

        public T context<T>()
        {
            return (T)context_;
        }

        public DateTime start_time()
        {
            return start_time_;
        }

        public DateTime end_time()
        {
            return end_time_;
        }

        public ulong duration_millis()
        {
            return (ulong)(end_time() - start_time()).TotalMilliseconds;
        }

        ulong duration_micros()
        {
            return 1000 * duration_millis();
        }

        List<KeyValuePair<string, string>> headers()
        {
            return new List<KeyValuePair<string, string>>();//response_->headers();
        }

        //operator bool() 
        //{
        //  return successful();
        //}
    }
}
