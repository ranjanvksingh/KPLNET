using System;
using System.Net;

namespace KPLNET.Core.Clients.KinesisClient
{
    public class AwsKinesisResult
    {
        private string error_;
        private object context_;
        DateTime start_time_;
        DateTime end_time_;
        private string p;
        private HttpStatusCode status_Code_;
        private AwsKinesisResponse response_;        

        public AwsKinesisResult(AwsKinesisResponse response, object context, DateTime start_time, DateTime end_time)
        {
            response_ = response;
            context_ = context;
            start_time_ = start_time;
            end_time_ = end_time;
            status_Code_ = response.PutRecordsResponse != null ? response.PutRecordsResponse.HttpStatusCode : (response.DescribeStreamResponse != null ? response.DescribeStreamResponse.HttpStatusCode : HttpStatusCode.BadRequest);
        }

        public AwsKinesisResult(string error, object context, DateTime start_time, DateTime end_time)
        {
            error_ = error;
            context_ = context;
            start_time_ = start_time;
            end_time_ = end_time;
        }

        public AwsKinesisResult(string error, AwsKinesisResponse response, object context, DateTime start_time, DateTime end_time)
        {
            response_ = response;
            error_ = error;
            context_ = context;
            start_time_ = start_time;
            end_time_ = end_time;
        }

        public bool successful() { return string.IsNullOrEmpty(error_); }

        public string error()
        {
            return error_;
        }

        public AwsKinesisResponse response()
        {
            return response_;
        }

        public HttpStatusCode status_code()
        {
            return response_ != null ? response_.PutRecordsResponse != null ? response_.PutRecordsResponse.HttpStatusCode : response_.DescribeStreamResponse != null ? response_.DescribeStreamResponse.HttpStatusCode : HttpStatusCode.Unused : HttpStatusCode.Unused;
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
    }
}
