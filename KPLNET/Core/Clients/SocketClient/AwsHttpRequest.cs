using System;
using System.Web;

using HttpMachine;

namespace KPLNET.Http
{
    public class AwsHttpRequest : AwsHttpMessage, IHttpRequestParserDelegate
    {
        private string preamble_;
        private string method_;
        private string path_;
        private string http_version_;

        public AwsHttpRequest() : base(false, http_parser_type.HTTP_REQUEST)
        {
        }

        public HttpRequest GetRequest()
        {
            return null;
        }

        public AwsHttpRequest(string method, string path, string http_version = "HTTP/1.1")
            : base(false, http_parser_type.HTTP_REQUEST)
        {
            method_ = method.ToUpper();
            path_ = path;
            http_version_ = http_version;
        }

        public string to_string()
        {
            generate_preamble();
            return preamble_ + data_;
        }

        public string Method() { return method_; }

        public string Path() { return path_; }

        protected void generate_preamble()
        {
            string ss = method_ + " " + path_ + " " + http_version_ + "\r\n";
            foreach (var h in headers_)
                ss += h.Key + ":" + h.Value + "\r\n";

            ss += "\r\n";
            preamble_ = ss;
        }

        public void OnMethod(HttpParser parser, string method)
        {
            method_ = method;
        }

        public void OnRequestUri(HttpParser parser, string requestUri)
        {
        }

        public void OnPath(HttpParser parser, string path)
        {
            path_ = path;
        }

        public void OnFragment(HttpParser parser, string fragment)
        {
        }

        public void OnQueryString(HttpParser parser, string queryString)
        {
        }

        protected override HttpParser CreateParser(http_parser_type parse_type)
        {
            if (parse_type != http_parser_type.HTTP_REQUEST)
                throw new Exception("AwsHttpResponse can only create HTTP_REQUEST type parser");

            return new HttpParser(this);
        }


        //public enum http_method
        //{
        //    HTTP_DELETE,
        //    HTTP_GET,
        //    HTTP_HEAD,
        //    HTTP_POST,
        //    HTTP_PUT,
        //    HTTP_UNKNOWN
        //}

        //void on_message_complete(http_method method)
        //{
        //    switch (method)
        //    {
        //        case http_method.HTTP_DELETE:
        //            method_ = "DELETE";
        //            break;
        //        case http_method.HTTP_GET:
        //            method_ = "GET";
        //            break;
        //        case http_method.HTTP_POST:
        //            method_ = "POST";
        //            break;
        //        case http_method.HTTP_HEAD:
        //            method_ = "HEAD";
        //            break;
        //        case http_method.HTTP_PUT:
        //            method_ = "PUT";
        //            break;
        //        default:
        //            method_ = "UNKNOWN";
        //            break;
        //    }
        //}
    }
}
