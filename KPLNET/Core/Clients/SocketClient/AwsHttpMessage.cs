using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;

using HttpMachine;

namespace KPLNET.Http
{
    public enum http_parser_type { HTTP_REQUEST, HTTP_RESPONSE, HTTP_BOTH };
    public class AwsHttpMessage : IHttpParserDelegate
    {
        protected Dictionary<string, string> headers_ = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
        protected string data_;
        protected bool complete_;
        protected bool no_content_length_;
        static string kContentLength = "Content-Length";
        protected HttpParser parser_;

        protected string method, requestUri, path, queryString, fragment, headerName, headerValue, statusReason;
        protected int versionMajor = -1, versionMinor = -1;
        protected int? statusCode;
        protected List<ArraySegment<byte>> body = new List<ArraySegment<byte>>();
        protected bool onHeadersEndCalled, shouldKeepAlive;
        
        protected virtual HttpParser CreateParser(http_parser_type parse_type) { throw new NotImplementedException(); }
        public AwsHttpMessage(bool no_parser, http_parser_type parse_type)
        {
            if (no_parser)
            {
                complete_ = true;
                return;
            }

            complete_ = false;
            if (http_parser_type.HTTP_REQUEST == parse_type)
                parser_ = CreateParser(parse_type);
        }

        public void OnMessageBegin(HttpParser parser)
        {
            //static auto default_cb = [](auto parser) {return 0;};
            //settings_.on_message_begin = default_cb;

            headers_ = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);
            body = new List<ArraySegment<byte>>();
        }

        public void OnHeaderName(HttpParser parser, string name)
        {
            //settings_.on_header_field = [](auto parser, auto data, auto len) 
            //    {
            //      auto m = static_cast<HttpMessage*>(parser->data);
            //      auto header_name = std::string(data, len);
            //      m->add_header(header_name, "");
            //      return 0;
            //    };

            if (!string.IsNullOrEmpty(headerValue))
                CommitHeader();

            headerName = name;

        }

        public void OnHeaderValue(HttpParser parser, string value)
        {
            //settings_.on_header_value = [](auto parser, auto data, auto len) 
            //    {
            //      auto m = static_cast<HttpMessage*>(parser->data);
            //      auto header_val = std::string(data, len);
            //      m->headers_.back().second = header_val;
            //      return 0;
            //    };

            if (string.IsNullOrEmpty(headerName))
                throw new Exception("Got header value without name.");

            headerValue = value;

        }

        public void OnHeadersEnd(HttpParser parser)
        {
            //settings_.on_headers_complete = default_cb;

            onHeadersEndCalled = true;

            if (!string.IsNullOrEmpty(headerValue))
                CommitHeader();

            versionMajor = parser.MajorVersion;
            versionMinor = parser.MinorVersion;
            shouldKeepAlive = parser.ShouldKeepAlive;
        }

        void CommitHeader()
        {
            //StdErrorOut.Instance.StdOut(LogLevel.debug, "Committing header '" + headerName + "' : '" + headerValue + "'");
            headers_[headerName] = headerValue;
            headerName = headerValue = null;
        }

        public void OnBody(HttpParser parser, ArraySegment<byte> data)
        {
            //settings_.on_body = [](auto parser, auto data, auto len) 
            //    {
            //      auto m = static_cast<HttpMessage*>(parser->data);
            //      m->data_.append(data, len);
            //      return 0;
            //    };

            body.Add(data);
        }

        public void OnMessageEnd(HttpParser parser)
        {
            //settings_.on_message_complete = [](auto parser) 
            //    {
            //      auto m = static_cast<HttpMessage*>(parser->data);
            //      m->complete_ = true;
            //      m->on_message_complete(parser);
            //      return 0;
            //    };

            var length = body.Aggregate(0, (s, b) => s + b.Count);
            if (length > 0)
            {
                var bytes = new byte[length];
                int where = 0;
                foreach (var buf in body)
                {
                    Buffer.BlockCopy(buf.Array, buf.Offset, bytes, where, buf.Count);
                    where += buf.Count;
                }
                data_ = Encoding.UTF8.GetString(bytes);
            }

            complete_ = true;
        }

        public void add_header(string name, string value)
        {
            headers_.Add(name, value);
        }

        public int remove_headers(string name)
        {
            name = name.Trim().ToLower();

            int count = 0;
            foreach (var it in headers_)
            {
                var n = it.Key.Trim().ToLower();
                if (n == name)
                {
                    headers_.Remove(it.Key);
                    count++;
                }
            }
            return count;
        }

        public void set_data(string s)
        {
            data_ = s;

            if (!no_content_length_)
            {
                if(headers_.Keys.Contains(kContentLength))
                {
                    headers_[kContentLength] = data_.Length.ToString();
                    return;
                }

                add_header(kContentLength, data_.Length.ToString());
            }
        }

        public void update(byte[] buffer, int len)
        {
            int parsedLength = parser_.Execute(new ArraySegment<byte>(buffer, 0, len));
            if (parsedLength != len) 
            {
                string s = "Error parsing http message, last input was: \n" + Encoding.UTF8.GetString(buffer) + "\n" + "Input was " + len + " bytes, only consumed " + parsedLength;
              throw new Exception(s);
            }
        }

        public Dictionary<string, string> headers() { return headers_; }
        public string data() { return data_; }
        public bool complete() { return complete_; }
        public void set_no_content_length(bool v) { no_content_length_ = v; }

    }
}
