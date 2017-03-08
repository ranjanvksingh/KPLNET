using System;

using HttpMachine;

namespace KPLNET.Http
{
    public class AwsHttpResponse : AwsHttpMessage, IHttpResponseParserDelegate
		{
		 private int status_code_;
         private string status_reason_;
		 public int Status_code()   
         {	
             return status_code_; 
         }
         public string Status_reason()
         {
             return status_reason_;
         }

         public AwsHttpResponse()  : base(false, http_parser_type.HTTP_RESPONSE)
         {
         }

		public AwsHttpResponse(int code) : this()
        { 
            status_code_ = code; 
        }

		string to_string() 
		{
		  string ss = "HTTP/1.1 "  + status_code_ + " "	 + status_string(status_code_)	 + "\r\n";
		  foreach (var h in  headers_) 
			ss +=  h.Key + ": " + h.Value + "\r\n";

		  ss += "\r\n" + data_;
		  return ss;
		}

		string status_string(int code) 
		{
		  switch (code) 
		  {
			case 200:
			  return "OK";
			case 400:
			  return "BAD REQUEST";
			case 401:
			  return "UNAUTHORIZED";
			case 403:
			  return "FORBIDDEN";
			case 404:
			  return "NOT FOUND";
			case 500:
			  return "INTERNAL SERVER ERROR";
			case 501:
			  return "NOT IMPLEMENTED";
			case 503:
			  return "SERVICE UNAVAILABLE";
			case 504:
			  return "GATEWAY TIMEOUT";
			default:
			  return "UNKNOWN";
		  }
		}

        public void OnResponseCode(HttpParser parser, int statusCode, string statusReason)
        {

            status_code_ = statusCode;
            status_reason_ = statusReason;
        }

        protected override HttpParser CreateParser(http_parser_type parse_type)
        {
            if (parse_type != http_parser_type.HTTP_RESPONSE)
                throw new Exception("AwsHttpResponse can only create HTTP_RESPONSE type parser");

            return new HttpParser(this);
        }
    }
}
