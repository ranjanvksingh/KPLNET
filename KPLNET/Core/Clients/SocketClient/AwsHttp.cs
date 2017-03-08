using System;

using KPLNET.Utils;

namespace KPLNET.Http
{
  public class AwsHttp
  {
    private const string kVersion = "0.10.2";

    public static AwsHttpRequest create_kinesis_request(string region, string api_method, string data)
    {
      AwsHttpRequest awsHttpRequest = new AwsHttpRequest(string.IsNullOrEmpty(data) ? "GET" : "POST", "/", "HTTP/1.1");
      awsHttpRequest.add_header("Host", "kinesis." + region + ".amazonaws.com");
      awsHttpRequest.add_header("Connection", "Keep-Alive");
      awsHttpRequest.add_header("X-Amz-Target", "Kinesis_20131202." + api_method);
      awsHttpRequest.add_header("User-Agent", AwsHttp.user_agent());
      if (!string.IsNullOrEmpty(data))
        awsHttpRequest.add_header("Content-Type", "application/x-amz-json-1.1");
      StdErrorOut.Instance.StdOut(LogLevel.debug, "before req.set_data(data)");
      awsHttpRequest.set_data(data);
      StdErrorOut.Instance.StdOut(LogLevel.debug, "after req.set_data(data)");
      return awsHttpRequest;
    }

    private static string user_agent()
    {
      return "KinesisProducerLibrary/0.10.2 | Windows | " + (object) Environment.Version + "." + (object) Environment.OSVersion.Platform;
    }
  }
}
