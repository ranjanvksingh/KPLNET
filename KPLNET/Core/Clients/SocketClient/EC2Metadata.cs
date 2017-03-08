using System;
using System.Linq;
using System.Text.RegularExpressions;

using KPLNET.Utils;

namespace KPLNET.Http
{
    using Callback = Action<bool, string>;
    using System.Web;
    
    using KPLNET.Http;
    using System.Threading;
    public class Ec2Metadata
    {
        private const double kTimeout = 100; //ms
        private static readonly Regex kRegionFromAzRegex = new Regex("^([a-z]+-[a-z]+-[0-9])[a-z]$", RegexOptions.ECMAScript | RegexOptions.Compiled);
        private Executor executor_;
        private AwsHttpClient http_client_;
        private ISocketFactory socketFactory_;


        public Ec2Metadata(Executor executor, ISocketFactory socketFactory, string ip = "169.254.169.254", int port = 80, bool secure = false)
        {
            executor_ = executor;
            socketFactory_ = socketFactory;
            http_client_ = new AwsHttpClient(executor, socketFactory_, ip, port, secure, false, 1, 1, 500, 500);
        }

        public void Get(string path, Callback callback)
        {
            var done = false;
            executor_.Schedule(() => { if (!done) { done = true; callback(false, "Timed out"); } }, kTimeout);

            AwsHttpRequest req = new AwsHttpRequest("GET", path);
            http_client_.put(req, (result) => { if (!done) { done = true; callback(result.successful(), result.successful() ? result.response_body() : result.error()); } }, new object(), DateTime.Now, DateTime.Now);
        }

        public void get_az(Callback callback)
        {
            Get("/latest/meta-data/placement/availability-zone/", callback);
        }

        public string get_region()
        {
            string region = null;
            ManualResetEvent ev = new ManualResetEvent(false);
            get_region
              (
                  (success, result) =>
                  {
                      //aws::unique_lock<aws::mutex> lock(mutex);
                      if (success)
                          region = result;

                      ev.Set();
                  }
              );

            ev.WaitOne();
            return region;
        }

        public void get_region(Callback callback)
        {
            get_az
            (
                (success, result) =>
                {
                    if (!success)
                    {
                        callback(false, result);
                        return;
                    }

                    Match m = kRegionFromAzRegex.Match(result);
                    if (m.Success)
                    {
                        callback(true, m.Value);
                    }
                    else
                    {
                        callback(false, "Could not read region, got unexpected result " + result);
                    }
                }
            );
        }

        public void get_instance_profile_name(Callback callback)
        {
            Get
            (
                  "/latest/meta-data/iam/security-credentials/",
                  (success, result) =>
                  {
                      if (!success)
                      {
                          callback(false, result);
                          return;
                      }

                      try
                      {
                          callback(true, get_first_line(result));
                      }
                      catch (Exception ex)
                      {
                          callback(false, ex.ToString());
                      }
                  }
            );
        }

        public void get_instance_profile_credentials(Callback callback)
        {
            get_instance_profile_name
            (
                (success, result) =>
                {
                    if (!success)
                    {
                        callback(false, result);
                        return;
                    }

                    var path = "/latest/meta-data/iam/security-credentials/" + result + "/";
                    Get(path, callback);
                }
            );
        }

        public string get_first_line(string s)
        {
            return s.Split("\r\n".ToArray())[0];
        }
    }
}
