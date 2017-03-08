using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

using KPLNET.Http;
using KPLNET.Utils;

namespace KPLNET.Auth
{
    public class AwsCredentials
    {
        private readonly string akid;
        private readonly string secret_key;
        private readonly string session_token;
        public AwsCredentials(string akid, string secret_key, string session_token = "")
        {
            this.akid = akid;
            this.secret_key = secret_key;
            this.session_token = session_token;
        }

        public string Akid() { return akid; }
        public string Access_key_id() { return akid; }
        public string Secret_key() { return secret_key; }
        public string Session_token() { return session_token; }
    }

    public class AwsCredentialsProvider
    {
        protected virtual AwsCredentials get_credentials_impl() { throw new NotImplementedException(); }
        public AwsCredentials get_credentials()
        {
            AwsCredentials creds = this.get_credentials_impl();
            if (!verify_credentials_format(creds))
                throw new Exception("Credentials did not match expected patterns");

            return creds;
        }

        AwsCredentials try_get_credentials()
        {
            try
            {
                return get_credentials();
            }
            catch (Exception)
            {
                return null;
            }
        }

        Regex kAkidRegex = new Regex(@"^[A-Z0-9]{20}$", RegexOptions.ECMAScript | RegexOptions.Compiled);
        Regex kSkRegex = new Regex(@"^[A-Za-z0-9/+=]{40}$", RegexOptions.ECMAScript | RegexOptions.Compiled);

        bool verify_credentials_format(AwsCredentials creds)
        {
            return kAkidRegex.Match(creds.Akid()).Success && kSkRegex.Match(creds.Secret_key()).Success;
        }

    };

    public class EnvVarAwsCredentialsProvider : AwsCredentialsProvider
    {
        protected override AwsCredentials get_credentials_impl()
        {
            string akid = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
            string sk = Environment.GetEnvironmentVariable("AWS_SECRET_KEY");
            if (sk == null)
            {
                sk = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
            }
            if (akid == null || sk == null)
            {
                throw new Exception("Could not get credentials from env vars");
            }
            return new AwsCredentials(akid, sk);
        }
    }

    public class InstanceProfileAwsCredentialsProvider : AwsCredentialsProvider
    {
        public InstanceProfileAwsCredentialsProvider(Executor executor, Ec2Metadata ec2_metadata)
        {
            this.executor = executor;
            this.ec2_metadata = ec2_metadata;
            this.attempt = 0;
            refresh();
        }

        public void refresh()
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "Credentials.refresh called.");
            ec2_metadata.get_instance_profile_credentials
              (
                (bool success, string result) =>
                {
                    this.handle_result(success, result);
                }
              );
        }

        protected override AwsCredentials get_credentials_impl()
        {
            lock (mutex)
            {
                if (string.IsNullOrEmpty(akid))
                {
                    throw new Exception("Instance profile credentials not loaded from ec2 metadata yet");
                }
                return new AwsCredentials(akid, secret_key, session_token);
            }
        }
        private void handle_result(bool success, string result)
        {
            lock (mutex)
            {
                Action retry =
                            () =>
                            {
                                attempt++;
                                if (attempt >= 10)
                                {
                                    StdErrorOut.Instance.StdError(string.Format("Could not fetch instance profile credentials after {0} attempts.", attempt));
                                    return;
                                }

                                executor.Schedule
                                    (
                                        () => { refresh(); },
                                        DateTime.Now.AddMilliseconds(attempt * 100)
                                    );
                            };

                if (!success)
                {
                    retry();
                    return;
                }

                try
                {
                    var json = System.Web.Helpers.Json.Decode(result);
                    akid = (string)json["AccessKeyId"];
                    secret_key = (string)json["SecretAccessKey"];
                    var token = json["Token"];
                    if (token)
                        session_token = (string)token;

                    var expires_at = DateTime.Parse(json["Expiration"]);
                    DateTime refresh_at = expires_at.AddMinutes(-3);
                    executor.Schedule(() => { refresh(); }, refresh_at);

                    StdErrorOut.Instance.StdOut(LogLevel.info, string.Format("Got credentials from instance profile. Refreshing in {0} hours", (refresh_at - DateTime.Now).TotalHours));
                    attempt = 0;
                }
                catch (Exception ex)
                {
                    StdErrorOut.Instance.StdError(string.Format("Error parsing instance profile credentials json: {0}", ex.ToString()));
                    retry();
                }
            }
        }

        private Executor executor;
        private Ec2Metadata ec2_metadata;
        private string akid;
        private string secret_key;
        private string session_token;
        private int attempt;
        private object mutex = new object();
    };

    class AwsCredentialsProviderChain : AwsCredentialsProvider
    {
        public static AwsCredentialsProviderChain create(List<AwsCredentialsProvider> providers)
        {
            var c = new AwsCredentialsProviderChain();
            c.reset(providers);
            return c;
        }

        public AwsCredentialsProviderChain() { }

        public AwsCredentialsProviderChain(List<AwsCredentialsProvider> providers)
        {
            providers = new List<AwsCredentialsProvider>(providers);
        }

        private List<AwsCredentialsProvider> providers = new List<AwsCredentialsProvider>();

        public void reset(List<AwsCredentialsProvider> new_providers)
        {
            lock (mutex)
            {
                providers.Clear();
                providers.AddRange(new_providers);
            }
        }

        protected override AwsCredentials get_credentials_impl()
        {
            lock (mutex)
            {

                if (providers.Count == 0)
                    throw new Exception("The AwsCredentialsProviderChain is empty");

                foreach (var p in providers)
                {
                    var creds = p.get_credentials();
                    if (creds != null)
                        return creds;
                }

                throw new Exception("Could not get credentials from any of the providers in the AwsCredentialsProviderChain");
            }
        }
        private object mutex = new object();
    }

    class BasicAwsCredentialsProvider : AwsCredentialsProvider
    {
        public BasicAwsCredentialsProvider(string akid, string secret_key, string session_token = "")
        {
            creds = new AwsCredentials(akid, secret_key, session_token);
        }

        protected override AwsCredentials get_credentials_impl()
        {
            return creds;
        }

        private AwsCredentials creds;
    };

    class DefaultAwsCredentialsProviderChain : AwsCredentialsProviderChain
    {
        public DefaultAwsCredentialsProviderChain(Executor executor, Ec2Metadata ec2_metadata)
            : base(new List<AwsCredentialsProvider> { new EnvVarAwsCredentialsProvider(), new InstanceProfileAwsCredentialsProvider(executor, ec2_metadata) })
        {
        }
    };
}
