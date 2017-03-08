using System;
using System.Collections.Generic;
using System.Threading;

using Aws.Kinesis.Protobuf;

using KPLNET.Http;
using KPLNET.Auth;
using KPLNET.Kinesis.Core;
using KPLNET.Utils;
using KPLNETInterface;

namespace KPLNET
{
    public class KPLNETService
    {
        public static void StartChildProcessAsDeamon(KPLNETInterface.KPLDaemonOptions options)
        {
            Ec2Metadata ec2_md = null;
            try
            {
                var config = Get_config(options.Configuration);
                set_log_level(config.logLevel);
                var executor = get_executor();
                var socket_factory = get_socket_factory();
                //ec2_md = get_ec2_Metadata(executor, socket_factory);
                var region = get_region(config, ec2_md);
                var creds_providers = get_creds_providers(executor, ec2_md, options.Credentials, options.MetricsCredentials);
                UpdateConfigWithCredentials(config, creds_providers);
                var ipc_manager = get_ipc_manager(options.Inpipe, options.Outpipe);
                var kp = get_kp(ipc_manager, socket_factory, region, config, creds_providers.Key, creds_providers.Value, executor);

                // Never returns
                kp.Join();
                throw new Exception("Join returned :(.");
            }
            catch (Exception e)
            {
                StdErrorOut.Instance.StdError(e.ToString());
                throw new Exception("Service Creation failed.", e);
            }
        }

        public static void StartChildProcess(KPLNETConfiguration config, ILogger logger)
        {
            Ec2Metadata ec2_md = null;
            try
            {
                SetDirectLogging(config.logLevel, logger);
                var executor = get_executor();
                var socket_factory = get_socket_factory();
                var region = get_region(config, ec2_md);
                var creds_providers = get_creds_providers(executor, ec2_md, config);
                var ipc_manager = DirectMessageService.Instance.GetChildProcessMessageManager();
                var kp = get_kp(ipc_manager, socket_factory, region, config, creds_providers.Key, creds_providers.Value, executor);
            }
            catch (Exception e)
            {
                StdErrorOut.Instance.StdError(e.ToString());
                throw new Exception("Service Creation failed.", e);
            }
        }

        static void SetDirectLogging(string min_level, ILogger logger)
        {
            set_log_level(min_level);
            StdErrorOut.Instance.Type = LogType.Logger;
            StdErrorOut.Instance.Logger = logger;
        }

        static void set_log_level(string min_level)
        {
            LogLevel level;
            if(!Enum.TryParse<LogLevel>(min_level, out level))
                level = LogLevel.info;

            StdErrorOut.Instance.Level = level;
        }

        static KPLNETConfiguration Get_config(string hex)
        {
            Message msg = null;
            try
            {
                msg = hex.DeserializeAsMessage();
            }
            catch (Exception e)
            {
                StdErrorOut.Instance.StdError(string.Format("Could not deserialize config: {0}",e.ToString()));
            }

            if (msg.Configuration == null)
            {
                StdErrorOut.Instance.StdError("Protobuf message did not contain a Configuration message\n");
            }

            var config = new KPLNETConfiguration();
            try
            {
                config.transfer_from_protobuf_msg(msg);
            }
            catch (Exception e)
            {
                StdErrorOut.Instance.StdError(string.Format("Error in config: {0}\n", e.ToString()));
                throw e;
            }

            return config;
        }

        static string get_region(KPLNETConfiguration config, Ec2Metadata ec2_md)
        {
            if (AWSRegions.Unknown != config.region)
                return config.region.ToString();

            var ec2_region = ec2_md.get_region();
            if (null != ec2_region)
            {
                StdErrorOut.Instance.StdError("Could not configure the region. It was not given in the config and we were unable to retrieve it from EC2 metadata.");
                throw new Exception("Could not configure the region. It was not given in the config and we were unable to retrieve it from EC2 metadata.");
            }
            return ec2_region;
        }

        static void ParseAndCollectCredentials(string cred, List<SetCredentials> set_creds)
        {
            Message msg;
            try
            {
                msg = cred.DeserializeAsMessage();
            }
            catch (Exception e)
            {
                StdErrorOut.Instance.StdError(string.Format("Could not deserialize credentials: {0}", e.ToString()));
                throw new Exception("Credential DeserializeAsMessage failed.", e);
            }

            if (null == msg.SetCredentials)
            {
                StdErrorOut.Instance.StdError("Message is not a SetCredentials message");
                throw new Exception("Message is not a SetCredentials message");
            }
            set_creds.Add(msg.SetCredentials);
        }

        static KeyValuePair<AwsCredentialsProvider, AwsCredentialsProvider> get_creds_providers(Executor executor, Ec2Metadata ec2_md, string cred, string metricCred)
        {
            List<SetCredentials> set_creds = get_creds(cred, metricCred);
            return get_creds_providers(executor, ec2_md, set_creds);
        }
        
        static List<SetCredentials> get_creds(string cred, string metricCred)
        {
            List<SetCredentials> set_creds = new List<SetCredentials>();
            if (!string.IsNullOrEmpty(cred))
                ParseAndCollectCredentials(cred, set_creds);
            if (!string.IsNullOrEmpty(metricCred))
                ParseAndCollectCredentials(metricCred, set_creds);

            return set_creds;
        }
        
        static KeyValuePair<AwsCredentialsProvider, AwsCredentialsProvider> get_creds_providers(Executor executor, Ec2Metadata ec2_md, List<SetCredentials> set_creds)
        {
            AwsCredentialsProvider creds_provider = null;
            AwsCredentialsProvider metrics_creds_provider = null;

            foreach (var sc in set_creds)
            {
                var cp = new BasicAwsCredentialsProvider(sc.Credentials.Akid, sc.Credentials.SecretKey, sc.Credentials.Token);
                if (sc.ForMetrics)
                    metrics_creds_provider = cp;
                else
                    creds_provider = cp;
            }

            if (null == creds_provider)
            {
                creds_provider = new DefaultAwsCredentialsProviderChain(executor, ec2_md);
                Thread.Sleep(250);
            }

            if (null == creds_provider.get_credentials())
            {
                StdErrorOut.Instance.StdError("Could not retrieve credentials from anywhere.");
                throw new Exception("Could not retrieve credentials from anywhere.");
            }

            if (null == metrics_creds_provider)
                metrics_creds_provider = creds_provider;

            return new KeyValuePair<AwsCredentialsProvider, AwsCredentialsProvider>(creds_provider, metrics_creds_provider);
        }

        static KeyValuePair<AwsCredentialsProvider, AwsCredentialsProvider> get_creds_providers(Executor executor, Ec2Metadata ec2_md, KPLNETConfiguration config)
        {
            AwsCredentialsProvider creds_provider = null;
            AwsCredentialsProvider metrics_creds_provider = null;

            if (null != config.AWSCredentials)
            {
                creds_provider = new BasicAwsCredentialsProvider(config.AWSCredentials.Akid, config.AWSCredentials.SecretKey, config.AWSCredentials.SessionToken);
                if (null != config.AWSMetricsCredentials)
                    metrics_creds_provider = new BasicAwsCredentialsProvider(config.AWSMetricsCredentials.Akid, config.AWSMetricsCredentials.SecretKey, config.AWSMetricsCredentials.SessionToken);
                else
                    metrics_creds_provider = creds_provider;
            }
            else 
            {
                creds_provider = new DefaultAwsCredentialsProviderChain(executor, ec2_md);
                Thread.Sleep(250);
            }

            if (null == creds_provider.get_credentials())
            {
                StdErrorOut.Instance.StdError("Could not retrieve credentials from anywhere.");
                throw new Exception("Could not retrieve credentials from anywhere.");
            }

            return new KeyValuePair<AwsCredentialsProvider, AwsCredentialsProvider>(creds_provider, metrics_creds_provider);
        }

        static Executor get_executor()
        {
            try
            {
                int workers = Math.Min(8, Math.Max(1, Environment.ProcessorCount - 2));
                return new IoServiceExecutor(workers);
            }
            catch(Exception ex)
            {
                StdErrorOut.Instance.StdError("KPLNET Service get_executor failed.", ex);
                throw new Exception("get_executor failed.", ex);
            }
        }

        static IpcManager get_ipc_manager(string in_file, string out_file)
        {
            try
            {
                return new IpcManager(new IpcChannel(in_file, out_file));
            }
            catch (Exception ex)
            {
                StdErrorOut.Instance.StdError("KPLNET Service get_ipc_manager failed.", ex);
                throw new Exception("get_ipc_manager failed.", ex);
            }
        }

        static ISocketFactory get_socket_factory()
        {
            try
            { 
                return new IoServiceSocketFactory(); 
            }
            catch (Exception ex)
            {
                StdErrorOut.Instance.StdError("KPLNET Service get_socket_factory failed.", ex);
                throw new Exception("get_socket_factory failed.", ex);
            }
        }
        
        static Ec2Metadata get_ec2_Metadata(Executor executor, ISocketFactory socket_factory)
        {
            try
            {
                return new Ec2Metadata(executor, socket_factory);
            }
            catch (Exception ex)
            {
                StdErrorOut.Instance.StdError("KPLNET Service get_ec2_Metadata failed.", ex);
                throw new Exception("get_ec2_Metadata failed.", ex);
            }
        }

        static KinesisProducer get_kp(IMessageManager ipc_manager, ISocketFactory socket_factory, string region, KPLNETConfiguration config, AwsCredentialsProvider creds_provider, AwsCredentialsProvider metrics_creds_provider, Executor executor)
        {
            try
            {
                return new KinesisProducer(ipc_manager, socket_factory, region, config, creds_provider, metrics_creds_provider, executor);
            }
            catch (Exception ex)
            {
                StdErrorOut.Instance.StdError("KPLNET Service get_kp failed.", ex);
                throw new Exception("get_kp failed.", ex);
            }
        }

        static void UpdateConfigWithCredentials(KPLNETConfiguration config, KeyValuePair<AwsCredentialsProvider, AwsCredentialsProvider> creds_providers)
        {
            var cred1 = creds_providers.Key.get_credentials();
            var cred2 = creds_providers.Value.get_credentials();
            config.AWSCredentials = new AWSCredentials()
            {
                Akid = cred1.Access_key_id(),
                SecretKey = cred1.Secret_key(),
                SessionToken = cred1.Session_token()
            };
            config.AWSMetricsCredentials = new AWSCredentials()
            {
                Akid = cred2.Access_key_id(),
                SecretKey = cred2.Secret_key(),
                SessionToken = cred2.Session_token()
            };
        }

    }
}
