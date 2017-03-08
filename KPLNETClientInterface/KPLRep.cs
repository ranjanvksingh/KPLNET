using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Aws.Kinesis.Protobuf;
using Google.Protobuf;

using KPLNETInterface;
using KPLNET;

namespace KPLNETClientInterface
{
    class KPLRepBuilder
    {
        public static IKPLRep GetKPLRep(string pathToExecutable, Action<Message> messageHandler, Action<Exception> exceptionHandler, String workingDir, KPLNETConfiguration config, Dictionary<String, String> environmentVariables, ILogger log)
        {
            if (config.runAsDaemon)
                return new Daemon(pathToExecutable, messageHandler, exceptionHandler, workingDir, config, environmentVariables, log);
            else
                return new KPLRep(messageHandler, exceptionHandler, config, log);
        }
    }

    class KPLRepMessageBuilder
    {
        public static Message makeSetCredentialsMessage(string aKid, string sercretKey, string token, bool forMetrics)
        {
            Credentials c = new Credentials() { Akid = aKid, SecretKey = sercretKey, Token = token ?? "" };
            SetCredentials sc = new SetCredentials() { Credentials = c, ForMetrics = forMetrics };
            return new Message() { SetCredentials = sc, Id = long.MaxValue };
        }

        public static bool CheckAndUpdateCredentialIfChanged(AWSCredentials cred)
        {
            string akid = Environment.GetEnvironmentVariable(KinesisDotNetProducer.AWS_ACCESS_KEY_ID);
            string sKey = Environment.GetEnvironmentVariable(KinesisDotNetProducer.AWS_SECRET_ACCESS_KEY);
            if (akid == null || sKey == null || (cred.Akid == akid && cred.SecretKey == sKey))
                return false;

            cred.Akid = akid;
            cred.SecretKey = sKey;
            return true;
        }
    }

    public class KPLRep : IKPLRep
    {
        private long stop = 0;

        private readonly ILogger log;
        private readonly KPLNETConfiguration config;
        private readonly Action<Message> messageHandler;
        private readonly Action<Exception> exceptionHandler;

        IMessageManager messageManager = null; 

        public KPLRep(Action<Message> messageHandler, Action<Exception> exceptionHandler, KPLNETConfiguration config, ILogger log)
        {
            this.exceptionHandler = exceptionHandler;
            this.messageHandler = messageHandler;
            this.log = log;
            this.config = config;
            this.messageManager = DirectMessageService.Instance.GetClientInterfaceMessageManager();
            Task.Run(() =>
            {
                try
                {
                    startChildProcess();
                }
                catch (Exception e)
                {
                    HandleError("Error running child process", e, true);
                }
            });
        }

        public void add(Message m)
        {
            try
            {
                messageManager.put(m);
            }
            catch (ThreadInterruptedException e)
            {
                HandleError("Unexpected error", e, true);
            }
        }

        public void destroy()
        {
            throw new NotImplementedException();
        }

        public int getQueueSize()
        {
            return 0; //return messageManager.outgoingMessages.Count;
        }

        void HandleError(String message, Exception t, bool retryable)
        {
            if (exceptionHandler == null)
                return;

            if (retryable)
                exceptionHandler(t != null ? new Exception(message, t) : new Exception(message));
            else
                exceptionHandler(t != null ? new IrrecoverableError(message, t) : new IrrecoverableError(message));
        }

        private void startChildProcess()
        {
            try
            {
                Parallel.Invoke(new Action[]
                {
                    () => {KPLNETService.StartChildProcess(config, log); },
                    () => {while (Interlocked.Read(ref stop) == 0) returnMessage();},
                    () => {while (Interlocked.Read(ref stop) == 0) updateCredentials();}
                });
            }
            catch (Exception e)
            {
                HandleError("Unexpected error connecting to child process", e, true);
            }
        }

        private void returnMessage()
        {
            try
            {
                Message m = null;
                if(messageManager.try_take(out m) && m != null)
                {
                    if (messageHandler != null)
                    {
                        try
                        {
                            messageHandler(m);
                        }
                        catch (Exception e)
                        {
                            log.error("Error in message handler", e);
                        }
                    }
                }
                else
                    Thread.Sleep(5);
            }
            catch (ThreadInterruptedException e)
            {
                HandleError("Unexpected error", e, true);
            }
        }

        private void updateCredentials()
        {
            try
            {
                var cred = config.AWSCredentials;
                if (cred != null && KPLRepMessageBuilder.CheckAndUpdateCredentialIfChanged(cred))
                {
                    var m = KPLRepMessageBuilder.makeSetCredentialsMessage(cred.Akid, cred.SecretKey, cred.SessionToken, false);

                    add(m);

                    //var metricCred = config.AWSMetricsCredentials ?? config.AWSCredentials;
                    //if (metricCred == null)
                    //    outgoingMessages.Add(makeSetCredentialsMessage(metricCred.Akid, metricCred.SecretKey, metricCred.SessionToken, true));
                }
                else
                {

                }

                Thread.Sleep((int)config.credentialsRefreshDelay);
            }
            catch (ThreadInterruptedException e)
            {
                log.warn("Exception during updateCredentials", e);
            }
        }
    }
}
