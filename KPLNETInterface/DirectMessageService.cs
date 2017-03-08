using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

using Aws.Kinesis.Protobuf;

namespace KPLNETInterface
{
    public class DirectMessageService 
    {
        static DirectMessageService _instance;
        ConcurrentQueue<Message> reqMessageQueue;
        ConcurrentQueue<Message> resMessageQueue;
        ClientInterfaceMessageManager ciMM;
        ChildProcessMessageManager cpMM;
        static object constructMutex = new object();

        public IMessageManager GetClientInterfaceMessageManager()
        {
            return ciMM;
        }

        public IMessageManager GetChildProcessMessageManager()
        {
            return cpMM;
        }
        
        public static DirectMessageService Instance 
        { 
            get
            {
                lock (constructMutex)
                {
                    if (_instance == null)
                        _instance = new DirectMessageService();
                }
                return _instance;
            }  
        }

        private DirectMessageService()
        {
            reqMessageQueue = new ConcurrentQueue<Message>();
            resMessageQueue = new ConcurrentQueue<Message>();
            ciMM = new ClientInterfaceMessageManager(this);
            cpMM = new ChildProcessMessageManager(this);
        }

        class ClientInterfaceMessageManager : IMessageManager
        {
            DirectMessageService parent_;
            internal ClientInterfaceMessageManager(DirectMessageService parent)
            {
                parent_ = parent;
            }

            public void put(Message data)
            {
                lock (parent_.resMessageQueue)
                    parent_.reqMessageQueue.Enqueue(data);
            }

            public bool try_take(out Message dest)
            {
                dest = null;
                bool success = false;
                lock (parent_.resMessageQueue)
                {
                    if (parent_.resMessageQueue.Count > 0)
                    {
                        success =  parent_.resMessageQueue.TryDequeue(out dest);
                    }
                }
                return success;
            }
        }

        class ChildProcessMessageManager : IMessageManager
        {
            DirectMessageService parent_;
            internal ChildProcessMessageManager(DirectMessageService parent)
            {
                parent_ = parent;
            }

            public void put(Message data)
            {
                lock (parent_.resMessageQueue)
                    parent_.resMessageQueue.Enqueue(data);
            }

            public bool try_take(out Message dest)
            {
                dest = null;
                bool success = false;
                lock (parent_.reqMessageQueue)
                {
                    if (parent_.reqMessageQueue.Count > 0)
                    {
                        success = parent_.reqMessageQueue.TryDequeue(out dest);
                    }
                }
                return success;
            }
        }

    }
}
