using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Aws.Kinesis.Protobuf;

namespace KPLNETInterface
{
    public interface IMessageManager
    {
        void put(Message data);

        bool try_take(out Message dest);
    }
}
