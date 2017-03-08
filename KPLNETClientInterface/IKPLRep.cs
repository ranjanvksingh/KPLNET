namespace KPLNETClientInterface
{
    interface IKPLRep
    {
        void destroy();
        void add(Aws.Kinesis.Protobuf.Message m);

        int getQueueSize();
    }
}
