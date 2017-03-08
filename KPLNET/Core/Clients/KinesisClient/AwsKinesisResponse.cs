namespace KPLNET.Core.Clients.KinesisClient
{
    public class AwsKinesisResponse
    {
        Amazon.Kinesis.Model.PutRecordsResponse prr_;
        Amazon.Kinesis.Model.DescribeStreamResponse dsr_;
        public AwsKinesisResponse(Amazon.Kinesis.Model.PutRecordsResponse prr) { prr_ = prr; }
        public AwsKinesisResponse(Amazon.Kinesis.Model.DescribeStreamResponse dsr) { dsr_ = dsr;}
        public Amazon.Kinesis.Model.PutRecordsResponse PutRecordsResponse { get { return prr_; } } 
        public Amazon.Kinesis.Model.DescribeStreamResponse DescribeStreamResponse { get { return dsr_; } } 
    }
}
