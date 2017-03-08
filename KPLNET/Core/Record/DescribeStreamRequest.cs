
namespace KPLNET.Core
{
    public class DescribeStreamRequest
    {
        public string Stream { get; set; }
        public string ExclusiveStartShardId { get; set; }
        public int Limit { get; set; }
    }
}
