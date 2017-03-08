using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;


namespace KPLNET.Metrics
{
    public class MetricsFinder
    {
        private string canon = string.Empty;
        private List<KeyValuePair<int, int>> delims = new List<KeyValuePair<int, int>>();
        public MetricsFinder PushDimension(string k, string v)
        {
            delims.Add(new KeyValuePair<int, int>(canon.Length, k.Length));
            canon += (char)0;
            canon += k;
            canon += (char)1;
            canon += v;
            return this;
        }

        public MetricsFinder PopDimension()
        {
            if (delims.Count != 0)
            {
                canon.Remove(delims[delims.Count - 1].Key);
                delims.RemoveAt(delims.Count - 1);
            }
            return this;
        }

        public KeyValuePair<string, string> last_dimension()
        {
            if (empty())
                throw new Exception("Cannot call last_dimension() on a MetricsFinder that's empty");

            var d = delims[delims.Count - 1];
            return new KeyValuePair<string, string>(canon.Substring(d.Key + 1, d.Value), canon.Substring(d.Key + 1 + d.Value + 1));
        }

        public string Cannon() { return canon; }
        public bool empty() { return delims.Count == 0; }

    }
}
