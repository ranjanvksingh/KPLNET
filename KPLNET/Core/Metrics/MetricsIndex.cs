using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;


namespace KPLNET.Metrics
{
    public class MetricsIndex
    {
        public Metric get_metric(MetricsFinder metrics_finder)
        {
            if (metrics_finder.empty())
                return null;

			List<string> keys_to_add = new List<string>();
			List<KeyValuePair<string, string>> dims = new List<KeyValuePair<string, string>>();
			Metric last_node = null;
			MetricsFinder mf = metrics_finder;

			lock (mutex_)
			{
				if (metrics_.ContainsKey(metrics_finder.Cannon()))
				{
					var metric = metrics_[metrics_finder.Cannon()];
					if (metric != null)
						return metric;
				}

				while (!mf.empty())
				{
					if (metrics_.ContainsKey(mf.Cannon()))
						last_node = metrics_[mf.Cannon()];

					keys_to_add.Add(mf.Cannon());
					dims.Add(mf.last_dimension());
					mf.PopDimension();
				}

				for (int i = dims.Count - 1; i >= 0; i--)
				{
					var m = new Metric(last_node, dims[i]);
					last_node = m;
					metrics_.Add(keys_to_add[i], m);
				}
			}
            return last_node;
        }

        public List<Metric> get_all()
        {
            List<Metric> v = new List<Metric>();
            lock (mutex_)
                foreach (var p in metrics_)
                    v.Add(p.Value);

            return v;
        }

        private Dictionary<string, Metric> metrics_ = new Dictionary<string, Metric>();
        private object mutex_ = new object();

    }
}
