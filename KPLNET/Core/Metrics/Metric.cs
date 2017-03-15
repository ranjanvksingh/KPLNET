using System.Collections.Generic;

namespace KPLNET.Metrics
{
    using Dimension = KeyValuePair<string, string>;
    public class Metric
    {
        private Metric parent;
        private Dimension dimension;
        private List<Dimension> all_dimensions = new List<Dimension>();
        private AccumulatorImpl accumulator;
        public Metric(Metric parent, Dimension d)
        {
            this.parent = parent;
            this.dimension = d;
            this.accumulator = new AccumulatorImpl();

            if (parent != null)
                all_dimensions.InsertRange(all_dimensions.Count, parent.All_dimensions());

            all_dimensions.Add(dimension);
        }

        public Metric() {}

        public void Put(double val)
        {
            accumulator.put(val);
            if (parent != null)
                parent.Put(val);
        }

        public AccumulatorImpl Accumulator() { return accumulator; }
        public Dimension Dimension() { return dimension; }
        public List<Dimension> All_dimensions() { return all_dimensions; }
        public Metric Parent() { return parent; }
    }
}
