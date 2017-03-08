using System;
using System.Collections.Generic;

namespace KPLNET.Utils
{
    public class TokenStream
    {
        private double max_;
        private double rate_;
        private double tokens_;
        private DateTime last_;
        public TokenStream(double max, double rate)
        {
            max_ = max;
            rate_ = rate;
            tokens_ = 0;
        }

        public double tokens()
        {
            var now = DateTime.Now;
            // We don't set the last_ timestamp if growth is zero because we might end
            // up never growing the tokens if the invocations are so close together
            // that growth is always zero. This can happen if the clock does not have
            // enough resolution such that the number of seconds returned is 0.
            double growth = (rate_ * (now - last_).TotalMilliseconds) / 1000;
            if (growth > 0)
            {
                tokens_ = Math.Min(max_, tokens_ + growth);
                last_ = now;
            }
            return tokens_;
        }

        public void take(double n)
        {
            if (n > tokens())
                throw new Exception("Not enough tokens");

            tokens_ -= n;
        }
    }

    public class TokenBucket
    {
        object mutex = new object();
        private List<TokenStream> streams_ = new List<TokenStream>();

        public void add_token_stream(double max, double rate)
        {
            lock (mutex)
                streams_.Add(new TokenStream(max, rate));
        }

        public bool try_take(List<double> num_tokens)
        {
            if (!can_take(num_tokens))
                return false;

            lock (mutex)
            {
                for (int i = 0; i < num_tokens.Count; i++)
                {
                    streams_[i].take(num_tokens[i]);
                }

                return true;
            }
        }

        public bool can_take(List<double> num_tokens)
        {
            if (num_tokens.Count != streams_.Count)
                throw new Exception("Size of num_tokens list must be the same as the number of token streams in the bucket");

            lock (mutex)
            {
                for(int i = 0; i< num_tokens.Count ; i++)
                {
                    if (streams_[i] == null)
                        StdErrorOut.Instance.StdOut(LogLevel.warn, "streams_[i] is null");

                    if (num_tokens[i] > streams_[i].tokens())
                        return false;
                }

                return true;
            }
        }
    }
}
