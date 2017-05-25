using System;
using System.Linq;
using System.Text;
using System.Numerics;
using System.Security.Cryptography;
using System.Collections.Generic;

using Google.Protobuf;
using Aws.Kinesis.Protobuf;

namespace KPLNETInterface
{
    public static class Util
    {
        public static string SerializeAsString(this Aws.Kinesis.Protobuf.Message message)
        {
            var byteString = message.ToByteString();
            return byteString.ToString(Encoding.Default);
        }

        public static string SerializeAsString(this Aws.Kinesis.Protobuf.AggregatedRecord ar)
        {
            var byteString = ar.ToByteString();
            return byteString.ToString(Encoding.Default);
        }

        public static Message DeserializeAsMessage(this string hex)
        {
            ByteString bytes;

            try
            {
                bytes = ByteString.CopyFrom(hex, Encoding.Default);
            }
            catch (Exception e)
            {
                throw new Exception("Input is not valid hexadecimal");
            }

            Message msg = Message.Parser.ParseFrom(bytes);

            if (msg == null)
                throw new Exception("Could not deserialize protobuf message");

            return msg;
        }

    }

    public class Utils
    {

        // random printable string
        public static List<string> split_on_first(string s, string delim) 
		{
		  var it_range = s.IndexOf(delim);

		  if (it_range < 1 ) 
			return new List<string>{s};
		  else
            return new List<string>
			{
			  s.Substring(0, it_range),
			  s.Substring(it_range + delim.Length)
			};
		}

        public static string random_string(int len = 24)
        {
            StringBuilder ss = new StringBuilder();
            Random rn = new Random();
            for (int i = 0; i < len; ++i)
                ss.Append((char)rn.Next(32, 126));

            return ss.ToString();
        }

		public static byte[] GetMD5(byte[] inputBytes)
		{
			// Use input string to calculate MD5 hash
			using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
				return md5.ComputeHash(inputBytes);
		}

		public static byte[] Combine(byte[] x, byte[] y)
		{		 
			var z = new byte[x.Length + y.Length];
			x.CopyTo(z, 0);
			y.CopyTo(z, x.Length);
			return z;
		}

		public static bool AreArrayEqual(byte[] x, byte[] y)
		{
			if (x == null || y == null || x.Length != y.Length)
				return false;

			for (int i = 0; i < x.Length; i++)
				if (x[i] != y[i])
					return false;

			return true;
		}

		public static byte[] GetMD5(string inputString)
		{
			// Use input string to calculate MD5 hash
			using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
				return md5.ComputeHash(Encoding.UTF8.GetBytes(inputString));
		}

		public static string CreateMD5(string input)
        {
            // Use input string to calculate MD5 hash
            using (System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create())
            {
                byte[] inputBytes = System.Text.Encoding.Default.GetBytes(input);
                byte[] hashBytes = md5.ComputeHash(inputBytes);

                // Convert the byte array to hexadecimal string
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hashBytes.Length; i++)
                {
                    sb.Append(hashBytes[i].ToString("X2"));
                }
                return sb.ToString();
            }
        }

        public static BigInteger GetDecimalHashKey(string partition_key)
        {
            
            var ret = new BigInteger(System.Security.Cryptography.MD5.Create().ComputeHash(System.Text.Encoding.Default.GetBytes(partition_key)).Concat(new byte[] { 0 }).ToArray());
			return ret * ret.Sign;
       }

        public static bool verifyMd5Hash(string input, string hash)
        {
            // Hash the input.
            string hashOfInput = CreateMD5(input);

            // Create a StringComparer an compare the hashes.
            StringComparer comparer = StringComparer.OrdinalIgnoreCase;

            if (0 == comparer.Compare(hashOfInput, hash))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static string url_encode(string s)
        {
            return System.Web.HttpUtility.UrlEncode(s);
        }

        public static Tuple<string, string> get_date_time(DateTime dt)
        {
            return new Tuple<string, string>(dt.ToString("%Y%m%d"), dt.ToString("%H%M%S"));
        }

        public static string format_ptime(DateTime pt, string format = "%Y-%m-%dT%H:%M:%SZ")
        {
            if (pt == DateTime.MinValue)
                pt = DateTime.UtcNow;

            return pt.ToString(format);
        }
    }

    public class RandomGenerator
    {
        RNGCryptoServiceProvider _rng = new RNGCryptoServiceProvider();

        public RandomGenerator() { }

        public int GetNextInt32()
        { return GetNextInt32(int.MinValue, int.MaxValue); }

        public int GetNextInt32(int low, int hi)
        { return (int)GetNextInt64(low, hi); }

        public long GetNextInt64()
        { return GetNextInt64(long.MinValue, long.MaxValue); }

        public long GetNextInt64(long low, long hi)
        {
            if (low >= hi)
                throw new ArgumentException("low must be < hi");
            byte[] buf = new byte[8];
            double num;

            //Generate a random double
            _rng.GetBytes(buf);
            num = Math.Abs(BitConverter.ToDouble(buf, 0));

            //We only use the decimal portion
            num = num - Math.Truncate(num);

            //Return a number within range
            return (long)(num * ((double)hi - (double)low) + low);
        }

        public double GetNextDouble()
        { return GetNextDouble(double.MinValue, double.MaxValue); }

        public double GetNextDouble(double low, double hi)
        {
            if (low >= hi)
                throw new ArgumentException("low must be < hi");
            byte[] buf = new byte[8];
            double num;

            //Generate a random double
            _rng.GetBytes(buf);
            num = Math.Abs(BitConverter.ToDouble(buf, 0));

            //We only use the decimal portion
            num = num - Math.Truncate(num);

            //Return a number within range
            return num * (hi - low) + low;
        }
    }
}
