using System;
using System.Linq;
using System.Text;
using System.Web.Helpers;
using System.Collections.Generic;
using System.Security.Cryptography;

using KPLNET.Http;
using KPLNET.Utils;

namespace KPLNET.Auth
{
  public class Sigv4
  {
    public static readonly List<string> kCanonHeaderBlacklist = Sigv4.canon_header_blacklist();
    public const string kAlgo = "AWS4-HMAC-SHA256";

    public static byte[] HmacSHA256(string data, byte[] key)
    {
      KeyedHashAlgorithm keyedHashAlgorithm = KeyedHashAlgorithm.Create("HmacSHA256");
      keyedHashAlgorithm.Key = key;
      return keyedHashAlgorithm.ComputeHash(Encoding.UTF8.GetBytes(data));
    }

    public static byte[] getSignatureKey(string key, string dateStamp, string regionName, string serviceName)
    {
      byte[] bytes = Encoding.UTF8.GetBytes(("AWS4" + key).ToCharArray());
      byte[] key1 = Sigv4.HmacSHA256(dateStamp, bytes);
      byte[] key2 = Sigv4.HmacSHA256(regionName, key1);
      return Sigv4.HmacSHA256("aws4_request", Sigv4.HmacSHA256(serviceName, key2));
    }

    public static List<string> canon_header_blacklist()
    {
      return new List<string>() { "user-agent" };
    }

    public static bool should_include_in_canon_headers(KeyValuePair<string, string> h)
    {
      return Sigv4.kCanonHeaderBlacklist.FindLast((Predicate<string>) (i => i == h.Key)) == Sigv4.kCanonHeaderBlacklist.Last<string>();
    }

    public static void hmac_sha256(byte[] key, string sourceFile, byte[] dest)
    {
      using (HMACSHA256 hmacshA256 = new HMACSHA256(key))
        hmacshA256.ComputeHash(hmacshA256.ComputeHash(Encoding.Default.GetBytes(sourceFile))).CopyTo((Array) dest, 0);
    }

    public static string sha256_hex(string input)
    {
      return Crypto.SHA256(input);
    }
  }

  internal class SigV4Context
  {
      internal string region;
      internal string service;
      private AwsCredentialsProvider credentials_provider;

      public SigV4Context(string region, string service, AwsCredentialsProvider credentials_provider)
      {
          this.region = region;
          this.service = service;
          this.credentials_provider = credentials_provider;
      }

      public AwsCredentialsProvider Credentials_provider()
      {
          return this.credentials_provider;
      }
  }
  internal class RequestSigner
  {
      private AwsHttpRequest req;
      private SigV4Context sig_v4_ctx;
      private Dictionary<string, string> headers;
      private string date;
      private string time;
      private string date_time;
      private string canon_headers;
      private string canon_path;
      private string canon_query_args;
      private string credential_scope;
      private string signed_headers;
      private string canon_request;
      private string str_to_sign;
      private string auth_header;
      private string akid;
      private string secret_key;

      public RequestSigner(AwsHttpRequest req, SigV4Context ctx, bool x_amz_date = true, Tuple<string, string> dt = null)
      {
          if (dt == null)
              dt = KPLNETInterface.Utils.get_date_time(DateTime.UtcNow);
          this.req = req;
          this.sig_v4_ctx = ctx;
          this.date = dt.Item1;
          this.time = dt.Item2;
          this.date_time = this.date + "T" + this.time + "Z";
          if (!x_amz_date)
              return;
          req.headers().Add("x-amz-date", this.date_time);
      }

      public void sign()
      {
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign1");
          AwsCredentials credentials = this.sig_v4_ctx.Credentials_provider().get_credentials();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign2");
          this.akid = credentials.Akid();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign3");
          this.secret_key = credentials.Secret_key();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign4");
          if (string.IsNullOrEmpty(credentials.Session_token()))
              this.req.headers().Add("x-amz-security-token", credentials.Session_token());
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign5");
          this.req.headers().Remove("authorization");
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign6");
          this.calculate_canon_query_args();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign7");
          this.calculate_headers();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign8");
          this.calculate_canon_headers();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign9");
          this.calculate_signed_headers();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign10");
          this.calculate_credential_scope();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign11");
          this.calculate_canon_request();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign12");
          this.calculate_str_to_sign();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign13");
          this.calculate_auth_header();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign14");
          this.req.headers().Add("Authorization", this.auth_header);
          StdErrorOut.Instance.StdOut(LogLevel.debug, "sign15");
      }

      public string Canon_request()
      {
          return this.canon_request;
      }

      public string Str_to_sign()
      {
          return this.str_to_sign;
      }

      public string Auth_header()
      {
          return this.auth_header;
      }

      private void calculate_canon_query_args()
      {
          List<string> stringList = KPLNETInterface.Utils.split_on_first(this.req.Path(), "?");
          StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args1");
          List<KeyValuePair<string, string>> keyValuePairList = new List<KeyValuePair<string, string>>();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args2");
          if (stringList.Count >= 2)
          {
              StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args3");
              foreach (string str in stringList[1].Split("&".ToArray<char>()))
              {
                  StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args4");
                  string[] strArray = str.Split("=".ToArray<char>());
                  StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args5");
                  for (int index = 0; index < strArray.Length; ++index)
                  {
                      StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args6");
                      strArray[index] = strArray[index].Trim();
                  }
                  if (strArray.Length == 2)
                  {
                      StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args7");
                      keyValuePairList.Add(new KeyValuePair<string, string>(strArray[0].Trim(), strArray[1].Trim()));
                      StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args8");          
                  }
              }
              StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args9");
              keyValuePairList.Sort();
              StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args10");
          
          }
          StringBuilder stringBuilder = new StringBuilder();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args11");
          bool removelastChar = false;
          foreach (KeyValuePair<string, string> keyValuePair in keyValuePairList)
          {
              StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args12");
              stringBuilder.Append(keyValuePair.Key).Append("=").Append(keyValuePair.Value).Append("&");
              StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args13");
              removelastChar = true;
          }
          StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args14");
          if(removelastChar)
              stringBuilder.Remove(stringBuilder.Length - 2, 1);
          StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args15");
          this.canon_query_args = stringBuilder.ToString();
          StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args16");
          this.canon_path = stringList[0];
          StdErrorOut.Instance.StdOut(LogLevel.debug, "calculate_canon_query_args17");
      }

      private void calculate_headers()
      {
          this.headers = new Dictionary<string, string>();
          foreach (KeyValuePair<string, string> header in this.req.headers())
          {
              string lower = header.Key.Trim().ToLower();
              if (this.headers.ContainsKey(lower))
              {
                  Dictionary<string, string> headers;
                  string index;
                  (headers = this.headers)[index = lower] = headers[index] + "," + header.Value.Trim();
              }
              else
                  this.headers[lower] = header.Value.Trim();
          }
      }

      private void calculate_canon_headers()
      {
          StringBuilder stringBuilder = new StringBuilder();
          foreach (KeyValuePair<string, string> header in this.headers)
          {
              if (Sigv4.should_include_in_canon_headers(header))
                  stringBuilder.Append(header.Key).Append(":").Append(header.Value).Append("\n");
          }
          this.canon_headers = stringBuilder.ToString();
      }

      private void calculate_credential_scope()
      {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.Append(this.date).Append("/").Append(this.sig_v4_ctx.region).Append("/").Append(this.sig_v4_ctx.service).Append("/").Append("aws4_request");
          this.credential_scope = stringBuilder.ToString();
      }

      private void calculate_signed_headers()
      {
          StringBuilder stringBuilder = new StringBuilder();
          foreach (KeyValuePair<string, string> header in this.headers)
          {
              if (Sigv4.should_include_in_canon_headers(header))
                  stringBuilder.Append(header.Key).Append(";");
          }
          stringBuilder.Remove(stringBuilder.Length - 1, 1);
          this.signed_headers = stringBuilder.ToString();
      }

      private void calculate_canon_request()
      {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.Append(this.req.Method()).Append("\n").Append(this.canon_path).Append("\n").Append(this.canon_query_args).Append("\n").Append(this.canon_headers).Append("\n").Append(this.signed_headers).Append("\n").Append(Sigv4.sha256_hex(this.req.data()));
          this.canon_request = stringBuilder.ToString();
      }

      private void calculate_str_to_sign()
      {
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.Append("AWS4-HMAC-SHA256").Append("\n").Append(this.date_time).Append("\n").Append(this.credential_scope).Append("\n").Append(Sigv4.sha256_hex(this.canon_request));
          this.str_to_sign = stringBuilder.ToString();
      }

      private void calculate_auth_header()
      {
          byte[] numArray1 = new byte[32];
          byte[] numArray2 = new byte[32];
          Sigv4.hmac_sha256(Encoding.Default.GetBytes("AWS4" + this.secret_key), this.date, numArray1);
          Sigv4.hmac_sha256(numArray1, this.sig_v4_ctx.region, numArray2);
          Sigv4.hmac_sha256(numArray2, this.sig_v4_ctx.service, numArray1);
          Sigv4.hmac_sha256(numArray1, "aws4_request", numArray2);
          Sigv4.hmac_sha256(numArray2, this.str_to_sign, numArray1);
          string str = BitConverter.ToString(numArray1).Replace("-", "");
          StringBuilder stringBuilder = new StringBuilder();
          stringBuilder.Append("AWS4-HMAC-SHA256").Append(" ").Append("Credential=").Append(this.akid).Append("/").Append(this.credential_scope).Append(", ").Append("SignedHeaders=").Append(this.signed_headers).Append(", ").Append("Signature=").Append(str);
          this.auth_header = stringBuilder.ToString();
      }

      public static void sign_v4(AwsHttpRequest req, SigV4Context ctx)
      {
          new RequestSigner(req, ctx).sign();
      }
  }
}
