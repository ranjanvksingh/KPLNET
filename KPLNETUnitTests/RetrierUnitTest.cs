using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using KPLNET.Kinesis.Core;
using KPLNET.Core.Clients.KinesisClient;

namespace KPLNETUnitTests
{
    [TestClass]
    public class RetrierUnitTest
    {
        AwsKinesisResult make_prr_ctx(int num_kr, int num_ur_per_kr, string error, Amazon.Kinesis.Model.PutRecordsResponse outcome, DateTime start, DateTime end)
        {
            List<KinesisRecord> krs = new List<KinesisRecord>();
            for (int i = 0; i < num_kr; i++)
            {
                var kr = new KinesisRecord();
                for (int j = 0; j < num_ur_per_kr; j++)
                {
                    var ur = TestUtil.make_user_record();
                    ur.Predicted_shard(i);
                    kr.add(ur);
                }
                krs.Add(kr);
            }
            AwsKinesisResult result = new AwsKinesisResult(error, new AwsKinesisResponse(outcome), new PutRecordsRequest(), start, end);
            result.context<PutRecordsRequest>().Items().AddRange(krs);
            return result;
        }

        // Case where there are no errors
        [TestMethod]
        public void RetrierUnitTest_Success()
        {
            int num_ur_per_kr = 10;
            int num_kr = 2;
            DateTime start = DateTime.Now;
            Amazon.Kinesis.Model.PutRecordsResponse response = new Amazon.Kinesis.Model.PutRecordsResponse();
            response.HttpStatusCode = System.Net.HttpStatusCode.OK;
            response.FailedRecordCount = 0;
            response.Records = new System.Collections.Generic.List<Amazon.Kinesis.Model.PutRecordsResultEntry>();
            response.Records.Add(new Amazon.Kinesis.Model.PutRecordsResultEntry() { SequenceNumber = "1234", ShardId = "shardId-000000000000" });
            response.Records.Add(new Amazon.Kinesis.Model.PutRecordsResultEntry() { SequenceNumber = "4567", ShardId = "shardId-000000000001" });
            AwsKinesisResult result = make_prr_ctx(num_kr, num_ur_per_kr, null, response, start, start.AddMilliseconds(5));


            int count = 0;
            Retrier retrier = new Retrier(
                new KPLNETInterface.KPLNETConfiguration(),
                (ur) =>
                {
                    var attempts = ur.Attempts();
                    Assert.AreEqual(attempts.Count, 1);
                    Assert.IsTrue(attempts[0].Start() == start);
                    Assert.IsTrue(attempts[0].End() == start.AddMilliseconds(5));
                    Assert.IsTrue(attempts[0].Success());

                    if (count++ / num_ur_per_kr == 0)
                    {
                        Assert.AreEqual(attempts[0].Sequence_number(), "1234");
                        Assert.AreEqual(attempts[0].Shard_id(), "shardId-000000000000");
                    }
                    else
                    {
                        Assert.AreEqual(attempts[0].Sequence_number(), "4567");
                        Assert.AreEqual(attempts[0].Shard_id(), "shardId-000000000001");
                    }
                },
                (ur) =>
                {
                    Assert.Fail("Retry should not be called");
                },
                (dt) =>
                {
                    Assert.Fail("Shard map invalidate should not be called");
                });

            retrier.put(result);

            Assert.AreEqual(count, num_kr * num_ur_per_kr);
        }

        Amazon.Kinesis.Model.PutRecordsResponse error_outcome(System.Net.HttpStatusCode errorCode) 
        {
          var resp = new Amazon.Kinesis.Model.PutRecordsResponse();
          resp.HttpStatusCode = errorCode;
            //resp.
            //  Aws::Client::AWSError<Aws::Kinesis::KinesisErrors>(
            //      Aws::Kinesis::KinesisErrors::UNKNOWN,`3s    3```1
            //      name,
            //      msg,
            //      false));

            return resp;
        }

        [TestMethod]
        public void RetrierUnitTest_RequestFailure() 
        {
          var start = DateTime.Now;
          var end = start.AddMilliseconds(5);
          var ctx = make_prr_ctx(1, 10, "error_msg", error_outcome(System.Net.HttpStatusCode.InternalServerError), start, end);

          int count = 0;
          Retrier retrier = new Retrier(new KPLNETInterface.KPLNETConfiguration(),
              (ur) => {
                Assert.Fail("Finish should not be called");
              },
              (ur) =>
              {
                count++;
                var attempts = ur.Attempts();
                Assert.AreEqual(attempts.Count, 1);
                Assert.IsTrue(attempts[0].Start() == start);
                Assert.IsTrue(attempts[0].End() == start.AddMilliseconds(5));
                Assert.IsTrue(!(bool) attempts[0].Success());
                Assert.AreEqual(attempts[0].Error_code(), "InternalServerError");
                Assert.AreEqual(attempts[0].Error_message(), "error_msg");
              },
              (dt) => 
              {
                Assert.Fail("Shard map invalidate should not be called");
              });

          retrier.put(ctx);

          Assert.AreEqual(count, 10);
        }


        // A mix of success and failures in a PutRecordsResult.
        [TestMethod]
        public void RetrierUnitTest_Partial() 
        {
          var num_ur_per_kr = 10;
          var num_kr = 6;

          var ctx = make_prr_ctx(
              num_kr,
              num_ur_per_kr,
              null,
              new Amazon.Kinesis.Model.PutRecordsResponse()
              {
                  HttpStatusCode = System.Net.HttpStatusCode.OK,
                  FailedRecordCount = 4,
                  Records = new List<Amazon.Kinesis.Model.PutRecordsResultEntry>()
                  {
                      new Amazon.Kinesis.Model.PutRecordsResultEntry()
                      {
                          SequenceNumber = "1234",
                          ShardId = "shardId-000000000000"
                      },
                      new Amazon.Kinesis.Model.PutRecordsResultEntry()
                      {
                          SequenceNumber = "4567",
                          ShardId = "shardId-000000000001"
                      },
                      new Amazon.Kinesis.Model.PutRecordsResultEntry()
                      {
                          ErrorCode = "xx",
                          ErrorMessage = "yy"
                      },
                      new Amazon.Kinesis.Model.PutRecordsResultEntry()
                      {
                          ErrorCode = "InternalFailure",
                          ErrorMessage = "Internal service failure."
                      },
                      new Amazon.Kinesis.Model.PutRecordsResultEntry()
                      {
                          ErrorCode = "ServiceUnavailable",
                          ErrorMessage = ""
                      },
                      new Amazon.Kinesis.Model.PutRecordsResultEntry()
                      {
                          ErrorCode = "ProvisionedThroughputExceededException",
                          ErrorMessage = "..."
                      },
                  }
              },
              DateTime.Now,
              DateTime.Now.AddMilliseconds(5));

          int count = 0;
          Retrier retrier = new Retrier(
              new KPLNETInterface.KPLNETConfiguration(),
              (ur) => {
                var attempts = ur.Attempts();
                Assert.AreEqual(attempts.Count, 1);

                var record_group = count++ / num_ur_per_kr;

                if (record_group == 0) 
                {
                  Assert.IsTrue(attempts[0].Success());
                  Assert.AreEqual(attempts[0].Sequence_number(), "1234");
                  Assert.AreEqual(attempts[0].Shard_id(), "shardId-000000000000");
                } else if (record_group == 1)  {
                    Assert.IsTrue(attempts[0].Success());
                  Assert.AreEqual(attempts[0].Sequence_number(), "4567");
                  Assert.AreEqual(attempts[0].Shard_id(), "shardId-000000000001");
                } else if (record_group == 2) {
                    Assert.IsFalse(attempts[0].Success());
                  Assert.AreEqual(attempts[0].Error_code(), "xx");
                  Assert.AreEqual(attempts[0].Error_message(), "yy");
                }
              },
              (ur) => {
                var attempts = ur.Attempts();
                Assert.AreEqual(attempts.Count, 1);
                Assert.IsFalse(attempts[0].Success());

                var record_group = count++ / num_ur_per_kr;

                if (record_group == 3) {
                  Assert.AreEqual(attempts[0].Error_code(), "InternalFailure");
                  Assert.AreEqual(attempts[0].Error_message(), "Internal service failure.");
                } else if (record_group == 4)  {
                  Assert.AreEqual(attempts[0].Error_code(), "ServiceUnavailable");
                  Assert.AreEqual(attempts[0].Error_message(), "");
                } else if (record_group == 5)  {
                  Assert.AreEqual(attempts[0].Error_code(), "ProvisionedThroughputExceededException");
                  Assert.AreEqual(attempts[0].Error_message(), "...");
                }
              },
              (dt) => {
                Assert.Fail("Shard map invalidate should not be called");
              });

          retrier.put(ctx);

          Assert.AreEqual(count, num_kr * num_ur_per_kr);
        }

        [TestMethod]
        public void RetrierUnitTest_FailIfThrottled() 
        {
          var ctx = make_prr_ctx(
                          1,
                          10,
                          null,
                          new Amazon.Kinesis.Model.PutRecordsResponse()
                              {
                                  FailedRecordCount = 1,
                                  Records = new List<Amazon.Kinesis.Model.PutRecordsResultEntry>()
                                  {
                                      new Amazon.Kinesis.Model.PutRecordsResultEntry()
                                      {
                                          ErrorCode = "ProvisionedThroughputExceededException",
                                          ErrorMessage = "..."
                                      }
                                  }
                              },
                              DateTime.Now,
                              DateTime.Now.AddMilliseconds(5)
                  );

          var config = new KPLNETInterface.KPLNETConfiguration();
          config.failIfThrottled = true;

          int count = 0;
          Retrier retrier = new Retrier(
              config,
              (ur) => {
                count++;
                var attempts = ur.Attempts();
                Assert.AreEqual(attempts.Count, 1);
                Assert.IsFalse(attempts[0].Success());
                Assert.AreEqual(attempts[0].Error_code(), "ProvisionedThroughputExceededException");
                Assert.AreEqual(attempts[0].Error_message(), "...");
              },
              (ur) => {
                Assert.Fail("Retry should not be called");
              },
              (dt) => {
                  Assert.Fail("Shard map invalidate should not be called");
              });

          retrier.put(ctx);

          Assert.AreEqual(count, 10);
        }

        [TestMethod]
        public void RetrierUnitTest_WrongShard() 
        {
          var ctx = make_prr_ctx
              (
                  1,
                  1,
                  null,
                  new Amazon.Kinesis.Model.PutRecordsResponse()
                  {
                      FailedRecordCount = 0,
                      Records = new List<Amazon.Kinesis.Model.PutRecordsResultEntry>()
                      {
                          new Amazon.Kinesis.Model.PutRecordsResultEntry()
                          {
                              SequenceNumber = "1234",
                              ShardId = "shardId-000000000004"
                          }
                      },
                      HttpStatusCode = System.Net.HttpStatusCode.OK
                  },
                  DateTime.Now,
                  DateTime.Now.AddMilliseconds(5)              
              );

          int count = 0;
          bool shard_map_invalidated = false;

          Retrier retrier = new Retrier(
              new KPLNETInterface.KPLNETConfiguration(),
              (ur) => 
              {
                Assert.Fail("Finish should not be called");
              },
              (ur) => 
              {
                count++;
                var attempts = ur.Attempts();
                Assert.AreEqual(attempts.Count, 1);
                Assert.IsTrue(!attempts[0].Success());
                Assert.AreEqual(attempts[0].Error_code(), "Wrong Shard");
              },
              (dt) => 
              {
                shard_map_invalidated = true;
              });

          retrier.put(ctx);

          Assert.IsTrue(shard_map_invalidated); //"Shard map should've been invalidated."
          Assert.AreEqual(count, 1);
        }
    }
}
