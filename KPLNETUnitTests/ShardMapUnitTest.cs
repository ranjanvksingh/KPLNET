using System;
using System.Numerics;
using System.Threading;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using KPLNETInterface;

namespace KPLNETUnitTests
{
    [TestClass]
    public class ShardMapUnitTest
    {
        const string kStreamName = "myStream";

        public class MockKinesisClient : KPLNET.Core.Clients.KinesisClient.AwsKinesisClient
        {

            List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes_;
            public Action callback_;
            static Amazon.Kinesis.AmazonKinesisConfig fake_client_cfg()
            {
                Amazon.Kinesis.AmazonKinesisConfig cfg = new Amazon.Kinesis.AmazonKinesisConfig();
                cfg.RegionEndpoint = Amazon.RegionEndpoint.USWest1;
                cfg.ProxyHost = "localhost";
                cfg.ProxyPort = 61666;
                return cfg;
            }

            const Amazon.Runtime.AWSCredentials kEmptyCreds = null;
            public MockKinesisClient(List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes, Action callback = null)
                : base(new KPLNET.Utils.IoServiceExecutor(1), new AWSCredentials(), AWSRegions.USWest1)
            {
                outcomes_ = outcomes;
                callback_ = callback;
                if (callback_ == null)
                    callback_ = () => { };
            }

            public void DescribeStreamAsync(Amazon.Kinesis.Model.DescribeStreamRequest request, Action<KPLNET.Core.Clients.KinesisClient.AwsKinesisClient, Amazon.Kinesis.Model.DescribeStreamRequest, Amazon.Kinesis.Model.DescribeStreamResponse, object> handler, object context)
            {
                executor_.Schedule(() =>
                {
                    if (outcomes_.Count == 0)
                        return;

                    var outcome = outcomes_[0];
                    outcomes_.RemoveAt(0);
                    handler(this, request, outcome, context);
                    callback_();
                }, DateTime.Now.AddMilliseconds(20));
            }
        }

        public class Wrapper : KPLNET.Kinesis.Core.ShardMap
        {
            public Wrapper(List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes, int delay = 1500)
                : base(
                    new KPLNET.Utils.IoServiceExecutor(1),
                    new KPLNETConfiguration(),
                    null,
                    new MockKinesisClient(outcomes, () => { }),
                    null,
                    "US-West-1",
                    kStreamName,
                    null,
                    100,
                    1000)
            {
                //foreach (var res in outcomes)
                //    update_callback(new KPLNET.Core.Clients.KinesisClient.AwsKinesisResult(new KPLNET.Core.Clients.KinesisClient.AwsKinesisResponse(res), null, DateTime.Now, DateTime.Now.AddMilliseconds(5)));

                System.Threading.Thread.Sleep(delay);
            }

            protected override void KinesisClientUpdate(string start_shard_id = "")
            {
                (kinesis_client as MockKinesisClient).DescribeStreamAsync
                    (
                        null, 
                        (client, dsreq, dsres, cntx) => 
                        { 
                            update_callback(new KPLNET.Core.Clients.KinesisClient.AwsKinesisResult(new KPLNET.Core.Clients.KinesisClient.AwsKinesisResponse(dsres), null, DateTime.Now, DateTime.Now.AddMilliseconds(5))); 
                        }, 
                        null
                    );
            }

            public long shard_id(string key)
            {
                return base.Shard_id(BigInteger.Parse(key));
            }
        }

        [TestMethod]
        public void ShardMapUnitTest_Basic()
        {
            List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes = new List<Amazon.Kinesis.Model.DescribeStreamResponse>();
            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
            {
                HttpStatusCode = System.Net.HttpStatusCode.OK,
                StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                {
                    StreamStatus = "ACTIVE",
                    StreamName = "test",
                    StreamARN = @"arn:aws:kinesis:us-west-2:263868185958:stream\/test",
                    Shards = new List<Amazon.Kinesis.Model.Shard>()
                    {
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "170141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549167410945534708633744510750617797212193316405248018"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "85070591730234615865843651857942052862",
                                StartingHashKey = "0"
                            },
                            ShardId = "shardId-000000000002",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978943246555030591128013184047489460388642160674"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "170141183460469231731687303715884105727",
                                StartingHashKey = "85070591730234615865843651857942052863"
                            },
                            ShardId = "shardId-000000000003",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978965547300229121751154719765762108750148141106"
                            }
                        }
                    }
                }
            });

            Wrapper wrapper = new Wrapper(outcomes);

            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105728"), 1);
            Assert.AreEqual(wrapper.shard_id("340282366920938463463374607431768211455"), 1);
            Assert.AreEqual(wrapper.shard_id("0"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052862"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052863"), 3);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105727"), 3);
        }

        [TestMethod]
        public void ShardMapUnitTest_ClosedShards()
        {

            List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes = new List<Amazon.Kinesis.Model.DescribeStreamResponse>();
            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
            {
                HttpStatusCode = System.Net.HttpStatusCode.OK,
                StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                {
                    StreamStatus = "ACTIVE",
                    StreamName = "test",
                    StreamARN = @"arn:aws:kinesis:us-west-2:263868185958:stream\/test",
                    Shards = new List<Amazon.Kinesis.Model.Shard>()
                    {
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "170141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                EndingSequenceNumber = "49549167410956685081233009822320176730553508082787287058",
                                StartingSequenceNumber = "49549167410945534708633744510750617797212193316405248018"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "85070591730234615865843651857942052862",
                                StartingHashKey = "0"
                            },
                            ShardId = "shardId-000000000002",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978943246555030591128013184047489460388642160674"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "170141183460469231731687303715884105727",
                                StartingHashKey = "85070591730234615865843651857942052863"
                            },
                            ShardId = "shardId-000000000003",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978965547300229121751154719765762108750148141106"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "270141183460469231731687303715884105727",
                                StartingHashKey = "170141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000004",
                            ParentShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549295168948777979169149491056351269437634281436348482"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "270141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000005",
                            ParentShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549295168971078724367680114197886987710282642942328914"
                            }
                        }
                    }
                }
            });

            Wrapper wrapper = new Wrapper(outcomes);

            Assert.AreEqual(wrapper.shard_id("0"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052862"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052863"), 3);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105727"), 3);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105728"), 4);
            Assert.AreEqual(wrapper.shard_id("270141183460469231731687303715884105728"), 5);
            Assert.AreEqual(wrapper.shard_id("340282366920938463463374607431768211455"), 5);
        }

        [TestMethod]
        public void ShardMapUnitTest_PaginatedResults()
        {
            List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes = new List<Amazon.Kinesis.Model.DescribeStreamResponse>();
            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
            {
                HttpStatusCode = System.Net.HttpStatusCode.OK,
                StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                {
                    StreamStatus = "ACTIVE",
                    HasMoreShards = true,
                    StreamName = "test",
                    StreamARN = @"arn:aws:kinesis:us-west-2:263868185958:stream\/test",
                    Shards = new List<Amazon.Kinesis.Model.Shard>()
                    {
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "170141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                EndingSequenceNumber = "49549167410956685081233009822320176730553508082787287058",
                                StartingSequenceNumber = "49549167410945534708633744510750617797212193316405248018"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "85070591730234615865843651857942052862",
                                StartingHashKey = "0"
                            },
                            ShardId = "shardId-000000000002",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978943246555030591128013184047489460388642160674"
                            }
                        }
                        
                    }
                }
            });

            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
            {
                HttpStatusCode = System.Net.HttpStatusCode.OK,
                StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                {
                    StreamStatus = "ACTIVE",
                    HasMoreShards = false,
                    StreamName = "test",
                    StreamARN = @"arn:aws:kinesis:us-west-2:263868185958:stream\/test",
                    Shards = new List<Amazon.Kinesis.Model.Shard>()
                    {                        
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "170141183460469231731687303715884105727",
                                StartingHashKey = "85070591730234615865843651857942052863"
                            },
                            ShardId = "shardId-000000000003",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978965547300229121751154719765762108750148141106"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "270141183460469231731687303715884105727",
                                StartingHashKey = "170141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000004",
                            ParentShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549295168948777979169149491056351269437634281436348482"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "270141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000005",
                            ParentShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549295168971078724367680114197886987710282642942328914"
                            }
                        }
                    }
                }
            });

            Wrapper wrapper = new Wrapper(outcomes);

            Assert.AreEqual(wrapper.shard_id("0"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052862"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052863"), 3);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105727"), 3);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105728"), 4);
            Assert.AreEqual(wrapper.shard_id("270141183460469231731687303715884105728"), 5);
            Assert.AreEqual(wrapper.shard_id("340282366920938463463374607431768211455"), 5);
        }

        [TestMethod]
        public void ShardMapUnitTest_Retry()
        {
            List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes = new List<Amazon.Kinesis.Model.DescribeStreamResponse>();
            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
                {
                    HttpStatusCode = System.Net.HttpStatusCode.BadRequest,
                    StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                    {
                        StreamName = "test"
                    }
                });
            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
            {
                HttpStatusCode = System.Net.HttpStatusCode.OK,
                StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                {
                    StreamStatus = "ACTIVE",
                    StreamName = "test",
                    StreamARN = @"arn:aws:kinesis:us-west-2:263868185958:stream\/test",
                    Shards = new List<Amazon.Kinesis.Model.Shard>()
                    {
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "170141183460469231731687303715884105728"
                            },
                            ShardId = "shardId-000000000001",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549167410945534708633744510750617797212193316405248018"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "85070591730234615865843651857942052862",
                                StartingHashKey = "0"
                            },
                            ShardId = "shardId-000000000002",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978943246555030591128013184047489460388642160674"
                            }
                        },
                        new Amazon.Kinesis.Model.Shard()
                        {
                            HashKeyRange = new Amazon.Kinesis.Model.HashKeyRange()
                            {
                                EndingHashKey = "170141183460469231731687303715884105727",
                                StartingHashKey = "85070591730234615865843651857942052863"
                            },
                            ShardId = "shardId-000000000003",
                            ParentShardId = "shardId-000000000000",
                            SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange()
                            {
                                StartingSequenceNumber = "49549169978965547300229121751154719765762108750148141106"
                            }
                        }
                        
                    }
                }
            });

            Wrapper wrapper = new Wrapper(outcomes);

            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105728"), 1);
            Assert.AreEqual(wrapper.shard_id("340282366920938463463374607431768211455"), 1);
            Assert.AreEqual(wrapper.shard_id("0"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052862"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052863"), 3);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105727"), 3);
        }

        [TestMethod]
        public void ShardMapUnitTest_Invalidate()
        {
            List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes = new List<Amazon.Kinesis.Model.DescribeStreamResponse>();
            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
                {
                    HttpStatusCode = System.Net.HttpStatusCode.OK,
                    StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                    {
                        StreamStatus = "ACTIVE",
                        StreamName = "test",
                        StreamARN = @"arn:aws:kinesis:us-west-2:263868185958:stream\/test",
                        Shards = new List<Amazon.Kinesis.Model.Shard>()
                        {
                            new Amazon.Kinesis.Model.Shard()
                            {
                              HashKeyRange= new Amazon.Kinesis.Model.HashKeyRange() 
                              {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "170141183460469231731687303715884105728"
                              },
                              ShardId = "shardId-000000000001",
                              SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange() 
                              {
                                StartingSequenceNumber = "49549167410945534708633744510750617797212193316405248018"
                              }
                            },
                            new Amazon.Kinesis.Model.Shard()
                                    {
                              HashKeyRange= new Amazon.Kinesis.Model.HashKeyRange() 
                              {
                                EndingHashKey = "85070591730234615865843651857942052862",
                                StartingHashKey = "0"
                              },
                              ShardId = "shardId-000000000002",
                              ParentShardId = "shardId-000000000000",
                              SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange() 
                              {
                                StartingSequenceNumber = "49549169978943246555030591128013184047489460388642160674"
                              }
                            },
                            new Amazon.Kinesis.Model.Shard()
                                    {
                              HashKeyRange= new Amazon.Kinesis.Model.HashKeyRange() 
                              {
                                EndingHashKey = "170141183460469231731687303715884105727",
                                StartingHashKey = "85070591730234615865843651857942052863"
                              },
                              ShardId = "shardId-000000000003",
                              ParentShardId = "shardId-000000000000",
                              SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange() 
                              {
                                StartingSequenceNumber = "49549169978965547300229121751154719765762108750148141106"
                              }
                            }
                        }
                    }
                });

            outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
                {
                    HttpStatusCode = System.Net.HttpStatusCode.OK,
                    StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                    {
                        StreamStatus = "ACTIVE",
                        StreamName = "test",
                        StreamARN = @"arn:aws:kinesis:us-west-2:263868185958:stream\/test",
                        Shards = new List<Amazon.Kinesis.Model.Shard>()
                    {
                        new Amazon.Kinesis.Model.Shard()
                        {
                              HashKeyRange= new Amazon.Kinesis.Model.HashKeyRange() 
                              {
                                EndingHashKey = "340282366920938463463374607431768211455",
                                StartingHashKey = "170141183460469231731687303715884105728"
                              },
                              ShardId = "shardId-000000000005",
                              SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange() 
                              {
                                StartingSequenceNumber = "49549167410945534708633744510750617797212193316405248018"
                              }
                            },
                            new Amazon.Kinesis.Model.Shard()
                                    {
                              HashKeyRange= new Amazon.Kinesis.Model.HashKeyRange() 
                              {
                                EndingHashKey = "85070591730234615865843651857942052862",
                                StartingHashKey = "0"
                              },
                              ShardId = "shardId-000000000006",
                              ParentShardId = "shardId-000000000000",
                              SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange() 
                              {
                                StartingSequenceNumber = "49549169978943246555030591128013184047489460388642160674"
                              }
                            },
                            new Amazon.Kinesis.Model.Shard()
                                    {
                              HashKeyRange= new Amazon.Kinesis.Model.HashKeyRange() 
                              {
                                EndingHashKey = "170141183460469231731687303715884105727",
                                StartingHashKey = "85070591730234615865843651857942052863"
                              },
                              ShardId = "shardId-000000000007",
                              ParentShardId = "shardId-000000000000",
                              SequenceNumberRange = new Amazon.Kinesis.Model.SequenceNumberRange() 
                              {
                                StartingSequenceNumber = "49549169978965547300229121751154719765762108750148141106"
                              }
                            }
                        }
                    }
                });
            Wrapper wrapper = new Wrapper(outcomes);

            // Calling invalidate with a timestamp that's before the last update should
            // not actually invalidate the shard map.
            wrapper.invalidate(DateTime.Now.AddSeconds(-15));

            // Shard map should continue working
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105728"), 1);
            Assert.AreEqual(wrapper.shard_id("340282366920938463463374607431768211455"), 1);
            Assert.AreEqual(wrapper.shard_id("0"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052862"), 2);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052863"), 3);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105727"), 3);

            
            // On the other hand, calling invalidate with a timestamp after the last
            // update should actually invalidate it and trigger an update.
            wrapper.invalidate(DateTime.Now);

            Assert.IsTrue(-1 == wrapper.shard_id("0"));

            //// Calling invalidate again during update should not trigger more requests.
            //for (int i = 0; i < 5; i++)
            //{
            //    wrapper.invalidate(DateTime.Now);
            //    Thread.Sleep(2);
            //}

            Assert.IsTrue(-1 == wrapper.shard_id("0"));

            Thread.Sleep(500);

            // A new shard map should've been fetched
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105728"), 5);
            Assert.AreEqual(wrapper.shard_id("340282366920938463463374607431768211455"), 5);
            Assert.AreEqual(wrapper.shard_id("0"), 6);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052862"), 6);
            Assert.AreEqual(wrapper.shard_id("85070591730234615865843651857942052863"), 7);
            Assert.AreEqual(wrapper.shard_id("170141183460469231731687303715884105727"), 7);
        }

        [TestMethod]
        public void ShardMapUnitTest_Backoff()
        {
            List<Amazon.Kinesis.Model.DescribeStreamResponse> outcomes = new List<Amazon.Kinesis.Model.DescribeStreamResponse>();
            for (int i = 0; i < 25; i++)
            {
                outcomes.Add(new Amazon.Kinesis.Model.DescribeStreamResponse()
                {
                    HttpStatusCode = System.Net.HttpStatusCode.BadRequest,
                    StreamDescription = new Amazon.Kinesis.Model.StreamDescription()
                    {
                        StreamName = "test"
                    }
                });
            }

            Wrapper wrapper = new Wrapper(outcomes, 0);

            var start = DateTime.Now;

            // We have initial backoff = 100, growth factor = 1.5, so the 6th attempt
            // should happen 1317ms after the 1st attempt.
            //while (wrapper.num_req_received() < 6) 
            //{
            //  aws::this_thread::yield();
            //}
            //BOOST_CHECK_CLOSE(aws::utils::seconds_since(start), 1.317, 20);

            // The backoff should reach a cap of 1000ms, so after 5 more seconds, there
            // should be 5 additional attempts, for a total of 11.
            //while (wrapper.num_req_received() < 11) 
            //{
            //  aws::this_thread::yield();
            //}
            //BOOST_CHECK_CLOSE(aws::utils::seconds_since(start), 6.317, 20);

            Thread.Sleep(500);
        }
    }
}
