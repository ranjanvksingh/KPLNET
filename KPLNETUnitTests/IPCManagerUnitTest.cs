using System;
using System.Threading;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using Aws.Kinesis.Protobuf;

using KPLNET.Kinesis.Core;

namespace KPLNETUnitTests
{
    [TestClass]
    public class IPCManagerUnitTest
    {
        class Wrapper
        {
            Random rand = new Random();
            IpcManager ipcManager_;
            KPLNETClientInterface.Daemon testDaemon_;
            public Wrapper()
            {
                string inPipe = "testInPipe" + rand.Next() + "_deleteme";
                string outPipe = "testOutPipe" + rand.Next() + "_deleteme";
                testDaemon_ = KPLNETClientInterface.Daemon.GetTestDaemon(outPipe, inPipe);
                Thread.Sleep(100);
                ipcManager_ = new IpcManager(new IpcChannel(inPipe, outPipe));
                Thread.Sleep(100);
            }

            public void Dispose()
            {
                testDaemon_.destroy();
                ipcManager_.shutdown();
                ipcManager_.close_write_channel();
                ipcManager_.close_read_channel();
                Thread.Sleep(100);
            }

            public void put(Message msg)
            {
                ipcManager_.put(msg);
                testDaemon_.add(msg);
            }

            public bool try_take(out Message target)
            {
                return ipcManager_.try_take(out target);
            }

            public bool try_takeDeamon(out Message target)
            {
                return testDaemon_.Try_Take(out target, true);
            }

        }

        // We hook two IpcManagers up, sending data from one to the other. We check that
        // the data read is the same as the data wrote. We test different data sizes,
        // including those near and beyond the allowed range.
        [TestMethod]
        public void IPCManagerUnitTest_DataIntegrity()
        {
            Wrapper wrapper = new Wrapper();

            List<Message> read = new List<Message>();
            List<Message> readDeamon = new List<Message>();
            List<Message> wrote = new List<Message>();

            Action<int> f = (sz) =>
            {
                string data = TestUtil.random_string(sz);
                string data_copy = data;
                var mWrote = new Message() { PutRecord = new PutRecord() { Data = Google.Protobuf.ByteString.CopyFrom(data, System.Text.Encoding.Default) } };
                wrapper.put(mWrote);
                wrote.Add(mWrote);
            };

            for (int i = 0; i < 1024; i++)
            {
                f(i);
            }

            for (int i = 2 * 1024 * 1024 - 24; i <= 2 * 1024 * 1024-16; i++)
            {
                f(i);
            }

            for (int i = 2 * 1024 * 1024 + 1; i < 2 * 1024 * 1024 + 8; i++)
            {
                try
                {
                    f(i);
                    Assert.Fail("Shoudld've failed for data larger than 2MB");
                }
                catch (Exception e)
                {
                    // ok
                }
            }

            Thread.Sleep(500);

            Message mRead = null;
            while (wrapper.try_take(out mRead))
            {
                read.Add(mRead);
            }
            while (wrapper.try_takeDeamon(out mRead))
            {
                readDeamon.Add(mRead);
            }
            wrapper.Dispose();

            Assert.AreEqual(read.Count, wrote.Count);
            Assert.AreEqual(readDeamon.Count, wrote.Count);

            for (int i = 0; i < read.Count; i++)
            {
                Assert.AreEqual(read[i].PutRecord.Data.ToString(System.Text.Encoding.Default), wrote[i].PutRecord.Data.ToString(System.Text.Encoding.Default));
                Assert.AreEqual(readDeamon[i].PutRecord.Data.ToString(System.Text.Encoding.Default), wrote[i].PutRecord.Data.ToString(System.Text.Encoding.Default));
            }
        }
    }
}
