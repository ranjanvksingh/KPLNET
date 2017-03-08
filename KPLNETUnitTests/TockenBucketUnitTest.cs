using System.Threading;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace KPLNETUnitTests
{
    [TestClass]
    public class TockenBucketUnitTest
    {
        [TestMethod]
        public void TockenBucketUnitTest_Basic()
        {
            int max = 200;
            int rate = 1000;

            KPLNET.Utils.TokenBucket b = new KPLNET.Utils.TokenBucket();
            b.add_token_stream(max, rate);

            Assert.IsFalse(b.try_take(new List<double>() { max + 1 }));
            Assert.IsTrue(b.try_take(new List<double>() { max }));

            Thread.Sleep(100);
            Assert.IsFalse(b.try_take(new List<double>() { 150 }));
            Assert.IsTrue(b.try_take(new List<double>() { 90 }));

            Thread.Sleep(max + 200);

            Assert.IsFalse(b.try_take(new List<double>() { max + 1}));
            Assert.IsTrue(b.try_take(new List<double>() { max }));
        }

        [TestMethod]
        public void TockenBucketUnitTest_Multiple()
        {
            KPLNET.Utils.TokenBucket b = new KPLNET.Utils.TokenBucket();
            b.add_token_stream(200, 1000);
            b.add_token_stream(500, 2000);

            Assert.IsFalse(b.try_take(new List<double>() { 201, 0 }));
            Assert.IsFalse(b.try_take(new List<double>() { 0, 501 }));
            Assert.IsTrue(b.try_take(new List<double>() { 200, 500 }));

            Thread.Sleep(100);
            Assert.IsFalse(b.try_take(new List<double>() { 110, 0 }));
            Assert.IsFalse(b.try_take(new List<double>() { 0, 220 }));
            Assert.IsTrue(b.try_take(new List<double>() { 90, 180 }));

            Thread.Sleep(5000);

            Assert.IsTrue(!b.try_take(new List<double>() { 201, 0 }));
            Assert.IsTrue(!b.try_take(new List<double>() { 0, 501 }));
            Assert.IsTrue(b.try_take(new List<double>() { 200, 500 }));
        }
    }
}
