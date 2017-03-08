using System;
using System.Threading;
using KPLNETInterface;

namespace KPLNET.Utils
{
    public class StdErrorOut
    {
        public LogType Type { get; set; }
        public LogLevel Level {get; set;}
        public ILogger Logger { get; set; }

        public static readonly StdErrorOut Instance = new StdErrorOut();

        private StdErrorOut() { }

        public void StdOut(LogLevel level, string message)
        {
            if (level < Level)
                return;

            if (Type == LogType.Console)
            {
                Console.WriteLine(string.Format("++++ \n{0}: [Daemon:{1}] {2} \n----", level.ToString(), Thread.CurrentThread.ManagedThreadId.ToString(), message));
            }
            else if (Type == LogType.Logger && Logger != null)
            {
                string msgFormatted = string.Format("[Daemon:{0}] {1}", Thread.CurrentThread.ManagedThreadId.ToString(), message);
                if (level == LogLevel.debug)
                    Logger.debug(msgFormatted);
                else if (level == LogLevel.error)
                    Logger.error(msgFormatted);
                else if (level == LogLevel.info)
                    Logger.info(msgFormatted);
                else if (level == LogLevel.trace)
                    Logger.trace(msgFormatted);
                else if (level == LogLevel.warn)
                    Logger.warn(msgFormatted);
            }

        }

        public void StdError(string message)
        {
            if (Type == LogType.Console)
                Console.WriteLine(string.Format("++++ \n{0} : [Daemon:{1}] {2} \n----", LogLevel.error.ToString(), Thread.CurrentThread.ManagedThreadId.ToString(), message));
            else if (Logger != null)
                Logger.error(string.Format("[Daemon:{0}] {1}", Thread.CurrentThread.ManagedThreadId.ToString(), message));
        }

        public void StdError(string message, Exception ex)
        {
            if (Type == LogType.Console)
                Console.WriteLine(string.Format("[Daemon:{0}] {1} {2}", Thread.CurrentThread.ManagedThreadId.ToString(), message, ex.ToString()));
        }
    }

    public enum LogType
    {
        Console,
        Logger
    }

    public enum LogLevel
    {
        trace = 0,
        debug = 1,
        info = 2,
        warn = 3,
        error = 4
    }
}
