using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace KPLNETInterface
{
    public class Log4NetLogger : ILogger
    {
        ILog log = null;
        public log4net.Core.Level LogLevel { get { return ((log4net.Repository.Hierarchy.Hierarchy)LogManager.GetRepository()).Root.Level; } }
        public Log4NetLogger(Type t)
        {
            log4net.Config.XmlConfigurator.Configure();
            log = LogManager.GetLogger(t);
        }

        public void trace(string message)
        {
            log.Debug((object)message);
        }

        public void debug(string message)
        {
            log.Debug((object)message);
        }

        public void info(string message)
        {
            log.Info((object)message);
        }

        public void warn(string message)
        {
            log.Warn((object)message);
        }

        public void error(string message)
        {
            log.Error((object)message);
        }

        public void warn(string message, Exception ex)
        {
            log.Warn((object)message, ex);
        }

        public void error(string message, Exception ex)
        {
            log.Error((object)message, ex);
        }
    }
}
