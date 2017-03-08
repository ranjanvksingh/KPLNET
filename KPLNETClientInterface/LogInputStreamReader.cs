using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

using KPLNETInterface;

namespace KPLNETClientInterface
{
    class LogInputStreamReader
    {
        private readonly ILogger log = null;
        private static readonly Regex LEVEL_REGEX = new Regex(@"(?<level>trace|debug|info|warn(?:ing)?|error|fatal)", RegexOptions.Multiline);
        private static readonly Dictionary<String, Action<ILogger, string>> EMITTERS = makeEmitters();
        private readonly String streamType;
        private readonly StreamReader reader;
        private readonly Action<ILogger, string> logFunction;
        private volatile bool running = true;
        private volatile bool shuttingDown = false;
        private bool isReadingRecord = false;
        private readonly List<String> messageData = new List<String>();
        private static Dictionary<String, Action<ILogger, string>> makeEmitters()
        {
            var emitters = new Dictionary<String, Action<ILogger, string>>();
            emitters.Add("trace", (log, message) => { log.trace(message); });
            emitters.Add("debug", (log, message) => { log.debug(message); });
            emitters.Add("info", (log, message) => { log.info(message); });
            emitters.Add("warn", (log, message) => { log.warn(message); });
            emitters.Add("warning", (log, message) => { log.warn(message); });
            emitters.Add("error", (log, message) => { log.error(message); });
            emitters.Add("fatal", (log, message) => { log.error(message); });

            return emitters;
        }

        public LogInputStreamReader(StreamReader reader, string streamType, Action<ILogger, string> logFunction, ILogger log)
        {
            this.logFunction = logFunction;
            this.reader = reader;
            this.streamType = streamType;
            this.log = log;
        }

        public void Run()
        {
            while (running)
            {
                String logLine;
                try
                {
                    logLine = reader.ReadLine();
                    //log.trace(string.Format("Considering({0}): {1}", streamType, logLine));
                    if (logLine == null)
                        continue;

                    if (logLine.StartsWith("++++"))
                    {
                        startRead();
                    }
                    else if (logLine.StartsWith("----"))
                    {
                        finishRead();
                    }
                    else if (isReadingRecord)
                    {
                        messageData.Add(logLine);
                    }
                    else
                    {
                        logFunction(log, logLine);
                    }
                }
                catch (IOException ioex)
                {
                    if (shuttingDown)
                    {
                        // Since the Daemon calls destroy instead of letting the process exit normally
                        // the input streams coming from the process will end up truncated.
                        // When we know the process is shutting down we can report the exception as info
                        if (ioex.Message == null || !ioex.Message.Contains("Stream closed"))
                        {
                            // If the message is "Stream closed" we can safely ignore it. This is probably a bug
                            // with the UNIXProcess#ProcessPipeInputStream that it throws the exception. There
                            // is no other way to detect the other side of the request being closed.
                            log.info(string.Format("Received IO Exception during shutdown.  This can happen, but should indicate  that the stream has been closed: {0}", ioex.Message));

                        }
                    }
                    else
                    {
                        log.error("Caught IO Exception while reading log line" + ioex.ToString());
                    }

                }
            }
            if (messageData.Count != 0)
            {
                logFunction(log, makeMessage());
            }
        }

        private void finishRead()
        {
            if (!isReadingRecord)
            {
                log.warn(string.Format("{0}: Terminator encountered, but wasn't reading record.", streamType));
            }
            isReadingRecord = false;
            if (messageData.Count != 0)
            {
                String message = makeMessage();
                string lvlName;
                GetLevelOrDefaultLoggingFunction(message, out lvlName)(log, string.IsNullOrEmpty(lvlName) ? message : message.Replace(lvlName + ":" , ""));
            }
            else
            {
                log.warn(string.Format("{0}: Finished reading record, but didn't find any message data.", streamType));
            }
            messageData.Clear();
        }

        private void startRead()
        {
            isReadingRecord = true;
            if (messageData.Count != 0)
            {
                log.warn(string.Format(@"{0}: New log record started, but message data has existing data: {1}", streamType, makeMessage()));
                messageData.Clear();
            }
        }

        private Action<ILogger, string> GetLevelOrDefaultLoggingFunction(String message, out string levelName)
        {
            levelName = "";
            Match match = LEVEL_REGEX.Match(message);
            if (match.Success)
            {
                levelName = match.Groups["level"].Value;
                if (levelName != null)
                {
                    var res = EMITTERS[levelName.ToLower()];
                    if (res != null)
                        return res;
                }
            }

            return (log, msg) => { log.warn("!!Failed to extract level!! - " + msg); };
        }

        private string makeMessage()
        {
            return string.Join("\n", messageData.ToArray());
        }

        public void shutdown()
        {
            this.running = false;
        }

        public void prepareForShutdown()
        {
            this.shuttingDown = true;
        }
    }
}
