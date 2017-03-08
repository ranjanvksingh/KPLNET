using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using CommandLine;
using CommandLine.Text;

namespace KPLNETInterface
{
    public class KPLDaemonOptions
    {
        [Option('o', "outpipe", Required = true, HelpText = "Out Pipe Absolute Path.")]
        public string Outpipe { get; set; }

        [Option('i', "inpipe", Required = true, HelpText = "In Pipe Absolute Path.")]
        public string Inpipe { get; set; }

        [Option('c', "config", Required = true, HelpText = "KPLNET Configuration.")]
        public string Configuration { get; set; }

        [Option('k', "cred", Required = true, HelpText = "KPLNET credentials.")]
        public string Credentials { get; set; }

        [Option('w', "metricscred", DefaultValue = "", HelpText = "KPLNET metrics credentials.")]
        public string MetricsCredentials { get; set; }

        [Option('t', "clientType", DefaultValue = "KinesisClient", HelpText = "KPLNET client type (HttpClient or KinesisClient).")]
        public string ClientType { get; set; }

        [ParserState]
        public IParserState LastParserState { get; set; }

        [HelpOption]
        public string GetUsage()
        {
            return HelpText.AutoBuild(this, (HelpText current) => HelpText.DefaultParsingErrorsHandler(this, current));
        }
    }
}
