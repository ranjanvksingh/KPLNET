namespace KPLNETDaemon
{
    class Program
    {
        public static void Main(string[] args)
        {
            var options = new KPLNETInterface.KPLDaemonOptions();
            if (!CommandLine.Parser.Default.ParseArguments(args, options))
            {
                KPLNET.Utils.StdErrorOut.Instance.StdError("Usage:\n KPLNET.exe {in_pipe} {out_pipe} {serialized Configuration} + [serializaed SetCredentials...]\n");
                return;
            }

            KPLNET.KPLNETService.StartChildProcessAsDeamon(options);
        }
    }
}
