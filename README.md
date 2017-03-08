# KPLNET (Kinesis Producer Library .NET)
KPL Wrapper and Native libraries written in .NET.

Libraries:

1. KPLNETClientInterface is KPL wrapper module written in .NET.

2. KPLNET is KPL Native module written in .NET

3. KPLNETDaemon is .NET console application to run KPLNET as a daemon.

4. KPLNETTest is a sample application using KPLNET to publish records to a Kinesis stream.

5. KinesisConsumerTest is a sample application to consume KPLNET aggregated records from a Kinesis stream.

Note:
KPL can be configured to run as daemon or can be invoked as a library using the KPLNETConfiguration runAsDaemon true or false repectively.

The HTTPMachine module taken from the github repository https://github.com/bvanderveen/httpmachine. This is currently not in use since I have used AWS .NET SDK to put records to a kinesis stream. 
