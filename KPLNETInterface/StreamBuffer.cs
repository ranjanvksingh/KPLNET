using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace KPLNETInterface
{
    public class StreamBuffer
    {
        private Stream ioStream;

        public StreamBuffer(Stream ioStream)
        {
            this.ioStream = ioStream;
        }

        public byte[] ReadBuffer()
        {
            int len = 0;
            for (int i = 0; i < 4; i++) //read all 4 bytes to create int
                len = len * 256 + ioStream.ReadByte();

            byte[] inBuffer = new byte[len];
            ioStream.Read(inBuffer, 0, len);
            return inBuffer;
        }

        public int WriteBuffer(byte[] outBuffer)
        {
            int len = outBuffer.Length;
            byte[] lenBytes = BitConverter.GetBytes(len);
            for (int i = 3; i >= 0; i--) //read all 4 bytes to create int
                ioStream.WriteByte(lenBytes[i]);

            ioStream.Write(outBuffer, 0, len);
            ioStream.Flush();

            return outBuffer.Length + 4;
        }
    }
}
