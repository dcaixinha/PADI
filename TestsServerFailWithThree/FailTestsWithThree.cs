using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PADI_DSTM;

namespace TestsServerFailWithThree
{
    class FailTestsWithThree
    {
        public void testFail()
        {
            Console.WriteLine("testEntry");

            Console.WriteLine("Register 3 servers and press any key...");
            Console.ReadLine();

            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid1 = 1;
            int value1 = 41;
            int uid2 = 1000000000;
            int value2 = 42;
            int uid3 = 5;
            int value3 = 43;
            int uid4 = 9;
            int value4 = 53;

            int faillingServerPort = 2003;

            PadInt padint = new PadInt(null, null, -1);

            try
            {
                Console.WriteLine("CreatePadint " + uid1 + "...");
                padint = PadiDstm.CreatePadInt(uid1);

                Console.WriteLine("Writing value " + value1 + "...");
                padint.Write(value1);

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();

                Console.WriteLine("TxBegin...");
                PadiDstm.TxBegin();

                Console.WriteLine("CreatePadint " + uid2 + "...");
                padint = PadiDstm.CreatePadInt(uid2);

                Console.WriteLine("Writing value " + value2 + "...");
                padint.Write(value2);

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();

                Console.WriteLine("TxBegin...");
                PadiDstm.TxBegin();

                Console.WriteLine("CreatePadint " + uid3 + "...");
                padint = PadiDstm.CreatePadInt(uid3);

                Console.WriteLine("Writing value " + value3 + "...");
                padint.Write(value3);

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();

                Console.WriteLine("TxBegin...");
                PadiDstm.TxBegin();

                Console.WriteLine("CreatePadint " + uid4 + "...");
                padint = PadiDstm.CreatePadInt(uid4);

                Console.WriteLine("Writing value " + value4 + "...");
                padint.Write(value4);

                Console.WriteLine("Status, 3 padints committed and a 4th on server 3 not committed");
                Console.WriteLine("Now observe the replicas... ");
                Console.WriteLine("Press any key to fail server " + faillingServerPort + "...");
                PadiDstm.Status();
                Console.ReadLine();

                Console.WriteLine("Failing Server " + DstmUtil.LocalIPAddress() + ":" + faillingServerPort + "...");
                bool res = PadiDstm.Fail("tcp://" + DstmUtil.LocalIPAddress() + ":" + faillingServerPort + "/Server");
                if (!res)
                {
                    Console.WriteLine("Failed while trying to fail? :(\r\n");
                    return;
                }

                Console.WriteLine("Status, server " + faillingServerPort + " failed but the others haven't noticed yet");
                Console.WriteLine("Press any key to continue the test...");
                PadiDstm.Status();
                Console.ReadLine();

            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            try
            {
                Console.WriteLine("Reading value (shouldn't be able to read anything here)...");
                int obtained = padint.Read();
                Console.WriteLine("Obtained: " + obtained);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
            catch (System.Runtime.Remoting.RemotingException) { }

            Console.WriteLine("Status, register a new server and press any key to continue...");
            PadiDstm.Status();
            Console.ReadLine();

            Console.WriteLine("Status, notice how the objects and replicas moved");
            Console.WriteLine("Press any key to create padint 4...");
            PadiDstm.Status();
            Console.ReadLine();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            Console.WriteLine("CreatePadint " + uid4 + "...");
            padint = PadiDstm.CreatePadInt(uid4);

            Console.WriteLine("Writing value " + value4 + "...");
            padint.Write(value4);

            Console.WriteLine("Committing...");
            PadiDstm.TxCommit();

            Console.WriteLine("Status, press any key to finish the test...");
            PadiDstm.Status();
            Console.ReadLine();

            PadiDstm.CloseChannel();
            Thread.Sleep(2000);
        }

        static void Main()
        {
            FailTestsWithThree test = new FailTestsWithThree();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.testFail();
            Console.ReadLine();
        }
    }
}
