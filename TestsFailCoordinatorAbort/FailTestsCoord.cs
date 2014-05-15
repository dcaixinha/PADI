using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PADI_DSTM;

namespace TestsFailCoordinatorAbort
{
    class FailTestsCoord
    {
        public void testFail()
        {
            Console.WriteLine("testFail");

            Console.WriteLine("Register 3 servers and press any key...");
            Console.ReadLine();

            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int obtained;
            int uid1 = 1;
            int value1 = 41;
            int uid2 = 1000000000;
            int value2 = 42;
            int uid3 = 5;
            int value3 = 43;

            int faillingServerPort = 2001;

            PadInt padint1 = new PadInt(null, null, -1);
            PadInt padint2 = new PadInt(null, null, -1);
            PadInt padint3 = new PadInt(null, null, -1);

            try
            {
                Console.WriteLine("CreatePadint " + uid1 + "...");
                padint1 = PadiDstm.CreatePadInt(uid1);

                Console.WriteLine("Writing value " + value1 + "...");
                padint1.Write(value1);

                Console.WriteLine("CreatePadint " + uid2 + "...");
                padint2 = PadiDstm.CreatePadInt(uid2);

                Console.WriteLine("Writing value " + value2 + "...");
                padint2.Write(value2);

                Console.WriteLine("CreatePadint " + uid3 + "...");
                padint3 = PadiDstm.CreatePadInt(uid3);

                Console.WriteLine("Writing value " + value3 + "...");
                padint3.Write(value3);

                Console.WriteLine("Reading value (should be " + value1 + ")...");
                obtained = padint1.Read();
                Console.WriteLine("Obtained: " + obtained);

                Console.WriteLine("Status, 3 padints not committed");
                Console.WriteLine("Now observe the replicas... ");
                Console.WriteLine("Press any key to fail the coordinator " + faillingServerPort + "...");
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
                Console.WriteLine("Press any key to to try reading a value from the first padint...");
                PadiDstm.Status();
                Console.ReadLine();

            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            try
            {
                Console.WriteLine("Reading value from padint 1 (should be " + value1 + ")...");
                obtained = padint1.Read();
                Console.WriteLine("Obtained: " + obtained);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
            catch (System.Runtime.Remoting.RemotingException) { }


            Console.WriteLine("Status, notice how the objects and replicas moved and press any key to abort...");
            PadiDstm.Status();
            Console.ReadLine();

            Console.WriteLine("Aborting...");
            PadiDstm.TxAbort();

            Console.WriteLine("Status, press any key to finish the test...");
            PadiDstm.Status();
            Console.ReadLine();

            PadiDstm.CloseChannel();
            Thread.Sleep(2000);
        }

        static void Main()
        {
            FailTestsCoord test = new FailTestsCoord();
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.testFail();
            Console.ReadLine();
        }
    }
}
