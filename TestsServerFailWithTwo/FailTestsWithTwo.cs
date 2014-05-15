using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PADI_DSTM;

namespace TestsServerFailWithTwo
{
    class FailTestsWithTwo
    {
        public void testFail()
        {
            Console.WriteLine("testFail");

            Console.WriteLine("Register 2 servers and press any key...");
            Console.ReadLine();

            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid2 = 1000000000;
            int value2 = 42;
            int uid1 = 1;
            int value1 = 41;

            int faillingServerPort = 2002;

            PadInt padint = new PadInt(null, null, -1);

            try
            {
                Console.WriteLine("CreatePadint " + uid2 + "...");
                padint = PadiDstm.CreatePadInt(uid2);

                Console.WriteLine("Writing value " + value2 + "...");
                padint.Write(value2);

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();

                Console.WriteLine("TxBegin...");
                PadiDstm.TxBegin();

                Console.WriteLine("CreatePadint " + uid1 + "...");
                padint = PadiDstm.CreatePadInt(uid1);

                Console.WriteLine("Writing value " + value1 + "...");
                padint.Write(value1);

                Console.WriteLine("Status, press any key to continue the test...");
                PadiDstm.Status();
                Console.ReadLine();

                Console.WriteLine("Failing Server " + DstmUtil.LocalIPAddress() + ":" + faillingServerPort + "...");
                bool res = PadiDstm.Fail("tcp://" + DstmUtil.LocalIPAddress() + ":" + faillingServerPort + "/Server");
                if (!res)
                {
                    Console.WriteLine("TEST FAILED!\r\n");
                    return;
                }

                Console.WriteLine("Status, server 2 failed but server 1 hasnt noticed yet");
                Console.WriteLine("Press any key to continue the test...");
                PadiDstm.Status();
                Console.ReadLine();

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            try
            {
                Console.WriteLine("TxBegin...");
                PadiDstm.TxBegin();

                Console.WriteLine("AccessPadint " + uid2 + " (will contact the failed server)...");
                padint = PadiDstm.AccessPadInt(uid2);

                Console.WriteLine("Reading value (should be " + value2 + ")...");
                int obtained = padint.Read();
                Console.WriteLine("Obtained: " + obtained);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("Status, press any key to finish the test...");
            PadiDstm.Status();
            Console.ReadLine();

            PadiDstm.CloseChannel();
            Thread.Sleep(2000);
        }

        static void Main()
        {
            FailTestsWithTwo test = new FailTestsWithTwo();
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.testFail();
            Console.ReadLine();
        }
    }
}
