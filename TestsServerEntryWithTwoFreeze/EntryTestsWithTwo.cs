using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PADI_DSTM;

namespace TestsServerEntryWithTwoFreeze
{
    class EntryTestsWithTwo
    {
        public void testEntry()
        {
            Console.WriteLine("testEntry");

            Console.WriteLine("Register 1 server and press any key...");
            Console.ReadLine();

            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            Console.WriteLine("Now register server 2 and press any key...");
            Console.ReadLine();

            int uid = 5;
            int value = 333;
            PadInt padint = new PadInt(null, null, -1);

            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                padint = PadiDstm.CreatePadInt(uid);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);

            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("Status...");
            PadiDstm.Status();

            Console.WriteLine("Freezing Server 2 -> " + DstmUtil.LocalIPAddress() + ":2002" + "...");
            bool res = PadiDstm.Freeze("tcp://" + DstmUtil.LocalIPAddress() + ":2002" + "/Server");
            if (!res)
            {
                Console.WriteLine("TEST FAILED!\r\n");
                return;
            }

            Console.WriteLine("Now register Server 3 and press any key...");
            Console.ReadLine();

            Console.WriteLine("Status...");
            PadiDstm.Status();

            Console.WriteLine("Calling Read, the method call won't return until Server 2 is unfrozen!");
            Console.WriteLine("Now launch a client debug instance and write the command:  recover <port of server 2>");
            try
            {
                int valueRead = padint.Read();
                Console.WriteLine("Value read was: " + valueRead);

                Console.WriteLine("Status...");
                PadiDstm.Status();

                Console.WriteLine("Press any key to commit...");
                Console.ReadLine();

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();

                Console.WriteLine("Status...");
                PadiDstm.Status();

                Console.WriteLine("Tentative writes should be empty, committed write should have the values...");
                Console.ReadLine();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            PadiDstm.CloseChannel();
            Thread.Sleep(2000);
        }

        static void Main()
        {
            EntryTestsWithTwo test = new EntryTestsWithTwo();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.testEntry();
            Console.ReadLine();
        }
    }
}
