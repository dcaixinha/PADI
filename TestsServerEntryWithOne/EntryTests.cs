using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PADI_DSTM;

namespace TestsServerEntryWithOne
{
    class EntryTests
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

            int uid = 1000000000;
            int value = 4;
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

            Console.WriteLine("Now register 1 more server and press any key...");
            Console.ReadLine();

            Console.WriteLine("Status...");
            PadiDstm.Status();

            Console.WriteLine("Check that padint 1000000000 was redistributed from server 1 to server 2...");
            Console.WriteLine("Check that server 1 is still the coordinator (as the tx has not committed or aborted)...");
            Console.WriteLine("Check that tx-server table on server 1 now points to server 2...");
            Console.WriteLine("Check that tx-obj table on server 1 no longer has the object (because serv 1 is no longer responsible for it)...");
            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();

            Console.WriteLine("Reading...");
            try
            {
                int valueRead = padint.Read();
                Console.WriteLine("Value read was: " + valueRead);

                Console.WriteLine("Status...");
                PadiDstm.Status();

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
            EntryTests test = new EntryTests();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.testEntry();
            Console.ReadLine();
        }
    }

}
