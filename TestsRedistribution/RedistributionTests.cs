using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PADI_DSTM;

namespace TestsRedistribution
{
    class RedistributionTests
    {

        //When more servers enter, those that are already running have to update their one-hop lookup tables
        //and one of the servers (whose interval will be cut in half) will potentially have to redistribute
        //some of its padints
        public void testRedistribution()
        {
            Console.WriteLine("testStatus");

            Console.WriteLine("Register 2 servers and press any key...");
            Console.ReadLine();

            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid = 0;
            int value = 4;
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                PadInt padint = PadiDstm.CreatePadInt(uid);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("Status...");
            PadiDstm.Status();

            Console.WriteLine("Now register 2 more servers and press any key...");
            Console.ReadLine();

            Console.WriteLine("Status...");
            PadiDstm.Status();

            Console.WriteLine("Check that padint 0 was redistributed from server 1 to serverr 4...");
            Console.WriteLine("Check that server 1 is still the coordinator (as the tx has not committed or aborted)...");
            Console.WriteLine("Check that tx-server table on server 1 now points to server 4...");
            Console.WriteLine("Check that tx-obj table on server 1 no longer has the object (because serv 1 is no longer responsible for it)...");
            Console.WriteLine("Press any key to commit...");
            Console.ReadLine();

            Console.WriteLine("Committing...");
            try
            {
                PadiDstm.TxCommit();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("Status...");
            PadiDstm.Status();

            Console.WriteLine("Tentative writes should be empty, committed write should have the values...");

            PadiDstm.CloseChannel();
            Thread.Sleep(2000);
        }

        static void Main()
        {
            RedistributionTests test = new RedistributionTests();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.testRedistribution();
            Console.ReadLine();
        }
    }

}
