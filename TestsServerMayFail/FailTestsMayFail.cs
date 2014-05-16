using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using PADI_DSTM;

namespace TestsServerMayFail
{
    class FailTestsMayFail
    {
        public void testFail()
        {
            Console.WriteLine("testFail");
            Console.WriteLine("Use IMayFailHere method on canCommit or Commit...");
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

                Console.WriteLine("Status, press any key to commit...");
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

                Console.WriteLine("AccessPadint " + uid3 + " (will contact the failed server)...");
                padint3 = PadiDstm.AccessPadInt(uid3);

                Console.WriteLine("Reading value (should be " + value3 + ")...");
                int obtained = padint3.Read();
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
            FailTestsMayFail test = new FailTestsMayFail();
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.testFail();
            Console.ReadLine();
        }
    }
}
