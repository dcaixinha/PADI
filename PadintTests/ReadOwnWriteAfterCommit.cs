using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using Client;
using DSTM;

namespace PadintTests
{
    class ReadOwnWriteAfterCommit
    {
        public void start()
        {
            Console.WriteLine("ReadOwnWritesAfterCommit");
            ClientNode cn = new ClientNode();
            Console.WriteLine("Init...");
            cn.Init();
            Thread.Sleep(2000);

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            int uid = 31;
            int value = 9;
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                PadInt padint = cn.CreatePadInt(uid);
                Thread.Sleep(2000);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);
                Thread.Sleep(2000);

                Console.WriteLine("Committing...");
                cn.TxCommit();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);
            try
            {
                Console.WriteLine("AccessPadint " + uid + "...");
                PadInt padint = cn.AccessPadInt(uid);
                Thread.Sleep(2000);

                Console.WriteLine("Reading value (should be " + value + ")...");
                Console.WriteLine("Obtained: " + value);
                if (padint.Read() == value)
                    Console.WriteLine("TEST PASSED!");
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
        }

        static void Main()
        {
            ReadsWritesTests test = new ReadsWritesTests();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test");
            Console.ReadLine();
            test.start();
            Console.ReadLine();
        }


    }
}
