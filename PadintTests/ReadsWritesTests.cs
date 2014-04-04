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
    class ReadsWritesTests
    {
        public void startReadOwnWritesBeforeCommit()
        {
            Console.WriteLine("startReadOwnWritesBeforeCommit");
            ClientNode cn = new ClientNode();
            Console.WriteLine("Init...");
            cn.Init();
            Thread.Sleep(2000);

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            int uid = 24;
            int value = 6;
            
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                PadInt padint = cn.CreatePadInt(uid);
                Thread.Sleep(2000);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);
                Thread.Sleep(2000);

                Console.WriteLine("Reading value (should be " + value + ")...");
                int obtained = padint.Read();
                Console.WriteLine("Obtained: " + obtained);
                if (obtained == value)
                    Console.WriteLine("TEST PASSED!");
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
        }

        public void startReadOwnWritesAfterCommit()
        {
            Console.WriteLine("startReadOwnWritesAfterCommit");
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
                Thread.Sleep(2000);
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
                int obtained = padint.Read();
                Console.WriteLine("Obtained: " + obtained);
                if (obtained == value)
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
            //SO UM TESTE DE CADA VEZ!

            //test.startReadOwnWritesBeforeCommit();
            test.startReadOwnWritesAfterCommit(); //TODO este esta a falhar... o valor n passa por o committed value...
            Console.ReadLine();
        }
    }
    
}
