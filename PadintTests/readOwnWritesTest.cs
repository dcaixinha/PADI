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
    class readOwnWritesTest
    {
        public void start()
        {
            ClientNode cn = new ClientNode();
            Console.WriteLine("Init...");
            cn.Init();
            Thread.Sleep(2000);

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            int uid = 24;
            int value = 6;
            Console.WriteLine("CreatePadint " + uid + "...");
            try
            {
                PadInt padint = cn.CreatePadInt(uid);
                Thread.Sleep(2000);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);
                Thread.Sleep(2000);

                Console.WriteLine("Reading value (should be " + value + ")...");
                Console.WriteLine("Obtained: "+value);
                if (padint.Read() == 6)
                    Console.WriteLine("TEST PASSED!");
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
        }

        static void Main()
        {
            readOwnWritesTest test = new readOwnWritesTest();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test");
            Console.ReadLine();
            test.start();
            Console.ReadLine();
        }
    }
    
}
