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
        public void startReadOwnWritesAndAbort()
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
                    Console.WriteLine("TEST PASSED!\r\n");
                else Console.WriteLine("FAILED!\r\n");

                cn.TxAbort();

                cn.CloseChannel();
                Thread.Sleep(2000);
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
                    Console.WriteLine("TEST PASSED!\r\n");
                else Console.WriteLine("FAILED!\r\n");

                cn.CloseChannel();
                Thread.Sleep(2000);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
        }

        public void startCanNotAccessAfterAbort()
        {
            Console.WriteLine("startCanNotAccessAfterAbort");
            ClientNode cn = new ClientNode();
            Console.WriteLine("Init...");
            cn.Init();
            Thread.Sleep(2000);

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            int uid = 3000;
            int value = 77;

            PadInt padint;
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                padint = cn.CreatePadInt(uid);
                Thread.Sleep(2000);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);
                Thread.Sleep(2000);

                Console.WriteLine("Aborting...");
                cn.TxAbort();
                Thread.Sleep(2000);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            padint = null;
            try
            {
                Console.WriteLine("AccessPadint " + uid + "...");
                padint = cn.AccessPadInt(uid);
            }
            catch (TxException) { //It should throw a txException
                Console.WriteLine("TEST PASSED!\r\n");
            } finally{
                if(padint!=null)
                    Console.WriteLine("FAILED!\r\n");
                cn.CloseChannel();
                Thread.Sleep(2000);
            }
        }


        static void Main()
        {
            ReadsWritesTests test = new ReadsWritesTests();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.startReadOwnWritesAndAbort();
            test.startReadOwnWritesAfterCommit();
            test.startCanNotAccessAfterAbort();
            Console.ReadLine();
        }
    }
    
}
