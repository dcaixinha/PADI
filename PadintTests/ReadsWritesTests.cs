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

        public void failServerTest()
        {
            Console.WriteLine("failServerTest");
            ClientNode cn = new ClientNode();
            Console.WriteLine("Init...");
            cn.Init();
            Thread.Sleep(2000);

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            int uid = 21;
            int value = 17;
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
                Console.WriteLine("Failing Server " + DstmUtil.LocalIPAddress() + ":4001" + "...");
                bool res = cn.Fail("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");
                if (!res)
                {
                    Console.WriteLine("TEST FAILED!\r\n");
                    return;
                }
                Thread.Sleep(2000);

                
                Console.WriteLine("AccessPadint " + uid + "...");
                PadInt padint = cn.AccessPadInt(uid);

                Console.WriteLine("TEST FAILED!\r\n");  // Se chegar aqui o teste falhou, devia ter gerado uma exception

                cn.CloseChannel();
                Thread.Sleep(2000);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
            catch (System.Runtime.Remoting.RemotingException) 
            {
                cn.CloseChannel();
                cn.Recover("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");       // Repor o estado do servidor...
                Console.WriteLine("Caught remoting exception!");
                Console.WriteLine("TEST PASSED!\r\n"); 
            }

        }

        public void freezeServerTest()
        {
            Console.WriteLine("freezeServerTest");
            ClientNode cn = new ClientNode();
            Console.WriteLine("Init...");
            cn.Init();
            Thread.Sleep(2000);

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            int uid = 23;
            int value = 13;
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

            try
            {
                Console.WriteLine("Freezing Server " + DstmUtil.LocalIPAddress() + ":4001" + "...");
                bool res = cn.Freeze("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");
                if (!res)
                {
                    Console.WriteLine("TEST FAILED!\r\n");
                    return;
                }

                // e preciso lancar uma instacia de cliente para fazer recover, pois este txbegin vai bloquear uma vez que o server esta freezed...
                Console.WriteLine("Calling TxBegin, now launch a client debug instance and write the command:  recover ");

                cn.TxBegin();                

                //Console.WriteLine("Recovering Server...");
                //bool recvResult = cn.Recover("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");
                Console.WriteLine("Check if in the Server Console was displayed 'HELLO WORLD!'. If yes, TEST PASSED! TEST FAILED otherwise.\r\n");

                cn.CloseChannel();
                Thread.Sleep(500);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

        }

        //When more servers enter, those that are already running have to update their one-hop lookup tables
        //and one of the servers (whose interval will be cut in half) will potentially have to redistribute
        //some of its padints
        public void testRedistribution()
        {
            Console.WriteLine("testStatus");
            ClientNode cn = new ClientNode();

            Console.WriteLine("Register 2 servers and press any key...");
            Console.ReadLine();

            Console.WriteLine("Init...");
            cn.Init();
            Thread.Sleep(2000);

            Console.WriteLine("TxBegin...");
            cn.TxBegin();
            Thread.Sleep(2000);

            int uid = 0;
            int value = 4;
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

            Console.WriteLine("Status...");
            cn.Status();
            Thread.Sleep(2000);

            Console.WriteLine("Now register 2 more servers and press any key...");
            Console.ReadLine();

            Console.WriteLine("Status...");
            cn.Status();
            Thread.Sleep(2000);

            Console.WriteLine("Verificar que o objecto 0 foi redistribuido do servidor 1 po servidor 4...");

            cn.CloseChannel();
            Thread.Sleep(2000);
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
            test.failServerTest();
            test.freezeServerTest();
            //test.testRedistribution();
            Console.ReadLine();
        }
    }
    
}
