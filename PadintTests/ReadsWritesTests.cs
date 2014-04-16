using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using Client;
using PADI_DSTM;

namespace PadintTests
{
    class ReadsWritesTests
    {
        public void startReadOwnWritesAndAbort()
        {
            Console.WriteLine("startReadOwnWritesBeforeCommit");
            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid = 24;
            int value = 6;
            
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                PadInt padint = PadiDstm.CreatePadInt(uid);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);

                Console.WriteLine("Reading value (should be " + value + ")...");
                int obtained = padint.Read();
                Console.WriteLine("Obtained: " + obtained);
                if (obtained == value)
                    Console.WriteLine("TEST PASSED!\r\n");
                else Console.WriteLine("FAILED!\r\n");

                PadiDstm.TxAbort();

                PadiDstm.CloseChannel();
                Thread.Sleep(2000);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
        }

        public void startReadOwnWritesAfterCommit()
        {
            Console.WriteLine("startReadOwnWritesAfterCommit");
            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid = 31;
            int value = 9;
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                PadInt padint = PadiDstm.CreatePadInt(uid);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();
            try
            {
                Console.WriteLine("AccessPadint " + uid + "...");
                PadInt padint = PadiDstm.AccessPadInt(uid);

                Console.WriteLine("Reading value (should be " + value + ")...");
                int obtained = padint.Read();
                Console.WriteLine("Obtained: " + obtained);
                if (obtained == value)
                    Console.WriteLine("TEST PASSED!\r\n");
                else Console.WriteLine("FAILED!\r\n");

                PadiDstm.CloseChannel();
                Thread.Sleep(2000);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
        }

        public void startCanNotAccessAfterAbort()
        {
            Console.WriteLine("startCanNotAccessAfterAbort");
            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid = 3000;
            int value = 77;

            PadInt padint;
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                padint = PadiDstm.CreatePadInt(uid);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);

                Console.WriteLine("Aborting...");
                PadiDstm.TxAbort();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            padint = null;
            try
            {
                Console.WriteLine("AccessPadint " + uid + "...");
                padint = PadiDstm.AccessPadInt(uid);
            }
            catch (TxException) { //It should throw a txException
                Console.WriteLine("TEST PASSED!\r\n");
            } finally{
                if(padint!=null)
                    Console.WriteLine("FAILED!\r\n");
                PadiDstm.CloseChannel();
                Thread.Sleep(2000);
            }
        }

        public void failServerTest()
        {
            Console.WriteLine("failServerTest");
            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid = 21;
            int value = 17;
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                PadInt padint = PadiDstm.CreatePadInt(uid);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();
            try
            {
                Console.WriteLine("Failing Server " + DstmUtil.LocalIPAddress() + ":4001" + "...");
                bool res = PadiDstm.Fail("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");
                if (!res)
                {
                    Console.WriteLine("TEST FAILED!\r\n");
                    return;
                }
 
                Console.WriteLine("AccessPadint " + uid + "...");
                PadInt padint = PadiDstm.AccessPadInt(uid);

                Console.WriteLine("TEST FAILED!\r\n");  // Se chegar aqui o teste falhou, devia ter gerado uma exception

                PadiDstm.CloseChannel();
                Thread.Sleep(2000);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }
            catch (System.Runtime.Remoting.RemotingException) 
            {
                PadiDstm.CloseChannel();
                PadiDstm.Recover("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");       // Repor o estado do servidor...
                Console.WriteLine("Caught remoting exception!");
                Console.WriteLine("TEST PASSED!\r\n"); 
            }

        }

        public void freezeServerTest()
        {
            Console.WriteLine("freezeServerTest");
            Console.WriteLine("Init...");
            PadiDstm.Init();

            Console.WriteLine("TxBegin...");
            PadiDstm.TxBegin();

            int uid = 23;
            int value = 13;
            try
            {
                Console.WriteLine("CreatePadint " + uid + "...");
                PadInt padint = PadiDstm.CreatePadInt(uid);

                Console.WriteLine("Writing value " + value + "...");
                padint.Write(value);

                Console.WriteLine("Committing...");
                PadiDstm.TxCommit();
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

            try
            {
                Console.WriteLine("TxBegin...");
                PadiDstm.TxBegin();

                Console.WriteLine("AccessPadint " + uid + "...");
                PadInt padint = PadiDstm.AccessPadInt(uid);
                
                Console.WriteLine("Freezing Server " + DstmUtil.LocalIPAddress() + ":4001" + "...");
                bool res = PadiDstm.Freeze("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");
                if (!res)
                {
                    Console.WriteLine("TEST FAILED!\r\n");
                    return;
                }

                Console.WriteLine("Reading value (should be " + value + ")...");

                // e preciso lancar uma instacia de cliente para fazer recover, pois este txbegin vai bloquear uma vez que o server esta freezed...
                Console.WriteLine("Calling Read, now launch a client debug instance and write the command:  recover ");

                
                int obtained = padint.Read();
                Console.WriteLine("Obtained: " + obtained);
                if (obtained == value)
                    Console.WriteLine("TEST PASSED!\r\n");
                else Console.WriteLine("FAILED!\r\n");
                                
                // nao se pode fazer isto aqui...
                //Console.WriteLine("Recovering Server...");
                //bool recvResult = cn.Recover("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server");

                PadiDstm.CloseChannel();
                Thread.Sleep(2000);
            }
            catch (TxException e) { Console.WriteLine(e.reason); }

        }

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
            ReadsWritesTests test = new ReadsWritesTests();
            Console.WriteLine("Register the server first...");
            Console.WriteLine("Press any key to start the test(s)");
            Console.ReadLine();

            test.startReadOwnWritesAndAbort();
            test.startReadOwnWritesAfterCommit();
            test.startCanNotAccessAfterAbort();
            test.freezeServerTest();
            test.failServerTest();
            //test.testRedistribution();
            Console.ReadLine();
        }
    }
    
}
