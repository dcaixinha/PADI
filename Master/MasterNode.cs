using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Collections.Generic;
using PADI_DSTM;

namespace Master {

	public class MasterNode {

		static void Main(string[] args) {
            TcpChannel channel = new TcpChannel(8086);
            ChannelServices.RegisterChannel(channel, false);

            RemotingConfiguration.RegisterWellKnownServiceType(typeof(Master),
                "Master", WellKnownObjectMode.Singleton);

			System.Console.WriteLine("<enter> para sair...");
			System.Console.ReadLine();
		}
	}

    //Objecto remoto do master, atraves do qual os servidores o contactam
    public class Master : MarshalByRefObject, IMasterClient, IMasterServer
    {
        public override object InitializeLifetimeService()
        {
            return null;
        }

        private Queue<string> roundRobin = new Queue<string>();
        private int txIdCounter = 0;

        private SortedDictionary<int, ServerInfo> servers = new SortedDictionary<int, ServerInfo>(); // ex: < (beginning of the interval), "192.12.51.42:4004" >

        //Locks
        private Object registerServerLock = new Object();
        private Object txIdLock = new Object();
        private Object BootstrapClientLock = new Object();
        private Object StatusLock = new Object();

        public MasterPackage RegisterServer(string serverAddrPort)
        {
            lock (registerServerLock)
            {
                if (!DstmUtil.ServerInfoContains(servers, serverAddrPort))
                {
                    roundRobin.Enqueue(serverAddrPort); //Acrescenta ah round robin de servidores
                    SInfoPackage pack = DstmUtil.InsertServer(serverAddrPort, servers, null);
                    Console.WriteLine("Registered: " + serverAddrPort);
                    return new MasterPackage(servers, pack.getServerWhoTransfers());
                }
            }
            return null;
        }

        public int getTxId()
        {
            lock (txIdLock)
            {
                return ++txIdCounter;
            }
        }

        //Client calls this to bootstrap himself and get a server
        public string BootstrapClient(string addrPort)
        {
            lock (BootstrapClientLock)
            {
                string serverAddrPort = roundRobin.Dequeue();
                roundRobin.Enqueue(serverAddrPort);
                return serverAddrPort;
            }
        }

        //Client calls this to show status
        public void Status()
        {
            lock (StatusLock)
            {
                printSelfStatus();
                //Contacta todos os servidores para lhes pedir para mostrar o status
                foreach (ServerInfo sInfo in servers.Values)
                {
                    string server = sInfo.getPortAddress();
                    try
                    {
                        IServerMaster serv = (IServerMaster)Activator.GetObject(typeof(IServerMaster),
                            "tcp://" + server + "/Server");
                        serv.printSelfStatus();
                    }
                    catch (Exception e) { Console.WriteLine(e); }
                }
            }
        }

        private void printSelfStatus(){
            Console.WriteLine("=============");
            Console.WriteLine("Master STATUS");
            Console.WriteLine("=============");
            Console.WriteLine("TxId counter: " + txIdCounter);
            DstmUtil.ShowQueue(roundRobin);
            DstmUtil.ShowServerList(servers);
        }
    }

}