using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Collections.Generic;
using DSTM;

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
        private Queue<string> roundRobin = new Queue<string>();
        private int txIdCounter = 0;

        private SortedDictionary<int, ServerInfo> servers = new SortedDictionary<int, ServerInfo>(); // ex: < (beginning of the interval), "192.12.51.42:4004" >


        public SortedDictionary<int, ServerInfo> RegisterServer(string serverAddrPort)
        {
            if (!DstmUtil.ServerInfoContains(servers, serverAddrPort))
            {
                roundRobin.Enqueue(serverAddrPort); //Acrescenta ah round robin de servidores
                DstmUtil.InsertServer(serverAddrPort, servers);

                return servers;
            }
            return null;
        }

        public int getTxId()
        {
            return ++txIdCounter;
        }

        //Client calls this to bootstrap himself and get a server
        public string BootstrapClient(string addrPort)
        {
            string serverAddrPort = roundRobin.Dequeue();
            roundRobin.Enqueue(serverAddrPort);
            return serverAddrPort;
        }

    }

}