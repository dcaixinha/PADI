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
        public string text = "";

        private SortedDictionary<int, ServerInfo> servers = new SortedDictionary<int, ServerInfo>(); // ex: < (beginning of the interval), "192.12.51.42:4004" >

        //CHAT
        public void Send(string message)
        {
            text = text + message;
            //Console.WriteLine(message);
            Broadcast(message);
        }

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

        //Metodo interno, que obtem invoca nos objectos remotos de cada servidor,
        //um metodo que vai conter a resposta do master (excepto no que enviou)
        private void Broadcast(string msg)
        {
            foreach (ServerInfo sInfo in servers.Values)
            {
                IServerMaster serv = (IServerMaster)Activator.GetObject(typeof(IServerMaster),
                    "tcp://" + sInfo.getPortAddress() + "/Server");
                try
                {
                    serv.Update(msg);
                }
                catch (Exception e) { Console.WriteLine(e); }
            }
        }

        public bool Fail(string serverURL)
        {
            try
            {
                IServerMaster serverFailing = (IServerMaster)Activator.GetObject(
                    typeof(IServerMaster),
                    "tcp://" + serverURL + "/Server");

                bool result = serverFailing.Fail();
                return result;
            }
            catch (Exception e)
            {
                if (e is System.Net.Sockets.SocketException || e is NullReferenceException)
                {
                    System.Console.WriteLine("The master could not locate server");
                    return false;
                }
                else throw;
            }
        }

        public bool Freeze(string serverURL)
        {
            try
            {
                IServerMaster serverFailing = (IServerMaster)Activator.GetObject(
                    typeof(IServerMaster),
                    "tcp://" + serverURL + "/Server");

                bool result = serverFailing.Freeze();
                return result;
            }
            catch (Exception e)
            {
                if (e is System.Net.Sockets.SocketException || e is NullReferenceException)
                {
                    System.Console.WriteLine("The master could not locate server");
                    return false;
                }
                else throw;
            }
        }

        public bool Recover(string serverURL)
        {
            try
            {
                IServerMaster serverRecovering = (IServerMaster)Activator.GetObject(
                    typeof(IServerMaster),
                    "tcp://" + serverURL + "/Server");

                bool result = serverRecovering.Recover();
                return result;
            }
            catch (Exception e)
            {
                if (e is System.Net.Sockets.SocketException || e is NullReferenceException)
                {
                    System.Console.WriteLine("The master could not locate server");
                    return false;
                }
                else throw;
            }
        }


    }

}