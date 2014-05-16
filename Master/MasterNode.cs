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
        private Dictionary<string, string> crashedServers = new Dictionary<string, string>(); //crashed addrPort, nextToCrashed (for commits/aborts)

        private SortedDictionary<int, ServerInfo> servers = new SortedDictionary<int, ServerInfo>(); // ex: < (inicio do intervalo), "192.12.51.42:4004" >

        //Locks
        private Object serversLock = new Object();
        private Object txIdLock = new Object();
        private Object roundRobinLock = new Object();
        private Object StatusLock = new Object();

        public MasterPackage RegisterServer(string serverAddrPort)
        {
            Boolean hasServerInfo;
            lock (serversLock)
            {
                hasServerInfo = DstmUtil.ServerInfoContains(servers, serverAddrPort);
            }
            if (!hasServerInfo)
            {
                SInfoPackage pack;
                lock (roundRobinLock)
                {
                    roundRobin.Enqueue(serverAddrPort); //Acrescenta ah round robin de servidores
                }
                lock (serversLock)
                {
                    pack = DstmUtil.InsertServer(serverAddrPort, servers, null);
                }
                Console.WriteLine("Registered: " + serverAddrPort);
                return new MasterPackage(servers, pack.getServerWhoTransfers());
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

        public string GetNextToCrashed(string crashedServerAddrPort)
        {
            lock (serversLock)
            {
                if(crashedServers.ContainsKey(crashedServerAddrPort))
                    return crashedServers[crashedServerAddrPort];
                else return null;
            }
        }

        //Retorna true se eh a primeira vez que o crash deste servidor eh detectado
        public Boolean DetectedCrash(string crashedServerAddrPort)
        {
            Boolean alreadyDetectedCrash;
            lock (serversLock)
            {
                alreadyDetectedCrash = crashedServers.ContainsKey(crashedServerAddrPort);
            }
            if (alreadyDetectedCrash)
                return false;
            else //Eh preciso remover este servidor
            {
                string serverNextToFailed = DstmUtil.GetNextServer(crashedServerAddrPort, servers);
                Boolean hasServerInfo = false;
                lock (serversLock)
                {
                    crashedServers.Add(crashedServerAddrPort, serverNextToFailed);
                    hasServerInfo = DstmUtil.ServerInfoContains(servers, crashedServerAddrPort);
                }
                if (hasServerInfo)
                {
                    //Remove o elemento da roundRobin
                    lock (roundRobinLock)
                    {
                        while (true)
                        {
                            string poppedServer = roundRobin.Dequeue();
                            if (poppedServer.Equals(crashedServerAddrPort))
                                break;
                            roundRobin.Enqueue(poppedServer);
                        }
                    }
                    lock (serversLock)
                    {
                        DstmUtil.RemoveServer(crashedServerAddrPort, servers, null);
                    }
                    Console.WriteLine("Unregistered: " + crashedServerAddrPort);
                }
                return true;
            }
        }

        //Quando o cliente detecta que o seu coordenador falhou pede ao master para lhe dar o endereço
        //do novo coordenador. Mas antes o master vai dar ordem ao seguinte do que falhou para que este passe
        //a tabela de coordenador que tinha como replica do que falhou para efectiva (para depois poder continuar
        //a tx do cliente).
        public string MyCoordinatorFailed(string clientAddrPort, string failedServerAddrPort)
        {
            Boolean iDetectedFirst = DetectedCrash(failedServerAddrPort);
            string nextServer = GetNextToCrashed(failedServerAddrPort);
            //Se eu fui o primeiro a detectar tenho de avisar todos os servidores (que sobreviveram) 
            //que este servidor caiu
            if (iDetectedFirst)
            {
                foreach (ServerInfo sInfo in servers.Values)
                {
                    string server = sInfo.getPortAddress();
                    IServerMaster serv = (IServerMaster)Activator.GetObject(typeof(IServerMaster),
                        "tcp://" + server + "/Server");
                    serv.UpdateNetworkAfterCrash(failedServerAddrPort);
                }
            }
            //Nao eh preciso avisar o novo coord que tem 1 novo cliente, porque ele ja vai ter essa informacao replicada
            //e quando for notificado da saida do servidor que falhou, ele torna-se no novo coordenador das tx que o antigo
            //coordenava

            //Retornar o endereço do next ao cliente (que sera o novo coordenador)
            return nextServer;
        }

        //Cliente chama este metodo para fazer bootstrap e ser-lhe atribuido um servidor coordenador
        public string BootstrapClient(string addrPort)
        {
            lock (roundRobinLock)
            {
                string serverAddrPort = roundRobin.Dequeue();
                roundRobin.Enqueue(serverAddrPort);
                return serverAddrPort;
            }
        }

        //Cliente chama o status
        public void Status()
        {
            lock (StatusLock)
            {
                printSelfStatus();
            }
            ServerInfo[] serverInfoArray = new ServerInfo[servers.Keys.Count];
            lock(serversLock){
                servers.Values.CopyTo(serverInfoArray, 0);
            }
            //Contacta todos os servidores para lhes pedir para mostrar o status
            foreach (ServerInfo sInfo in serverInfoArray)
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