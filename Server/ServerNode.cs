using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Net.Sockets;
using System.Windows.Forms;
using DSTM;
using System.Collections.Generic;

namespace Server {

	public class ServerNode {

        //Constants
        public static string masterAddrPort = "localhost:8086";

        SortedDictionary<int, ServerInfo> servers;
        IMasterServer masterObj;
        string address = DstmUtil.LocalIPAddress();
        string porto;
        string myself;
        Server serv; //O seu objecto remoto

        public void Register(string porto){

            //Cria o seu canal nesse porto
            TcpChannel channel = new TcpChannel(Convert.ToInt32(porto));
            ChannelServices.RegisterChannel(channel, false);

            //Instancia o seu objecto remoto, atraves do qual o master lhe envia respostas e o client pedidos
            //RemotingConfiguration.RegisterWellKnownServiceType(typeof(Server),
            //    "Server", WellKnownObjectMode.Singleton);
            serv = new Server(); 
            RemotingServices.Marshal(serv, "Server", typeof(Server) ); 

            //Obtem o objecto remoto do master bem conhecido
            masterObj = (IMasterServer)Activator.GetObject(typeof(IMasterServer),
                "tcp://" + masterAddrPort + "/Master");

            this.porto = porto;
            this.myself = address + ":" + porto;

            //O servidor regista-se no master
            servers = masterObj.RegisterServer(myself);
            //Actualiza o seu objecto remoto
            serv.UpdateServerList(myself, servers);

        }

        public void Send(string msg)
        {
            try
            {
                //Console.WriteLine(msg);
                masterObj.Send(msg+"\r\n");
            }catch(SocketException)
	 		{
	 			System.Console.WriteLine("Could not locate master");
	 		}
        }

        static void Main(string[] args)
        {
            ServerNode sn = new ServerNode();
            string input;
            while (true)
            {
                input = Console.ReadLine();
                string[] paramtrs = input.Split(' ');
                //REGISTER
                if (paramtrs[0].Equals("register"))
                {
                    if (paramtrs.Length > 1)
                    {
                        string porto = paramtrs[1];
                        if (!porto.Equals("") && Convert.ToInt32(porto) > 1024 && Convert.ToInt32(porto) <= 65535)
                            sn.Register(porto);
                        else Console.WriteLine("Couldn't register the server, port '" + porto +
                            "' is invalid!\r\nTry a port between 1025 and 65535");
                    }
                    else
                        Console.WriteLine("Wrong command format. User 'register <port>'");
                }
                else 
                    sn.Send(input);
            }
        }
	}

    //Objecto remoto dos servidores, atraves do qual o master envia respostas
    public class Server : MarshalByRefObject, IServerClient, IServerServer, IServerMaster
    {
        private SortedDictionary<string, int> clients = new SortedDictionary<string, int>(); // ex: < "193.34.126.54:6000", (txId) >
        private SortedDictionary<int, ServerInfo> servers; // ex: < begin, object ServerInfo(begin, end, portAddress) >
        private string myself;

        public string GetAddress() { return myself; }

        //Response from Master after successful registration
        public void UpdateServerList(string serverAddrPort, SortedDictionary<int, ServerInfo> servers)
        {
            this.servers = servers;
            this.myself = serverAddrPort;

            //Notify all others of the update
            foreach (ServerInfo sInfo in servers.Values)
            {
                string server = sInfo.getPortAddress();
                if (!server.Equals(myself))
                {
                    try
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + server + "/Server");
                        serv.UpdateNetwork(serverAddrPort);
                    }
                    catch (Exception e) { Console.WriteLine(e); }
                }
            }
            
        }

        //Other servers call this to change topology after node join or leave
        public void UpdateNetwork(string serverAddrPort)
        {
            if (!DstmUtil.ServerInfoContains(servers, serverAddrPort)) //se ainda nao contem o elemento
            { 
                DstmUtil.InsertServer(serverAddrPort, servers); //TODO ver que objectos tem que passar, ou se tem?
            }
        }

        //CHAT: Master calls this to update the server
        public void Update(string message)
        {
            Console.WriteLine(message);
            if (clients.Keys.Count > 0)
            {
                foreach (KeyValuePair<string, int> item in clients)
                {
                    IClientServer client = (IClientServer)Activator.GetObject(typeof(IClientServer), 
                        "tcp://" + item.Key + "/Client");
                    try
                    {
                        client.Update(message);
                    }
                    catch (Exception e) { Console.WriteLine(e); }
                }
            }
        }

        //Here the client already knows it is assigned to this server and registers with it
        public void RegisterClient(string addrPort)
        {
            clients.Add(addrPort, -1);
        }

        //CHAT: Client calls this to send a message to the server (this server is simply forwarding to the master)
        public void Send(string message, string porto)
        {
            IMasterServer master = (IMasterServer)Activator.GetObject(typeof(IMasterServer), 
                "tcp://" + ServerNode.masterAddrPort + "/Master");
            try
            {
                master.Send(message);
            }
            catch (Exception e) { Console.WriteLine(e); }
        }

        //Client to server
        public bool TxBegin(string clientPortAddress)
        {
            //Se o cliente ja tem 1 tx a decorrer
            if (!clients.ContainsKey(clientPortAddress) || clients[clientPortAddress] != -1)
                return false;
            else
            {
                //Contacta o master para obter um txId
                IMasterServer master = (IMasterServer)Activator.GetObject(typeof(IMasterServer),
                    "tcp://" + ServerNode.masterAddrPort + "/Master");
                int id = master.getTxId();
                clients[clientPortAddress] = id; //update tx ID for this client
                DstmUtil.ShowClientsList(clients);
                return true;
            }
        }

        //Client-Server
        public PadInt CreatePadInt(string clientPortAddress, int uid)
        {
            //verifica se o client tem 1 tx aberta
            if (clients[clientPortAddress] == -1) throw new TxException("O cliente nao tem nenhuma Tx aberta!");
            //hash do uid
            int hash = DstmUtil.HashMe(uid);
            //verifica quem eh o servidor responsavel
            string responsible = DstmUtil.GetResponsibleServer(servers, uid);
            PadInt result=null;

            if (responsible != null)
            {
                if (!responsible.Equals(myself)) //Se nao eh o prorio, procura o servidor correcto
                {                     
                    try
                    {   IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + responsible + "/Server");
                        result = serv.CreatePadInt(uid);
                    }
                    catch (Exception e) { Console.WriteLine(e); }

                    return result;
                }
                else //o proprio eh o responsavel
                {
                    return CreatePadInt(uid); 
                }
            }
            else
                return null;
        }

        //Server-Server TODO
        public PadInt CreatePadInt(int uid)
        {
            return null;
        }

    }

}