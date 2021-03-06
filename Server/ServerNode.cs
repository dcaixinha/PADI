using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Net.Sockets;
using System.Threading;
using System.Windows.Forms;
using PADI_DSTM;
using System.Collections.Generic;
using System.Linq;

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
            MasterPackage pack = masterObj.RegisterServer(myself);
            servers = pack.getServers();
            //Actualiza o seu objecto remoto
            serv.UpdateServerList(myself, pack);

        }

        static void Main(string[] args)
        {
            ServerNode sn = new ServerNode();
            string input;
            Console.WriteLine("Commands:");
            Console.WriteLine("register <port>");
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
                        Console.WriteLine("Wrong command format. Use 'register <port>'");
                }
            }
        }
	}

    //Objecto remoto dos servidores, atraves do qual o master envia respostas
    public class Server : MarshalByRefObject, IServerClient, IServerServer, IServerMaster
    {
        public override object InitializeLifetimeService()
        {
            return null;
        }

        private SortedDictionary<int, ServerInfo> servers; // ex: < begin, object ServerInfo(begin, end, portAddress) >
        
        private SortedDictionary<int, PadIntInsider> myPadInts = new SortedDictionary<int, PadIntInsider>(); //uid, padint
        private SortedDictionary<int, PadIntInsider> replicatedPadInts = new SortedDictionary<int, PadIntInsider>();
        private ServerInfo mySInfo = new ServerInfo(0,0,"");//Guarda o meu intervalo (begin, end, portAdress) 
                                                            //Ter acesso directo a esta estrutura permite rapida
                                                            //verificacao se eh preciso ou nao redistribuir certo padint

        private SortedDictionary<string, int> clients = new SortedDictionary<string, int>(); // ex: < "193.34.126.54:6000", (txId) >

        private SortedDictionary<string, int> replicatedClients = new SortedDictionary<string, int>();

        // Lista mantida pelo coordenador
        // Esta lista serve para no fim, o coordenador saber quem tem que contactar para o commit
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txObjCoordList = new SortedDictionary<int, List<int>>();

        // Lista mantida pelo next de cada coordenador
        // Esta lista serve para no fim, se o coordenador falhar, o master, ou quem detectou a falha
        // saber quem tem que contactar para o commit (ou abort) das tx que o servidor que falhou coordenava.
        // Sao acrecentados elementos a esta lista quando eh feito um create ou access no coordenador
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txObjReplicatedCoordList = new SortedDictionary<int, List<int>>();

        // Lista mantida pelos responsaveis por objectos envolvidos em txs
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txObjList = new SortedDictionary<int, List<int>>();

        // Lista mantida por quem tem replicas do seu previous
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txReplicatedObjList = new SortedDictionary<int, List<int>>();

        // Eh preciso manter uma lista nos responsaveis, dos objectos criados na tx actual, para em caso de abort,
        // o objecto ser removido <txId, lista<uid>>
        private SortedDictionary<int, List<int>> txCreatedObjList = new SortedDictionary<int, List<int>>();

        private SortedDictionary<int, List<int>> txReplicatedCreatedObjList = new SortedDictionary<int, List<int>>();

        private string myself;
        private Boolean midRecovery = false;

        //Pedidos pendentes (durante o freeze) para este servidor < nome do comando, lista de argumentos > 
        private Queue<Action> pendingCommands = new Queue<Action>();
        private List<ManualResetEvent> _queuedThreads = new List<ManualResetEvent>();

        // estados em que o servidor pode estar
        enum State {Normal, Failed, Frozen};

        // estado atual do servidor (para saber se pode aceitar pedidos ou se esta failed/freezed)
        private State currentState = State.Normal;

        //Locks
        private Object stateLock = new Object(); //protege o estado
        private Object serverLock = new Object(); //protege acesso as tabelas de clients and servers
        private Object txLock = new Object(); //protege as tx tables
        private ReaderWriterLockSlim padintsLock = new ReaderWriterLockSlim();
        private Dictionary<int, Object> padintLocks = new Dictionary<int, Object>(); //protegido pela padintsLock

        private ReaderWriterLockSlim replicasLock = new ReaderWriterLockSlim();
        private Dictionary<int, Object> replicaLocks = new Dictionary<int, Object>(); //protegido pela replicasLock

        private Boolean waitingForObjects = true;
        //Enquanto este waiting tiver a false, lanca phantomException nos metodos que sao chamados entre
        //servidores: Create, access, canCoomit, Commit, Abort

        //Listas com valores para enviar
        Dictionary<int, int> objTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
        Dictionary<int, int> objCreatedTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
        List<PadIntInsider> padintToSendList = new List<PadIntInsider>();

        public string GetAddress() { return myself; }


        //ServerNode faz update a este objecto depois do registo com sucesso no master
        public void UpdateServerList(string serverAddrPort, MasterPackage masterPack)
        {
            List<ServerInfo> serversInfo;
            string serverWhoHasMyObjects = masterPack.getServerWhoTransfers();
            Console.WriteLine("Server who has my objects: " + serverWhoHasMyObjects);
            lock (serverLock)
            {
                this.servers = masterPack.getServers();
                this.myself = serverAddrPort;
                serversInfo = servers.Values.ToList();
            }
            //Notifica todos os outros do update
            foreach (ServerInfo sInfo in serversInfo)
            {
                string server = sInfo.getPortAddress();
                if (!server.Equals(myself))
                {
                    try
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + server + "/Server");
                        serv.UpdateNetwork(myself);
                    }
                    catch (Exception e) { Console.WriteLine(e); }
                }
                else
                {
                    lock (txLock)
                    {
                        mySInfo = sInfo;    //Eu actualizo o meu ServerInfo (mantenho isto, para mais tarde a 
                    }                       //redistribuicao dos objectos ser feita mais rapidamente
                }
            }
            //Depois de notificar todos, vou contactar o servidor que podera ter objectos para mim, se esse
            //servidor existir (se eu nao for o primeiro)
            if (serverWhoHasMyObjects != null)
            {
                ServerPackage serverPack = new ServerPackage(null, null, null, null);
                try
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + serverWhoHasMyObjects + "/Server");
                    serverPack = serv.GiveMeMyObjects();
                }
                catch (Exception e) { Console.WriteLine(e); }
                List<PadIntInsider> receivedList = serverPack.GetPadintToSendList();
                Dictionary<int, int> receivedObjTxDict = serverPack.GetObjTxToSendDict();
                Dictionary<int, int> receivedObjCreatedTxDict = serverPack.GetObjCreatedTxToSendDict();
                List<PadIntInsider> receivedReplicas = serverPack.GetReplicas();
                

                if (receivedList != null && receivedList.Count != 0)
                {
                    foreach (PadIntInsider padint in receivedList)
                    {
                        lock (txLock)
                        {
                            myPadInts.Add(padint.UID, padint);
                        }
                        //Cria um lock por cada padint recebido
                        padintsLock.EnterWriteLock();
                        try
                        {
                            padintLocks[padint.UID] = new Object();
                        }
                        finally
                        {
                            padintsLock.ExitWriteLock();
                        }
                            
                    }
                }
                if (receivedObjTxDict != null && receivedObjTxDict.Count != 0)
                {
                    foreach (KeyValuePair<int, int> kvp in receivedObjTxDict)
                    {
                        lock (txLock)
                        {
                            int txId = kvp.Value;
                            int uid = kvp.Key;
                            if (!txObjList.ContainsKey(txId))
                                txObjList.Add(txId, new List<int>());
                            txObjList[txId].Add(uid);
                        }
                    }
                }
                if (receivedObjCreatedTxDict != null && receivedObjCreatedTxDict.Count != 0)
                {
                    foreach (KeyValuePair<int, int> kvp in receivedObjCreatedTxDict)
                    {
                        lock (txLock)
                        {
                            int txId = kvp.Value;
                            int uid = kvp.Key;
                            if (!txCreatedObjList.ContainsKey(txId))
                                txCreatedObjList.Add(txId, new List<int>());
                            txCreatedObjList[txId].Add(uid);
                        }
                    }
                }
                //Faz os set ahs suas proprias replicas com o conteudo que vinha no serverPackage, e envia as 
                //as suas proprias replicas para o seu next
                SetReplicas(receivedReplicas);
                //Envia a lista de replicas para o servidor seguinte (que eh responsavel pelas suas replicas)
                SendNewReplicas(MakeReplicas());
                //Envio a minha info de coordenador ao meu next (que neste caso, como acabei de entrar vao as tabelas
                //vazias vao apenas ter o efeito de limpar as tabelas do meu seguinte da info antiga que conteriam)
                SendCoordInfoToNext();

                //Enviar para o seu next info sobre os seus tx-obj para que possam ser replicados
                SendTxObjInfoToNext();
            }
            lock (txLock)
            {
                waitingForObjects = false; //ja nao estou ah espera quando recebo objectos.
            }
        }

        //Server-Server
        //Os outros servidores chamam este metodo para mudar a topologia da rede depois de um join
        public void UpdateNetwork(string serverAddrPort)
        {
            threadSafeStateCheck();

            Boolean doesNotContainServer;
            lock (serverLock)
            {
                doesNotContainServer = !DstmUtil.ServerInfoContains(servers, serverAddrPort);
            }
            if (doesNotContainServer) //se ainda nao contem o elemento
            {
                ServerInfo newMySInfo;
                string affectedServer = "";
                lock (serverLock)
                {
                    //Insere o novo servidor na lista de servers, e retorna o meu novo (ou nao) ServerInfo
                    newMySInfo = DstmUtil.InsertServer(serverAddrPort, servers, myself).getServerInfo();
                }
                affectedServer = newMySInfo.getPortAddress();
                //Verificar se eu fui afectado pela entrada
                if (affectedServer.Equals(myself))
                {
                    List<PadIntInsider> padints;
                    lock(txLock){
                        //Se sim, tenho de actualizar o mySInfo
                        mySInfo = newMySInfo;
                        padints = myPadInts.Values.ToList();
                    }
                    //obter o meu intervalo novo e ver se eu tenho objectos fora desse intervalo
                    foreach (PadIntInsider padint in padints)
                    {
                        int beginning = newMySInfo.getBegin();
                        int ending = newMySInfo.getEnd();
                        int hashedUid = DstmUtil.HashMe(padint.UID);
                        if (hashedUid > ending || hashedUid < beginning ) //esta fora do intervalo
                        {
                            padintToSendList.Add(padint);
                            List<KeyValuePair<int, List<int>>> txObject;
                            List<KeyValuePair<int, List<int>>> txObjectCreated;
                            lock(txLock){
                                txObject = txObjList.ToList();
                                txObjectCreated = txCreatedObjList.ToList();
                            }
                            //compilar lista de objectos e tx afectadas pela mudanca
                            foreach (KeyValuePair<int, List<int>> kvp in txObject)
                            {
                                if (kvp.Value.Contains(padint.UID))
                                    objTxToSendDict[padint.UID] = kvp.Key;
                            }
                            foreach (KeyValuePair<int, List<int>> kvp in txObjectCreated)
                            {
                                if (kvp.Value.Contains(padint.UID))
                                    objCreatedTxToSendDict[padint.UID] = kvp.Key;
                            }
                        }
                    }
                }
                //Se nao fui afectado pela entrada do novo servidor, nao tenho de fazer mais nada              
            }
        }

        //Server-Server
        //O servidor que entrou chama este metodo no servidor que podera ter objectos do novo.
        //Este metodo so eh chamado quando o que entrou ja recebeu return de todos, i.e.
        //tal como num 2-phase, so vou pedir os objectos quando ja todos souberem que eu sou
        //o novo responsavel pelo range desses objectos.
        //Alem dos objectos, o anterior tambem envia ao novo as suas listas de txObjCoord e a
        //lista de clientes.
        public ServerPackage GiveMeMyObjects()
        {
            //Se tenho objectos para redistribuir, removo-os das minhas listas e envio-os
            if (padintToSendList.Count != 0)
            {
                Console.WriteLine("Transfering " + padintToSendList.Count + " objects to the new server...");
                lock (txLock)
                {
                    //removo da minha lista de padints
                    foreach (PadIntInsider padint in padintToSendList)
                    {
                        myPadInts.Remove(padint.UID);
                    }
                }
                //Remove locks of the objects I will no longer hold
                foreach (PadIntInsider padint in padintToSendList)
                {
                    padintsLock.EnterWriteLock();
                    try
                    {
                        padintLocks.Remove(padint.UID);
                    }
                    finally
                    {
                        padintsLock.ExitWriteLock();
                    }
                }
                //Actualizar: txServerList, txObjList, txCreatedObjList
                //removo da minha lista de tx-obj, pq ja n sou responsavel pelo objecto
                foreach (KeyValuePair<int, int> kvp in objTxToSendDict)
                {
                    int txId = kvp.Value;
                    int uid = kvp.Key;
                    lock (txLock)
                    {
                        txObjList[txId].Remove(uid);
                        if (txObjList[txId].Count == 0)  //se a lista ficou vazia, elimina a entrada
                            txObjList.Remove(txId);
                    }
                }
                //removo da minha lista de tx-objCreated, pq ja n sou responsavel pelo objecto
                foreach (KeyValuePair<int, int> kvp in objCreatedTxToSendDict)
                {
                    int txId = kvp.Value;
                    int uid = kvp.Key;
                    lock (txLock)
                    {
                        txCreatedObjList[txId].Remove(uid);
                        if (txCreatedObjList[txId].Count == 0) //se a lista ficou vazia, elimina a entrada
                        {
                            Thread.Sleep(1000);
                            txCreatedObjList.Remove(txId);
                        }
                    }
                }
                //crio as replicas para enviar ao novo
                List<PadIntInsider> replicas = new List<PadIntInsider>();
                lock (txLock)
                {
                    //removo da minha lista de padints
                    foreach(PadIntInsider padint in myPadInts.Values)
                    {
                        replicas.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
                    }
                }

                ServerPackage pack = new ServerPackage(padintToSendList, objTxToSendDict, objCreatedTxToSendDict, replicas);
                //Envio replicas da minha informacao de coordenador ao seguinte
                SendCoordInfoToNext();
                //Enviar po meu next info sobre os meus txObj restantes para que ele possa replicar
                SendTxObjInfoToNext();
                //Faz reset as listas de send
                objTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
                objCreatedTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
                padintToSendList = new List<PadIntInsider>();
                return pack;
            }
            else
            {
                //Se o meu intervalo foi afetado, mas nao tenho objectos para enviar, envio a mensagem
                //que diz que nao tinha obejctos para enviar, para q o novo saiba que ja pode aceitar 
                //novos pedidos
                Console.WriteLine("No objects to transfer to the new server...");

                //clear ahs listas temporarias
                objTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
                objCreatedTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
                padintToSendList = new List<PadIntInsider>();
                return new ServerPackage(null, null, null, null);
            }
        }

        //Server-Server
        //Metodo invocado pelo servidor que aglomera o previous server. Alem das replicas de padints
        //este servidor tambem envia as listas tx-obj e tx-created obj para serem replicadas
        public ReplicaPackage GiveMeYourReplicas()
        {
            SortedDictionary<int, List<int>> txObjListReplica;
            SortedDictionary<int, List<int>> txCreatedObjListReplica;
            lock (txLock)
            {
                txObjListReplica = DstmUtil.GetTxObjReplicaFrom(txObjList);
                txCreatedObjListReplica = DstmUtil.GetTxObjReplicaFrom(txCreatedObjList);
            }
            List<PadIntInsider> padintReplicas = MakeReplicas();
            ReplicaPackage pack = new ReplicaPackage(padintReplicas, txObjListReplica, txCreatedObjListReplica);
            return pack;
        }

        //Client-Server
        public bool TxBegin(string clientAddressPort)
        {
            threadSafeStateCheck();
            
            bool clientHasTx;
            lock (serverLock)
            {
                clientHasTx = clients.ContainsKey(clientAddressPort);
            }
            //Se o cliente ja tem 1 tx a decorrer
            if (clientHasTx){
                return false;
            }
            else
            {
                //Contacta o master para obter um txId
                IMasterServer master = (IMasterServer)Activator.GetObject(typeof(IMasterServer),
                    "tcp://" + ServerNode.masterAddrPort + "/Master");
                int txId = master.getTxId();
                lock(serverLock){
                    clients.Add(clientAddressPort, txId);//update tx ID for this client
                }
                //Envia para o seu next uma replica da lista de tx-obj que coordena E a tabela dos seus clientes
                SendCoordInfoToNext();
                //DstmUtil.ShowClientsList(clients);
                return true;
            }
            
        }

        //Client-Server
        public PadInt CreatePadInt(string clientAddressPort, int uid)
        {
            threadSafeStateCheck();
            
            string responsible;
            int txId;
            lock (serverLock)
            {
                //verifica se o client tem 1 tx aberta
                if (!clients.ContainsKey(clientAddressPort)) throw new TxException("O cliente nao tem nenhuma Tx aberta!");
                txId = clients[clientAddressPort];

                //verifica quem eh o servidor responsavel (o hash eh feito la dentro do metodo)
                responsible = DstmUtil.GetResponsibleServer(servers, uid);
            }
            if (responsible == null)
                return null; //Nao deve acontecer...
            
            if (!responsible.Equals(myself)) //Se nao eh o prorio, procura o servidor correcto
            {
                try
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + responsible + "/Server");
                    serv.CreatePadInt(uid, txId);
                }
                //Server failure detection
                catch (System.Runtime.Remoting.RemotingException) 
                { 
                    StartRecoveryChain(responsible);
                    //Esperar ate ter a tabela em ordem e repetir a operacao
                    WaitWhileInRecoveryProcess();
                    IServerServer serv = GetResponsibleServerAfterRecovery(uid);
                    serv.CreatePadInt(uid, txId);
                }
                catch (Exception e) { Console.WriteLine(e); }
            }
            else //o proprio eh o responsavel
            {
                CreatePadInt(uid, txId);
            }
            //Nao deve enviar a referencia remota do padint criado
            //Deve criar 1 obejcto proxy local a este servidor, independentemente de ter sido este servidor
            //o responsavel pelo padint ou outro qualquer! 
            //Portanto, o coordenador fica com esta ref remota (uid), e envia um proxy ao cliente.

            lock(txLock){
                //Actualiza a lista de objectos usados nesta tx
                if (!txObjCoordList.ContainsKey(txId))
                    txObjCoordList.Add(txId, new List<int>());
                txObjCoordList[txId].Add(uid);
            }
            //Envia para o seu next uma replica da lista de tx-obj que coordena E a tabela dos seus clientes
            SendCoordInfoToNext();
            //Devolve um proxy ao cliente, com o qual o cliente vai comunicar com este servidor
            PadInt proxy = new PadInt(myself, clientAddressPort, uid);
            return proxy;
            
        }


        //Client-Server
        public PadInt AccessPadInt(string clientAddressPort, int uid)
        {
            threadSafeStateCheck();
            
            string responsible;
            int txId;
            lock (serverLock)
            {
                //verifica se o client tem 1 tx aberta
                if (!clients.ContainsKey(clientAddressPort)) throw new TxException("O cliente nao tem nenhuma Tx aberta!");
                txId = clients[clientAddressPort];

                //devolve o portAddress do servidor responsavel pelo uid
                responsible = DstmUtil.GetResponsibleServer(servers, uid);
            }
            if (responsible == null)
                return null; //Nao deve acontecer...

            if (!responsible.Equals(myself)) //Se nao eh o prorio, procura o servidor correcto
            {
                try
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + responsible + "/Server");
                    serv.AccessPadInt(uid, txId);
                }
                //Server failure detection
                catch (System.Runtime.Remoting.RemotingException)
                {
                    StartRecoveryChain(responsible);
                    //Esperar ate ter a tabela em ordem e repetir a operacao
                    WaitWhileInRecoveryProcess();
                    IServerServer serv = GetResponsibleServerAfterRecovery(uid);
                    serv.AccessPadInt(uid, txId);
                }
                catch (Exception e) { Console.WriteLine(e); }
            }
            else //o proprio eh o responsavel
            {
                AccessPadInt(uid, txId);
            }

            lock (txLock)
            {
                //Actualiza a lista de objectos usados nesta tx
                if (!txObjCoordList.ContainsKey(txId))
                    txObjCoordList.Add(txId, new List<int>());
                txObjCoordList[txId].Add(uid);
            }
            //Envia para o seu next uma replica da lista de tx-obj que coordena
            SendCoordInfoToNext();
            //Devolve um proxy ao cliente, com o qual o cliente vai comunicar com este servidor
            PadInt proxy = new PadInt(myself, clientAddressPort, uid);
            return proxy;
              
        }

        //Client-Server
        public int Read(string clientAddressPort, int uid)
        {
            threadSafeStateCheck();
            
            string responsible;
            int txId;
            lock (serverLock)
            {
                //verifica se o client tem 1 tx aberta
                txId = clients[clientAddressPort];
                if (txId == -1) throw new TxException("O cliente nao tem nenhuma Tx aberta!");

                //verifica quem eh o servidor responsavel (o hash eh feito la dentro do metodo)
                responsible = DstmUtil.GetResponsibleServer(servers, uid);
            }
            int value = -1;

            if (responsible == null)
                throw new TxException("There was no server responsible for this uid!"); //Nao deve acontecer...

            if (!responsible.Equals(myself)) //Se nao eh o prorio, procura o servidor correcto
            {
                try
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + responsible + "/Server");
                    value = serv.Read(uid, txId);
                }
                catch (TxException) { throw; } //Quando tenta ler de um servidor q ainda n tem o objecto (vindo d redistribuicao)
                //Server failure detection
                catch (System.Runtime.Remoting.RemotingException)
                {
                    StartRecoveryChain(responsible);
                    //Esperar ate ter a tabela em ordem e repetir a operacao
                    WaitWhileInRecoveryProcess();
                    IServerServer serv = GetResponsibleServerAfterRecovery(uid);
                    value = serv.Read(uid, txId);
                }
                catch (Exception e) { Console.WriteLine(e); }
            }
            else //o proprio eh o responsavel
            {
                value = Read(uid, txId);
            }
            return value;
        }

        //Client-Server
        public void Write(string clientAddressPort, int uid, int value)
        {
            threadSafeStateCheck();
            
            string responsible;
            int txId;
            lock (serverLock)
            {
                //verifica se o client tem 1 tx aberta
                txId = clients[clientAddressPort];
                if (txId == -1) throw new TxException("O cliente nao tem nenhuma Tx aberta!");

                //verifica quem eh o servidor responsavel (o hash eh feito la dentro do metodo)
                responsible = DstmUtil.GetResponsibleServer(servers, uid);
            }
            if (responsible == null)
                throw new TxException("There was no server responsible for this uid!"); //Nao deve acontecer...

            if (!responsible.Equals(myself)) //Se nao eh o prorio, procura o servidor correcto
            {
                try
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + responsible + "/Server");
                    serv.Write(uid, txId, value);
                }
                catch (TxException) { throw; } //Quando tenta ler de um servidor q ainda n tem o objecto (vindo d redistribuicao)
                //Server failure detection
                catch (System.Runtime.Remoting.RemotingException)
                {
                    StartRecoveryChain(responsible);
                    //Esperar ate ter a tabela em ordem e repetir a operacao
                    WaitWhileInRecoveryProcess();
                    IServerServer serv = GetResponsibleServerAfterRecovery(uid);
                    serv.Write(uid, txId, value);
                }
                catch (Exception e) { Console.WriteLine(e); }
            }
            else //o proprio eh o responsavel
            {
                Write(uid, txId, value);
            }
        }

        //Client-Server
        public bool TxCommit(string clientAddressPort) 
        {
            threadSafeStateCheck();
            
            int txId;
            lock (serverLock)
            {
                //obtem a tx aberta
                txId = clients[clientAddressPort];
            }
            List<string> serverList;
            int numWaitingResponse;
            lock (txLock)
            {
                //obtem os servidores que guardam objectos que participaram na tx
                serverList = DstmUtil.GetInvolvedServersList(servers, txObjCoordList[txId]);
            }
            numWaitingResponse = serverList.Count;
            bool canCommit = false;
            bool result;
            string serverFailed = ""; //usado se o servidor falhar
            //Envio dos canCommits
            foreach (string server in serverList)
            {
                if (!server.Equals(myself))
                {
                    try
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + server + "/Server");
                        canCommit = serv.CanCommit(txId);
                    }
                    //Server failure detection
                    catch (System.Runtime.Remoting.RemotingException)
                    {
                        StartRecoveryChain(server);
                        canCommit = false;
                        serverFailed = server;
                        //Se um falhou antes do canCommit, a tx vai abortar
                    }
                    if (canCommit)
                        numWaitingResponse--;
                }
                else
                {
                    canCommit = CanCommit(txId);
                    if (canCommit)
                        numWaitingResponse--;
                }
            }
            //Antes de avancar tenho de ter a certeza de que nao estou a meio de um processo de recuperacao
            //depois de uma falha (que pode ter sido iniciado no canCommit), para garantir que tenho as tabelas actualizadas
            WaitWhileInRecoveryProcess();

            //Se todos responderam ao canCommit: Envio dos commits a todos
            if (numWaitingResponse == 0)
            {
                foreach (string server in serverList)
                {
                    if (!server.Equals(myself))
                    {
                        try
                        {
                            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                                "tcp://" + server + "/Server");
                            serv.Commit(txId);
                        }
                        //Server failure detection
                        catch (System.Runtime.Remoting.RemotingException)
                        {
                            string serverNextToFailed;
                            lock (serverLock)
                            {
                                serverNextToFailed = DstmUtil.GetNextServer(server, servers);
                            }
                            //Se algum foi abaixo antes de poder ser commitado, 
                            //eh preciso enviar a ordem de commit ao novo servidor responsavel,
                            //ou seja ao seu next antigo onde estao as replicas enviadas durante o canCommit...
                            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                                "tcp://" + serverNextToFailed + "/Server");
                            serv.CommitReplicas(txId);

                            StartRecoveryChain(server);
                        }
                    }
                    else Commit(txId); //O proprio faz commit se pertencer a esta lista de responsaveis
                }
                result = true;
            }
            //Caso contrario: Envio de aborts a todos
            else
            {
                serverList.Remove(serverFailed);
                foreach (string server in serverList)
                {
                    Console.WriteLine("Making server abort: " + server);
                    if (!server.Equals(myself))
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + server + "/Server");
                        serv.Abort(txId);
                    }
                    else Abort(txId); //O proprio faz abort se pertencer a esta lista de responsaveis
                }

                result = false;
            }
            lock (txLock)
            {
                //Remover a tx da lista de txObj do coordenador
                txObjCoordList.Remove(txId);
            }
            lock (serverLock)
            {
                //Remover a tx da lista dos clientes-tx
                string clientAddrPort = clients.Where(item => item.Value == txId).First().Key;
                clients.Remove(clientAddrPort);
            }
            //Envia para o seu next uma replica da lista de tx-obj e clients list que coordena
            SendCoordInfoToNext();
            return result;
            
        }

        //Client-Server
        public bool TxAbort(string clientAddressPort)
        {
            threadSafeStateCheck();
            
            int txId;
            lock (serverLock)
            {
                //obtem a tx aberta
                txId = clients[clientAddressPort];
            }
            List<string> serverList;
            lock(txLock){
                serverList = DstmUtil.GetInvolvedServersList(servers, txObjCoordList[txId]);
            }
            int numWaitingResponse = serverList.Count;

            //Envio dos Aborts
            foreach (string server in serverList)
            {
                if (!server.Equals(myself))
                {
                    try
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + server + "/Server");
                        serv.Abort(txId);
                    }
                    //Server failure detection
                    catch (System.Runtime.Remoting.RemotingException)
                    {
                        StartRecoveryChain(server);
                        //Se falhou ao dar abort nao faz mal pq na replica so esta a ultima versao committed
                        //fica tudo coerente.
                    }
                }
                else
                {
                    Abort(txId);
                }
            }
            lock(txLock){
                //Remover a tx da lista de txObj do coordenador
                txObjCoordList.Remove(txId);
            }
            lock(serverLock){
                //Remover a tx da lista dos clientes-tx
                string clientAddrPort = clients.Where(item => item.Value == txId).First().Key;
                clients.Remove(clientAddrPort);
            }
            //Envia para o seu next uma replica da lista de tx-obj que coordena
            SendCoordInfoToNext();
            return true;
        }


        //Server-Server
        //So o servidor responsavel pelo padint vai correr este metodo
        //Se o coordenador for ele proprio o responsavel, entao este metodo eh chamado localmente
        public void CreatePadInt(int uid, int txId)
        {
            threadSafeStateCheck();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while(isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if(isWaiting)
                    System.Threading.Thread.Sleep(250);
            }
            lock (txLock)
            {
                //Verifica se o padint ja existe
                if (myPadInts.ContainsKey(uid))
                    throw new TxException("PadInt with id '" + uid.ToString() + "' already exists!");

                //Adiciona o novo padint ah sua lista de padints
                PadIntInsider novo = new PadIntInsider(uid);
                myPadInts.Add(uid, novo);
            }
            //Cria o lock para este padint
            padintsLock.EnterWriteLock();
            try
            {
                padintLocks[uid] = new Object();
            }
            finally
            {
                padintsLock.ExitWriteLock();
            }
            lock (txLock)
            {
                //Actualiza a sua lista de objectos que participam nesta tx
                if (!txObjList.ContainsKey(txId))
                    txObjList.Add(txId, new List<int>());
                txObjList[txId].Add(uid);

                //Actualiza a lista dos objectos deste servidor criados nesta tx (para em caso de abort poderem ser removidos)
                if (!txCreatedObjList.ContainsKey(txId))
                    txCreatedObjList.Add(txId, new List<int>());
                txCreatedObjList[txId].Add(uid);
            }
            
        }

        //Server-Server
        //So o servidor responsavel pelo padint vai correr este metodo
        //Se o coordenador for ele proprio o responsavel, entao este metodo eh chamado localmente
        //Nao tem de devolver a referencia remota, nem o valor, pq o valor vai obtido pelo cliente
        //atraves de uma chamada remota pelo protocolo...
        public void AccessPadInt(int uid, int txId)
        {
            threadSafeStateCheck();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }
            lock (txLock)
            {
                //Verifica se o padint ja existe
                if (myPadInts.ContainsKey(uid))
                {
                    //Actualiza a sua lista de objectos que participam nesta tx
                    if (!txObjList.ContainsKey(txId))
                        txObjList.Add(txId, new List<int>());
                    txObjList[txId].Add(uid);
                }
                else
                    throw new TxException("PadInt with id '" + uid.ToString() + "' doesn't exist!");
            }
        }

        //Server-Server
        public int Read(int uid, int txId) {

            threadSafeStateCheck();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }

            //Obtem a entrada da txObjList e txCreatedObjList referente a esta tx para enviar para a replica
            List<int> uids;
            List<int> createdUids = null;
            lock(txLock){
                uids = txObjList[txId];
                if (txCreatedObjList.ContainsKey(txId))
                    createdUids = txCreatedObjList[txId];
            }
            SortedDictionary<int, List<int>> txObjListToSend = new SortedDictionary<int, List<int>>();
            SortedDictionary<int, List<int>> txCreatedObjListToSend = new SortedDictionary<int, List<int>>();
            txObjListToSend.Add(txId, uids);
            txCreatedObjListToSend.Add(txId, createdUids);

            //Vai preencher a lista de padints envolvidos nesta tx para enviar para a replica
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();

            bool hasPadInt;
            //search for the padint
            PadIntInsider padint;
            lock (txLock)
            {
                hasPadInt = myPadInts.TryGetValue(uid, out padint);
            }
            if (hasPadInt)
            {
                Object padintLock;
                padintsLock.EnterReadLock();
                try
                {
                    padintLock = padintLocks[uid];
                }
                finally
                {
                    padintsLock.ExitReadLock();
                }
                int result = -4;
                while (result == -4) //espera q a tx anterior faca abort ou commit
                {
                    lock (padintLock)
                    {
                        result = padint.Read(txId);
                    }
                    if (result == -4)
                        System.Threading.Thread.Sleep(250);
                }
                lock (padintLock)
                {
                    //cria a unica replica que vai enviar
                    replicasToSend.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
                }
                //Envia a lista de replicas (com este padint apenas) para o servidor seguinte
                SendUpdatedReplicas(replicasToSend, txObjListToSend, txCreatedObjListToSend);
                return result;
            }
            else
                throw new TxException("PadInt not present in the responsible server!");
            
        }

        //Server-Server
        public void Write(int uid, int txId, int value) {

            threadSafeStateCheck();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }

            //Obtem a entrada da txObjList e txCreatedObjList referente a esta tx para enviar para a replica
            List<int> uids;
            List<int> createdUids = null;
            lock (txLock)
            {
                uids = txObjList[txId];
                if (txCreatedObjList.ContainsKey(txId))
                    createdUids = txCreatedObjList[txId];
            }
            SortedDictionary<int, List<int>> txObjListToSend = new SortedDictionary<int, List<int>>();
            SortedDictionary<int, List<int>> txCreatedObjListToSend = new SortedDictionary<int, List<int>>();
            txObjListToSend.Add(txId, uids);
            txCreatedObjListToSend.Add(txId, createdUids);

            //Vai preencher a lista de padints envolvidos nesta tx para enviar para a replica
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();

            PadIntInsider padint;
            bool hasPadInt;
            Object padintLock;
            lock (txLock)
            {
                //procura pelo padint
                hasPadInt = myPadInts.TryGetValue(uid, out padint);
            }
            padintsLock.EnterReadLock();
            try
            {
                padintLock = padintLocks[uid];
            }
            finally
            {
                padintsLock.ExitReadLock();
            }
            if (hasPadInt)
            {
                lock (padintLock)
                {
                    padint.Write(txId, value);
                    //cria a unica replica que vai enviar
                    replicasToSend.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
                }
                //Envia a lista de replicas (com este padint apenas) para o servidor seguinte
                SendUpdatedReplicas(replicasToSend, txObjListToSend, txCreatedObjListToSend);
            }
            else
                throw new TxException("PadInt not present in the responsible server!");
        }

        //Server-Server
        //Aqui no fim do canCommit envia para o seu next, replicas dos padints envolvidos nesta tx, 
        //juntamente a entrada na tabela tx-objs referente a esta tx
        public bool CanCommit(int txId)
        {
            threadSafeStateCheck();
            //IMayFailHere();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }

            List<int> uids;
            List<int> createdUids = new List<int>();
            lock (txLock)
            {
                //obtem os objectos usados nesta tx
                uids = txObjList[txId];
                if (txCreatedObjList.ContainsKey(txId))
                    createdUids = txCreatedObjList[txId];
            }
            //Obtem a entrada da txObjList referente a esta tx para enviar para a replica
            SortedDictionary<int, List<int>> txObjListToSend = new SortedDictionary<int, List<int>>();
            SortedDictionary<int, List<int>> txCreatedObjListToSend = new SortedDictionary<int, List<int>>();
            txObjListToSend.Add(txId, uids);
            txCreatedObjListToSend.Add(txId, createdUids);
            //Vai preencher a lista de padints envolvidos nesta tx para enviar para a replica
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();
            //para cada um destes objectos:
            foreach (int uid in uids)
            {
                Object padintLock;
                PadIntInsider padint;
                lock (txLock)
                {
                    padint = myPadInts[uid];
                }
                padintsLock.EnterReadLock();
                try
                {
                    padintLock = padintLocks[uid];
                }
                finally
                {
                    padintsLock.ExitReadLock();
                }
                lock (padintLock)
                {
                    padint.CanCommit(txId);
                    replicasToSend.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
                }
            }
            //Envia a lista de replicas para o servidor seguinte (que eh responsavel pelas suas replicas)
            SendUpdatedReplicas(replicasToSend, txObjListToSend, txCreatedObjListToSend);
            return true;
        }

        //Server-Server
        public void Commit(int txId)
        {
            threadSafeStateCheck();
            //IMayFailHere();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }

            int result;
            List<int> uids;
            lock (txLock)
            {
                //obtem os objectos usados nesta tx
                uids = txObjList[txId];
            }
            Object padintLock;
            PadIntInsider padint;
            //Faco replicas dos objectos usados nesta tx
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();
            foreach (int uid in uids)
            {
                lock (txLock)
                {
                    //para cada um destes objectos
                    padint = myPadInts[uid];
                }
                padintsLock.EnterReadLock();
                try
                {
                    padintLock = padintLocks[uid];
                }
                finally
                {
                    padintsLock.ExitReadLock();
                }
                lock (padintLock)
                {
                    result = padint.Commit(txId);
                }
                while (result == -4)
                {
                    lock (padintLock)
                    {
                        result = padint.Commit(txId);
                    }
                    if (result == -4)
                        System.Threading.Thread.Sleep(250);
                }
                lock (padintLock)
                {
                    //criar uma replica deste padint e adicionar ah lista que mais tarde sera enviada
                    replicasToSend.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
                }
            }
            lock (txLock)
            {
                //Limpa a lista dos objectos que ele tem nesta tx
                txObjList.Remove(txId);
                //Limpa a lista dos objectos que ele criou para esta tx
                txCreatedObjList.Remove(txId);
            }
            //Limpar as replicas para esta tx (no seu next)
            SortedDictionary<int, List<int>> txObjListToSend = new SortedDictionary<int, List<int>>();
            SortedDictionary<int, List<int>> txCreatedObjListToSend = new SortedDictionary<int, List<int>>();
            txObjListToSend.Add(txId, null);
            txCreatedObjListToSend.Add(txId, null);
            //Envia a lista de replicas para o servidor seguinte (que eh responsavel pelas suas replicas)
            SendUpdatedReplicas(replicasToSend, txObjListToSend, txCreatedObjListToSend);
        }

        //Server-Server
        public void Abort(int txId)
        {
            threadSafeStateCheck();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }
            List<int> uids;
            lock (txLock)
            {
                //obtem os objectos usados nesta tx
                uids = txObjList[txId];
            }
            Object padintLock;
            //Fazer replicas dos objectos usados nesta tx
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();
            foreach (int uid in uids)
            {
                //para cada um destes objectos
                PadIntInsider padint;
                lock (txLock)
                {
                    padint = myPadInts[uid];
                }
                padintsLock.EnterReadLock();
                try
                {
                    padintLock = padintLocks[uid];
                }
                finally
                {
                    padintsLock.ExitReadLock();
                }
                lock (padintLock)
                {
                    padint.Abort(txId);
                    //criar uma replica deste padint e adicionar ah lista que mais tarde sera enviada
                    replicasToSend.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
                }
            }

            //Se houverem padints a serem removidos (e cuja informacao precisamos propagar ahs replicas,
            //preenchemos a createdObjListToSend com as replicas que serao destruidas. A decisao para destruir
            //eh feita fazendo o check se o uid na lista craetedObj nao estiver tambem na objList, entao
            //esta replica sera destruida)
            SortedDictionary<int, List<int>> txCreatedObjListToSend = new SortedDictionary<int, List<int>>();
            txCreatedObjListToSend[txId] = new List<int>();
            lock (txLock)
            {
                //Limpa a lista dos objectos que ele tem nesta tx
                txObjList.Remove(txId);
                //Para cada objecto que criou (se criou algum!) no contexto desta tx, remove-o
                if (txCreatedObjList.ContainsKey(txId))
                {
                    List<int> createdUids = txCreatedObjList[txId];
                    foreach (int uid in createdUids)
                    {
                        myPadInts.Remove(uid);
                        txCreatedObjListToSend[txId].Add(uid);
                    }
                }
                //Limpa a lista dos objectos que ele criou para esta tx
                txCreatedObjList.Remove(txId);
            }
            //Limpar as replicas para esta tx (no seu next)
            SortedDictionary<int, List<int>> txObjListToSend = new SortedDictionary<int, List<int>>();
            txObjListToSend.Add(txId, null);
            //Envia a lista de replicas para o servidor seguinte (que eh responsavel pelas suas replicas)
            SendUpdatedReplicas(replicasToSend, txObjListToSend, txCreatedObjListToSend);
        }

        //Server-Server
        //Metodo chamado quando a detecao de que um servidor falhou eh feita ao chamar o commit sobre esse servidor,
        //este metodo eh invocado no servidor seguinte ao que falhou para commitar as replicas do que falhou.
        public void CommitReplicas(int txId)
        {
            threadSafeStateCheck();

            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }

            int result;
            List<int> uids;
            lock (txLock)
            {
                //get the replicas used in this txId
                uids = txReplicatedObjList[txId];
            }
            Object replicaLock;
            PadIntInsider replica;
            foreach (int uid in uids)
            {
                lock (txLock)
                {
                    //for each of these objects:
                    replica = replicatedPadInts[uid];
                }
                replicasLock.EnterReadLock();
                try
                {
                    replicaLock = replicaLocks[uid];
                }
                finally
                {
                    replicasLock.ExitReadLock();
                }
                lock (replicaLock)
                {
                    result = replica.Commit(txId);
                }
                while (result == -4)
                {
                    lock (replicaLock)
                    {
                        result = replica.Commit(txId);
                    }
                    if (result == -4)
                        System.Threading.Thread.Sleep(250);
                }
            }
            lock (txLock)
            {
                //Limpa a lista dos objectos que ele tem nesta tx
                txReplicatedObjList.Remove(txId);
                //Limpa a lista dos objectos que ele criou para esta tx
                txReplicatedCreatedObjList.Remove(txId);
            }
        }

        //Server-Server
        //Metodo chamado quando a detecao de que um servidor falhou eh feita ao chamar o abort sobre esse servidor,
        //este metodo eh invocado no servidor seguinte ao que falhou para abortar as replicas do que falhou.
        public void AbortReplicas(int txId)
        {
            threadSafeStateCheck();
            //Console.WriteLine("Aborting replicas on " + myself);
            Boolean isWaiting;
            lock (txLock)
            {
                isWaiting = waitingForObjects;
            }
            while (isWaiting)
            {
                lock (txLock)
                {
                    isWaiting = waitingForObjects;
                }
                if (isWaiting)
                    System.Threading.Thread.Sleep(250);
            }
            List<int> uids;
            lock (txLock)
            {
                //get the objects used in this txId
                uids = txReplicatedObjList[txId];
            }
            Object replicaLock;
            foreach (int uid in uids)
            {
                //for each of these objects:
                PadIntInsider replica;
                lock (txLock)
                {
                    replica = replicatedPadInts[uid];
                }
                replicasLock.EnterReadLock();
                try
                {
                    replicaLock = replicaLocks[uid];
                }
                finally
                {
                    replicasLock.ExitReadLock();
                }
                lock (replicaLock)
                {
                    replica.Abort(txId);
                }
            }

            lock (txLock)
            {
                //Limpa a lista dos objectos que ele tem nesta tx
                txReplicatedObjList.Remove(txId);
                //Para cada objecto que criou (se criou algum!) no contexto desta tx, remove-o
                if (txReplicatedCreatedObjList.ContainsKey(txId))
                {
                    List<int> createdUids = txReplicatedCreatedObjList[txId];
                    foreach (int uid in createdUids)
                        replicatedPadInts.Remove(uid);
                }
                //Limpa a lista dos objectos que ele criou para esta tx
                txReplicatedCreatedObjList.Remove(txId);
            }
        }

        //Server-Server
        //Metodo chamado quando um servidor faz commit num grupo de objectos seus no contexto de uma tx
        //para actualizar as replicas no servidor seguinte (ou pelo canCommit)
        public void UpdateReplicas(List<PadIntInsider> replicasToSend, SortedDictionary<int, List<int>> txObjListToSend, SortedDictionary<int, List<int>> txCreatedObjListToSend)
        {
            //Tem de actualizar as replicas
            lock (txLock)
            {
                foreach(PadIntInsider replica in replicasToSend){
                    replicatedPadInts[replica.UID] = replica;

                    //Cria um lock para esta replica
                    replicasLock.EnterWriteLock();
                    try
                    {
                        if (!replicaLocks.ContainsKey(replica.UID))
                            replicaLocks[replica.UID] = new Object();
                    }
                    finally
                    {
                        replicasLock.ExitWriteLock();
                    }
                    
                }
                if (txObjListToSend != null) //Se for null veio de um redistribution after crash
                {
                    KeyValuePair<int, List<int>> kvp = txObjListToSend.FirstOrDefault();
                    if (kvp.Value != null)
                        txReplicatedObjList[kvp.Key] = kvp.Value; //can Commit (out read ou write)
                    else
                        txReplicatedObjList.Remove(kvp.Key); //Commit ou abort
                }
                if (txCreatedObjListToSend != null)
                {
                    KeyValuePair<int, List<int>> kvp = txCreatedObjListToSend.FirstOrDefault();
                    if (kvp.Value != null)
                    {
                        if (txObjListToSend != null)
                            txReplicatedCreatedObjList[kvp.Key] = kvp.Value; //can Commit (out read ou write)
                        List<int> uidsToEliminate = new List<int>();
                        foreach(int uid in kvp.Value) //Abort: verificar se as replicas precisam ser destruidas
                        {                               //porque o padint original tb foi destruido no decurso duma tx
                            //Se o txReplicatedObjList nao contem o elemento, eh pq eh para remover esse elemento
                            if (!txReplicatedObjList.ContainsKey(kvp.Key) || !txReplicatedObjList[kvp.Key].Contains(uid))
                                uidsToEliminate.Add(uid);
                        }
                        foreach (int uid in uidsToEliminate)
                        {
                            replicatedPadInts.Remove(uid);
                            txReplicatedCreatedObjList[kvp.Key].Remove(uid);
                        }
                        //Se a lista ficou vazia, remover a entrada desta tx...
                        if (txReplicatedCreatedObjList[kvp.Key].Count == 0)
                            txReplicatedCreatedObjList.Remove(kvp.Key);
                    }
                    else
                        txReplicatedCreatedObjList.Remove(kvp.Key); //Commit
                }
            }
        }

        //Server-server
        //Metodo chamado para fazer set ahs replicas, apagando quaisquer replicas pre-existentes
        //Eh chamado quando entra um novo servidor, pelo novo sobre o seu next.
        //O servidor que antecede o novo vai enviar-lhe as replicas num ServerPackage como resposta
        //ah chamada ao metodo SendObjectsTo, de seguida o novo executa este metodo em si proprio.
        //Tambem eh chamado quando alguem crasha, pelo novo responsavel sobre o seu next.
        public void SetReplicas(List<PadIntInsider> replicasToSend)
        {
            if (replicasToSend != null)
            {
                //Tem de actualizar as replicas
                lock (txLock)
                {
                    replicatedPadInts.Clear();
                    foreach (PadIntInsider replica in replicasToSend)
                    {
                        replicatedPadInts[replica.UID] = replica;
                    }
                }
            }
        }

        //Server-Server
        //Server-Master
        //Se o cliente for o primeiro a detectar que o seu coordenador caiu, o master eh quem avisa todos os servidores
        public void UpdateNetworkAfterCrash(string crashedServerAddrPort){
            SInfoPackage pack;
            ServerInfo newMySInfo;
            int myBeginInterval;
            string newServerResponsible;
            lock (serverLock)
            {
                //Removo o servidor que crashou da lista de servers, e retorna o meu novo (ou nao) ServerInfo
                pack = DstmUtil.RemoveServer(crashedServerAddrPort, servers, myself);
                newMySInfo = pack.getServerInfo();
                newServerResponsible = pack.getServerWhoTransfers(); //no caso de saidas este campo indica o servidor que aglomera
            }
            //Verificar se eu fui afectado pela saida. Se fui afectado tenho que passar as minhas replicas
            //para padints, e tenho de contactar o meu novo previous para obter as replicas dele. E finalmente
            //tenho de enviar as minhas replicas para o meu next.
            //Tambem passo a tabela de coordenador do meu antecessor (replica) para efectiva minha assim como os clientes...
            //Console.WriteLine("new guy: '" + newServerResponsible + "' myself: '" + myself + "' #######################");
            if (newServerResponsible.Equals(myself))
            {
                List<PadIntInsider> replicas;
                lock (txLock)
                {
                    //Se sim, tenho de actualizar o mySInfo
                    mySInfo = newMySInfo;
                    myBeginInterval = mySInfo.getBegin();
                    replicas = replicatedPadInts.Values.ToList();
                }
                //Pegar nas minha replicas e adicionar aos meus padints, porque eu agora controlo todos
                foreach (PadIntInsider padint in replicas)
                {
                    lock(txLock){
                        replicatedPadInts.Remove(padint.UID);
                        myPadInts.Add(padint.UID, padint);
                    }
                    //Cria o lock para este padint
                    padintsLock.EnterWriteLock();
                    try
                    {
                        padintLocks[padint.UID] = new Object();
                    }
                    finally
                    {
                        padintsLock.ExitWriteLock();
                    }
                }
                //Passar o tx-obj e tx-createdObj que tava a replicar para efectivos
                lock(txLock){
                    foreach(KeyValuePair<int, List<int>> kvp in txReplicatedObjList){
                        if (txObjList.ContainsKey(kvp.Key)){ //Se ja tem objectos nesta txId, acrescenta os novos 1 a 1
                            foreach (int uid in kvp.Value)
                                txObjList[kvp.Key].Add(uid);
                        }
                        else
                            txObjList.Add(kvp.Key, kvp.Value);
                    }
                    txReplicatedObjList.Clear();
                    foreach (KeyValuePair<int, List<int>> kvp in txReplicatedCreatedObjList)
                    {
                        if (txCreatedObjList.ContainsKey(kvp.Key))
                        { //Se ja tem objectos nesta txId, acrescenta os novos 1 a 1
                            foreach (int uid in kvp.Value)
                                txCreatedObjList[kvp.Key].Add(uid);
                        }
                        else
                            txCreatedObjList.Add(kvp.Key, kvp.Value);
                    }
                    txReplicatedCreatedObjList.Clear();
                }
                //Passar a tabela de coordenador do meu antecessor (replica) e adicionar ah minha e adicionar aos meus clientes os do antigo
                BecomeCoordinatorIfPreviousWas();
                //Contactar o meu novo previous para obter as replicas dele
                string previousServer = null;
                lock(serverLock){
                    previousServer = DstmUtil.GetPreviousServer(myBeginInterval, servers);
                }
                ReplicaPackage replicaPack = null;
                if (previousServer != null)
                {
                    try
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + previousServer + "/Server");
                        replicaPack = serv.GiveMeYourReplicas();
                    }
                    catch (Exception e) { Console.WriteLine(e); }
                    UpdateReplicas(replicaPack.GetReplicas(), replicaPack.GetTxObj(), replicaPack.GetTxCreatedObj());
                }
                //Enviar as minhas replicas para o meu next
                SendNewReplicas(MakeReplicas());
            }
            //Se nao fui afectado pela saida nao faco mais nada
            lock (txLock)
            {
                midRecovery = false;
            }
        }

        //Server-Server
        //Metodo chamado sempre que um previous server (que eh um coordinator) eh-lhe pedido um access ou create
        //de um padint no espaco de enderecamento dele
        public void SetCoordReplicationInfo(SortedDictionary<int, List<int>> txObjCoordListReceived, SortedDictionary<string, int> clientsReceived)
        {
            lock (txLock)
            {
                txObjReplicatedCoordList = txObjCoordListReceived;
            }
            lock (serverLock)
            {
                replicatedClients = clientsReceived;
            }
        }

        //Server-Server
        public void SetTxObjReplicationInfo(SortedDictionary<int, List<int>> txObjListReceived, SortedDictionary<int, List<int>> txCreatedObjListReceived)
        {
            lock (txLock)
            {
                txReplicatedObjList = txObjListReceived;
                txReplicatedCreatedObjList = txCreatedObjListReceived;
            }
        }

        //Internal method - ALREADY LOCK PROTECTED!
        //Cria replicas a partir dos myPadInts
        private List<PadIntInsider> MakeReplicas()
        {
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();
            lock (txLock)
            {
                foreach (PadIntInsider padint in myPadInts.Values)
                {
                    replicasToSend.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
                }
            }
            return replicasToSend;
        }

        //Internal method - LOCK PROTECTED
        //Recebe um addrPort do servidor que falhou e comeca a chain para remover esse servidor da rede
        private void StartRecoveryChain(string crashedServerAddrPort)
        {
            //Contacto o master a indicar que este servidor caiu, recebo como resposta do master
            //se eu fui o primeiro a descobrir a falha. Se eu nao fui o primeiro nao fa�o mais nada,
            //porque ja esta alguem a tratar disso. Se for eu o primeiro a detectar, tenho d avisar os
            //restantes para que eles possam retirar este servidor das suas listas.

            lock(txLock){
                midRecovery = true;
            }

            //Contacta o master para avisar do crash detectado
            IMasterServer master = (IMasterServer)Activator.GetObject(typeof(IMasterServer),
                "tcp://" + ServerNode.masterAddrPort + "/Master");
            Boolean iDetectedFirst = master.DetectedCrash(crashedServerAddrPort); 
            //Se eu for o primeiro a detectar tenho de retirar da minha lista, e avisar os restantes
            if (iDetectedFirst)
            {
                UpdateNetworkAfterCrash(crashedServerAddrPort); //altera o servers localmente
                //Ja so envio para todos menos o que crashou, pq eu ja o removi
                List<ServerInfo> serversInfo;
                lock (serverLock)
                {
                    serversInfo = servers.Values.ToList();
                }
                //Notifico os outros do update
                foreach (ServerInfo sInfo in serversInfo)
                {
                    string server = sInfo.getPortAddress();
                    if (!server.Equals(myself))
                    {
                        try
                        {
                            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                                "tcp://" + server + "/Server");
                            serv.UpdateNetworkAfterCrash(crashedServerAddrPort);
                        }
                        catch (Exception e) { Console.WriteLine(e); }
                    }
                }
            }
            //Se nao fui o primeiro a detectar, nao preciso de fazer mais nada (o primeiro vai contactar-me)
        }

        //Internal method - LOCK PROTECTED
        //Envia uma lista de replicas para o next, que vai fazer reset ah sua lista de replicas
        //e faz set desta lista como nova
        private void SendNewReplicas(List<PadIntInsider> replicasToSend)
        {
            int myBeginInterval;
            string nextServerAddrPort;
            lock (txLock)
            {
                myBeginInterval = mySInfo.getBegin();
            }
            lock (serverLock)
            {
                nextServerAddrPort = DstmUtil.GetNextServer(myBeginInterval, servers);
            }
            try
            {
                if (nextServerAddrPort != null) //Se eu sou o unico servidor, nao envio replicas para mim proprio
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + nextServerAddrPort + "/Server");
                    serv.SetReplicas(replicasToSend);
                }
            }
            catch (Exception e) { Console.WriteLine(e); }
        }

        //Internal Method - LOCK PROTECTED
        //Envia uma lista de replicas para o next, que vao fazer update a essas replicas
        private void SendUpdatedReplicas(List<PadIntInsider> replicasToSend, SortedDictionary<int, List<int>> txObjListToSend, SortedDictionary<int, List<int>> txCreatedObjListToSend)
        {
            int myBeginInterval;
            string nextServerAddrPort;
            lock (txLock)
            {
                myBeginInterval = mySInfo.getBegin();
            }
            lock (serverLock)
            {
                nextServerAddrPort = DstmUtil.GetNextServer(myBeginInterval, servers);
            }
            try
            {
                if (nextServerAddrPort != null) //Se eu sou o unico servidor, nao envio replicas para mim proprio
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + nextServerAddrPort + "/Server");
                    serv.UpdateReplicas(replicasToSend, txObjListToSend, txCreatedObjListToSend);
                }
            }
            catch (Exception e) { Console.WriteLine(e); }
        }

        //Internal method - LOCK PROTECTED
        //Envia a info de coordenador para o seu next
        private void SendCoordInfoToNext()
        {
            string nextServer;
            lock (serverLock)
            {
                nextServer = DstmUtil.GetNextServer(myself, servers);
            }
            if (nextServer != null)
            {
                SortedDictionary<int, List<int>> txObjCoordListToSend;
                lock (txLock)
                {
                    txObjCoordListToSend = DstmUtil.GetCoordListReplicaFrom(txObjCoordList);
                }
                //Replicar e enviar a lista dos clients...
                SortedDictionary<string, int> clientsToSend;
                lock (serverLock)
                {
                    clientsToSend = DstmUtil.GetClientListReplicaFrom(clients);
                }
                IServerServer srv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + nextServer + "/Server");
                srv.SetCoordReplicationInfo(txObjCoordListToSend, clientsToSend);
            }
        }

        //Internal method - LOCK PROTECTED
        //Envia uma replica da sua informcao de tx-obj para o seu next
        private void SendTxObjInfoToNext()
        {
            string nextServer;
            lock (serverLock)
            {
                nextServer = DstmUtil.GetNextServer(myself, servers);
            }
            if (nextServer != null)
            {
                SortedDictionary<int, List<int>> txObjListToSend;
                SortedDictionary<int, List<int>> txCreatedObjListToSend;
                lock (txLock)
                {
                    txObjListToSend = DstmUtil.GetTxObjReplicaFrom(txObjList);
                    txCreatedObjListToSend = DstmUtil.GetTxObjReplicaFrom(txCreatedObjList);
                }
                IServerServer srv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + nextServer + "/Server");
                srv.SetTxObjReplicationInfo(txObjListToSend, txCreatedObjListToSend);
            }
        }

        //Metodo interno -LOCK PROTECTED
        //Se o servidor anterior, que crashou, era um coordenador, eu passo agora a ser o novo coordenador
        //para essas txs e clients. No fim limpo as estruturas de dados de replicacao depois de ter tornado
        //os dados que eram replicados a efectivos.
        private void BecomeCoordinatorIfPreviousWas()
        {
            lock (txLock)
            {
                foreach (int txId in txObjReplicatedCoordList.Keys)
                {
                    if (!txObjCoordList.ContainsKey(txId))
                        txObjCoordList[txId] = new List<int>();
                    foreach (int objectId in txObjReplicatedCoordList[txId])
                        txObjCoordList[txId].Add(objectId);
                }
                txObjReplicatedCoordList.Clear();
            }
            lock (serverLock)
            {
                foreach (string clientAddrPort in replicatedClients.Keys)
                {
                    if (!clients.ContainsKey(clientAddrPort)) //O mesmo cliente so pode estar envolvido numa tx de cada vez
                        clients[clientAddrPort] = replicatedClients[clientAddrPort];
                }
                replicatedClients.Clear();
            }
        }

        //Internal method -  LOCK PROTECTED
        //Depois de um servidor falhar, quando eu detecto que ele falhou vou ter a variavel 'midRecovery' a true
        //esta variavel so sera posta a false outra vez quando eu terminar de correr o metodo de recuperacao
        //invocado ou por mim (se eu fui o primeiro a detectar a falha) ou por algum outro servido (ou ate pelo master)
        //Ha certas operacoes que eu nao quero fazer enquanto tiver as tabelas dos servidores inconsistentes, como
        //repetir uma operacao no servidor que foi abaixo, por exemplo.
        private void WaitWhileInRecoveryProcess(){
            Boolean isInRecovery;
            lock (txLock)
            {
                isInRecovery = midRecovery;
            }
            while (isInRecovery)
            {
                lock (txLock)
                {
                    isInRecovery = midRecovery;
                }
                if (isInRecovery)
                    System.Threading.Thread.Sleep(100);
            }
        }

        //Internal method
        private IServerServer GetResponsibleServerAfterRecovery(int uid)
        {
            string responsible;
            lock (serverLock)
            {
                //returns the portAddress of the server responsible for that uid
                responsible = DstmUtil.GetResponsibleServer(servers, uid);
            }
            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                "tcp://" + responsible + "/Server");
            return serv;
        }

        public bool Fail()
        {
            State state;
            lock (stateLock)
            {
                state = currentState;
            }
            if (state == State.Failed)      // Ja esta failed, retorna false
                return false;
                
            else
            {

                lock (stateLock)
                {
                    currentState = State.Failed;
                }
                lock (serverLock)
                {
                    clients = new SortedDictionary<string, int>();
                    replicatedClients = new SortedDictionary<string, int>();
                    servers = new SortedDictionary<int, ServerInfo>();
                }
                lock (txLock)
                {
                    mySInfo = new ServerInfo(0, 0, "");
                    myPadInts = new SortedDictionary<int, PadIntInsider>();
                    replicatedPadInts = new SortedDictionary<int, PadIntInsider>();
                    txObjCoordList = new SortedDictionary<int, List<int>>();
                    txObjReplicatedCoordList = new SortedDictionary<int, List<int>>();
                    txObjList = new SortedDictionary<int, List<int>>();
                    txReplicatedObjList = new SortedDictionary<int, List<int>>();
                    txCreatedObjList = new SortedDictionary<int, List<int>>();
                    txReplicatedCreatedObjList = new SortedDictionary<int, List<int>>();
                }
                _queuedThreads.Clear();
                return true;
            }           
        }

        public bool Freeze()
        {
            State state;
            lock (stateLock)
            {
                state = currentState;
            }
            if (state == State.Frozen || state == State.Failed)    // Ja esta frozen, retorna false. Se estiver failed n�o deve responder a isto (so a recov)
                return false;

            else
            {
                lock (stateLock)
                {
                    currentState = State.Frozen;
                }
                _queuedThreads.Clear();
                return true;
            }
        }

        public bool Recover()
        {
            State state;
            lock (stateLock)
            {
                state = currentState;
            }
            if (state == State.Normal)    // Ja esta normal, nao ha recover a fazer
                return false;
            else if (state == State.Frozen)
            {
                lock (stateLock)
                {
                    currentState = State.Normal;
                }
                bool res = processQueueCommands();
                if (res)
                    return true;
                else
                    return false;
            }
            else
            {
                lock (stateLock) // Estava "Failed", volta ao normal
                {
                    currentState = State.Normal;
                } 
                return true;
            }
            
        }

        private bool processQueueCommands()
        {
            try
            {
                for (int i = 0; i < _queuedThreads.Count; i++) // Acordar todas as threads adormecidas devido ao "recover"
                {
                    ManualResetEvent mre = _queuedThreads[i];
                    mre.Set();
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Caught exception on processQueueCommands. Reason: {0}", e.StackTrace);
                return false;
            }
        }

        private void threadSafeStateCheck()
        {
            State state;
            lock (stateLock)
            {
                state = currentState;
            }
            //verifica se o server esta no estado "Normal".
            //Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (state == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (state == State.Frozen)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
            }
        }

        //Server - Master
        //Para o status
        public void printSelfStatus()
        {
            Console.WriteLine("=============");
            Console.WriteLine("Server STATUS");
            Console.WriteLine("=============");
            lock (serverLock)
            {
                DstmUtil.ShowServerList(servers);
                DstmUtil.ShowClientsList(clients, "Clients List (Coordinator)");
                DstmUtil.ShowClientsList(replicatedClients, "Replicated clients List (previous coordinator)");
            }
            lock (txLock)
            {
                DstmUtil.ShowTxServersList(txObjCoordList, servers, "Tx-Servers List (Coordinator)");
                DstmUtil.ShowTxServersList(txObjReplicatedCoordList, servers, "Replicated tx-Servers List (previous coordinator)");
                DstmUtil.ShowServerIntervals(mySInfo);
                DstmUtil.ShowPadIntsList(myPadInts);
                DstmUtil.ShowReplicas(replicatedPadInts);
                DstmUtil.ShowTxObjectsList(txObjList, "Tx-Object List (Responsible)");
                DstmUtil.ShowTxObjectsList(txReplicatedObjList, "Replicated tx-Object List");
                DstmUtil.ShowTxCreatedObjectsList(txCreatedObjList, "Tx-Created Object List (Responsible)");
                DstmUtil.ShowTxCreatedObjectsList(txReplicatedCreatedObjList, "Replicated tx-Created Object List");
            }
        }

        //Metodo para teste
        //Obriga o 3o servidor (porto 2003) a falhar quando este metodo eh chamado (ou no canCommit ou no commit)
        private void IMayFailHere()
        {
            if (myself.EndsWith("2003"))
            {
                Fail();
                throw new RemotingException();
            }
        }

    }

}