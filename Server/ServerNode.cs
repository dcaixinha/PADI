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

        private SortedDictionary<string, int> clients = new SortedDictionary<string, int>(); // ex: < "193.34.126.54:6000", (txId) >
        private SortedDictionary<int, ServerInfo> servers; // ex: < begin, object ServerInfo(begin, end, portAddress) >
        
        private SortedDictionary<int, PadIntInsider> myPadInts = new SortedDictionary<int, PadIntInsider>(); //uid, padint
        private SortedDictionary<int, PadIntInsider> replicatedPadInts = new SortedDictionary<int, PadIntInsider>();
        private ServerInfo mySInfo = new ServerInfo(0,0,"");//Guarda o meu intervalo (begin, end, portAdress) 
                                                            //Ter acesso directo a esta estrutura permite rapida
                                                            //verificacao se eh preciso ou nao redistribuir certo padint
        // Lista mantida pelo coordenador
        // Esta lista serve para no fim, o coordenador saber quem tem que contactar para o commit
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txObjCoordList = new SortedDictionary<int, List<int>>();

        // Lista mantida pelos responsaveis por objectos envolvidos em txs
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txObjList = new SortedDictionary<int, List<int>>();

        // Lista mantida por quem tem replicas do seu previous
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txReplicatedObjList = new SortedDictionary<int, List<int>>();

        // Eh preciso manter uma lista nos responsaveis, dos objectos criados na tx actual, para em caso de abort,
        // o objecto ser removido <txId, lista<uid>>
        private SortedDictionary<int, List<int>> txCreatedObjList = new SortedDictionary<int, List<int>>();

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
        private Object stateLock = new Object(); //protects the state
        private Object serverLock = new Object(); //this lock protects access to clients and servers tables
        private Object txLock = new Object(); //protects all tx tables (and padints -> not anymore)
        private ReaderWriterLockSlim padintsLock = new ReaderWriterLockSlim();
        private Dictionary<int, Object> padintLocks = new Dictionary<int, Object>(); //this will be protected by txLock

        private ReaderWriterLockSlim replicasLock = new ReaderWriterLockSlim();
        private Dictionary<int, Object> replicaLocks = new Dictionary<int, Object>(); //this will be protected by txLock

        private Boolean waitingForObjects = true;
        //Enquanto este waiting tiver a false, lanca phantomException nos metodos que sao chamados entre
        //servidores: Create, access, canCoomit, Commit, Abort

        //Listas com valores para enviar
        Dictionary<int, int> objTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
        Dictionary<int, int> objCreatedTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
        List<PadIntInsider> padintToSendList = new List<PadIntInsider>();

        public string GetAddress() { return myself; }


        //ServerNode updates this object after successful registration with master
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
            //Notify all others of the update
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
                        //Create a lock for each padint received
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
            }
            lock (txLock)
            {
                waitingForObjects = false; //ja nao estou ah espera quando recebo objectos.
            }
        }

        //Server-Server
        //Other servers call this to change topology after node join or leave
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
        //Este metodo so eh chamado quando o que entrou tem ja recebeu return de todos, i.e.
        //tal como num 2-phase, so vou pedir os objectos quando ja todos souberem que eu sou
        //o novo responsavel pelo range desses objectos.
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
                        replicas.Add(DstmUtil.GetPadintReplicaFrom(padint));
                    }
                }

                ServerPackage pack = new ServerPackage(padintToSendList, objTxToSendDict, objCreatedTxToSendDict, replicas);
                //Reset the send lists
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
        //Method called by the server who aglomerates on its new previous server.
        public List<PadIntInsider> GiveMeYourReplicas()
        {
            return MakeReplicas();
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
                //TODO: aqui deve ser possivel criar este padint fora duma tx em q eh committed automaticamente
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
                    //Enviar algo para tras, para o client que indique que a tx tem de abortar...
                    throw new TxException("The server responsible for this range was down!");
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
            //DstmUtil.ShowTxServersList(txServersList);
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

                //returns the portAddress of the server responsible for that uid
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
                    //Enviar algo para tras, para o client que indique que a tx tem de abortar...
                    throw new TxException("The server responsible for this range was down!");
                }
                catch (Exception e) { Console.WriteLine(e); }
            }
            else //o proprio eh o responsavel
            {
                AccessPadInt(uid, txId);
            }
            //Nao deve enviar a referencia remota do padint criado
            //Deve criar 1 obejcto proxy local a este servidor, independentemente de ter sido este servidor
            //o responsavel pelo padint ou outro qualquer! 
            //Portanto, o coordenador fica com esta ref remota (uid), e envia um proxy ao cliente.

            lock (txLock)
            {
                //Actualiza a lista de objectos usados nesta tx
                if (!txObjCoordList.ContainsKey(txId))
                    txObjCoordList.Add(txId, new List<int>());
                txObjCoordList[txId].Add(uid);
            }
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
                    //Enviar algo para tras, para o client que indique que a tx tem de abortar...
                    throw new TxException("The server responsible for this range was down!");
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
                    //Enviar algo para tras, para o client que indique que a tx tem de abortar...
                    throw new TxException("The server responsible for this range was down!");
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
                        //If one server fails and cannot answer canCommit, this tx will have to abort
                        //because its during the canCommit that it will send replicas with tentative versions
                        //to its next server
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
            //depois de uma falha, para garantir que tenho as tabelas actualizadas
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
                    System.Threading.Thread.Sleep(250);
            }

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
                            StartRecoveryChain(server);
                            //Contacta o master para obter o servidor a seguir ao que falhou. Isto garante
                            //que mesmo que eu ja tenha sido actualizado pelo primeiro que detectou a falha,
                            //tenho o servidor seguinte ao que crashou correcto.
                            IMasterServer master = (IMasterServer)Activator.GetObject(typeof(IMasterServer),
                                "tcp://" + ServerNode.masterAddrPort + "/Master");
                            string serverNextToFailed = master.GetNextToCrashed(server);
                            //Se algum foi abaixo antes de poder ser commitado, 
                            //eh preciso enviar a ordem de commit ao novo servidor responsavel,
                            //ou seja ao seu next antigo onde estao as replicas enviadas durante o canCommit...
                            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                                "tcp://" + serverNextToFailed + "/Server");
                            serv.CommitReplicas(txId);
                        }
                    }
                    else Commit(txId); //O proprio faz commit se pertencer a esta lista de responsaveis
                }
                result = true;
            }
            //Caso contrario: Envio de aborts a todos
            else
            {
                foreach (string server in serverList)
                {
                    try
                    {
                        if (!server.Equals(myself))
                        {
                            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                                "tcp://" + server + "/Server");
                            serv.Abort(txId);
                        }
                        else Abort(txId); //O proprio faz abort se pertencer a esta lista de responsaveis
                    }
                    //Server failure detection
                    catch (System.Runtime.Remoting.RemotingException){
                        StartRecoveryChain(server);
                        //Contacta o master para obter o servidor a seguir ao que falhou. Isto garante
                        //que mesmo que eu ja tenha sido actualizado pelo primeiro que detectou a falha,
                        //tenho o servidor seguinte ao que crashou correcto.
                        IMasterServer master = (IMasterServer)Activator.GetObject(typeof(IMasterServer),
                            "tcp://" + ServerNode.masterAddrPort + "/Master");
                        string serverNextToFailed = master.GetNextToCrashed(server);
                        //Se algum foi abaixo antes de poder ser abortado, 
                        //eh preciso enviar a ordem de abort ao novo servidor responsavel,
                        //ou seja ao seu next antigo para limpar as tentative versions das replicas...
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + serverNextToFailed + "/Server");
                        serv.AbortReplicas(txId);
                    } 
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

            PadIntInsider padint;
            bool hasPadInt;
            Object padintLock;
            lock (txLock)
            {
                //search for the padint
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
                }
            }
            else
                throw new TxException("PadInt not present in the responsible server!");
        }

        //Server-Server
        //TODO Aqui no fim do canCommit envia para o seu next, replicas dos padints envolvidos nesta tx, 
        //juntamente a entrada na tabela tx-objs referente a esta tx
        public bool CanCommit(int txId)
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
                //get the objects used in this txId
                uids = txObjList[txId];
            }
            //Obtem a entrada da txObjList referente a esta tx para enviar para a replica
            SortedDictionary<int, List<int>> txObjListToSend = new SortedDictionary<int, List<int>>();
            txObjListToSend.Add(txId, uids);
            //Vai preencher a lista de padints envolvidos nesta tx para enviar para a replica
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();
            //for each of these objects:
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
                }
                replicasToSend.Add(DstmUtil.GetPadintFullReplicaFrom(padint));
            }
            //Envia a lista de replicas para o servidor seguinte (que eh responsavel pelas suas replicas)
            SendUpdatedReplicas(replicasToSend, txObjListToSend);
            return true;
        }

        //Server-Server
        public void Commit(int txId)
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
                //get the objects used in this txId
                uids = txObjList[txId];
            }
            Object padintLock;
            PadIntInsider padint;
            //Make replicas of the objects used in this Tx
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();
            foreach (int uid in uids)
            {
                lock (txLock)
                {
                    //for each of these objects:
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
                //criar uma replica deste padint e adicionar ah lista que mais tarde sera enviada
                replicasToSend.Add(DstmUtil.GetPadintReplicaFrom(padint));
            }
            lock (txLock)
            {
                //Limpa a lista dos objectos que ele tem nesta tx
                txObjList.Remove(txId);
                //Limpa a lista dos objectos que ele criou para esta tx
                txCreatedObjList.Remove(txId);
            }
            SortedDictionary<int, List<int>> txObjListToSend = new SortedDictionary<int, List<int>>();
            txObjListToSend.Add(txId, null);
            //Envia a lista de replicas para o servidor seguinte (que eh responsavel pelas suas replicas)
            SendUpdatedReplicas(replicasToSend, txObjListToSend);
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
                //get the objects used in this txId
                uids = txObjList[txId];
            }
            Object padintLock;
            foreach (int uid in uids)
            {
                //for each of these objects:
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
                }
            }

            lock (txLock)
            {
                //Limpa a lista dos objectos que ele tem nesta tx
                txObjList.Remove(txId);
                //Para cada objecto que criou (se criou algum!) no contexto desta tx, remove-o
                if (txCreatedObjList.ContainsKey(txId))
                {
                    List<int> createdUids = txCreatedObjList[txId];
                    foreach (int uid in createdUids)
                        myPadInts.Remove(uid);
                }
                //Limpa a lista dos objectos que ele criou para esta tx
                txCreatedObjList.Remove(txId);
            }
        }

        //Server-Server
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
            }
        }

        //Server-Server
        public void AbortReplicas(int txId)
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
            }
        }

        //Server-Server
        //Metodo chamado quando um servidor faz commit num grupo de objectos seus no contexto de uma tx
        //para actualizar as replicas no servidor seguinte (ou pelo canCommit)
        public void UpdateReplicas(List<PadIntInsider> replicasToSend, SortedDictionary<int, List<int>> txObjListToSend)
        {
            //Tem de actualizar as replicas
            lock (txLock)
            {
                foreach(PadIntInsider replica in replicasToSend){
                    replicatedPadInts[replica.UID] = replica;

                    //Creates the lock for this replica
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
                        txReplicatedObjList[kvp.Key] = kvp.Value; //can Commit
                    else
                        txReplicatedObjList.Remove(kvp.Key); //Commit
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
            //Console.WriteLine("new guy: '" + newServerResponsible + "' myself: '" + myself + "' #######################");
            if (newServerResponsible.Equals(myself))
            {
                //Console.WriteLine("I AM THE SERVER RESPOSIBLE NOW!! ############################################");
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
                //Contactar o meu novo previous para obter as replicas dele
                string previousServer = null;
                lock(serverLock){
                    previousServer = DstmUtil.GetPreviousServer(myBeginInterval, servers);
                }
                List<PadIntInsider> receivedReplicas = null;
                if (previousServer != null)
                {
                    try
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + previousServer + "/Server");
                        receivedReplicas = serv.GiveMeYourReplicas();
                    }
                    catch (Exception e) { Console.WriteLine(e); }
                    UpdateReplicas(receivedReplicas, null);
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

        //Internal method - ALREADY LOCK PROTECTED!
        //Makes replicas from myPadInts (only if those padints have a committed write version)
        private List<PadIntInsider> MakeReplicas()
        {
            List<PadIntInsider> replicasToSend = new List<PadIntInsider>();
            lock (txLock)
            {
                foreach (PadIntInsider padint in myPadInts.Values)
                {
                    if (!padint.COMMITWRITE.Equals(Tuple.Create(0, 0))) //so copia se ja tiver uma versao committed
                        replicasToSend.Add(DstmUtil.GetPadintReplicaFrom(padint));
                }
            }
            return replicasToSend;
        }

        //Internal method
        //Receives the addrPort of the failed server and starts the chain to remove this server from the network
        private void StartRecoveryChain(string crashedServerAddrPort)
        {
            //Contacto o master a indicar que este servidor caiu, recebo como resposta do master
            //se eu fui o primeiro a descobrir a falha. Se eu nao fui o primeiro nao faço mais nada,
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
                //Notify all others of the update
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

        //Internal method
        //Sends a list of replicas to the next, which will reset its replica list and set this list as the new list.
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
                if (nextServerAddrPort != null) //If i'm the only server, there's no point in sending replicas to myself
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + nextServerAddrPort + "/Server");
                    serv.SetReplicas(replicasToSend);
                }
            }
            catch (Exception e) { Console.WriteLine(e); }
        }

        //Internal Method
        //Sends a list of replicas to the next server, who will update the ones received
        private void SendUpdatedReplicas(List<PadIntInsider> replicasToSend, SortedDictionary<int, List<int>> txObjListToSend)
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
                if (nextServerAddrPort != null) //If i'm the only server, there's no point in sending replicas to myself
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + nextServerAddrPort + "/Server");
                    serv.UpdateReplicas(replicasToSend, txObjListToSend);
                }
            }
            catch (Exception e) { Console.WriteLine(e); }
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
                    servers = new SortedDictionary<int, ServerInfo>();
                }
                lock (txLock)
                {
                    mySInfo = new ServerInfo(0, 0, "");
                    myPadInts = new SortedDictionary<int, PadIntInsider>();
                    replicatedPadInts = new SortedDictionary<int, PadIntInsider>();
                    txObjCoordList = new SortedDictionary<int, List<int>>();
                    txObjList = new SortedDictionary<int, List<int>>();
                    txCreatedObjList = new SortedDictionary<int, List<int>>();
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
            if (state == State.Frozen || state == State.Failed)    // Ja esta frozen, retorna false. Se estiver failed não deve responder a isto (so a recov)
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
                DstmUtil.ShowClientsList(clients);
            }
            lock (txLock)
            {
                DstmUtil.ShowTxServersList(txObjCoordList, servers);
                DstmUtil.ShowServerIntervals(mySInfo);
                DstmUtil.ShowPadIntsList(myPadInts);
                DstmUtil.ShowReplicas(replicatedPadInts);
                DstmUtil.ShowTxObjectsList(txObjList);
                DstmUtil.ShowTxCreatedObjectsList(txCreatedObjList);
            }
        }

    }

}