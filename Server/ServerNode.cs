using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Net.Sockets;
using System.Threading;
using System.Windows.Forms;
using DSTM;
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
            servers = masterObj.RegisterServer(myself);
            //Actualiza o seu objecto remoto
            serv.UpdateServerList(myself, servers);

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
        private SortedDictionary<string, int> clients = new SortedDictionary<string, int>(); // ex: < "193.34.126.54:6000", (txId) >
        private SortedDictionary<int, ServerInfo> servers; // ex: < begin, object ServerInfo(begin, end, portAddress) >
        
        private SortedDictionary<int, PadIntInsider> myPadInts = new SortedDictionary<int, PadIntInsider>(); //uid, padint
        private ServerInfo mySInfo = new ServerInfo(0,0,"");//Guarda o meu intervalo (begin, end, portAdress) 
                                                            //Ter acesso directo a esta estrutura permite rapida
                                                            //verificacao se eh preciso ou nao redistribuir certo padint
        // Lista mantida pelo coordenador
        // Esta lista serve para no fim, o coordenador saber quem tem que contactar para o commit,
        // basta guardar os endereços dos servidores que têm objectos desta tx para os contactar
        private SortedDictionary<int, List<string>> txServersList = new SortedDictionary<int, List<string>>();

        // Lista mantida pelos responsaveis por objectos envolvidos em txs
        // <txId, lista< uid > >
        private SortedDictionary<int, List<int>> txObjList = new SortedDictionary<int, List<int>>();

        // Eh preciso manter uma lista nos responsaveis, dos objectos criados na tx actual, para em caso de abort,
        // o objecto ser removido <txId, lista<uid>>
        private SortedDictionary<int, List<int>> txCreatedObjList = new SortedDictionary<int, List<int>>();

        private string myself;

        //Pedidos pendentes (durante o freeze) para este servidor < nome do comando, lista de argumentos > 
        private Queue<Action> pendingCommands = new Queue<Action>();
        private List<ManualResetEvent> _queuedThreads = new List<ManualResetEvent>();

        // estados em que o servidor pode estar
        enum State {Normal, Failed, Frozen};

        // estado atual do servidor (para saber se pode aceitar pedidos ou se esta failed/freezed)
        private State currentState = State.Normal;

        //Locks
        private Object serverLock = new Object(); //this lock protects access to clients and servers tables
        private Object txLock = new Object(); //protects all tx tables and padints
        private Object stateLock = new Object(); //protects the state


        public string GetAddress() { return myself; }


        //ServerNode updates this object after successful registration with master
        public void UpdateServerList(string serverAddrPort, SortedDictionary<int, ServerInfo> servers)
        {
            List<ServerInfo> serversInfo;
            lock (serverLock)
            {
                this.servers = servers;
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
                        serv.UpdateNetwork(serverAddrPort);
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
        }

        //Server-Server
        //Other servers call this to change topology after node join or leave
        public void UpdateNetwork(string serverAddrPort)
        {
            Boolean doesNotContainServer;
            lock (serverLock)
            {
                doesNotContainServer = !DstmUtil.ServerInfoContains(servers, serverAddrPort);
            }
            if (doesNotContainServer) //se ainda nao contem o elemento
            {
                ServerInfo newMySInfo;
                int oldIntervalEnd;
                lock (serverLock)
                {
                    //Insere o novo servidor na lista de servers, e retorna o meu novo (ou nao) ServerInfo
                    newMySInfo = DstmUtil.InsertServer(serverAddrPort, servers, myself);
                }
                lock(txLock){
                    oldIntervalEnd = mySInfo.getEnd();
                }
                //Verificar se eu fui afectado pela entrada (os novos ficam sempre com a 2a metade
                //do intervalo original, por isso basta ver se o limite superior do meu intervalo mudou)
                if (oldIntervalEnd != newMySInfo.getEnd())
                {
                    Dictionary<int, int> objTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
                    Dictionary<int, int> objCreatedTxToSendDict = new Dictionary<int, int>(); //<uid, txid>
                    List<PadIntInsider> padintToSendList = new List<PadIntInsider>();
                    List<PadIntInsider> padints;
                    lock(txLock){
                        //Se sim, tenho de actualizar o mySInfo
                        mySInfo = newMySInfo;
                        padints = myPadInts.Values.ToList();
                    }
                    //obter o meu intervalo novo e ver se eu tenho objectos fora desse intervalo
                    foreach (PadIntInsider padint in padints)
                    {
                        int ending = newMySInfo.getEnd();
                        int hashedUid = DstmUtil.HashMe(padint.UID);
                        if (ending < hashedUid) //ja esta fora do intervalo
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
                    //Se tenho objectos para redistribuir, removo-os das minhas listas e envio-os
                    //TODO: so posso enviar quando tiver a certeza que nao estao envolvidos em
                    //nenhuma das minhas tx (ou de qualquer um...)... sera que isto eh problema?
                    if (padintToSendList.Count != 0)
                    {
                        lock (txLock)
                        {
                            //removo da minha lista de padints
                            foreach (PadIntInsider padint in padintToSendList)
                            {
                                myPadInts.Remove(padint.UID);
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
                                {
                                    txObjList.Remove(txId);
                                    //Se eu nao tenho objectos meus nesta tx, retiro-me da lista tx-server, se a entrada existir,
                                    //ou seja, se eu for o coordenador da tx que tinha objectos meus que foram redistribuidos
                                    if (txServersList.ContainsKey(txId))
                                        txServersList[txId].Remove(myself);
                                }
                                //Se eu coordenar esta tx, actualizo a minha tx-server list, para dizer q o novo servidor 
                                //participa em txs que eu coordeno
                                if (txServersList.ContainsKey(txId) && !txServersList[txId].Contains(serverAddrPort))
                                    txServersList[txId].Add(serverAddrPort);
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
                        try
                        {
                            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                                "tcp://" + serverAddrPort + "/Server");
                            serv.UpdateObjects(padintToSendList, objTxToSendDict, objCreatedTxToSendDict);
                        }
                        catch (Exception e) { Console.WriteLine(e); }
                    }
                }
                //Se nao fui afectado pela entrada do novo servidor, nao tenho de fazer mais nada              
            }
        }

        //Server-Server
        //Quando este servidor entra, eh possivel que algum outro tenho objectos que agora pertencem a este
        //portanto ao ser invocado este metodo, este servidor vai fazer update eh sua lista de objectos mantidos
        public void UpdateObjects(List<PadIntInsider> toSendList, Dictionary<int, int> objTxToSendDict, Dictionary<int, int> objCreatedTxToSendDict)
        {
            lock (txLock)
            {
                foreach (PadIntInsider padint in toSendList)
                {
                    myPadInts.Add(padint.UID, padint);
                }
                foreach (KeyValuePair<int, int> kvp in objTxToSendDict)
                {
                    int txId = kvp.Value;
                    int uid = kvp.Key;
                    if (!txObjList.ContainsKey(txId))
                        txObjList.Add(txId, new List<int>());
                    txObjList[txId].Add(uid);
                }
                foreach (KeyValuePair<int, int> kvp in objCreatedTxToSendDict)
                {
                    int txId = kvp.Value;
                    int uid = kvp.Key;
                    if (!txCreatedObjList.ContainsKey(txId))
                        txCreatedObjList.Add(txId, new List<int>());
                    txCreatedObjList[txId].Add(uid);
                }
            }
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
                lock (txLock)
                {
                    txServersList.Add(txId, new List<string>());
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
                //Actualiza a lista de servidores com objectos usados nesta tx: Add( txId, coordAddrPort)
                if (!txServersList[txId].Contains(responsible))
                    txServersList[txId].Add(responsible);
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
                //Actualiza a lista de servidores com objectos usados nesta tx: Add( txId, coordAddrPort)
                if (!txServersList[txId].Contains(responsible))
                    txServersList[txId].Add(responsible);
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
                catch (Exception e) { Console.WriteLine(e); }
            }
            else //o proprio eh o responsavel
            {
                Write(uid, txId, value);
            }
        }

        //Client-Server
        //DUVIDA: em q situações devolve false e em q situaçoes lança except
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
                serverList = txServersList[txId];
            }
            numWaitingResponse = serverList.Count;
            bool canCommit = false;
            bool result;
            //Envio dos canCommits
            foreach (string server in serverList)
            { //TODO: paralelizar o envio destes pedidos
                if (!server.Equals(myself))
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + server + "/Server");
                    canCommit = serv.CanCommit(txId);
                    if (canCommit)
                        numWaitingResponse--;

                }
                else
                {
                    CanCommit(txId);
                    numWaitingResponse--; //Nao fica ah espera de si mesmo
                }
            }
            //Se todos responderam ao canCommit: Envio dos commits a todos
            if (numWaitingResponse == 0)
            {
                foreach (string server in serverList)
                {
                    if (!server.Equals(myself))
                    {
                        IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                            "tcp://" + server + "/Server");
                        serv.Commit(txId);
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
                //Remover a tx da lista de tx-servidores:
                txServersList.Remove(txId);
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
                //obtem os servidores que guardam objectos que participaram na tx
                serverList = txServersList[txId];
            }
            int numWaitingResponse = serverList.Count;

            //Envio dos Aborts
            foreach (string server in serverList)
            { //TODO: paralelizar o envio destes pedidos
                if (!server.Equals(myself))
                {
                    IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                        "tcp://" + server + "/Server");
                    serv.Abort(txId);
                }
                else
                {
                    Abort(txId);
                }
            }
            lock(txLock){
                //Remover a tx da lista de tx-servidores:
                txServersList.Remove(txId);
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
            lock (txLock)
            {
                //Verifica se o padint ja existe
                if (myPadInts.ContainsKey(uid))
                    throw new TxException("PadInt with id '" + uid.ToString() + "' already exists!");

                //Adiciona o novo padint ah sua lista de padints
                PadIntInsider novo = new PadIntInsider(uid);
                myPadInts.Add(uid, novo);

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
            lock (txLock)
            {
                //search for the padint
                PadIntInsider padint;
                bool hasPadInt = myPadInts.TryGetValue(uid, out padint);
                if (hasPadInt)
                {
                    int result = padint.Read(txId);
                    return result;
                }
                else throw new TxException("PadInt not present in the responsible server! (Redistribution was late)");
            }
        }

        //Server-Server
        public void Write(int uid, int txId, int value) {
            lock (txLock)
            {
                //search for the padint
                PadIntInsider padint;
                bool hasPadInt = myPadInts.TryGetValue(uid, out padint);
                if (hasPadInt)
                {
                    padint.Write(txId, value);
                    //returns immediately?
                }
                else throw new TxException("PadInt not present in the responsible server! (Redistribution was late)");
            }
        }

        //Server-Server
        //DUVIDA: em que situacao o canCommit retorna false?
        public bool CanCommit(int txId)
        {
            lock (txLock)
            {
                //get the objects used in this txId
                List<int> uids = txObjList[txId];

                //for each of these objects:
                foreach (int uid in uids)
                {
                    PadIntInsider obj = myPadInts[uid];
                    obj.CanCommit(txId);
                }
                return true;
            }
        }

        //Server-Server
        public void Commit(int txId)
        {
            lock (txLock)
            {
                //get the objects used in this txId
                List<int> uids = txObjList[txId];

                foreach (int uid in uids)
                {
                    //for each of these objects:
                    PadIntInsider obj = myPadInts[uid];
                    obj.Commit();
                }
                //Limpa a lista dos objectos que ele tem nesta tx
                txObjList.Remove(txId);
                //Limpa a lista dos objectos que ele criou para esta tx
                txCreatedObjList.Remove(txId);
            }
        }

        //Server-Server
        public void Abort(int txId)
        {
            lock (txLock)
            {
                //get the objects used in this txId
                List<int> uids = txObjList[txId];

                foreach (int uid in uids)
                {
                    //for each of these objects:
                    PadIntInsider obj = myPadInts[uid];
                    obj.Abort();
                }
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
                    txServersList = new SortedDictionary<int, List<string>>();
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
                DstmUtil.ShowTxServersList(txServersList);
                DstmUtil.ShowServerIntervals(mySInfo);
                DstmUtil.ShowPadIntsList(myPadInts);
                DstmUtil.ShowTxObjectsList(txObjList);
                DstmUtil.ShowTxCreatedObjectsList(txCreatedObjList);
            }
        }

    }

}