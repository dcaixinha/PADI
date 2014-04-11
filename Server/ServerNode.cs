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
        enum State {Normal, Failed, Freezed};

        // estado atual do servidor (para saber se pode aceitar pedidos ou se esta failed/freezed)
        private State currentState = State.Normal;

        public string GetAddress() { return myself; }

        //ServerNode updates this object after successful registration with master
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
                else
                {
                    mySInfo = sInfo;    //Eu actualizo o meu ServerInfo (mantenho isto, para mais tarde a 
                                        //redistribuicao dos objectos ser feita mais rapidamente
                }
            }
            
        }

        //Server-Server
        //Other servers call this to change topology after node join or leave
        public void UpdateNetwork(string serverAddrPort)
        {
            if (!DstmUtil.ServerInfoContains(servers, serverAddrPort)) //se ainda nao contem o elemento
            { 
                //Insere o novo servidor na lista de servers, e retorna o meu novo (ou nao) ServerInfo
                ServerInfo newMySInfo = DstmUtil.InsertServer(serverAddrPort, servers, myself);
                //Verificar se eu fui afectado pela entrada (os novos ficam sempre com a 2a metade
                //do intervalo original, por isso basta ver se o limite superior do meu intervalo mudou)
                if (mySInfo.getEnd() != newMySInfo.getEnd())
                {
                    //Se sim, tenho de actualizar o mySInfo
                    mySInfo = newMySInfo;

                    List<PadIntInsider> toSendList = new List<PadIntInsider>(); 
                    //obter o meu intervalo novo e ver se eu tenho objectos fora desse intervalo
                    foreach(PadIntInsider padint in myPadInts.Values){
                        int ending = mySInfo.getEnd();
                        int hashedUid = DstmUtil.HashMe(padint.UID);
                        if (ending < hashedUid) //ja esta fora do intervalo
                        { 
                            toSendList.Add(padint);
                        }
                    }

                    //Se tenho objectos para redistribuir, removo-os da minha lista e envio-os
                    if (toSendList.Count != 0)
                    {
                        foreach (PadIntInsider padint in toSendList)
                        {
                            myPadInts.Remove(padint.UID); //removo da minha propria lista
                        }
                        try
                        {
                            IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
                                "tcp://" + serverAddrPort + "/Server");
                            serv.UpdateObjects(toSendList);
                        }
                        catch (Exception e) { Console.WriteLine(e); }
                    }
                }
                //Se nao fui afectado pela entrada, nao tenho de fazer mais nada
            }
        }

        //Server-Server
        //Quando este servidor entra, eh possivel que algum outro tenho objectos que agora pertencem a este
        //portanto ao ser invocado este metodo, este servidor vai fazer update eh sua lista de objectos mantidos
        public void UpdateObjects(List<PadIntInsider> toSendList)
        {
            foreach (PadIntInsider padint in toSendList)
            {
                myPadInts.Add(padint.UID, padint);
            }
        }
        

        //Client-Server
        public bool TxBegin(string clientAddressPort)
        {
            //verifica se o server esta no estado "Normal". Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (currentState == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (currentState == State.Freezed)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
                Console.WriteLine("HELLO WORLD!!!");
            }

            //Se o cliente ja tem 1 tx a decorrer
            if (clients.ContainsKey(clientAddressPort))
                return false;
            else
            {
                //Contacta o master para obter um txId
                IMasterServer master = (IMasterServer)Activator.GetObject(typeof(IMasterServer),
                    "tcp://" + ServerNode.masterAddrPort + "/Master");
                int txId = master.getTxId();
                clients.Add(clientAddressPort, txId);//update tx ID for this client
                txServersList.Add(txId, new List<string>());
                DstmUtil.ShowClientsList(clients);
                return true;
            }
        }

        //Client-Server
        public PadInt CreatePadInt(string clientAddressPort, int uid)
        {

            //verifica se o server esta no estado "Normal". Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (currentState == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (currentState == State.Freezed)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
            }

            //verifica se o client tem 1 tx aberta
            if (!clients.ContainsKey(clientAddressPort)) throw new TxException("O cliente nao tem nenhuma Tx aberta!");
            //TODO: aqui deve ser possivel criar este padint fora duma tx em q eh committed automaticamente
            int txId = clients[clientAddressPort];

            //verifica quem eh o servidor responsavel (o hash eh feito la dentro do metodo)
            string responsible = DstmUtil.GetResponsibleServer(servers, uid);

            if (responsible == null)
                return null; //Nao deve acontecer...

            if (!responsible.Equals(myself)) //Se nao eh o prorio, procura o servidor correcto
            {                     
                try
                {   IServerServer serv = (IServerServer)Activator.GetObject(typeof(IServerServer),
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


            //Actualiza a lista de servidores com objectos usados nesta tx: Add( txId, coordAddrPort)
            if (!txServersList[txId].Contains(responsible))
                txServersList[txId].Add(responsible);

            DstmUtil.ShowTxServersList(txServersList);
            //Devolve um proxy ao cliente, com o qual o cliente vai comunicar com este servidor
            PadInt proxy = new PadInt(myself, clientAddressPort, uid);
            return proxy;             
        }


        //Client-Server
        public PadInt AccessPadInt(string clientAddressPort, int uid)
        {

            //verifica se o server esta no estado "Normal". Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (currentState == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (currentState == State.Freezed)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
            }


            //verifica se o client tem 1 tx aberta
            if (!clients.ContainsKey(clientAddressPort)) throw new TxException("O cliente nao tem nenhuma Tx aberta!");
            int txId = clients[clientAddressPort];

            //returns the portAddress of the server responsible for that uid
            string responsible = DstmUtil.GetResponsibleServer(servers, uid);

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

            //Actualiza a lista de servidores com objectos usados nesta tx: Add( txId, coordAddrPort)
            if(!txServersList[txId].Contains(responsible))
                txServersList[txId].Add(responsible); 
            //Devolve um proxy ao cliente, com o qual o cliente vai comunicar com este servidor
            PadInt proxy = new PadInt(myself, clientAddressPort, uid);
            return proxy;
            
                
        }

        //Client-Server
        public int Read(string clientAddressPort, int uid)
        {

            //verifica se o server esta no estado "Normal". Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (currentState == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (currentState == State.Freezed)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
            }

            //verifica se o client tem 1 tx aberta
            int txId = clients[clientAddressPort];
            if (txId == -1) throw new TxException("O cliente nao tem nenhuma Tx aberta!");

            //verifica quem eh o servidor responsavel (o hash eh feito la dentro do metodo)
            string responsible = DstmUtil.GetResponsibleServer(servers, uid);
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

            //verifica se o server esta no estado "Normal". Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (currentState == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (currentState == State.Freezed)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
            }

            //verifica se o client tem 1 tx aberta
            int txId = clients[clientAddressPort];
            if (txId == -1) throw new TxException("O cliente nao tem nenhuma Tx aberta!");

            //verifica quem eh o servidor responsavel (o hash eh feito la dentro do metodo)
            string responsible = DstmUtil.GetResponsibleServer(servers, uid);

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

            //verifica se o server esta no estado "Normal". Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (currentState == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (currentState == State.Freezed)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
            }

            //obtem a tx aberta
            int txId = clients[clientAddressPort];

            //obtem os servidores que guardam objectos que participaram na tx
            List<string> serverList = txServersList[txId];
            int numWaitingResponse = serverList.Count;
            bool canCommit = false;
            bool result;
            //Envio dos canCommits
            foreach(string server in serverList){ //TODO: paralelizar o envio destes pedidos
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
            //Remover a tx da lista de tx-servidores:
            txServersList.Remove(txId);
            //Remover a tx da lista dos clientes-tx
            string clientAddrPort = clients.Where(item => item.Value == txId).First().Key;
            clients.Remove(clientAddrPort);
            return result;
        }

        //Client-Server
        public bool TxAbort(string clientAddressPort)
        {

            //verifica se o server esta no estado "Normal". Se nao estiver, nao pode responder a este pedido (chamada de metodo)
            if (currentState == State.Failed)
                throw new RemotingException("Server has failed!");
            else if (currentState == State.Freezed)
            {
                var mre = new ManualResetEvent(false);
                _queuedThreads.Add(mre);
                mre.WaitOne();
            }

            //obtem a tx aberta
            int txId = clients[clientAddressPort];

            //obtem os servidores que guardam objectos que participaram na tx
            List<string> serverList = txServersList[txId];
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
            //Remover a tx da lista de tx-servidores:
            txServersList.Remove(txId);
            //Remover a tx da lista dos clientes-tx
            string clientAddrPort = clients.Where(item => item.Value == txId).First().Key;
            clients.Remove(clientAddrPort);
            return true;
        }


        //Server-Server
        //So o servidor responsavel pelo padint vai correr este metodo
        //Se o coordenador for ele proprio o responsavel, entao este metodo eh chamado localmente
        public void CreatePadInt(int uid, int txId)
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

        //Server-Server
        //So o servidor responsavel pelo padint vai correr este metodo
        //Se o coordenador for ele proprio o responsavel, entao este metodo eh chamado localmente
        //Nao tem de devolver a referencia remota, nem o valor, pq o valor vai obtido pelo cliente
        //atraves de uma chamada remota pelo protocolo...
        public void AccessPadInt(int uid, int txId)
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

        //Server-Server
        public int Read(int uid, int txId) {
            //search for the padint
            PadIntInsider padint;
            myPadInts.TryGetValue(uid, out padint);
            int result = padint.Read(txId);
            return result;
        }

        //Server-Server
        public void Write(int uid, int txId, int value) {
            //search for the padint
            PadIntInsider padint;
            myPadInts.TryGetValue(uid, out padint);
            padint.Write(txId, value);
            //returns immediately?
        }

        //Server-Server
        //DUVIDA: em que situacao o canCommit retorna false?
        public bool CanCommit(int txId)
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

        //Server-Server
        public void Commit(int txId)
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

        //Server-Server
        public void Abort(int txId)
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

        public bool Fail()
        {
            if (currentState == State.Failed)      // Ja esta failed, retorna false
                return false;
            else if (currentState == State.Freezed) 
            {
                clients = new SortedDictionary<string, int>();
                myPadInts = new SortedDictionary<int, PadIntInsider>();
                servers = null;
                mySInfo = new ServerInfo(0, 0, "");
                txServersList = new SortedDictionary<int, List<string>>();
                txObjList = new SortedDictionary<int, List<int>>();
                txCreatedObjList = new SortedDictionary<int, List<int>>();

                _queuedThreads.Clear();
                currentState = State.Failed;
                return true;
            }
            else                                // Caso esteja no estado "Normal"
            {
                currentState = State.Failed;
                return true;
            }

        }

        public bool Freeze()
        {
            if (currentState == State.Freezed || currentState == State.Failed)    // Ja esta freezed, retorna false. Se estiver failed não deve responder a isto (so a recov)
                return false;
            else
            {
                _queuedThreads.Clear();
                currentState = State.Freezed;
                return true;
            }
        }

        public bool Recover()
        {
            if (currentState == State.Normal)    // Ja esta normal, nao ha recover a fazer
                return false;
            else if (currentState == State.Freezed)
            {
                currentState = State.Normal;
                bool res = processQueueCommands();
                if (res)
                    return true;
                else
                    return false;
            }
            else
            {
                currentState = State.Normal; // Estava "Failed", volta ao normal
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

        //Server - Master
        //Para o status
        public void printSelfStatus()
        {
            Console.WriteLine("=============");
            Console.WriteLine("Server STATUS");
            Console.WriteLine("=============");
            Console.WriteLine("My interval: [" + mySInfo.getBegin() + ", "+ mySInfo.getEnd() + "]");
            DstmUtil.ShowPadIntsList(myPadInts);
            DstmUtil.ShowServerList(servers);
            DstmUtil.ShowClientsList(clients);
            DstmUtil.ShowTxServersList(txServersList);
        }

        // DEBUG METHOD ---- TO BE DELETED LATER
        public void debugRecover(string s)
        {
            Console.WriteLine(s);
        }

    }

}