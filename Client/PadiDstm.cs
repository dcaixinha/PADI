using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Net.Sockets;
using System.Windows.Forms;
using PADI_DSTM;
using System.Collections.Generic;

namespace PADI_DSTM
{
    public class PadiDstm //previously ClientNode
    {
        public static IServerClient serverObj; //inicializado na resposta ao bootstrap
        public static IMasterClient masterObj; //inicializado durante o bootstrap. Usado para os fails, freezes, etc.
        public static string masterAddrPort = "localhost:8086";
        public static string serverAddrPort;
        static string address = DstmUtil.LocalIPAddress();
        static string porto = getRandomPort();
        static string myself = address + ":" + porto;
        static TcpChannel channel;

        //public ClientNode()
        //{
        //    porto = 
        //    myself = 
        //}

        //Metodo interno que gera um porto aleatorio para o cliente
        private static string getRandomPort()
        {
            Random random = new Random();
            return random.Next(1024, 65535).ToString();
        }

        //Metodo que desliga o channel
        //Usado para nos testes podermos abrir o canal sempre que eh inicializada a library, depois
        //de outro teste ja ter aberto um canal. No final de cada teste este metodo eh chamado.
        public static void CloseChannel()
        {
            channel.StopListening(null);
            ChannelServices.UnregisterChannel(channel);
            channel = null;
        }

        //INIT
        public static bool Init()
        {
            try
            {   //Cria o seu canal num porto aleatorio
                channel = new TcpChannel(Convert.ToInt32(porto));
                ChannelServices.RegisterChannel(channel, false);
            }
            // Caso ja haja 1 cliente na mesma maquina que escolheu o mesmo porto.. improvavel mas..
            catch (SocketException) { return false; }

            //Faz bootstrap no master
            try{
                masterObj = (IMasterClient)Activator.GetObject(typeof(IMasterClient),
                    "tcp://" + PadiDstm.masterAddrPort + "/Master");

                serverAddrPort = masterObj.BootstrapClient(myself);
                serverObj = (IServerClient)Activator.GetObject(
                    typeof(IServerClient),
                    "tcp://" + serverAddrPort + "/Server");

                //O cliente regista-se no servidor
           //     serverObj.RegisterClient(myself);

                //Escreve localmente quem eh o seu coordenador
                Console.WriteLine("Server coordenador atribuido: " + serverAddrPort);
            }
            catch (SocketException)
            {
                //Se falhou o bootstrap
                Console.WriteLine("Falhou o bootstrap no master!");
            }

            return true;
        }

        //TX BEGIN
        /// <exception cref="TxException"></exception>
        public static bool TxBegin()
        {
            try
            {
                bool result = serverObj.TxBegin(myself);
                return result;
            }
            catch (SocketException)
            {
                throw new TxException("Falhou a tentar começar uma Tx!");
            }
            catch (TxException e)
            {
                Console.WriteLine("TxException: " + e.reason);
                return false;
            }
            catch (RemotingException) //coordenador em baixo
            {
                //Avisar o master que o coordenador falhou para lhe ser atribuido um novo coordenador
                //O novo coordenador terá toda a informacao necessaria para continuar a tx deste cliente,
                //portanto resta no fim, repetir o pedido ao novo coordenador
                UpdateCoordinator();

                bool result = serverObj.TxBegin(myself);
                return result;
            }
        }

        //Creates a new shared object with the given uid. Returns null if the object already exists.
        public static PadInt CreatePadInt(int uid)
        {
            try
            {
                PadInt result = serverObj.CreatePadInt(myself, uid);
                result.SetMasterObj(masterAddrPort); //Intercept the padint proxy to give it the masterObj
                PadInt.SetCoordinator(result.GetCoordinatorForInterception());
                return result;
            }
            catch (TxException) { throw; }
            catch (RemotingException) //coordenador em baixo
            {
                //Avisar o master que o coordenador falhou para lhe ser atribuido um novo coordenador
                //O novo coordenador terá toda a informacao necessaria para continuar a tx deste cliente,
                //portanto resta no fim, repetir o pedido ao novo coordenador
                UpdateCoordinator();
                
                PadInt result = serverObj.CreatePadInt(myself, uid);
                result.SetMasterObj(masterAddrPort); //Intercept the padint proxy to give it the masterObj
                PadInt.SetCoordinator(result.GetCoordinatorForInterception());
                return result;
            }

        }

        //Returns a reference to a shared object with the given uid. Returns null if the object does not exist.
        public static PadInt AccessPadInt(int uid)
        {
            try
            {
                PadInt result = serverObj.AccessPadInt(myself, uid);
                result.SetMasterObj(masterAddrPort); //Intercept the padint proxy to give it the masterObj
                PadInt.SetCoordinator(result.GetCoordinatorForInterception());
                return result;
            }
            catch (TxException) { throw; }
            catch (RemotingException) //coordenador em baixo
            {
                //Avisar o master que o coordenador falhou para lhe ser atribuido um novo coordenador
                //O novo coordenador terá toda a informacao necessaria para continuar a tx deste cliente,
                //portanto resta no fim, repetir o pedido ao novo coordenador
                UpdateCoordinator();

                PadInt result = serverObj.AccessPadInt(myself, uid);
                result.SetMasterObj(masterAddrPort); //Intercept the padint proxy to give it the masterObj
                PadInt.SetCoordinator(result.GetCoordinatorForInterception());
                return result;
            }
        }

        public static bool TxCommit()
        {
            try
            {
                bool result = serverObj.TxCommit(myself);
                return result;
            }
            catch (TxException) { throw; }
            catch (RemotingException) //coordenador em baixo
            {
                //Avisar o master que o coordenador falhou para lhe ser atribuido um novo coordenador
                //O novo coordenador terá toda a informacao necessaria para continuar a tx deste cliente,
                //portanto resta no fim, repetir o pedido ao novo coordenador
                UpdateCoordinator();

                bool result = serverObj.TxCommit(myself);
                return result;
            }
        }

        public static bool TxAbort()
        {
            try
            {
                bool result = serverObj.TxAbort(myself);
                return result;
            }
            catch (RemotingException) //coordenador em baixo
            {
                //Avisar o master que o coordenador falhou para lhe ser atribuido um novo coordenador
                //O novo coordenador terá toda a informacao necessaria para continuar a tx deste cliente,
                //portanto resta no fim, repetir o pedido ao novo coordenador
                UpdateCoordinator();

                bool result = serverObj.TxAbort(myself);
                return result;
            }
        }

        public static bool Status()
        {
            try
            {
                masterObj.Status();
                return true; //TODO: qdo eh q o status deve enviar false?
            }
            catch (Exception){ throw; }
        }

        public static bool Fail(string serverURL)
        {
            try
            {
                IServerClient serverFailing = (IServerClient)Activator.GetObject(
                    typeof(IServerClient),
                    serverURL);

                bool result = serverFailing.Fail();
                return result;
            }
            catch (Exception e)
            {
                if (e is System.Net.Sockets.SocketException || e is NullReferenceException)
                {
                    System.Console.WriteLine("The client could not locate the server to fail!");
                    return false;
                }
                else throw;
            }
        }

        public static bool Freeze(string serverURL)
        {

            try
            {
                IServerClient serverFailing = (IServerClient)Activator.GetObject(
                    typeof(IServerClient),
                    serverURL );

                bool result = serverFailing.Freeze();
                return result;
            }
            catch (Exception e)
            {
                if (e is System.Net.Sockets.SocketException || e is NullReferenceException)
                {
                    System.Console.WriteLine("The client could not locate the server to freeze!");
                    return false;
                }
                else throw;
            }
        }

        public static bool Recover(string serverURL)
        {
            try
            {
                IServerClient serverRecovering = (IServerClient)Activator.GetObject(
                    typeof(IServerClient),
                    serverURL);

                bool result = serverRecovering.Recover();
                return result;
            }
            catch (Exception e)
            {
                if (e is System.Net.Sockets.SocketException || e is NullReferenceException)
                {
                    System.Console.WriteLine("The client could not locate the server to recover!");
                    return false;
                }
                else throw;
            }
        }

        //Internal method
        private static void UpdateCoordinator()
        {
            string oldServer = serverAddrPort;

            serverAddrPort = masterObj.MyCoordinatorFailed(myself, oldServer);
            serverObj = (IServerClient)Activator.GetObject(
                typeof(IServerClient),
                "tcp://" + serverAddrPort + "/Server");
        }

        static void Main()
        {
            PadiDstm cn = new PadiDstm();
            string input;
            Console.WriteLine("Commands:");
            Console.WriteLine("init | txbegin | recover | create <uid>");
            while (true)
            {
                input = Console.ReadLine();
                if (input.Equals("init"))
                    Init();
                else if (input.Equals("txbegin"))
                    TxBegin();
                else if (input.StartsWith("recover")){
                    string port = input.Split(' ')[1];
                    Recover("tcp://" + DstmUtil.LocalIPAddress() + ":" + port + "/Server"); // Para o freeze server test, pois tem de ser outra instancia a fazer o recov.
                }else if (input.StartsWith("create")) //create <uid>
                {
                    string[] words = input.Split(' ');
                    int uid = Convert.ToInt32(words[1]);
                    try
                    {
                        PadInt padint = CreatePadInt(uid);
                        Console.WriteLine(padint.Read());
                    }
                    catch (TxException e) { Console.WriteLine(e.reason); }
                }
            }
        }
    }

}
