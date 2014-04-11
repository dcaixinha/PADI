using System;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels;
using System.Net.Sockets;
using System.Windows.Forms;
using DSTM;
using System.Collections.Generic;

namespace Client
{
    public class ClientNode
    {
        public static IServerClient serverObj; //inicializado na resposta ao bootstrap
        public static IMasterClient masterObj; //inicializado durante o bootstrap. Usado para os fails, freezes, etc.
        public static string masterAddrPort = "localhost:8086";
        string address = DstmUtil.LocalIPAddress();
        string porto;
        string myself;
        TcpChannel channel;

        public ClientNode()
        {
            porto = getRandomPort();
            myself = address + ":" + porto;
        }

        //Metodo interno que gera um porto aleatorio para o cliente
        private string getRandomPort()
        {
            Random random = new Random();
            return random.Next(1024, 65535).ToString();
        }

        //Metodo que desliga o channel
        //Usado para nos testes podermos abrir o canal sempre que eh inicializada a library, depois
        //de outro teste ja ter aberto um canal. No final de cada teste este metodo eh chamado.
        public void CloseChannel()
        {
            channel.StopListening(null);
            ChannelServices.UnregisterChannel(channel);
            channel = null;
        }

        //INIT
        public bool Init()
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
                    "tcp://" + ClientNode.masterAddrPort + "/Master");

                string serverAddrPort = masterObj.BootstrapClient(myself);
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
        public bool TxBegin()
        {
            try
            {
                bool result = serverObj.TxBegin(myself);
                return result;
            }
            catch (SocketException)
            {
                throw new Exception("Falhou a tentar começar uma Tx!");
            }
            catch (TxException e)
            {
                Console.WriteLine("TxException: " + e.reason);
                return false;
            }
        }

        //Creates a new shared object with the given uid. Returns null if the object already exists.
        public PadInt CreatePadInt(int uid)
        {
            try
            {
                PadInt result = serverObj.CreatePadInt(myself, uid);
                return result;
            }
            catch (TxException) { throw; }

        }

        //Returns a reference to a shared object with the given uid. Returns null if the object does not exist.
        public PadInt AccessPadInt(int uid)
        {
            try
            {
                PadInt result = serverObj.AccessPadInt(myself, uid);
                return result;
            }
            catch (TxException) { throw; }
        }

        public bool TxCommit()
        {
            try
            {
                bool result = serverObj.TxCommit(myself);
                return result;
            }
            catch (TxException) { throw; }
        }

        public bool TxAbort()
        {
            try
            {
                bool result = serverObj.TxAbort(myself);
                return result;
            }
            catch (TxException) { throw; }
        }

        public bool Status()
        {
            try
            {
                masterObj.Status();
                return true; //TODO: qdo eh q o status deve enviar false?
            }
            catch (Exception){ throw; }
        }

        public bool Fail(string serverURL)
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

        public bool Freeze(string serverURL)
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

        public bool Recover(string serverURL)
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

        static void Main()
        {
            ClientNode cn = new ClientNode();
            string input;
            Console.WriteLine("Commands:");
            Console.WriteLine("init | txbegin | recover | create <uid>");
            while (true)
            {
                input = Console.ReadLine();
                if (input.Equals("init"))
                    cn.Init();
                else if (input.Equals("txbegin"))
                    cn.TxBegin();
                else if (input.Equals("recover"))
                    cn.Recover("tcp://" + DstmUtil.LocalIPAddress() + ":4001" + "/Server"); // Para o freeze server test, pois tem de ser outra instancia a fazer o recov.
                else if (input.StartsWith("create")) //create <uid>
                {
                    string[] words = input.Split(' ');
                    int uid = Convert.ToInt32(words[1]);
                    try
                    {
                        PadInt padint = cn.CreatePadInt(uid);
                        Console.WriteLine(padint.Read());
                    }
                    catch (TxException e) { Console.WriteLine(e.reason); }
                }
            }
        }
    }

}
