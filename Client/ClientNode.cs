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

            //Instancia o seu objecto remoto, atraves do qual o servidor lhe envia respostas
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(Client),
                "Client", WellKnownObjectMode.Singleton);

            //Faz bootstrap num servidor bem conhecido
            try
            {
                IMasterClient master = (IMasterClient)Activator.GetObject(typeof(IMasterClient),
                    "tcp://" + ClientNode.masterAddrPort + "/Master");

                //O cliente faz bootstrap no master
                string serverAddrPort = master.BootstrapClient(myself);
                serverObj = (IServerClient)Activator.GetObject(
                    typeof(IServerClient),
                    "tcp://" + serverAddrPort + "/Server");
                //O cliente regista-se no servidor
                serverObj.RegisterClient(myself);

                //Escreve localmente quem eh o seu coordenador
                Console.WriteLine("Server coordenador atribuido: " + serverAddrPort + "\r\n");
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
            catch (TxException e) { Console.WriteLine(e.reason); }
            return null;

        }

        //Returns a reference to a shared object with the given uid. Returns null if the object does not exist.
        public PadInt AccessPadInt(int uid)
        {
            try
            {
                PadInt result = serverObj.AccessPadInt(myself, uid);
                return result;
            }
            catch (TxException) { }
            return null;
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

        //Chat
        public void Send(string msg)
        {
            try
            {
                Console.WriteLine("Sending "+msg+" to "+serverObj.GetAddress());
                serverObj.Send(msg + "\r\n", porto);
            }
            catch (Exception e)
            {
                if (e is SocketException || e is NullReferenceException)
                {
                    System.Console.WriteLine("Could not locate server");
                }
                else throw;
            }
        }
        static void Main()
        {
            ClientNode cn = new ClientNode();
            string input;
            Console.WriteLine("Commands:");
            Console.WriteLine("init | txbegin | create <uid>");
            while (true)
            {
                input = Console.ReadLine();
                if (input.Equals("init"))
                    cn.Init();
                else if (input.Equals("txbegin"))
                    cn.TxBegin();
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
                else
                    cn.Send(input);
            }
        }
    }

    //Objecto remoto dos clientes, atraves do qual o servidor envia respostas
    public class Client : MarshalByRefObject, IClientServer, IClientMaster
    {
        public void Update(string msg)
        {
            Console.WriteLine(msg);
        }

    }

}
