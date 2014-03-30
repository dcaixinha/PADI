﻿using System;
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

        //INIT
        public bool Init()
        {
            try
            {   //Cria o seu canal num porto aleatorio
                TcpChannel channel = new TcpChannel(Convert.ToInt32(porto));
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
        }

        //Creates a new shared object with the given uid. Returns null if the object already exists.
        public PadInt CreatePadInt(int uid)
        {
            PadInt result = serverObj.CreatePadInt(myself, uid);
            return result;
        }

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
            while (true)
            {
                input = Console.ReadLine();
                if (input.Equals("init"))
                    cn.Init();
                else if (input.Equals("txbegin"))
                    cn.TxBegin();
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