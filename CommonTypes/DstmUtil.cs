using System;
using System.Collections.Generic;
using System.Text;

using System.Net;
using System.Net.Sockets;

namespace DSTM
{
    public static class DstmUtil
    {
        public static string LocalIPAddress()
        {
            IPHostEntry host;
            string localIP = "";
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    localIP = ip.ToString();
                    break;
                }
            }
            return localIP;
        }

        //Used by Server, Master
        //Insere 1 servidor no dicionario ordenado
        //<int beginInterval, objct<int beginIntv, int endInterv, string portAddr>>
        //Retorna o ServerInfo do servidor que chamou este metodo (requester)
        public static ServerInfo InsertServer(string newServerAddrPort, SortedDictionary<int, ServerInfo> servers, string requester)
        {
            int intervalSize = 0; //the size of the interval to be splitted
            string serverToSplit = "";
            ServerInfo entry = new ServerInfo(-1, 0, null);
            ServerInfo requesterSInfo = new ServerInfo(-1,-1, requester); //inicializacao so por causa do warning
            foreach (KeyValuePair<int, ServerInfo> serverEntry in servers)
            {
                //procura o servidor que eh responsavel pelo maior intervalo
                if (serverEntry.Value.getSize() >= intervalSize)
                {
                    entry = serverEntry.Value;
                    intervalSize = entry.getSize();
                    serverToSplit = entry.getPortAddress();
                }
                //procura o ServerInfo do requester, para no fim o devolver (se este nao foi actualizado)
                if (requester != null && serverEntry.Value.getPortAddress().Equals(requester)) //requester == null se for o master a chamar este metodo
                    requesterSInfo = serverEntry.Value;
            }
            if (entry.getBegin() == -1) //se for o primeiro servidor a ser acrescentado
            {
                entry = new ServerInfo(0, int.MaxValue, newServerAddrPort);
                servers.Add(0, entry);
            }
            else
            {
                //remove a entrada referente ao servidor cujo intervalo vai ser dividido
                servers.Remove(entry.getBegin()); 
                //constroi os novos intervalos
                ServerInfo i1 = new ServerInfo(entry.getBegin(), entry.getBegin() + ((entry.getEnd() - entry.getBegin()) / 2), serverToSplit);
                ServerInfo i2 = new ServerInfo((entry.getBegin() + (entry.getEnd() - entry.getBegin()) / 2) + 1, entry.getEnd(), newServerAddrPort);
                servers.Add(i1.getBegin(), i1); //antigo com o intervalo mais curto
                servers.Add(i2.getBegin(), i2); //new server
                //Verifica se o requester foi afectado, actualiza o sInfo a ser retornado
                if (requester != null && serverToSplit.Equals(requester))
                    requesterSInfo = i1;
            }
            //ShowServerList(servers);
            return requesterSInfo;
        }

        //Used by: Servers, Master
        //Checks whether or not a addrPort is already registered in any ServerInfo object inside the servers dictionary
        public static bool ServerInfoContains(SortedDictionary<int, ServerInfo> servers, string addrPort)
        {
            int count = 0;
            foreach (ServerInfo sInfo in servers.Values)
            {
                if (!sInfo.getPortAddress().Equals(addrPort))
                {
                    count++;
                }
            }
            return count != servers.Count;
        }

        //Devolve o address:port do servidor responsavel pelo objecto com a uid dada
        public static string GetResponsibleServer(SortedDictionary<int, ServerInfo> servers, int uid)
        {
            int hashedUid = HashMe(uid);
            foreach (KeyValuePair<int, ServerInfo> serverEntry in servers)
            {
                if (serverEntry.Value.getBegin() <= hashedUid && 
                        serverEntry.Value.getEnd() >= hashedUid) //intervalo eh [ , ] -> o fim conta
                {
                    return serverEntry.Value.getPortAddress();
                }
            }
            return null; //Nunca deve acontecer!
        }

        //Uses an implementation of Murmur hashing
        public static int HashMe(int num)
        {
            Murmur3 m = new Murmur3();
            return Math.Abs((int)BitConverter.ToUInt64(m.ComputeHash(BitConverter.
                                GetBytes((ulong)num)), 0));
        }


        //METODOS PARA O STATUS
        public static void ShowServerList(SortedDictionary<int, ServerInfo> servers)
        {
            Console.WriteLine("Servers List");
            if (servers.Count > 0)
            {
                Console.WriteLine("begin-end | Server");
                foreach (ServerInfo sInfo in servers.Values)
                {
                    Console.WriteLine(sInfo.getBegin() + "-" + sInfo.getEnd() + " | " + sInfo.getPortAddress());
                }
            }
            else
                Console.WriteLine("Empty!");
        }

        public static void ShowClientsList(SortedDictionary<string, int> clients)
        {
            Console.WriteLine("Clients List");
            if (clients.Count > 0)
            {
                Console.WriteLine("Client | TxId");
                foreach (KeyValuePair<string, int> item in clients)
                {
                    Console.WriteLine(item.Key + " | " + item.Value);
                }
            }
            else
                Console.WriteLine("Empty!");
        }

        public static void ShowTxServersList(SortedDictionary<int, List<string>> txServersList)
        {
            Console.WriteLine("Tx - Servers List");
            if (txServersList.Count > 0)
            {
                Console.WriteLine("txId : server_1, server_2, ...");
                foreach (KeyValuePair<int, List<string>> kvp in txServersList)
                {
                    Console.Write(kvp.Key+" : ");
                    foreach(string addrPort in kvp.Value)
                        Console.Write(" " + addrPort);
                    Console.Write("\r\n");
                }
            }
            else
                Console.WriteLine("Empty!");
        }

        public static void ShowQueue(Queue<string> queue)
        {
            Console.WriteLine("Servers Queue:");

            string[] contents = queue.ToArray();
            if (contents.Length > 0)
            {
                foreach(string serverAddrPort in contents)
                {
                    Console.WriteLine(serverAddrPort);
                }
            }
            else
                Console.WriteLine("Empty!");
        }

        public static void ShowPadIntsList(SortedDictionary<int, PadIntInsider> padints)
        {
            Console.WriteLine("Padints List:");
            if (padints.Count > 0)
            {
                foreach (PadIntInsider padint in padints.Values)
                {
                    Console.WriteLine("--PADINT--");
                    Console.WriteLine("Uid: " + padint.UID);
                    Console.WriteLine("HashedUid: " + DstmUtil.HashMe(padint.UID));
                    Console.WriteLine("Committed read (txid): " + padint.COMMITREAD);
                    Console.WriteLine("Committed write (txid - value): " + padint.COMMITWRITE.Item1 + " - " + padint.COMMITWRITE.Item2);
                    Console.WriteLine("Tentative reads (txid list): ");
                    Console.WriteLine("----------");
                    foreach (int txid in padint.TENTREADS)
                    {
                        Console.Write(txid + " ");
                    }
                    Console.Write("\r\n");
                    Console.WriteLine("Tentative writes (txid - value list): ");
                    foreach (KeyValuePair<int, int> kvp in padint.TENTWRITES)
                    {
                        Console.WriteLine(kvp.Key + " - " + kvp.Value);
                    }
                }
            }
            else
                Console.WriteLine("Empty!");
        }

    }
}
