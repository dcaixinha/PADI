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
        public static void InsertServer(string serverAddrPort, SortedDictionary<int, ServerInfo> servers)
        {
            int intervalSize = 0; //the size of the interval to be splitted
            string serverToSplit = "";
            ServerInfo entry = new ServerInfo(-1, 0, null);
            foreach (KeyValuePair<int, ServerInfo> serverEntry in servers)
            {
                if (serverEntry.Value.getSize() >= intervalSize)
                {
                    entry = serverEntry.Value;
                    intervalSize = entry.getSize();
                    serverToSplit = entry.getPortAddress();
                }
            }
            if (entry.getBegin() == -1) //se for o primeiro servidor a ser acrescentado
            {
                entry = new ServerInfo(0, int.MaxValue, serverAddrPort);
                servers.Add(0, entry);
            }
            else
            {
                servers.Remove(entry.getBegin());
                ServerInfo i1 = new ServerInfo(entry.getBegin(), entry.getBegin() + ((entry.getEnd() - entry.getBegin()) / 2), serverToSplit);
                ServerInfo i2 = new ServerInfo((entry.getBegin() + (entry.getEnd() - entry.getBegin()) / 2) + 1, entry.getEnd(), serverAddrPort);
                servers.Add(i1.getBegin(), i1);
                servers.Add(i2.getBegin(), i2); //new server
            }
            ShowServerList(servers);
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
                        serverEntry.Value.getEnd() >= hashedUid)
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


        //METODOS PARA DEBUG
        public static void ShowServerList(SortedDictionary<int, ServerInfo> servers)
        {
            Console.WriteLine("ShowServersList");
            Console.WriteLine("begin-end | Server");
            foreach (ServerInfo sInfo in servers.Values)
            {
                Console.WriteLine(sInfo.getBegin() + "-" + sInfo.getEnd() + " | " + sInfo.getPortAddress());
            }
        }

        public static void ShowClientsList(SortedDictionary<string, int> clients)
        {
            Console.WriteLine("ShowClientsList");
            Console.WriteLine("Client | TxId");
            foreach (KeyValuePair<string, int> item in clients)
            {
                Console.WriteLine(item.Key + " | " + item.Value);
            }
        }

        public static void ShowTxServersList(SortedDictionary<int, List<string>> txServersList)
        {
            Console.WriteLine("ShowTxServersList");
            Console.WriteLine("txId : serverAddrPort1,...");
            foreach (KeyValuePair<int, List<string>> kvp in txServersList)
            {
                Console.Write(kvp.Key+" : ");
                foreach(string addrPort in kvp.Value)
                    Console.Write(" " + addrPort);
                Console.Write("\r\n");
            }
        }

    }
}
