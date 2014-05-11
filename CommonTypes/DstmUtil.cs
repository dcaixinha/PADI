using System;
using System.Collections.Generic;
using System.Text;

using System.Net;
using System.Net.Sockets;

namespace PADI_DSTM
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

        //Returns a replica of the current padint (committed values only)
        public static PadIntInsider GetPadintReplicaFrom(PadIntInsider padint)
        {
            int uid = padint.UID;
            PadIntInsider replica = new PadIntInsider(uid);
            replica.COMMITREAD = padint.COMMITREAD;
            replica.COMMITWRITE = padint.COMMITWRITE;
            return replica;
        }

        //Returns null if no elements, or if theres only one element
        //beginInterval eh o de quem procura
        public static string GetNextServer(int beginInterval, SortedDictionary<int, ServerInfo> servers)
        {
            Boolean passingFirstServer = true;
            ServerInfo firstServer = null;
            Boolean foundMyself = false;
            foreach (KeyValuePair<int, ServerInfo> serverEntry in servers)
            {
                if (passingFirstServer)
                {
                    passingFirstServer = false;
                    firstServer = serverEntry.Value;
                }
                if (foundMyself) //se no ciclo anterior eu me encontrei, este eh o meu next
                    return serverEntry.Value.getPortAddress();
                //procuro-me a mim
                if (serverEntry.Key == beginInterval)
                    foundMyself = true; 
            }
            //Se chegou ao fim sem retornar eh pq o proximo eh o primeiro elemento ou se nao ha elementos...
            if (firstServer != null && firstServer.getBegin() != beginInterval )
                return firstServer.getPortAddress();
            else return null;
        }

        //Returns null if no elements, or if theres only one element
        //beginInterval eh o de quem procura
        public static string GetPreviousServer(int beginInterval, SortedDictionary<int, ServerInfo> servers)
        {
            ServerInfo previousEntry = null;
            int numServers = servers.Keys.Count;
            int lastElementIndex = new List<int>(servers.Keys)[numServers - 1];
            foreach (KeyValuePair<int, ServerInfo> serverEntry in servers)
            {
                if (serverEntry.Key == beginInterval) //se eu me encontrei
                { 
                    if (previousEntry != null)
                        return previousEntry.getPortAddress();
                    else //se nao havia previous, eh pq sou o primeiro, logo o meu previous eh o ultimo
                    {
                        if (numServers > 1)
                        {
                            return servers[lastElementIndex].getPortAddress();
                        }
                        else return null;
                    }
                }
                previousEntry = serverEntry.Value;
            }
            return null; //nunca deve acontecer, eu devo estar presente nesta lista... sempre...
        }

        //Used by Server, Master
        //Insere 1 servidor no dicionario ordenado
        //<int beginInterval, objct<int beginIntv, int endInterv, string portAddr>>
        //Retorna o ServerInfo do servidor que chamou este metodo (requester)
        public static SInfoPackage InsertServer(string newServerAddrPort, SortedDictionary<int, ServerInfo> servers, string requester)
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
                ServerInfo i1 = new ServerInfo(0, 0, null);
                ServerInfo i2 = new ServerInfo(0, 0, null);
                if (entry.getBegin() < entry.getEnd())
                {
                    i1 = new ServerInfo(entry.getBegin(), entry.getBegin() + ((entry.getEnd() - entry.getBegin()) / 2), serverToSplit);
                    i2 = new ServerInfo((entry.getBegin() + (entry.getEnd() - entry.getBegin()) / 2) + 1, entry.getEnd(), newServerAddrPort);
                }
                else
                {
                    i1 = new ServerInfo(entry.getBegin(), Int32.MaxValue, serverToSplit);
                    i2 = new ServerInfo(0, entry.getEnd(), newServerAddrPort);
                }

                servers.Add(i1.getBegin(), i1); 
                servers.Add(i2.getBegin(), i2);
                //Verifica se o requester foi afectado, actualiza o sInfo a ser retornado
                if (requester != null && serverToSplit.Equals(requester))
                    requesterSInfo = i1;
            }
            //ShowServerList(servers);
            if (serverToSplit.Equals(""))
                serverToSplit = null;
            return new SInfoPackage(requesterSInfo, serverToSplit);
        }

        //Retira o servidor crashado da lista dos servidores, actualiza a serverInfo do server que esta a tentar remover o
        //crashado das suas listas
        public static SInfoPackage RemoveServer(string crashedServerAddrPort, SortedDictionary<int, ServerInfo> servers, string requester)
        {
            int crashedIntervalBegin = 0;
            Boolean passingFirstServer = true;
            ServerInfo firstServer = new ServerInfo(-1, 0, null);
            Boolean foundCrashedServer = false;
            string serverToAglomerate = "";
            ServerInfo entry = new ServerInfo(-1, 0, null);
            ServerInfo requesterSInfo = new ServerInfo(-1, -1, requester); //inicializacao so por causa do warning

            foreach (KeyValuePair<int, ServerInfo> serverEntry in servers)
            {
                if (passingFirstServer)
                {
                    passingFirstServer = false;
                    firstServer = serverEntry.Value;
                }
                if (serverEntry.Value.getPortAddress().Equals(crashedServerAddrPort))
                {
                    crashedIntervalBegin = serverEntry.Value.getBegin();
                    foundCrashedServer = true;
                }
                else
                {
                    //procura o servidor que vai aglomerar o intervalo do que caiu
                    if (foundCrashedServer)
                    {
                        entry = serverEntry.Value;
                        serverToAglomerate = entry.getPortAddress();
                        foundCrashedServer = false; //impede de entrar aqui outra vez
                    }
                    //procura o ServerInfo do requester, para no fim o devolver (se este nao foi actualizado)
                    if (requester != null && serverEntry.Value.getPortAddress().Equals(requester)) //requester == null se for o master a chamar este metodo
                        requesterSInfo = serverEntry.Value;
                }
            }
            if (serverToAglomerate.Equals(""))
            { //Se chegou aqui eh pq quem aglomera eh o primeiro (com o ultimo)
                entry = firstServer;
                serverToAglomerate = entry.getPortAddress();
            }
            
            //constroi o novo intervalo aglomerado
            ServerInfo i1 = new ServerInfo(crashedIntervalBegin, entry.getEnd(), entry.getPortAddress());
            //remove a entrada do servidor que crashou
            servers.Remove(crashedIntervalBegin);
            //remove a entrada referente ao servidor cujo intervalo vai ser dividido
            servers.Remove(entry.getBegin());

            //Se removendo o que crashou ficar so um servidor, este tem o range todo!
            if (servers.Count == 0)
            {
                i1.setBegin(0);
                i1.setEnd(Int32.MaxValue);
            }
            servers.Add(i1.getBegin(), i1); //adiciona o novo com o intervalo maior
            //Verifica se o requester foi afectado, actualiza o sInfo a ser retornado
            if (requester != null && serverToAglomerate.Equals(requester))
                requesterSInfo = i1;

            return new SInfoPackage(requesterSInfo, serverToAglomerate);
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

        //Used by coordinator servers, which have a map of tx-objects (coordinator) to find which servers to contact
        //for commit, abort
        public static List<string> GetInvolvedServersList(SortedDictionary<int, ServerInfo> servers, List<int> objectIds)
        {
            List<string> serverList = new List<string>();
            foreach(int uid in objectIds){
                string serverAddrPort = GetResponsibleServer(servers, uid);
                if (!serverList.Contains(serverAddrPort))
                    serverList.Add(serverAddrPort);
            }
            return serverList;
        }


        //METODOS PARA O STATUS
        public static void ShowServerList(SortedDictionary<int, ServerInfo> servers)
        {
            Console.WriteLine("=== Servers List (One-Hop) ===");
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
            Console.WriteLine("=== Clients List (Coordinator) ===");
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

        //Shows which servers are involved in which txs at this coordinator
        public static void ShowTxServersList(SortedDictionary<int, List<int>> txObjCoordList, SortedDictionary<int, ServerInfo> servers)
        {
            Console.WriteLine("=== Tx-Servers List (Coordinator) ===");
            if (txObjCoordList.Count > 0)
            {
                Console.WriteLine("txId : server_1 server_2 ...");
                foreach (KeyValuePair<int, List<int>> kvp in txObjCoordList)
                {
                    List<string> involvedServers = GetInvolvedServersList(servers, kvp.Value);
                    Console.Write(kvp.Key + " : ");
                    foreach (string addrPort in involvedServers)
                        Console.Write(" " + addrPort);
                    Console.Write("\r\n");
                }
            }
            else
                Console.WriteLine("Empty!");
        }


        public static void ShowQueue(Queue<string> queue)
        {
            Console.WriteLine("=== Servers Queue (Master) ===");

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
            Console.WriteLine("=== Padints List (Local) ===");
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
                    foreach (int txid in padint.TENTREADS)
                    {
                        Console.Write(txid + " ");
                    }
                    if (padint.TENTREADS.Count > 0)
                        Console.Write("\r\n");
                    else
                        Console.WriteLine("Empty!");
                    Console.WriteLine("Tentative writes (txid - value list): ");
                    foreach (KeyValuePair<int, int> kvp in padint.TENTWRITES)
                    {
                        Console.Write(kvp.Key + " - " + kvp.Value + "  ");
                    }
                    if (padint.TENTWRITES.Count > 0)
                        Console.Write("\r\n");
                    else
                        Console.WriteLine("Empty!");
                    Console.WriteLine("----------");
                }
            }
            else
                Console.WriteLine("Empty!");
        }

        public static void ShowReplicas(SortedDictionary<int, PadIntInsider> replicatedPadInts)
        {
            Console.WriteLine("=== Replicas ===");
            if (replicatedPadInts.Count == 0)
            {
                Console.WriteLine("Empty!");
                return;
            }
            else
            {
                foreach (PadIntInsider padint in replicatedPadInts.Values)
                    Console.WriteLine("Uid: " + padint.UID + ", commited write: " + padint.COMMITWRITE);
            }
        }

        public static void ShowTxObjectsList(SortedDictionary<int, List<int>> txObjList)
        {
            Console.WriteLine("=== Tx-Object List (Responsible) ===");
            if (txObjList.Keys.Count > 0)
            {
                Console.WriteLine("txId : uid_1 uid_2 ...");
                foreach (KeyValuePair<int, List<int>> kvp in txObjList)
                {
                    Console.Write(kvp.Key + " ");
                    foreach (int uid in kvp.Value)
                        Console.Write(uid + " ");
                    Console.Write("\r\n");
                }
            }
            else
                Console.WriteLine("Empty!");
        }

        public static void ShowTxCreatedObjectsList(SortedDictionary<int, List<int>> txCreatedObjList)
        {
            Console.WriteLine("=== Tx-Created Object List (Responsible) ===");
            if (txCreatedObjList.Keys.Count > 0)
            {
                Console.WriteLine("txId : uid_1 uid_2 ...");
                foreach (KeyValuePair<int, List<int>> kvp in txCreatedObjList)
                {
                    Console.Write(kvp.Key + " ");
                    foreach (int uid in kvp.Value)
                        Console.Write(uid + " ");
                    Console.Write("\r\n");
                }
            }
            else
                Console.WriteLine("Empty!");
        }

        public static void ShowServerIntervals(ServerInfo sInfo)
        {
            Console.WriteLine("=== My interval ===");
            Console.WriteLine("[" + sInfo.getBegin() + ", " + sInfo.getEnd() + "]");
        }

    }
}
