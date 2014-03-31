using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace DSTM
{

    public interface IClientServer
    {
        void Update(string message);
    }

    public interface IClientMaster
    {
        //void BootstrapResponse(string serverAddrPort, string clientAddrPort);
    }

    public interface IServerClient
    {
        //Usado pelo client para debug
        string GetAddress();

        //um cliente pode registar-se num servidor
        void RegisterClient(string addrPort);

        //um cliente pode enviar uma msg para o servidor
        void Send(string message, string porto);

        bool TxBegin(string clientPortAddress);

        PadInt CreatePadInt(string clientPortAddress, int uid);

    }

    public interface IServerServer
    {
        //outro servidor envia updates com a topologia da rede
        void UpdateNetwork(string serverAddrPort);
        PadInt CreatePadInt(int uid);
    }

    public interface IServerMaster
    {
        //Master envia updates para o servidor
        void Update(string message);

    }

    public interface IMasterServer
    {
        void Send(string message);
        SortedDictionary<int, ServerInfo> RegisterServer(string port);
        int getTxId();
    }

    public interface IMasterClient
    {
        string BootstrapClient(string addrPort);
    }

}