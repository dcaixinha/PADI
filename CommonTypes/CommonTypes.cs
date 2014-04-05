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
  //      void RegisterClient(string addrPort);

        //um cliente pode enviar uma msg para o servidor
        void Send(string message, string porto);

        bool TxBegin(string clientAddrPort);

        PadInt CreatePadInt(string clientAddrPort, int uid);
        PadInt AccessPadInt(string clientAddrPort, int uid);
        int Read(string clientAddrPort, int uid);
        void Write(string clientAddrPort, int uid, int value);
        bool TxCommit(string clientAddrPort);
        bool TxAbort(string clientAddrPort);

    }

    public interface IServerServer
    {
        //outro servidor envia updates com a topologia da rede
        void UpdateNetwork(string serverAddrPort);
        void CreatePadInt(int uid, int txId);
        void AccessPadInt(int uid, int txId);
        int Read(int uid, int txId);
        void Write(int uid, int txId, int value);
        bool CanCommit(int txId);
        void Commit(int txId);
        void Abort(int txId);
    }

    public interface IServerMaster
    {
        //Master envia updates para o servidor
        void Update(string message);
        bool Fail();
        bool Freeze();
        bool Recover();

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
        bool Fail(string serverURL);
        bool Freeze(string serverURL);
        bool Recover(string serverURL);
    }

}