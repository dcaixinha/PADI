using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace PADI_DSTM
{

    public interface IServerClient
    {
        //Usado pelo client para debug
        string GetAddress();

        bool TxBegin(string clientAddrPort);

        PadInt CreatePadInt(string clientAddrPort, int uid);
        PadInt AccessPadInt(string clientAddrPort, int uid);
        int Read(string clientAddrPort, int uid);
        void Write(string clientAddrPort, int uid, int value);
        bool TxCommit(string clientAddrPort);
        bool TxAbort(string clientAddrPort);
        bool Fail();
        bool Freeze();
        bool Recover();

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
        ServerPackage GiveMeMyObjects();
        void SetReplicas(List<PadIntInsider> replicas);
        void UpdateReplicas(List<PadIntInsider> replicasToSend);
        void UpdateNetworkAfterCrash(string crashedServerAddrPort);
        List<PadIntInsider> GiveMeYourReplicas();
    }

    public interface IServerMaster
    { 
        void printSelfStatus();
    }

    public interface IMasterServer
    {
        MasterPackage RegisterServer(string port);
        int getTxId();
        Boolean DetectedCrash(string crashedServerAddrPort); 
    }

    public interface IMasterClient
    {
        string BootstrapClient(string addrPort);

        void Status();
    }

}