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
        void UpdateReplicas(List<PadIntInsider> replicasToSend, SortedDictionary<int, List<int>> txObjListToSend, SortedDictionary<int, List<int>> txCreatedObjListToSend);
        void UpdateNetworkAfterCrash(string crashedServerAddrPort);
        ReplicaPackage GiveMeYourReplicas();
        void CommitReplicas(int txId);
        void AbortReplicas(int txId);
        void SetCoordReplicationInfo(SortedDictionary<int, List<int>> txObjCoordListToSend, SortedDictionary<string, int> clientsToSend);
        void SetTxObjReplicationInfo(SortedDictionary<int, List<int>> txObjListReceived, SortedDictionary<int, List<int>> txCreatedObjListReceived);
    }

    public interface IServerMaster
    { 
        void printSelfStatus();
        void UpdateNetworkAfterCrash(string crashedServerAddrPort);
    }

    public interface IMasterServer
    {
        MasterPackage RegisterServer(string port);
        int getTxId();
        Boolean DetectedCrash(string crashedServerAddrPort);
        string GetNextToCrashed(string crashedAddrPort);
        void Status();//Para o ultimo teste do mayFail
    }

    public interface IMasterClient
    {
        string BootstrapClient(string addrPort);
        string MyCoordinatorFailed(string clientAddrPort, string failedServerAddrPort);
        void Status();
    }

}