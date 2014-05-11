using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace PADI_DSTM
{
    [Serializable]
    public class ServerPackage
    {
        private Dictionary<int, int> objTxToSendDict; //<uid, txid>
        private Dictionary<int, int> objCreatedTxToSendDict; //<uid, txid>
        private List<PadIntInsider> padintToSendList;
        private List<PadIntInsider> replicas;

        public ServerPackage(List<PadIntInsider> padintToSendList, Dictionary<int, int> objTxToSendDict, Dictionary<int, int> objCreatedTxToSendDict, List<PadIntInsider> replicas)
        {
            this.objTxToSendDict = objTxToSendDict;
            this.objCreatedTxToSendDict = objCreatedTxToSendDict;
            this.padintToSendList = padintToSendList;
            this.replicas = replicas;
        }

        public Dictionary<int, int> GetObjTxToSendDict()
        {
            return objTxToSendDict;
        }

        public Dictionary<int, int> GetObjCreatedTxToSendDict()
        {
            return objCreatedTxToSendDict;
        }

        public List<PadIntInsider> GetPadintToSendList()
        {
            return padintToSendList;
        }

        public List<PadIntInsider> GetReplicas()
        {
            return replicas;
        }
    }
}
