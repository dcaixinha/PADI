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

        public ServerPackage(List<PadIntInsider> padintToSendList, Dictionary<int, int> objTxToSendDict, Dictionary<int, int> objCreatedTxToSendDict)
        {
            this.objTxToSendDict = objTxToSendDict;
            this.objCreatedTxToSendDict = objCreatedTxToSendDict;
            this.padintToSendList = padintToSendList;
        }

        public Dictionary<int, int> getObjTxToSendDict()
        {
            return objTxToSendDict;
        }

        public Dictionary<int, int> getObjCreatedTxToSendDict()
        {
            return objCreatedTxToSendDict;
        }

        public List<PadIntInsider> getPadintToSendList()
        {
            return padintToSendList;
        }
    }
}
