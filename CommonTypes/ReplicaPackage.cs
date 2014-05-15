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
    public class ReplicaPackage
    {
        private SortedDictionary<int, List<int>> txObjReplica;
        private SortedDictionary<int, List<int>> txCreatedObjReplica;
        private List<PadIntInsider> replicas;

        public ReplicaPackage(List<PadIntInsider> padintReplicas, SortedDictionary<int, List<int>> txObjListReplica, SortedDictionary<int, List<int>> txCreatedObjListReplica)
        {
            replicas = padintReplicas;
            txObjReplica = txObjListReplica;
            txCreatedObjReplica = txCreatedObjListReplica;
        }

        public List<PadIntInsider> GetReplicas()
        {
            return replicas;
        }

        public SortedDictionary<int, List<int>> GetTxObj() {
            return txObjReplica;
        }

        public SortedDictionary<int, List<int>> GetTxCreatedObj()
        {
            return txCreatedObjReplica;
        }
    }
}
