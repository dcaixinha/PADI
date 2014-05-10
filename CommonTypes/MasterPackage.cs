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
    public class MasterPackage
    {
        private SortedDictionary<int, ServerInfo> servers;
        private string serverWhoTransfers;

        public MasterPackage(SortedDictionary<int, ServerInfo> servers, string server)
        {
            this.servers = servers;
            serverWhoTransfers = server;
        }

        public SortedDictionary<int, ServerInfo> getServers()
        {
            return servers;
        }

        public string getServerWhoTransfers()
        {
            return serverWhoTransfers;
        }
    }
}
