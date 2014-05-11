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
    public class SInfoPackage
    {
        private ServerInfo sinfo;
        private string serverWhoTransfers;

        public SInfoPackage(ServerInfo sinfo, string server)
        {
            this.sinfo = sinfo;
            serverWhoTransfers = server;
        }

        public ServerInfo getServerInfo()
        {
            return sinfo;
        }

        public string getServerWhoTransfers()
        {
            return serverWhoTransfers;
        }
    }
}
