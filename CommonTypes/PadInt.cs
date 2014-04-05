using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace DSTM
{
    //Classe proxy que faz a interaçao entre o client e o coordenador
    [Serializable]
    public class PadInt
    {
        private string coordinatorAddressPort;
        private string clientAddressPort;
        private int uid;

        public PadInt(string coordAddrPort, string clientAddrPort, int id)
        {
            coordinatorAddressPort = coordAddrPort;
            clientAddressPort = clientAddrPort;
            uid = id;
        }

        //Metodos usados pelo cliente:
        // Reads the object in the context of the current transaction. Returns the value
        // of the object. This method may throw a TxException.
        public int Read()
        {
            //TODO faz 1 pedido de read ao coord
            try
            {
                IServerClient serv = (IServerClient)Activator.GetObject(typeof(IServerClient),
                    "tcp://" + coordinatorAddressPort + "/Server");
                int result = serv.Read(clientAddressPort, uid);
                return result;
            }
            catch (TxException) { throw; }
        }

        // Writes the object in the context of the current transaction. This
        // method may throw a TxException.
        public void Write(int value)
        {
            //TODO faz 1 pedido de read ao coord
            try
            {
                IServerClient serv = (IServerClient)Activator.GetObject(typeof(IServerClient),
                    "tcp://" + coordinatorAddressPort + "/Server");
                serv.Write(clientAddressPort, uid, value);
            }
            catch (TxException) { throw; }
            catch (Exception e) { Console.WriteLine(e); }
        }
    }
}
