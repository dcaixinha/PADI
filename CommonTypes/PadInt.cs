using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Remoting;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace PADI_DSTM
{
    //Classe proxy que faz a interaçao entre o client e o coordenador
    [Serializable]
    public class PadInt
    {
        private static string coordinatorAddressPort;
        private string coordinator;
        private string clientAddressPort;
        private int uid;
        private IMasterClient masterObj;
        private string master;

        public PadInt(string coordAddrPort, string clientAddrPort, int id)
        {
            coordinator = coordAddrPort;
            clientAddressPort = clientAddrPort;
            uid = id;
        }

        public static void SetCoordinator(string coord)
        {
            PadInt.coordinatorAddressPort = coord;
        }

        public string GetCoordinatorForInterception()
        {
            return coordinator;
        }

        public string GetCoordinator(){
            return PadInt.coordinatorAddressPort;
        }

        //Used by the library client so that read and write operations inside padint can contact the master
        public void SetMasterObj(string master){
            this.master = master;
            masterObj = (IMasterClient)Activator.GetObject(typeof(IMasterClient),
                    "tcp://" + master + "/Master");
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
                    "tcp://" + PadInt.coordinatorAddressPort + "/Server");
                int result = serv.Read(clientAddressPort, uid);
                return result;
            }
            catch (TxException) { throw; }
            catch (RemotingException) //coordenador em baixo
            {
                //Avisar o master que o coordenador falhou para lhe ser atribuido um novo coordenador
                //O novo coordenador terá toda a informacao necessaria para continuar a tx deste cliente,
                //portanto resta no fim, repetir o pedido ao novo coordenador
                IServerClient serverObj = UpdateCoordinatorForProxy();

                int result = serverObj.Read(clientAddressPort, uid);
                return result;
            }
        }

        // Writes the object in the context of the current transaction. This
        // method may throw a TxException.
        public void Write(int value)
        {
            //TODO faz 1 pedido de read ao coord
            try
            {
                IServerClient serv = (IServerClient)Activator.GetObject(typeof(IServerClient),
                    "tcp://" + PadInt.coordinatorAddressPort + "/Server");
                serv.Write(clientAddressPort, uid, value);
            }
            catch (TxException) { throw; }
            catch (RemotingException) //coordenador em baixo
            {
                //Avisar o master que o coordenador falhou para lhe ser atribuido um novo coordenador
                //O novo coordenador terá toda a informacao necessaria para continuar a tx deste cliente,
                //portanto resta no fim, repetir o pedido ao novo coordenador  
                IServerClient serverObj = UpdateCoordinatorForProxy();

                serverObj.Write(clientAddressPort, uid, value);
            }
            catch (Exception e) { Console.WriteLine(e); }
        }

        private IServerClient UpdateCoordinatorForProxy()
        {
            string oldServer = PadInt.coordinatorAddressPort;

            PadInt.coordinatorAddressPort = masterObj.MyCoordinatorFailed(clientAddressPort, oldServer);
            IServerClient serverObj = (IServerClient)Activator.GetObject(
                typeof(IServerClient),
                "tcp://" + PadInt.coordinatorAddressPort + "/Server");

            return serverObj;
        }
    }
}
