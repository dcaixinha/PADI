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
    //Classe proxy que faz a interacao entre o client e o coordenador
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

        //Usado pela PadiDstm para que as operacoes de read e write dentro do padint possam contactar com o master
        //quando necessario (quando recebem um remoting exception)
        public void SetMasterObj(string master){
            this.master = master;
            masterObj = (IMasterClient)Activator.GetObject(typeof(IMasterClient),
                    "tcp://" + master + "/Master");
        }

        //Metodos usados pelo cliente:
        // Le o objecto no contexto da tx actual. Devolve o valor do objecto.
        // Este metodo pode lancar uma TxException.
        public int Read()
        {
            //Faz 1 pedido de read ao coord
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

        // Escreve o valor no objecto, no contexto da tx actual.
        // Este metodo pode lancar uma TxException.
        public void Write(int value)
        {
            //Faz 1 pedido de write ao coord
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
