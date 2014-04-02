using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSTM
{
    //Classe proxy que faz a interaçao entre o client e o coordenador
    public class PadInt
    {
        private string coordinatorAddressPort;
        public PadInt(string addrPort)
        {
            coordinatorAddressPort = addrPort;
        }

        //Metodos usados pelo cliente:
        public int Read()
        {
            //TODO faz 1 pedido de read ao coord
            return -1;
        }
        public void Write(int value)
        {
            //TODO faz 1 pedido de read ao coord
        }
    }
}
