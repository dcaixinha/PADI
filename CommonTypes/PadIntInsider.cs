using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace DSTM
{
    public class PadIntInsider
    {
        private int uid;
        private int committedRead;
        private Tuple<int,int> committedWrite; //txid, value
        private List<int> tentativeReads;
        private Dictionary<int, int> tentativeWrites; //txId, value


        public PadIntInsider(int uid)
        {
            committedRead = 0; //initial values
            committedWrite = new Tuple<int, int>(0,0); 
            tentativeReads = new List<int>();
            tentativeWrites = new Dictionary<int, int>();
            this.uid = uid;
        }

        public int UID
        {
            get { return uid; }
        }

        public int Read(int txId)
        {
            //check if committed write timestamp is lower
            if ( txId > committedWrite.Item1) { 
                //check if tentative write timestamp is lower, if so read from committed
                if (committedWrite.Item1 > tentativeWrites.Keys.Max())
                {
                    //write a new tentative read
                    tentativeReads.Add(txId);
                    return committedWrite.Item2; //value
                }
                else if(tentativeWrites.Keys.Max() == txId){ //se quer ler um valor q a propria tx escreveu -> DUVIDA poss fazer isto?
                    int val;
                    tentativeWrites.TryGetValue(txId, out val);
                    return val;
                }
                else
                {
                    //Wait until the tentative write commits or aborts, then reapply the read rule 
                    //TODO - synchronization waits and notifys?
                    return -1;
                }
            } else 
                //Abort transaction Tc <- nos slides (mas aqui podemos lançar excepçao axo eu..)
                throw new TxException("Tx "+ txId +" chegou tarde demais ao recurso, nao pôde ler!");
        }

        public void Write(int txId, int value)
        {
            //check if the maximum read timestamp on that is lower
            if (txId > committedRead)
            {
                //then check if write committed is lower
                if (txId > committedWrite.Item1)
                {
                    //write a new tentative read
                    tentativeWrites.Add(txId, value);
                }
            }
            else
                //Abort transaction Tc <- nos slides (mas aqui podemos lançar excepçao axo eu..)
                throw new TxException("Tx "+ txId +" chegou tarde demais ao recurso, nao pôde escrever!");
        }

    }
}
