using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace PADI_DSTM
{
    [Serializable]
    public class PadIntInsider
    {
        private int uid;

        private int committedRead;
        private Tuple<int, int> committedWrite; //txid, value
        private List<int> tentativeReads;
        private Dictionary<int, int> tentativeWrites; //txId, value
        private int temporaryRead;
        private Tuple<int, int> temporaryWrite;


        public PadIntInsider(int uid)
        {
            committedRead = 0; //initial values
            committedWrite = new Tuple<int, int>(0,0); 
            tentativeReads = new List<int>();
            tentativeWrites = new Dictionary<int, int>();
            temporaryRead = -1;
            temporaryWrite = null;
            this.uid = uid;
        }

        public int UID
        {
            get { return uid; }
        }

        public int COMMITREAD
        {
            get { return committedRead; }
        }

        public Tuple<int, int> COMMITWRITE
        {
            get { return committedWrite; }
        }
        public List<int> TENTREADS
        {
            get { return tentativeReads; }
        }
        public Dictionary<int, int> TENTWRITES
        {
            get { return tentativeWrites; }
        }

        public int Read(int txId)
        {
            //check if committed write timestamp is lower
            if ( txId > committedWrite.Item1) { 
                //check if tentative write timestamp is lower, if so read from committed
                if (tentativeWrites.Count == 0 || committedWrite.Item1 > tentativeWrites.Keys.Max())
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

        public void CanCommit(int txId)
        {
            
            if (tentativeReads.Contains(txId))
            {
                //store temporarily the old committed value and the committed TxID
                temporaryRead = committedRead;
                //write the new value and TxID on the committed table
                committedRead = tentativeReads.First(item => item == txId);
                //Then remove the TxID from the tentative table
                tentativeReads.Remove(txId);
            }
            if (tentativeWrites.ContainsKey(txId))
            {
                //store temporarily the old committed value and the committed TxID
                temporaryWrite = committedWrite;
                //write the new value and TxID on the committed table
                KeyValuePair<int, int> novo = tentativeWrites.First(item => item.Key == txId);
                committedWrite = new Tuple<int, int>(novo.Key, novo.Value);
                //Then remove the TxID from the tentative table
                tentativeWrites.Remove(txId);
            }   
        }

        //Nao precisa de argumentos, pq se ja respondeu yes ao canCommit, entao vai fazer commit sobre essa
        //e ira responder nao a canCommits de outros
        public void Commit()
        { 
            //the temporary values can be discarded, and that's it
            temporaryWrite = null;
            temporaryRead = -1;
        }

        public void Abort()
        {
            //since that TxID was already removed from the tentatives table,
            //all that's left to do is rewrite the temporary values back as committed
            if (temporaryWrite != null)
            {
                committedWrite = temporaryWrite;
                temporaryWrite = null;
            }
            if (temporaryRead != -1)
            {
                committedRead = temporaryRead;
                temporaryRead = -1;
            }
        }

    }
}
