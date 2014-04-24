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

                int dSelected = GetMaxValueLowerThan(txId);
                //if there's no tentative writes lower than us
                if (dSelected == committedWrite.Item1)
                {
                    //write a new tentative read
                    tentativeReads.Add(txId);
                    return committedWrite.Item2; //value
                }
                else if(dSelected == txId){ //se quer ler um valor q a propria tx escreveu, e ja so ha esse que eh menor ou igual
                    int val;
                    tentativeWrites.TryGetValue(txId, out val);
                    return val;
                }
                else
                {
                    //Wait until the tentative write commits or aborts, then reapply the read rule 

                    //Console.WriteLine("########################################");
                    //Console.WriteLine("========================================");
                    //Console.WriteLine("txId: " + txId);
                    //Console.WriteLine("Num of tentative writes: " + tentativeWrites.Count);
                    //if (tentativeWrites.Count > 0)
                    //    Console.WriteLine("max tentative write: " + tentativeWrites.Keys.Max());
                    //Console.WriteLine("committedWrite: " + committedWrite.Item1);
                    //Console.WriteLine("========================================");
                    //Console.WriteLine("########################################");

                    //This value will be tested on the server who calls this method,
                    //and if it returns -4 it will lock try to read again and unlock
                    //until it has a valid return (not -4)
                    return -4;
                }
            } else 
                //Abort transaction Tc <- nos slides (mas aqui podemos lançar excepçao axo eu..)
                throw new TxException("Tx "+ txId +" chegou tarde demais ao recurso, nao pôde ler!");
        }

        //Metodo interno ao padintInsider, que devolve o maior valor de write timestamp que eh
        //menor que o valor passado por parametro, esteja na tabela committed ou tentative.
        //Se o maior for o proprio e for das tentatives, verifica se existe algum inferior
        //e devolve esse, ate nao haver mais inferiores na tabela das tentativas
        private int GetMaxValueLowerThan(int txId){
            if (tentativeWrites.Count == 0)
                return committedWrite.Item1;
            else if (tentativeWrites.Count == 1 && tentativeWrites.Keys.FirstOrDefault() == txId)
                return txId;
            else
            {
                List<int> lowers = new List<int>();
                foreach (int id in tentativeWrites.Keys)
                {
                    if (id <= txId)
                        lowers.Add(id);
                }
                lowers = lowers.OrderByDescending(x => x).ToList();
                if (lowers.Count == 1 && lowers.FirstOrDefault() == txId)
                    return lowers.FirstOrDefault();
                else
                {
                    foreach (int element in lowers)
                    {
                        if (element != txId)
                            return element;
                    }
                }
                return committedWrite.Item1; //There's no tentative writes lower than us. The only lower is the committed
            }
        }

        public void Write(int txId, int value)
        {
            //check if the maximum read timestamp on that is lower and if write committed is lower
            if (txId > committedRead && txId > committedWrite.Item1)
            {
                //write a new tentative read
                tentativeWrites.Add(txId, value);
            }
            else
                //Abort transaction Tc <- nos slides (mas aqui podemos lançar excepçao axo eu..)
                throw new TxException("Tx "+ txId +" chegou tarde demais ao recurso, nao pôde escrever!");
        }

        public bool CanCommit(int txId)
        {
            //if it already answered positively to a canCommit
            if (temporaryRead != -1 || temporaryWrite != null)
                return false; 
           
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
            return true;
        }

        //Nao precisa de argumentos, pq se ja respondeu yes ao canCommit, entao vai fazer commit sobre essa
        //e ira responder nao a canCommits de outros
        public void Commit()
        { 
            //the temporary values can be discarded, and that's it
            temporaryWrite = null;
            temporaryRead = -1;
        }

        public void Abort(int txId)
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
            //verifica as tabelas de tentative reads e writes e apaga de la esta txId se existir
            if (tentativeReads.Contains(txId))
                tentativeReads.Remove(txId);
            if (tentativeWrites.Keys.Contains(txId))
                tentativeWrites.Remove(txId);
        }

    }
}
