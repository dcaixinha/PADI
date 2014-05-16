using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace PADI_DSTM
{
    [Serializable]
    //Esta eh a classe interna aos servidores, que representa e guarda informacao sobre os padints, como as tabelas
    //das tentativas e committed, e o uid.
    public class PadIntInsider
    {
        private int uid;

        private int committedRead;
        private Tuple<int, int> committedWrite; //txid, value
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

        public int COMMITREAD
        {
            get { return committedRead; }
            set { this.committedRead = value; }
        }

        public Tuple<int, int> COMMITWRITE
        {
            get { return committedWrite; }
            set { this.committedWrite = value; }
        }
        public List<int> TENTREADS
        {
            get { return tentativeReads; }
            set { this.tentativeReads = value; }
        }
        public Dictionary<int, int> TENTWRITES
        {
            get { return tentativeWrites; }
            set { this.tentativeWrites = value; }
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
                    if (!tentativeReads.Contains(txId)) //permite varios reads sucessivos
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

                    Console.WriteLine("########################################");
                    Console.WriteLine("========================================");
                    Console.WriteLine("txId: " + txId);
                    Console.WriteLine("Num of tentative writes: " + tentativeWrites.Count);
                    if (tentativeWrites.Count > 0)
                        Console.WriteLine("max tentative write: " + tentativeWrites.Keys.Max());
                    Console.WriteLine("committedWrite: " + committedWrite.Item1);
                    Console.WriteLine("========================================");
                    Console.WriteLine("########################################");

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

        //Metodo interno
        private int getMaxReadTimestamp()
        {
            if (tentativeReads.Count > 0)
                return tentativeReads.Max();
            else return committedRead;
        }

        public void Write(int txId, int value)
        {
            //check if the maximum read timestamp on that is lower and if write committed is lower
            if (txId >= getMaxReadTimestamp()){
                if (txId > committedWrite.Item1)
                    //write a new tentative read
                    tentativeWrites[txId] = value;
                //else if(txId < committedWrite.Item1) { } //Ignore Obsolete Write Rule
            }
            else
                //Abort transaction Tc <- nos slides (mas aqui podemos lançar excepçao axo eu..)
                throw new TxException("Tx "+ txId +" chegou tarde demais ao recurso, nao pôde escrever!");
        }

        //2PC por default respondem sempre yes. Se for a baixo sera lancada 1 excepcao
        public bool CanCommit(int txId)
        {
            return true;
        }

        //Verifica se eh a tx com o id mais baixo, se nao for tem que esperar pelas outras
        public int Commit(int txId)
        {
            //Esperar até poder mesmo fazer commit
            int dSelected = GetMaxValueLowerThan(txId);
            //se nao houver tentative writes menor
            if (dSelected > txId) //so se o meu tentative write for o mais baixo eh q faço commit, senao espero
            {
                return -4; //Quem recebe este valor saber que vai ter de esperar num ciclo e voltar a tentar o commit
            }

            if (tentativeReads.Contains(txId))
            {
                //escreve o novo valor e TxID na committed table
                committedRead = tentativeReads.First(item => item == txId);
                //Remove o TxID da tentative table
                tentativeReads.Remove(txId);
            }
            if (tentativeWrites.ContainsKey(txId))
            {
                //escreve o novo valor e TxID na committed table
                KeyValuePair<int, int> novo = tentativeWrites.First(item => item.Key == txId);
                committedWrite = new Tuple<int, int>(novo.Key, novo.Value);
                //Remove o TxID da tentative table
                tentativeWrites.Remove(txId);
            }

            return 1;
        }

        public void Abort(int txId)
        {
            //verifica as tabelas de tentative reads e writes e apaga de la esta txId se existir
            if (tentativeReads.Contains(txId))
                tentativeReads.Remove(txId);
            if (tentativeWrites.Keys.Contains(txId))
                tentativeWrites.Remove(txId);
        }

    }
}
