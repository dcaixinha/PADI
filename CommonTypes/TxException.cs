using System;
using System.Collections.Generic;
using System.Text;

namespace DSTM
{
    public class TxException : Exception
    {
        public TxException(string info) : base(info) //TODO quando lancar?
        {

        } 
    }
}
