using System;
using System.Collections.Generic;
using System.Text;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace DSTM
{
    public class PadInt
    {
        private int uid;
        private int val;

        public PadInt(int uid)
        {
            this.uid = uid;
        }

        //public int getUid() { return uid; }
        //public int getValue() { return value; }
        //public void setValue(int value) { this.value = value; }

        public int Value
        {
            get { return val; }
            set { val = value; }
        }

        public int UID
        {
            get { return uid; }
        }

    }
}
