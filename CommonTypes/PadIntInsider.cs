using System;
using System.Collections.Generic;
using System.Text;

using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace DSTM
{
    public class PadIntInsider
    {
        private int uid;
        private int val;

        public PadIntInsider(int uid)
        {
            this.uid = uid;
        }

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
