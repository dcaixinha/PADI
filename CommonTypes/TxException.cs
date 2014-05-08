using System;
using System.Collections.Generic;
using System.Text;

namespace PADI_DSTM
{
    [Serializable]
    public class TxException : ApplicationException
    {
        public string reason;

        public TxException(string c)
        {
            reason = c;
        }

        public TxException(System.Runtime.Serialization.SerializationInfo info,
            System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
            reason = info.GetString("reason");
        }

        public override void GetObjectData(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("reason", reason);
        }
    }
}
