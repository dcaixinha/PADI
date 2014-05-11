using System;
using System.Collections.Generic;
using System.Text;

namespace PADI_DSTM
{
    //Used for servers lists
    [Serializable()]
    public class ServerInfo
    {
        int begin;
        int end;
        string portAddress;

        public ServerInfo(int begin, int end, string portAddress)
        {
            this.begin = begin;
            this.end = end;
            this.portAddress = portAddress;
        }

        public int getBegin() { return begin; }
        public int getEnd() { return end; }
        public int getSize()
        {
            if (begin > end)
                return (Int32.MaxValue - begin + end);
            else
                return (end - begin);
        }
        public string getPortAddress() { return portAddress; }

        public void setBegin(int b) { this.begin = b; }
        public void setEnd(int e) { this.end = e; }
        public void setPortAddress(string portAddress) { this.portAddress = portAddress; }
    }

    //[Serializable()]
    //public class IntervalComparer : IComparer<Interval>
    //{
    //    // compare intervals
    //    public int Compare(Interval x, Interval y)
    //    {
    //        if (x.getBegin() > y.getBegin())
    //        {
    //            return 1;
    //        }
    //        else
    //        {
    //            return -1;
    //        }

    //    }
    //}
}
