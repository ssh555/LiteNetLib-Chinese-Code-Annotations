using System.Collections.Generic;
using System.Threading;

namespace LiteNetLib
{
    /// <summary>
    /// 包体处理(发送和接收处理)通道
    /// </summary>
    internal abstract class BaseChannel
    {
        /// <summary>
        /// 通道所属的NetPeer
        /// </summary>
        protected readonly NetPeer Peer;
        /// <summary>
        /// 通道中的待发送数据包队列
        /// </summary>
        protected readonly Queue<NetPacket> OutgoingQueue = new Queue<NetPacket>(NetConstants.DefaultWindowSize);
        /// <summary>
        /// Channel是否被添加到Peer的SendChannelQueue
        /// </summary>
        private int _isAddedToPeerChannelSendQueue;

        /// <summary>
        /// 通道中剩余的数据包数量
        /// </summary>
        public int PacketsInQueue => OutgoingQueue.Count;

        protected BaseChannel(NetPeer peer)
        {
            Peer = peer;
        }

        public void AddToQueue(NetPacket packet)
        {
            lock (OutgoingQueue)
            {
                OutgoingQueue.Enqueue(packet);
            }
            AddToPeerChannelSendQueue();
        }

        protected void AddToPeerChannelSendQueue()
        {
            if (Interlocked.CompareExchange(ref _isAddedToPeerChannelSendQueue, 1, 0) == 0)
            {
                Peer.AddToReliableChannelSendQueue(this);
            }
        }

        public bool SendAndCheckQueue()
        {
            bool hasPacketsToSend = SendNextPackets();
            if (!hasPacketsToSend)
                Interlocked.Exchange(ref _isAddedToPeerChannelSendQueue, 0);

            return hasPacketsToSend;
        }

        protected abstract bool SendNextPackets();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="packet"></param>
        /// <returns>是否需要回收此包</returns>
        public abstract bool ProcessPacket(NetPacket packet);
    }
}
