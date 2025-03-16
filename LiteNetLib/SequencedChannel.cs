using System;

namespace LiteNetLib
{
    internal sealed class SequencedChannel : BaseChannel
    {
        private int _localSequence;
        private ushort _remoteSequence;
        /// <summary>
        /// 是否为可靠通道 -> ack
        /// </summary>
        private readonly bool _reliable;
        /// <summary>
        /// 最后发送的包
        /// </summary>
        private NetPacket _lastPacket;
        private readonly NetPacket _ackPacket;
        private bool _mustSendAck;
        private readonly byte _id;
        private long _lastPacketSendTime;

        public SequencedChannel(NetPeer peer, bool reliable, byte id) : base(peer)
        {
            _id = id;
            _reliable = reliable;
            if (_reliable)
                _ackPacket = new NetPacket(PacketProperty.Ack, 0) {ChannelId = id};
        }

        protected override bool SendNextPackets()
        {
            // 如果是可靠通道且发送队列为空，检查是否需要重传上一个包
            if (_reliable && OutgoingQueue.Count == 0)
            {
                long currentTime = DateTime.UtcNow.Ticks;
                long packetHoldTime = currentTime - _lastPacketSendTime;
                if (packetHoldTime >= Peer.ResendDelay * TimeSpan.TicksPerMillisecond)
                {
                    var packet = _lastPacket;
                    if (packet != null)
                    {
                        _lastPacketSendTime = currentTime;
                        Peer.SendUserData(packet);
                    }
                }
            }
            else
            {
                // 发送队列中的新包
                lock (OutgoingQueue)
                {
                    while (OutgoingQueue.Count > 0)
                    {
                        NetPacket packet = OutgoingQueue.Dequeue();
                        // 更新序列号
                        _localSequence = (_localSequence + 1) % NetConstants.MaxSequence;
                        packet.Sequence = (ushort)_localSequence;
                        packet.ChannelId = _id;
                        Peer.SendUserData(packet);

                        // 如果是可靠通道且队列为空，保存最后发送的包以便重传
                        if (_reliable && OutgoingQueue.Count == 0)
                        {
                            _lastPacketSendTime = DateTime.UtcNow.Ticks;
                            _lastPacket = packet;
                        }
                        // 非可靠模式下，发送后回收包
                        else
                        {
                            Peer.NetManager.PoolRecycle(packet);
                        }
                    }
                }
            }

            // 发送 ACK 包
            if (_reliable && _mustSendAck)
            {
                _mustSendAck = false;
                _ackPacket.Sequence = _remoteSequence;
                Peer.SendUserData(_ackPacket);
            }

            return _lastPacket != null;
        }

        public override bool ProcessPacket(NetPacket packet)
        {
            // 不处理分片包
            if (packet.IsFragmented)
                return false;
            // 如果接收到 ACK，确认最后发送的包已被接收
            if (packet.Property == PacketProperty.Ack)
            {
                if (_reliable && _lastPacket != null && packet.Sequence == _lastPacket.Sequence)
                    _lastPacket = null;
                return false;
            }
            int relative = NetUtils.RelativeSequenceNumber(packet.Sequence, _remoteSequence);
            bool packetProcessed = false;
            if (packet.Sequence < NetConstants.MaxSequence && relative > 0)
            {
                // 计算丢包情况
                if (Peer.NetManager.EnableStatistics)
                {
                    Peer.Statistics.AddPacketLoss(relative - 1);
                    Peer.NetManager.Statistics.AddPacketLoss(relative - 1);
                }

                // 更新远端序列号，处理包
                _remoteSequence = packet.Sequence;
                Peer.NetManager.CreateReceiveEvent(
                    packet,
                    _reliable ? DeliveryMethod.ReliableSequenced : DeliveryMethod.Sequenced,
                    (byte)(packet.ChannelId / NetConstants.ChannelTypeCount),
                    NetConstants.ChanneledHeaderSize,
                    Peer);
                packetProcessed = true;
            }

            // 如果是可靠通道，标记需要发送 ACK 并将通道加入发送队列
            if (_reliable)
            {
                _mustSendAck = true;
                AddToPeerChannelSendQueue();
            }

            return packetProcessed;
        }
    }
}
