using System;

namespace LiteNetLib
{
    internal sealed class ReliableChannel : BaseChannel
    {
        /// <summary>
        /// 已发送但未进行回传确认(|重复发送)的数据包
        /// </summary>
        private struct PendingPacket
        {
            private NetPacket _packet;
            private long _timeStamp;
            private bool _isSent;

            public override string ToString()
            {
                return _packet == null ? "Empty" : _packet.Sequence.ToString();
            }

            public void Init(NetPacket packet)
            {
                _packet = packet;
                _isSent = false;
            }

            //Returns true if there is a pending packet inside
            public bool TrySend(long currentTime, NetPeer peer)
            {
                if (_packet == null)
                    return false;

                if (_isSent) //check send time
                {
                    double resendDelay = peer.ResendDelay * TimeSpan.TicksPerMillisecond;
                    double packetHoldTime = currentTime - _timeStamp;
                    if (packetHoldTime < resendDelay)
                        return true;
                    NetDebug.Write($"[RC]Resend: {packetHoldTime} > {resendDelay}");
                }
                _timeStamp = currentTime;
                _isSent = true;
                peer.SendUserData(_packet);
                return true;
            }

            public bool Clear(NetPeer peer)
            {
                if (_packet != null)
                {
                    peer.RecycleAndDeliver(_packet);
                    _packet = null;
                    return true;
                }
                return false;
            }
        }

        /// <summary>
        /// for send acks
        /// 接收方向发送方发送的确认包
        /// </summary>
        private readonly NetPacket _outgoingAcks;
        /// <summary>
        /// for unacked packets and duplicates
        /// 待确认或者重复发送的包
        /// </summary>
        private readonly PendingPacket[] _pendingPackets;
        /// <summary>
        /// for order
        /// 按顺序接收到的数据包
        /// </summary>
        private readonly NetPacket[] _receivedPackets;
        /// <summary>
        /// for unordered
        /// 存储乱序接收到的包
        /// </summary>
        private readonly bool[] _earlyReceived;

        /// <summary>
        /// 当前本地发送的数据包序列号。每发送一个新的包，序列号递增
        /// </summary>
        private int _localSeqence;
        /// <summary>
        /// 当前已从远程接收到的数据包的最新序列号。它用于保证按序接收包
        /// </summary>
        private int _remoteSequence;
        /// <summary>
        /// 本地窗口起始位置，用于流控机制，表示本地发送数据的窗口范围起点
        /// </summary>
        private int _localWindowStart;
        /// <summary>
        /// 远程窗口起始位置，表示从远程接收到的包的窗口范围起
        /// </summary>
        private int _remoteWindowStart;

        /// <summary>
        /// 是否必须发送 ACK -> 有待发送的确认包
        /// </summary>
        private bool _mustSendAcks;

        /// <summary>
        /// 传输方式
        /// </summary>
        private readonly DeliveryMethod _deliveryMethod;
        /// <summary>
        /// 指示传输的数据包是否需要按顺序到达
        /// </summary>
        private readonly bool _ordered;
        /// <summary>
        /// 窗口大小，表示可以同时发送的未确认包的数量
        /// </summary>
        private readonly int _windowSize;
        private const int BitsInByte = 8;
        /// <summary>
        /// 连接的唯一标识符（ID），用于标识这个通道或连接
        /// </summary>
        private readonly byte _id;

        public ReliableChannel(NetPeer peer, bool ordered, byte id) : base(peer)
        {
            _id = id;
            _windowSize = NetConstants.DefaultWindowSize;
            _ordered = ordered;
            _pendingPackets = new PendingPacket[_windowSize];
            for (int i = 0; i < _pendingPackets.Length; i++)
                _pendingPackets[i] = new PendingPacket();

            if (_ordered)
            {
                _deliveryMethod = DeliveryMethod.ReliableOrdered;
                _receivedPackets = new NetPacket[_windowSize];
            }
            else
            {
                _deliveryMethod = DeliveryMethod.ReliableUnordered;
                _earlyReceived = new bool[_windowSize];
            }

            _localWindowStart = 0;
            _localSeqence = 0;
            _remoteSequence = 0;
            _remoteWindowStart = 0;
            _outgoingAcks = new NetPacket(PacketProperty.Ack, (_windowSize - 1) / BitsInByte + 2) {ChannelId = id};
        }

        //ProcessAck in packet
        private void ProcessAck(NetPacket packet)
        {
            if (packet.Size != _outgoingAcks.Size)
            {
                NetDebug.Write("[PA]Invalid acks packet size");
                return;
            }

            ushort ackWindowStart = packet.Sequence;
            int windowRel = NetUtils.RelativeSequenceNumber(_localWindowStart, ackWindowStart);
            // ACK 窗口起始位置比本地窗口序列号要小，或者序列号无效（超过最大序列号）
            if (ackWindowStart >= NetConstants.MaxSequence || windowRel < 0)
            {
                NetDebug.Write("[PA]Bad window start");
                return;
            }

            //check relevance
            // 相对序列号超出了当前窗口的大小
            if (windowRel >= _windowSize)
            {
                NetDebug.Write("[PA]Old acks");
                return;
            }

            // acksData 包含的是每个数据包是否被确认的比特位信息。每一个比特代表窗口中一个包是否被确认
            byte[] acksData = packet.RawData;
            lock (_pendingPackets)
            {
                for (int pendingSeq = _localWindowStart;
                    pendingSeq != _localSeqence;
                    pendingSeq = (pendingSeq + 1) % NetConstants.MaxSequence)
                {
                    int rel = NetUtils.RelativeSequenceNumber(pendingSeq, ackWindowStart);
                    if (rel >= _windowSize)
                    {
                        NetDebug.Write("[PA]REL: " + rel);
                        break;
                    }

                    int pendingIdx = pendingSeq % _windowSize;
                    // 计算当前数据包在 ACK 数据中的字节位置和比特位置
                    int currentByte = NetConstants.ChanneledHeaderSize + pendingIdx / BitsInByte;
                    int currentBit = pendingIdx % BitsInByte;
                    // 检查当前包是否被确认
                    // 如果该包未被确认（对应的比特位为 0），则跳过该包
                    if ((acksData[currentByte] & (1 << currentBit)) == 0)
                    {
                        if (Peer.NetManager.EnableStatistics)
                        {
                            Peer.Statistics.IncrementPacketLoss();
                            Peer.NetManager.Statistics.IncrementPacketLoss();
                        }

                        //Skip false ack
                        NetDebug.Write($"[PA]False ack: {pendingSeq}");
                        continue;
                    }

                    if (pendingSeq == _localWindowStart)
                    {
                        //Move window
                        _localWindowStart = (_localWindowStart + 1) % NetConstants.MaxSequence;
                    }

                    //clear packet
                    if (_pendingPackets[pendingIdx].Clear(Peer))
                        NetDebug.Write($"[PA]Removing reliableInOrder ack: {pendingSeq} - true");
                }
            }
        }

        protected override bool SendNextPackets()
        {
            // 有待发送的确认包
            if (_mustSendAcks)
            {
                _mustSendAcks = false;
                NetDebug.Write("[RR]SendAcks");
                lock(_outgoingAcks)
                    Peer.SendUserData(_outgoingAcks);
            }

            long currentTime = DateTime.UtcNow.Ticks;
            bool hasPendingPackets = false;
            // 处理未发送的队列
            lock (_pendingPackets)
            {
                //get packets from queue
                lock (OutgoingQueue)
                {
                    while (OutgoingQueue.Count > 0)
                    {
                        // 相对序列号超出了窗口大小 _windowSize，则停止发送
                        int relate = NetUtils.RelativeSequenceNumber(_localSeqence, _localWindowStart);
                        if (relate >= _windowSize)
                            break;

                        var netPacket = OutgoingQueue.Dequeue();
                        netPacket.Sequence = (ushort) _localSeqence;
                        netPacket.ChannelId = _id;
                        _pendingPackets[_localSeqence % _windowSize].Init(netPacket);
                        _localSeqence = (_localSeqence + 1) % NetConstants.MaxSequence;
                    }
                }

                //send
                for (int pendingSeq = _localWindowStart; pendingSeq != _localSeqence; pendingSeq = (pendingSeq + 1) % NetConstants.MaxSequence)
                {
                    // Please note: TrySend is invoked on a mutable struct, it's important to not extract it into a variable here
                    if (_pendingPackets[pendingSeq % _windowSize].TrySend(currentTime, Peer))
                        hasPendingPackets = true;
                }
            }
            // 仍有待发送的包||待发送的 ACK ||未处理的 OutgoingQueue
            return hasPendingPackets || _mustSendAcks || OutgoingQueue.Count > 0;
        }

        //Process incoming packet
        public override bool ProcessPacket(NetPacket packet)
        {
            // 处理确认包
            if (packet.Property == PacketProperty.Ack)
            {
                ProcessAck(packet);
                return false;
            }

            // 序列号超过了最大值 MaxSequence
            int seq = packet.Sequence;
            if (seq >= NetConstants.MaxSequence)
            {
                NetDebug.Write("[RR]Bad sequence");
                return false;
            }

            // 数据包序列号相对于当前远端窗口起始位置
            int relate = NetUtils.RelativeSequenceNumber(seq, _remoteWindowStart);
            // 数据包序列号相对于远端最新序列号的相对值
            int relateSeq = NetUtils.RelativeSequenceNumber(seq, _remoteSequence);

            // 相对远端最新序列号的值大于窗口大小，则说明该数据包已经超出了允许的范围
            if (relateSeq > _windowSize)
            {
                NetDebug.Write("[RR]Bad sequence");
                return false;
            }

            // 包太旧，不需要被处理
            //Drop bad packets
            if (relate < 0)
            {
                //Too old packet doesn't ack
                NetDebug.Write("[RR]ReliableInOrder too old");
                return false;
            }
            // 包太新，超过了当前窗口的处理范围
            if (relate >= _windowSize * 2)
            {
                //Some very new packet
                NetDebug.Write("[RR]ReliableInOrder too new");
                return false;
            }

            //If very new - move window
            int ackIdx;
            int ackByte;
            int ackBit;
            lock (_outgoingAcks)
            {
                // 包的序列号超出了当前窗口的范围，窗口需要向前移动
                if (relate >= _windowSize)
                {
                    //New window position
                    int newWindowStart = (_remoteWindowStart + relate - _windowSize + 1) % NetConstants.MaxSequence;
                    _outgoingAcks.Sequence = (ushort) newWindowStart;

                    //Clean old data
                    while (_remoteWindowStart != newWindowStart)
                    {
                        ackIdx = _remoteWindowStart % _windowSize;
                        ackByte = NetConstants.ChanneledHeaderSize + ackIdx / BitsInByte;
                        ackBit = ackIdx % BitsInByte;
                        _outgoingAcks.RawData[ackByte] &= (byte) ~(1 << ackBit);
                        _remoteWindowStart = (_remoteWindowStart + 1) % NetConstants.MaxSequence;
                    }
                }

                //Final stage - process valid packet
                //trigger acks send
                _mustSendAcks = true;

                // ACK 数据更新
                ackIdx = seq % _windowSize;
                ackByte = NetConstants.ChanneledHeaderSize + ackIdx / BitsInByte;
                ackBit = ackIdx % BitsInByte;
                // 如果该包是重复包，标记并丢弃该包，继续处理其他包
                if ((_outgoingAcks.RawData[ackByte] & (1 << ackBit)) != 0)
                {
                    NetDebug.Write("[RR]ReliableInOrder duplicate");
                    //because _mustSendAcks == true
                    AddToPeerChannelSendQueue();
                    return false;
                }

                //save ack
                _outgoingAcks.RawData[ackByte] |= (byte) (1 << ackBit);
            }

            AddToPeerChannelSendQueue();

            // 包是按顺序到达
            //detailed check
            if (seq == _remoteSequence)
            {
                NetDebug.Write("[RR]ReliableInOrder packet succes");
                Peer.AddReliablePacket(_deliveryMethod, packet);
                _remoteSequence = (_remoteSequence + 1) % NetConstants.MaxSequence;

                // 有序包
                if (_ordered)
                {
                    NetPacket p;
                    while ((p = _receivedPackets[_remoteSequence % _windowSize]) != null)
                    {
                        //process holden packet
                        _receivedPackets[_remoteSequence % _windowSize] = null;
                        Peer.AddReliablePacket(_deliveryMethod, p);
                        _remoteSequence = (_remoteSequence + 1) % NetConstants.MaxSequence;
                    }
                }
                // 无序包
                else
                {
                    while (_earlyReceived[_remoteSequence % _windowSize])
                    {
                        //process early packet
                        _earlyReceived[_remoteSequence % _windowSize] = false;
                        _remoteSequence = (_remoteSequence + 1) % NetConstants.MaxSequence;
                    }
                }
                return true;
            }

            //holden packet
            if (_ordered)
            {
                _receivedPackets[ackIdx] = packet;
            }
            else
            {
                _earlyReceived[ackIdx] = true;
                Peer.AddReliablePacket(_deliveryMethod, packet);
            }
            return true;
        }
    }
}
