#if DEBUG
#define STATS_ENABLED
#endif
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading;
using LiteNetLib.Utils;

namespace LiteNetLib
{
    /// <summary>
    /// Peer connection state
    /// </summary>
    [Flags]
    public enum ConnectionState : byte
    {
        /// <summary>
        /// 发起连接|未连接|P2P-> 尝试连接，尚未完成连接
        /// </summary>
        Outgoing         = 1 << 1,
        /// <summary>
        /// 已连接
        /// </summary>
        Connected         = 1 << 2,
        /// <summary>
        /// 连接请求关闭 -> 尝试关闭，尚未断连
        /// </summary>
        ShutdownRequested = 1 << 3,
        /// <summary>
        /// 断开连接
        /// </summary>
        Disconnected      = 1 << 4,
        /// <summary>
        /// 连接终端发生改变
        /// </summary>
        EndPointChange    = 1 << 5,
        /// <summary>
        /// 发起连接|已连接|连接请求关闭|终端改变
        /// </summary>
        Any = Outgoing | Connected | ShutdownRequested | EndPointChange
    }

    /// <summary>
    /// 连接请求的结果
    /// </summary>
    internal enum ConnectRequestResult
    {
        None,
        /// <summary>
        /// Peer 连接丢失 -> 请求连接失败
        /// </summary>
        P2PLose, //when peer connecting
        /// <summary>
        /// 当前已连接 -> 短时间断线重连(程序运行状态下的断线重连)
        /// </summary>
        Reconnection,  //when peer was connected
        /// <summary>
        /// 当前未连接 -> 新连接|长时间的断线重连
        /// </summary>
        NewConnection  //when peer was disconnected
    }

    /// <summary>
    /// 断开连接的结果
    /// </summary>
    internal enum DisconnectResult
    {
        None,
        /// <summary>
        /// 断开连接的请求被拒绝
        /// </summary>
        Reject,
        /// <summary>
        /// 连接已成功断开
        /// </summary>
        Disconnect
    }

    /// <summary>
    /// 关闭连接请求的结果
    /// </summary>
    internal enum ShutdownResult
    {
        None,
        /// <summary>
        /// 成功
        /// </summary>
        Success,
        /// <summary>
        /// 依旧连接
        /// </summary>
        WasConnected
    }

    /// <summary>
    /// Network peer. Main purpose is sending messages to specific peer.
    /// 网络节点 -> 主要用于网络节点的连接和消息的发送，不负责消息的接收处理，本地与远程IP终端连接
    /// </summary>
    public class NetPeer : IPEndPoint
    {
        //Ping and RTT -> RTT使用本地计时，可计算两端系统时差异
        private int _rtt;
        private int _avgRtt;
        private int _rttCount;
        private double _resendDelay = 27.0;
        private float _pingSendTimer;
        private float _rttResetTimer;
        private readonly Stopwatch _pingTimer = new Stopwatch();
        private volatile float _timeSinceLastPacket;
        private long _remoteDelta;

        //Common
        private readonly object _shutdownLock = new object();
        // 链表存储Peer
        internal volatile NetPeer NextPeer;
        internal NetPeer PrevPeer;

        /// <summary>
        /// Peer 进行的连接次数
        /// </summary>
        internal byte ConnectionNum
        {
            get => _connectNum;
            private set
            {
                _connectNum = value;
                _mergeData.ConnectionNumber = value;
                _pingPacket.ConnectionNumber = value;
                _pongPacket.ConnectionNumber = value;
            }
        }

        //Channels
        private NetPacket[] _unreliableSecondQueue;
        private NetPacket[] _unreliableChannel;
        private int _unreliablePendingCount;
        private readonly object _unreliableChannelLock = new object();

        /// <summary>
        /// 线程安全的队列
        /// </summary>
        private readonly ConcurrentQueue<BaseChannel> _channelSendQueue;
        /// <summary>
        /// 4种类型 [0,1,2,3] x N
        /// [ReliableUnordered, Sequenced, ReliableOrdered, ReliableSequenced]
        /// </summary>
        private readonly BaseChannel[] _channels;

        //MTU
        private int _mtu;
        private int _mtuIdx;
        private bool _finishMtu;
        private float _mtuCheckTimer;
        private int _mtuCheckAttempts;
        private const int MtuCheckDelay = 1000;
        private const int MaxMtuCheckAttempts = 4;
        private readonly object _mtuMutex = new object();

        /// <summary>
        /// Fragment -> 分段数据，多个数据包
        /// </summary>
        private class IncomingFragments
        {
            public NetPacket[] Fragments;
            public int ReceivedCount;
            public int TotalSize;
            public byte ChannelId;
        }
        private int _fragmentId;
        /// <summary>
        /// 开始接收但没有完全处理完的分片数据包
        /// </summary>
        private readonly Dictionary<ushort, IncomingFragments> _holdedFragments;
        /// <summary>
        /// 已经交付的分片数据包的数量(返回确认)
        /// </summary>
        private readonly Dictionary<ushort, ushort> _deliveredFragments;

        //Merging
        /// <summary>
        /// 也可能是单个数据包
        /// </summary>
        private readonly NetPacket _mergeData;
        // 合并的数据长度(不包括MergeHeader)
        private int _mergePos;
        private int _mergeCount;

        //Connection
        private int _connectAttempts;
        private float _connectTimer;
        /// <summary>
        /// 请求连接时的实际系统时 -> 也用于匹配远端和把本地端的ID
        /// </summary>
        private long _connectTime;
        /// <summary>
        /// Peer 进行的连接次数
        /// </summary>
        private byte _connectNum;
        private ConnectionState _connectionState;
        private NetPacket _shutdownPacket;
        private const int ShutdownDelay = 300;
        private float _shutdownTimer;
        private readonly NetPacket _pingPacket;
        private readonly NetPacket _pongPacket;
        private readonly NetPacket _connectRequestPacket;
        private readonly NetPacket _connectAcceptPacket;

        /// <summary>
        /// Peer parent NetManager
        /// </summary>
        public readonly NetManager NetManager;

        /// <summary>
        /// Current connection state
        /// </summary>
        public ConnectionState ConnectionState => _connectionState;

        /// <summary>
        /// Connection time for internal purposes
        /// </summary>
        internal long ConnectTime => _connectTime;

        /// <summary>
        /// Peer id can be used as key in your dictionary of peers -> 就是另一端的RemoteId
        /// </summary>
        public readonly int Id;

        /// <summary>
        /// Id assigned from server -> 就是另一端的Id
        /// </summary>
        public int RemoteId { get; private set; }

        /// <summary>
        /// Current one-way ping (RTT/2) in milliseconds
        /// </summary>
        public int Ping => _avgRtt/2;

        /// <summary>
        /// Round trip time in milliseconds
        /// </summary>
        public int RoundTripTime => _avgRtt;

        /// <summary>
        /// Current MTU - Maximum Transfer Unit ( maximum udp packet size without fragmentation )
        /// </summary>
        public int Mtu => _mtu;

        /// <summary>
        /// Delta with remote time in ticks (not accurate)
        /// positive - remote time > our time
        /// </summary>
        public long RemoteTimeDelta => _remoteDelta;

        /// <summary>
        /// Remote UTC time (not accurate)
        /// </summary>
        public DateTime RemoteUtcTime => new DateTime(DateTime.UtcNow.Ticks + _remoteDelta);

        /// <summary>
        /// Time since last packet received (including internal library packets) in milliseconds
        /// </summary>
        public float TimeSinceLastPacket => _timeSinceLastPacket;

        internal double ResendDelay => _resendDelay;

        /// <summary>
        /// Application defined object containing data about the connection
        /// </summary>
        public object Tag;

        /// <summary>
        /// Statistics of peer connection
        /// </summary>
        public readonly NetStatistics Statistics;

        /// <summary>
        /// 自己的IP终端
        /// </summary>
        private SocketAddress _cachedSocketAddr;
        /// <summary>
        /// 自己对象的HashCode
        /// </summary>
        private int _cachedHashCode;

        /// <summary>
        /// IP终端转为Byte的数据
        /// </summary>
        internal byte[] NativeAddress;

        /// <summary>
        /// IPEndPoint serialize
        /// </summary>
        /// <returns>SocketAddress</returns>
        public override SocketAddress Serialize()
        {
            return _cachedSocketAddr;
        }

        public override int GetHashCode()
        {
            //uses SocketAddress hash in NET8 and IPEndPoint hash for NativeSockets and previous NET versions
            return _cachedHashCode;
        }

        /// <summary>
        /// incoming connection constructor -> 连接确认后与服务器连接的NetPeer
        /// </summary>
        /// <param name="netManager">本地所属的NetManager</param>
        /// <param name="remoteEndPoint">连接的远程IP端口</param>
        /// <param name="id">自己的本地ID</param>
        internal NetPeer(NetManager netManager, IPEndPoint remoteEndPoint, int id) : base(remoteEndPoint.Address, remoteEndPoint.Port)
        {
            Id = id;
            Statistics = new NetStatistics();
            NetManager = netManager;

            _cachedSocketAddr = base.Serialize();
            // 直接使用Socket进行信息的传输与接收，提升速度减少GC
            if (NetManager.UseNativeSockets)
            {
                NativeAddress = new byte[_cachedSocketAddr.Size];
                for (int i = 0; i < _cachedSocketAddr.Size; i++)
                    NativeAddress[i] = _cachedSocketAddr[i];
            }
#if NET8_0_OR_GREATER
            _cachedHashCode = NetManager.UseNativeSockets ? base.GetHashCode() : _cachedSocketAddr.GetHashCode();
#else
            _cachedHashCode = base.GetHashCode();
#endif

            ResetMtu();

            _connectionState = ConnectionState.Connected;
            // 1500 - MaxUdpHeaderSize字节
            _mergeData = new NetPacket(PacketProperty.Merged, NetConstants.MaxPacketSize);
            _pongPacket = new NetPacket(PacketProperty.Pong, 0);
            _pingPacket = new NetPacket(PacketProperty.Ping, 0) {Sequence = 1};

            _unreliableSecondQueue = new NetPacket[8];
            _unreliableChannel = new NetPacket[8];
            _holdedFragments = new Dictionary<ushort, IncomingFragments>();
            _deliveredFragments = new Dictionary<ushort, ushort>();

            // 4种类型 * 每种的通道数
            _channels = new BaseChannel[netManager.ChannelsCount * NetConstants.ChannelTypeCount];
            _channelSendQueue = new ConcurrentQueue<BaseChannel>();
        }

        /// <summary>
        /// 初始化远程IP终端变化状态
        /// </summary>
        internal void InitiateEndPointChange()
        {
            ResetMtu();
            _connectionState = ConnectionState.EndPointChange;
        }

        /// <summary>
        /// 完成远程IP终端改变
        /// </summary>
        /// <param name="newEndPoint"></param>
        internal void FinishEndPointChange(IPEndPoint newEndPoint)
        {
            if (_connectionState != ConnectionState.EndPointChange)
                return;
            _connectionState = ConnectionState.Connected;

            Address = newEndPoint.Address;
            Port = newEndPoint.Port;

            if (NetManager.UseNativeSockets)
            {
                NativeAddress = new byte[_cachedSocketAddr.Size];
                for (int i = 0; i < _cachedSocketAddr.Size; i++)
                    NativeAddress[i] = _cachedSocketAddr[i];
            }
            _cachedSocketAddr = base.Serialize();
#if NET8_0_OR_GREATER
            _cachedHashCode = NetManager.UseNativeSockets ? base.GetHashCode() : _cachedSocketAddr.GetHashCode();
#else
            _cachedHashCode = base.GetHashCode();
#endif
        }

        internal void ResetMtu()
        {
            _finishMtu = false;
            // 使用NetManager的MTU覆盖所有在此NetManager注册的Peer的MTU
            if (NetManager.MtuOverride > 0)
                OverrideMtu(NetManager.MtuOverride);
            // 576字节 - MaxUdpHeaderSize
            else if (NetManager.UseSafeMtu)
                SetMtu(0);
            // 1024字节
            else
                SetMtu(1);
        }

        private void SetMtu(int mtuIdx)
        {
            _mtuIdx = mtuIdx;
            _mtu = NetConstants.PossibleMtu[mtuIdx] - NetManager.ExtraPacketSizeForLayer;
        }

        private void OverrideMtu(int mtuValue)
        {
            _mtu = mtuValue;
            _finishMtu = true;
        }

        /// <summary>
        /// Returns packets count in queue for reliable channel
        /// 只能获取通道中的可靠排序和可靠不排序通道中的数据包数量
        /// </summary>
        /// <param name="channelNumber">number of channel 0-63</param>
        /// <param name="ordered">type of channel ReliableOrdered or ReliableUnordered</param>
        /// <returns>packets count in channel queue</returns>
        public int GetPacketsCountInReliableQueue(byte channelNumber, bool ordered)
        {
            int idx = channelNumber * NetConstants.ChannelTypeCount +
                       (byte) (ordered ? DeliveryMethod.ReliableOrdered : DeliveryMethod.ReliableUnordered);
            var channel = _channels[idx];
            return channel != null ? ((ReliableChannel)channel).PacketsInQueue : 0;
        }

        /// <summary>
        /// Create temporary packet (maximum size MTU - headerSize) to send later without additional copieskl;'
        /// 从数据包对象池中取空闲数据包传入通道并返回空闲数据包(已经从对象池取出)
        /// </summary>
        /// <param name="deliveryMethod">Delivery method (reliable, unreliable, etc.)</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <returns>PooledPacket that you can use to write data starting from UserDataOffset</returns>
        public PooledPacket CreatePacketFromPool(DeliveryMethod deliveryMethod, byte channelNumber)
        {
            //multithreaded variable
            int mtu = _mtu;
            var packet = NetManager.PoolGetPacket(mtu);
            if (deliveryMethod == DeliveryMethod.Unreliable)
            {
                packet.Property = PacketProperty.Unreliable;
                return new PooledPacket(packet, mtu, 0);
            }
            else
            {
                packet.Property = PacketProperty.Channeled;
                return new PooledPacket(packet, mtu, (byte)(channelNumber * NetConstants.ChannelTypeCount + (byte)deliveryMethod));
            }
        }

        /// <summary>
        /// Sends pooled packet without data copy
        /// </summary>
        /// <param name="packet">packet to send</param>
        /// <param name="userDataSize">size of user data you want to send</param>
        public void SendPooledPacket(PooledPacket packet, int userDataSize)
        {
            if (_connectionState != ConnectionState.Connected)
                return;
            packet._packet.Size = packet.UserDataOffset + userDataSize;
            if (packet._packet.Property == PacketProperty.Channeled)
            {
                CreateChannel(packet._channelNumber).AddToQueue(packet._packet);
            }
            else
                EnqueueUnreliable(packet._packet);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnqueueUnreliable(NetPacket packet)
        {
            lock (_unreliableChannelLock)
            {
                if (_unreliablePendingCount == _unreliableChannel.Length)
                    Array.Resize(ref _unreliableChannel, _unreliablePendingCount*2);
                _unreliableChannel[_unreliablePendingCount++] = packet;
            }
        }

        private BaseChannel CreateChannel(byte idx)
        {
            BaseChannel newChannel = _channels[idx];
            if (newChannel != null)
                return newChannel;
            switch ((DeliveryMethod)(idx % NetConstants.ChannelTypeCount))
            {
                case DeliveryMethod.ReliableUnordered:
                    newChannel = new ReliableChannel(this, false, idx);
                    break;
                case DeliveryMethod.Sequenced:
                    newChannel = new SequencedChannel(this, false, idx);
                    break;
                case DeliveryMethod.ReliableOrdered:
                    newChannel = new ReliableChannel(this, true, idx);
                    break;
                case DeliveryMethod.ReliableSequenced:
                    newChannel = new SequencedChannel(this, true, idx);
                    break;
            }
            // 确保多线程下不会重复创建idx的通道
            BaseChannel prevChannel = Interlocked.CompareExchange(ref _channels[idx], newChannel, null);
            if (prevChannel != null)
                return prevChannel;

            return newChannel;
        }

        /// <summary>
        /// "Connect to" constructor -> 连接远程IP端口，等待连接确认
        /// </summary>
        /// <param name="netManager">创建这个Peer&管理的本地NetManager(Client)</param>
        /// <param name="remoteEndPoint"></param>
        /// <param name="id">可能会复用之前断连的ID，使用connectNum可以与之前的区分</param>
        /// <param name="connectNum">Peer进行的连接次数，包含断线重连(程序运行中的重连)</param>
        /// <param name="connectData">发送连接请求包的额外数据</param>
        internal NetPeer(NetManager netManager, IPEndPoint remoteEndPoint, int id, byte connectNum, NetDataWriter connectData)
            : this(netManager, remoteEndPoint, id)
        {
            _connectTime = DateTime.UtcNow.Ticks;
            _connectionState = ConnectionState.Outgoing;
            ConnectionNum = connectNum;

            //Make initial packet
            _connectRequestPacket = NetConnectRequestPacket.Make(connectData, remoteEndPoint.Serialize(), _connectTime, id);
            _connectRequestPacket.ConnectionNumber = connectNum;

            //Send request
            NetManager.SendRaw(_connectRequestPacket, this);

            NetDebug.Write(NetLogLevel.Trace, $"[CC] ConnectId: {_connectTime}, ConnectNum: {connectNum}");
        }

        /// <summary>
        /// "Accept" incoming constructor -> 服务器接受连接的Peer，用于和客户端保持连接
        /// </summary>
        /// <param name="netManager">接受连接请求的NetManager</param>
        /// <param name="request">接收到的连接请求</param>
        /// <param name="id">本地的PeerID，不是RemoteID(在request中)</param>
        internal NetPeer(NetManager netManager, ConnectionRequest request, int id)
            : this(netManager, request.RemoteEndPoint, id)
        {
            // 发送连接请求时的客户端记录的系统时
            _connectTime = request.InternalPacket.ConnectionTime;
            ConnectionNum = request.InternalPacket.ConnectionNumber;
            RemoteId = request.InternalPacket.PeerId;

            //Make initial packet
            _connectAcceptPacket = NetConnectAcceptPacket.Make(_connectTime, ConnectionNum, id);

            //Make Connected -> 服务器更改为连接
            _connectionState = ConnectionState.Connected;

            //Send
            NetManager.SendRaw(_connectAcceptPacket, this);

            NetDebug.Write(NetLogLevel.Trace, $"[CC] ConnectId: {_connectTime}");
        }

        /// <summary>
        /// Reject -> 拒绝连接请求
        /// </summary>
        /// <param name="requestData">Request</param>
        /// <param name="data">额外数据(rejectData)</param>
        /// <param name="start"></param>
        /// <param name="length"></param>
        internal void Reject(NetConnectRequestPacket requestData, byte[] data, int start, int length)
        {
            _connectTime = requestData.ConnectionTime;
            _connectNum = requestData.ConnectionNumber;
            Shutdown(data, start, length, false);
        }

        /// <summary>
        /// 处理连接接受数据包(请求连接端)
        /// </summary>
        /// <param name="packet"></param>
        /// <returns></returns>
        internal bool ProcessConnectAccept(NetConnectAcceptPacket packet)
        {
            if (_connectionState != ConnectionState.Outgoing)
                return false;

            //check connection id
            if (packet.ConnectionTime != _connectTime)
            {
                NetDebug.Write(NetLogLevel.Trace, $"[NC] Invalid connectId: {packet.ConnectionTime} != our({_connectTime})");
                return false;
            }
            //check connect num
            ConnectionNum = packet.ConnectionNumber;
            RemoteId = packet.PeerId;

            NetDebug.Write(NetLogLevel.Trace, "[NC] Received connection accept");
            Interlocked.Exchange(ref _timeSinceLastPacket, 0);
            // 客户端更改为连接
            _connectionState = ConnectionState.Connected;
            return true;
        }

        /// <summary>
        /// Gets maximum size of packet that will be not fragmented.
        /// </summary>
        /// <param name="options">Type of packet that you want send</param>
        /// <returns>size in bytes</returns>
        public int GetMaxSinglePacketSize(DeliveryMethod options)
        {
            return _mtu - NetPacket.GetHeaderSize(options == DeliveryMethod.Unreliable ? PacketProperty.Unreliable : PacketProperty.Channeled);
        }

        /// <summary>
        /// Send data to peer with delivery event called
        /// 确认收到事件，userdata为发送端确认收到事件的参数
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Delivery method (reliable, unreliable, etc.)</param>
        /// <param name="userData">User data that will be received in DeliveryEvent</param>
        /// <exception cref="ArgumentException">
        ///     If you trying to send unreliable packet type<para/>
        /// </exception>
        public void SendWithDeliveryEvent(byte[] data, byte channelNumber, DeliveryMethod deliveryMethod, object userData)
        {
            if (deliveryMethod != DeliveryMethod.ReliableOrdered && deliveryMethod != DeliveryMethod.ReliableUnordered)
                throw new ArgumentException("Delivery event will work only for ReliableOrdered/Unordered packets");
            SendInternal(data, 0, data.Length, channelNumber, deliveryMethod, userData);
        }

        /// <summary>
        /// Send data to peer with delivery event called
        /// 使用通道发送data
        /// 确认收到事件，userdata为发送端确认收到事件的参数
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="start">Start of data</param>
        /// <param name="length">Length of data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Delivery method (reliable, unreliable, etc.)必须是Reliable通道</param>
        /// <param name="userData">User data that will be received in DeliveryEvent</param>
        /// <exception cref="ArgumentException">
        ///     If you trying to send unreliable packet type<para/>
        /// </exception>
        public void SendWithDeliveryEvent(byte[] data, int start, int length, byte channelNumber, DeliveryMethod deliveryMethod, object userData)
        {
            if (deliveryMethod != DeliveryMethod.ReliableOrdered && deliveryMethod != DeliveryMethod.ReliableUnordered)
                throw new ArgumentException("Delivery event will work only for ReliableOrdered/Unordered packets");
            SendInternal(data, start, length, channelNumber, deliveryMethod, userData);
        }

        /// <summary>
        /// Send data to peer with delivery event called
        /// 使用通道发送data
        /// 确认收到事件，userdata为发送端确认收到事件的参数
        /// </summary>
        /// <param name="dataWriter">Data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Delivery method (reliable, unreliable, etc.)必须是Reliable通道</param>
        /// <param name="userData">User data that will be received in DeliveryEvent</param>
        /// <exception cref="ArgumentException">
        ///     If you trying to send unreliable packet type<para/>
        /// </exception>
        public void SendWithDeliveryEvent(NetDataWriter dataWriter, byte channelNumber, DeliveryMethod deliveryMethod, object userData)
        {
            if (deliveryMethod != DeliveryMethod.ReliableOrdered && deliveryMethod != DeliveryMethod.ReliableUnordered)
                throw new ArgumentException("Delivery event will work only for ReliableOrdered/Unordered packets");
            SendInternal(dataWriter.Data, 0, dataWriter.Length, channelNumber, deliveryMethod, userData);
        }

        /// <summary>
        /// Send data to peer (channel - 0)
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="deliveryMethod">Send options (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(byte[] data, DeliveryMethod deliveryMethod)
        {
            SendInternal(data, 0, data.Length, 0, deliveryMethod, null);
        }

        /// <summary>
        /// Send data to peer (channel - 0)
        /// </summary>
        /// <param name="dataWriter">DataWriter with data</param>
        /// <param name="deliveryMethod">Send options (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(NetDataWriter dataWriter, DeliveryMethod deliveryMethod)
        {
            SendInternal(dataWriter.Data, 0, dataWriter.Length, 0, deliveryMethod, null);
        }

        /// <summary>
        /// Send data to peer (channel - 0)
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="start">Start of data</param>
        /// <param name="length">Length of data</param>
        /// <param name="options">Send options (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(byte[] data, int start, int length, DeliveryMethod options)
        {
            SendInternal(data, start, length, 0, options, null);
        }

        /// <summary>
        /// Send data to peer
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Send options (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(byte[] data, byte channelNumber, DeliveryMethod deliveryMethod)
        {
            SendInternal(data, 0, data.Length, channelNumber, deliveryMethod, null);
        }

        /// <summary>
        /// Send data to peer
        /// </summary>
        /// <param name="dataWriter">DataWriter with data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Send options (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(NetDataWriter dataWriter, byte channelNumber, DeliveryMethod deliveryMethod)
        {
            SendInternal(dataWriter.Data, 0, dataWriter.Length, channelNumber, deliveryMethod, null);
        }

        /// <summary>
        /// Send data to peer
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="start">Start of data</param>
        /// <param name="length">Length of data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Delivery method (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(byte[] data, int start, int length, byte channelNumber, DeliveryMethod deliveryMethod)
        {
            SendInternal(data, start, length, channelNumber, deliveryMethod, null);
        }

        /// <summary>
        /// 发送数据包的实际调用函数
        /// </summary>
        /// <param name="data"></param>
        /// <param name="start"></param>
        /// <param name="length"></param>
        /// <param name="channelNumber">计算完成后的值</param>
        /// <param name="deliveryMethod"></param>
        /// <param name="userData"></param>
        private void SendInternal(
            byte[] data,
            int start,
            int length,
            byte channelNumber,
            DeliveryMethod deliveryMethod,
            object userData)
        {
            if (_connectionState != ConnectionState.Connected || channelNumber >= _channels.Length)
                return;

            //Select channel
            PacketProperty property;
            BaseChannel channel = null;

            if (deliveryMethod == DeliveryMethod.Unreliable)
            {
                property = PacketProperty.Unreliable;
            }
            else
            {
                property = PacketProperty.Channeled;
                channel = CreateChannel((byte)(channelNumber * NetConstants.ChannelTypeCount + (byte)deliveryMethod));
            }

            //Prepare
            NetDebug.Write("[RS]Packet: " + property);

            //Check fragmentation
            int headerSize = NetPacket.GetHeaderSize(property);
            //Save mtu for multithread
            int mtu = _mtu;
            // 分片，但是必须是可靠传输
            if (length + headerSize > mtu)
            {
                //if cannot be fragmented
                if (deliveryMethod != DeliveryMethod.ReliableOrdered && deliveryMethod != DeliveryMethod.ReliableUnordered)
                    throw new TooBigPacketException("Unreliable or ReliableSequenced packet size exceeded maximum of " + (mtu - headerSize) + " bytes, Check allowed size by GetMaxSinglePacketSize()");

                int packetFullSize = mtu - headerSize;
                int packetDataSize = packetFullSize - NetConstants.FragmentHeaderSize;
                int totalPackets = length / packetDataSize + (length % packetDataSize == 0 ? 0 : 1);

                NetDebug.Write($@"FragmentSend:
 MTU: {mtu}
 headerSize: {headerSize}
 packetFullSize: {packetFullSize}
 packetDataSize: {packetDataSize}
 totalPackets: {totalPackets}");

                if (totalPackets > ushort.MaxValue)
                    throw new TooBigPacketException("Data was split in " + totalPackets + " fragments, which exceeds " + ushort.MaxValue);

                ushort currentFragmentId = (ushort)Interlocked.Increment(ref _fragmentId);

                for(ushort partIdx = 0; partIdx < totalPackets; partIdx++)
                {
                    int sendLength = length > packetDataSize ? packetDataSize : length;

                    NetPacket p = NetManager.PoolGetPacket(headerSize + sendLength + NetConstants.FragmentHeaderSize);
                    p.Property = property;
                    p.UserData = userData;
                    p.FragmentId = currentFragmentId;
                    p.FragmentPart = partIdx;
                    p.FragmentsTotal = (ushort)totalPackets;
                    p.MarkFragmented();

                    Buffer.BlockCopy(data, start + partIdx * packetDataSize, p.RawData, NetConstants.FragmentedHeaderTotalSize, sendLength);
                    channel.AddToQueue(p);

                    length -= sendLength;
                }
                return;
            }

            //Else just send
            NetPacket packet = NetManager.PoolGetPacket(headerSize + length);
            packet.Property = property;
            Buffer.BlockCopy(data, start, packet.RawData, headerSize, length);
            packet.UserData = userData;

            if (channel == null) //unreliable
            {
                EnqueueUnreliable(packet);
            }
            else
            {
                channel.AddToQueue(packet);
            }
        }

#if LITENETLIB_SPANS || NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NETCOREAPP2_1 || NETCOREAPP3_1 || NET5_0 || NETSTANDARD2_1
        /// <summary>
        /// Send data to peer with delivery event called
        /// 确认收到事件，userdata为发送端确认收到事件的参数
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Delivery method (reliable, unreliable, etc.)</param>
        /// <param name="userData">User data that will be received in DeliveryEvent</param>
        /// <exception cref="ArgumentException">
        ///     If you trying to send unreliable packet type<para/>
        /// </exception>
        public void SendWithDeliveryEvent(ReadOnlySpan<byte> data, byte channelNumber, DeliveryMethod deliveryMethod, object userData)
        {
            if (deliveryMethod != DeliveryMethod.ReliableOrdered && deliveryMethod != DeliveryMethod.ReliableUnordered)
                throw new ArgumentException("Delivery event will work only for ReliableOrdered/Unordered packets");
            SendInternal(data, channelNumber, deliveryMethod, userData);
        }

        /// <summary>
        /// Send data to peer (channel - 0)
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="deliveryMethod">Send options (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(ReadOnlySpan<byte> data, DeliveryMethod deliveryMethod)
        {
            SendInternal(data, 0, deliveryMethod, null);
        }

        /// <summary>
        /// Send data to peer
        /// </summary>
        /// <param name="data">Data</param>
        /// <param name="channelNumber">Number of channel (from 0 to channelsCount - 1)</param>
        /// <param name="deliveryMethod">Send options (reliable, unreliable, etc.)</param>
        /// <exception cref="TooBigPacketException">
        ///     If size exceeds maximum limit:<para/>
        ///     MTU - headerSize bytes for Unreliable<para/>
        ///     Fragment count exceeded ushort.MaxValue<para/>
        /// </exception>
        public void Send(ReadOnlySpan<byte> data, byte channelNumber, DeliveryMethod deliveryMethod)
        {
            SendInternal(data, channelNumber, deliveryMethod, null);
        }

        /// <summary>
        /// 发送数据包的实际调用函数
        /// </summary>
        /// <param name="data"></param>
        /// <param name="channelNumber"></param>
        /// <param name="deliveryMethod"></param>
        /// <param name="userData"></param>
        private void SendInternal(
            ReadOnlySpan<byte> data,
            byte channelNumber,
            DeliveryMethod deliveryMethod,
            object userData)
        {
            if (_connectionState != ConnectionState.Connected || channelNumber >= _channels.Length)
                return;

            //Select channel
            PacketProperty property;
            BaseChannel channel = null;

            if (deliveryMethod == DeliveryMethod.Unreliable)
            {
                property = PacketProperty.Unreliable;
            }
            else
            {
                property = PacketProperty.Channeled;
                channel = CreateChannel((byte)(channelNumber * NetConstants.ChannelTypeCount + (byte)deliveryMethod));
            }

            //Prepare
            NetDebug.Write("[RS]Packet: " + property);

            //Check fragmentation
            int headerSize = NetPacket.GetHeaderSize(property);
            //Save mtu for multithread
            int mtu = _mtu;
            int length = data.Length;
            if (length + headerSize > mtu)
            {
                //if cannot be fragmented
                if (deliveryMethod != DeliveryMethod.ReliableOrdered && deliveryMethod != DeliveryMethod.ReliableUnordered)
                    throw new TooBigPacketException("Unreliable or ReliableSequenced packet size exceeded maximum of " + (mtu - headerSize) + " bytes, Check allowed size by GetMaxSinglePacketSize()");

                int packetFullSize = mtu - headerSize;
                int packetDataSize = packetFullSize - NetConstants.FragmentHeaderSize;
                int totalPackets = length / packetDataSize + (length % packetDataSize == 0 ? 0 : 1);

                if (totalPackets > ushort.MaxValue)
                    throw new TooBigPacketException("Data was split in " + totalPackets + " fragments, which exceeds " + ushort.MaxValue);

                ushort currentFragmentId = (ushort)Interlocked.Increment(ref _fragmentId);

                for (ushort partIdx = 0; partIdx < totalPackets; partIdx++)
                {
                    int sendLength = length > packetDataSize ? packetDataSize : length;

                    NetPacket p = NetManager.PoolGetPacket(headerSize + sendLength + NetConstants.FragmentHeaderSize);
                    p.Property = property;
                    p.UserData = userData;
                    p.FragmentId = currentFragmentId;
                    p.FragmentPart = partIdx;
                    p.FragmentsTotal = (ushort)totalPackets;
                    p.MarkFragmented();

                    data.Slice(partIdx * packetDataSize, sendLength).CopyTo(new Span<byte>(p.RawData, NetConstants.FragmentedHeaderTotalSize, sendLength));
                    channel.AddToQueue(p);

                    length -= sendLength;
                }
                return;
            }

            //Else just send
            NetPacket packet = NetManager.PoolGetPacket(headerSize + length);
            packet.Property = property;
            data.CopyTo(new Span<byte>(packet.RawData, headerSize, length));
            packet.UserData = userData;

            if (channel == null) //unreliable
            {
                EnqueueUnreliable(packet);
            }
            else
            {
                channel.AddToQueue(packet);
            }
        }
#endif

        public void Disconnect(byte[] data)
        {
            NetManager.DisconnectPeer(this, data);
        }

        public void Disconnect(NetDataWriter writer)
        {
            NetManager.DisconnectPeer(this, writer);
        }

        public void Disconnect(byte[] data, int start, int count)
        {
            NetManager.DisconnectPeer(this, data, start, count);
        }

        public void Disconnect()
        {
            NetManager.DisconnectPeer(this);
        }

        internal DisconnectResult ProcessDisconnect(NetPacket packet)
        {
            if ((_connectionState == ConnectionState.Connected || _connectionState == ConnectionState.Outgoing) &&
                packet.Size >= 9 &&
                BitConverter.ToInt64(packet.RawData, 1) == _connectTime &&
                packet.ConnectionNumber == _connectNum)
            {
                return _connectionState == ConnectionState.Connected
                    ? DisconnectResult.Disconnect
                    : DisconnectResult.Reject;
            }
            return DisconnectResult.None;
        }

        internal void AddToReliableChannelSendQueue(BaseChannel channel)
        {
            _channelSendQueue.Enqueue(channel);
        }

        /// <summary>
        /// 关闭连接
        /// </summary>
        /// <param name="data">发送数据</param>
        /// <param name="start"></param>
        /// <param name="length"></param>
        /// <param name="force">直接断连，不发送断连消息包</param>
        /// <returns></returns>
        internal ShutdownResult Shutdown(byte[] data, int start, int length, bool force)
        {
            lock (_shutdownLock)
            {
                //trying to shutdown already disconnected
                if (_connectionState == ConnectionState.Disconnected ||
                    _connectionState == ConnectionState.ShutdownRequested)
                {
                    return ShutdownResult.None;
                }

                var result = _connectionState == ConnectionState.Connected
                    ? ShutdownResult.WasConnected
                    : ShutdownResult.Success;

                //don't send anything
                if (force)
                {
                    _connectionState = ConnectionState.Disconnected;
                    return result;
                }

                //reset time for reconnect protection
                Interlocked.Exchange(ref _timeSinceLastPacket, 0);

                //send shutdown packet
                _shutdownPacket = new NetPacket(PacketProperty.Disconnect, length) {ConnectionNumber = _connectNum};
                FastBitConverter.GetBytes(_shutdownPacket.RawData, 1, _connectTime);
                if (_shutdownPacket.Size >= _mtu)
                {
                    //Drop additional data
                    NetDebug.WriteError("[Peer] Disconnect additional data size more than MTU - 8!");
                }
                else if (data != null && length > 0)
                {
                    Buffer.BlockCopy(data, start, _shutdownPacket.RawData, 9, length);
                }
                _connectionState = ConnectionState.ShutdownRequested;
                NetDebug.Write("[Peer] Send disconnect");
                NetManager.SendRaw(_shutdownPacket, this);
                return result;
            }
        }

        private void UpdateRoundTripTime(int roundTripTime)
        {
            _rtt += roundTripTime;
            _rttCount++;
            _avgRtt = _rtt/_rttCount;
            _resendDelay = 25.0 + _avgRtt * 2.1; // 25 ms + double rtt
        }

        /// <summary>
        /// 添加接收的可信赖的数据包 -> 需要处理|正在处理 -> 已收到此数据包(可能会回传确认-NetManager)
        /// </summary>
        /// <param name="method"></param>
        /// <param name="p"></param>
        internal void AddReliablePacket(DeliveryMethod method, NetPacket p)
        {
            if (p.IsFragmented)
            {
                NetDebug.Write($"Fragment. Id: {p.FragmentId}, Part: {p.FragmentPart}, Total: {p.FragmentsTotal}");
                //Get needed array from dictionary
                ushort packetFragId = p.FragmentId;
                byte packetChannelId = p.ChannelId;
                if (!_holdedFragments.TryGetValue(packetFragId, out var incomingFragments))
                {
                    incomingFragments = new IncomingFragments
                    {
                        Fragments = new NetPacket[p.FragmentsTotal],
                        ChannelId = p.ChannelId
                    };
                    _holdedFragments.Add(packetFragId, incomingFragments);
                }

                //Cache
                var fragments = incomingFragments.Fragments;

                //Error check
                if (p.FragmentPart >= fragments.Length ||
                    fragments[p.FragmentPart] != null ||
                    p.ChannelId != incomingFragments.ChannelId)
                {
                    NetManager.PoolRecycle(p);
                    NetDebug.WriteError("Invalid fragment packet");
                    return;
                }
                //Fill array
                fragments[p.FragmentPart] = p;

                //Increase received fragments count
                incomingFragments.ReceivedCount++;

                //Increase total size
                incomingFragments.TotalSize += p.Size - NetConstants.FragmentedHeaderTotalSize;

                //Check for finish
                if (incomingFragments.ReceivedCount != fragments.Length)
                    return;

                //just simple packet
                NetPacket resultingPacket = NetManager.PoolGetPacket(incomingFragments.TotalSize);

                int pos = 0;
                for (int i = 0; i < incomingFragments.ReceivedCount; i++)
                {
                    var fragment = fragments[i];
                    int writtenSize = fragment.Size - NetConstants.FragmentedHeaderTotalSize;

                    if (pos+writtenSize > resultingPacket.RawData.Length)
                    {
                        _holdedFragments.Remove(packetFragId);
                        NetDebug.WriteError($"Fragment error pos: {pos + writtenSize} >= resultPacketSize: {resultingPacket.RawData.Length} , totalSize: {incomingFragments.TotalSize}");
                        return;
                    }
                    if (fragment.Size > fragment.RawData.Length)
                    {
                        _holdedFragments.Remove(packetFragId);
                        NetDebug.WriteError($"Fragment error size: {fragment.Size} > fragment.RawData.Length: {fragment.RawData.Length}");
                        return;
                    }

                    //Create resulting big packet
                    Buffer.BlockCopy(
                        fragment.RawData,
                        NetConstants.FragmentedHeaderTotalSize,
                        resultingPacket.RawData,
                        pos,
                        writtenSize);
                    pos += writtenSize;

                    //Free memory
                    NetManager.PoolRecycle(fragment);
                    fragments[i] = null;
                }

                //Clear memory
                _holdedFragments.Remove(packetFragId);

                //Send to process
                NetManager.CreateReceiveEvent(resultingPacket, method, (byte)(packetChannelId / NetConstants.ChannelTypeCount), 0, this);
            }
            else //Just simple packet
            {
                NetManager.CreateReceiveEvent(p, method, (byte)(p.ChannelId / NetConstants.ChannelTypeCount), NetConstants.ChanneledHeaderSize, this);
            }
        }

        /// <summary>
        /// 处理MTU检查数据包(确认MTU)
        /// </summary>
        /// <param name="packet"></param>
        private void ProcessMtuPacket(NetPacket packet)
        {
            //header + int -> 最小为574
            if (packet.Size < NetConstants.PossibleMtu[0])
                return;

            //first stage check (mtu check and mtu ok)
            int receivedMtu = BitConverter.ToInt32(packet.RawData, 1);
            int endMtuCheck = BitConverter.ToInt32(packet.RawData, packet.Size - 4);
            if (receivedMtu != packet.Size || receivedMtu != endMtuCheck || receivedMtu > NetConstants.MaxPacketSize)
            {
                NetDebug.WriteError($"[MTU] Broken packet. RMTU {receivedMtu}, EMTU {endMtuCheck}, PSIZE {packet.Size}");
                return;
            }

            // 检查通过 -> 回传确认
            if (packet.Property == PacketProperty.MtuCheck)
            {
                _mtuCheckAttempts = 0;
                NetDebug.Write("[MTU] check. send back: " + receivedMtu);
                packet.Property = PacketProperty.MtuOk;
                NetManager.SendRawAndRecycle(packet, this);
            }
            // 检查确认
            else if(receivedMtu > _mtu && !_finishMtu) //MtuOk
            {
                //invalid packet
                if (receivedMtu != NetConstants.PossibleMtu[_mtuIdx + 1] - NetManager.ExtraPacketSizeForLayer)
                    return;

                lock (_mtuMutex)
                {
                    SetMtu(_mtuIdx+1);
                }
                //if maxed - finish.
                if (_mtuIdx == NetConstants.PossibleMtu.Length - 1)
                    _finishMtu = true;
                NetManager.PoolRecycle(packet);
                NetDebug.Write("[MTU] ok. Increase to: " + _mtu);
            }
        }

        /// <summary>
        /// Tick更新MTU -> Check合适的MTU
        /// </summary>
        /// <param name="deltaTime"></param>
        private void UpdateMtuLogic(float deltaTime)
        {
            // 已完成更新
            if (_finishMtu)
                return;

            _mtuCheckTimer += deltaTime;
            if (_mtuCheckTimer < MtuCheckDelay)
                return;

            _mtuCheckTimer = 0;
            _mtuCheckAttempts++;
            if (_mtuCheckAttempts >= MaxMtuCheckAttempts)
            {
                _finishMtu = true;
                return;
            }

            lock (_mtuMutex)
            {
                if (_mtuIdx >= NetConstants.PossibleMtu.Length - 1)
                    return;

                //Send increased packet -> MTU数量增大
                int newMtu = NetConstants.PossibleMtu[_mtuIdx + 1] - NetManager.ExtraPacketSizeForLayer;
                var p = NetManager.PoolGetPacket(newMtu);
                p.Property = PacketProperty.MtuCheck;
                FastBitConverter.GetBytes(p.RawData, 1, newMtu);         //place into start
                FastBitConverter.GetBytes(p.RawData, p.Size - 4, newMtu);//and end of packet

                //Must check result for MTU fix
                if (NetManager.SendRawAndRecycle(p, this) <= 0)
                    _finishMtu = true;
            }
        }

        /// <summary>
        /// 服务器处理连接请求
        /// </summary>
        /// <param name="connRequest"></param>
        /// <returns></returns>
        internal ConnectRequestResult ProcessConnectRequest(NetConnectRequestPacket connRequest)
        {
            //current or new request
            switch (_connectionState)
            {
                //P2P case
                case ConnectionState.Outgoing:
                    //fast check
                    if (connRequest.ConnectionTime < _connectTime)
                    {
                        return ConnectRequestResult.P2PLose;
                    }
                    //slow rare case check
                    if (connRequest.ConnectionTime == _connectTime)
                    {
                        var localBytes = connRequest.TargetAddress;
                        for (int i = _cachedSocketAddr.Size-1; i >= 0; i--)
                        {
                            byte rb = _cachedSocketAddr[i];
                            if (rb == localBytes[i])
                                continue;
                            if (rb < localBytes[i])
                                return ConnectRequestResult.P2PLose;
                        }
                    }
                    break;
                // 断线重连(短暂断开，客户端重新进行连接)
                case ConnectionState.Connected:
                    //Old connect request
                    if (connRequest.ConnectionTime == _connectTime)
                    {
                        //just reply accept
                        NetManager.SendRaw(_connectAcceptPacket, this);
                    }
                    //New connect request
                    else if (connRequest.ConnectionTime > _connectTime)
                    {
                        return ConnectRequestResult.Reconnection;
                    }
                    break;
                // 新的连接|实际断开后再次连接
                case ConnectionState.Disconnected:
                case ConnectionState.ShutdownRequested:
                    if (connRequest.ConnectionTime >= _connectTime)
                        return ConnectRequestResult.NewConnection;
                    break;
            }
            return ConnectRequestResult.None;
        }

        /// <summary>
        /// Process incoming packet - 处理接收的数据包(不包括连接、断开连接相关，仅为数据或者检查数据包)
        /// </summary>
        /// <param name="packet"></param>
        internal void ProcessPacket(NetPacket packet)
        {
            //not initialized
            if (_connectionState == ConnectionState.Outgoing || _connectionState == ConnectionState.Disconnected)
            {
                NetManager.PoolRecycle(packet);
                return;
            }
            // 客户端关闭连接确认
            if (packet.Property == PacketProperty.ShutdownOk)
            {
                if (_connectionState == ConnectionState.ShutdownRequested)
                    _connectionState = ConnectionState.Disconnected;
                NetManager.PoolRecycle(packet);
                return;
            }
            // 旧连接数据包 -> 丢弃
            if (packet.ConnectionNumber != _connectNum)
            {
                NetDebug.Write(NetLogLevel.Trace, "[RR]Old packet");
                NetManager.PoolRecycle(packet);
                return;
            }
            Interlocked.Exchange(ref _timeSinceLastPacket, 0);

            NetDebug.Write($"[RR]PacketProperty: {packet.Property}");
            switch (packet.Property)
            {
                // 多个小数据包合并为的一个大数据包
                case PacketProperty.Merged:
                    int pos = NetConstants.HeaderSize;
                    while (pos < packet.Size)
                    {
                        ushort size = BitConverter.ToUInt16(packet.RawData, pos);
                        pos += 2;
                        // 剩余数据不完整
                        if (packet.RawData.Length - pos < size)
                            break;

                        NetPacket mergedPacket = NetManager.PoolGetPacket(size);
                        Buffer.BlockCopy(packet.RawData, pos, mergedPacket.RawData, 0, size);
                        mergedPacket.Size = size;

                        if (!mergedPacket.Verify())
                            break;

                        pos += size;
                        ProcessPacket(mergedPacket);
                    }
                    NetManager.PoolRecycle(packet);
                    break;
                //If we get ping, send pong
                case PacketProperty.Ping:
                    // Ping在Pong之后 -> 新的PingPacket
                    if (NetUtils.RelativeSequenceNumber(packet.Sequence, _pongPacket.Sequence) > 0)
                    {
                        NetDebug.Write("[PP]Ping receive, send pong");
                        FastBitConverter.GetBytes(_pongPacket.RawData, 3, DateTime.UtcNow.Ticks);
                        _pongPacket.Sequence = packet.Sequence;
                        NetManager.SendRaw(_pongPacket, this);
                    }
                    NetManager.PoolRecycle(packet);
                    break;

                //If we get pong, calculate ping time and rtt
                case PacketProperty.Pong:
                    // Ping-Pong
                    if (packet.Sequence == _pingPacket.Sequence)
                    {
                        _pingTimer.Stop();
                        int elapsedMs = (int)_pingTimer.ElapsedMilliseconds;
                        // 远程系统时-本地系统时
                        _remoteDelta = BitConverter.ToInt64(packet.RawData, 3) + (elapsedMs * TimeSpan.TicksPerMillisecond ) / 2 - DateTime.UtcNow.Ticks;
                        UpdateRoundTripTime(elapsedMs);
                        NetManager.ConnectionLatencyUpdated(this, elapsedMs / 2);
                        NetDebug.Write($"[PP]Ping: {packet.Sequence} - {elapsedMs} - {_remoteDelta}");
                    }
                    NetManager.PoolRecycle(packet);
                    break;

                // 确认包直接回收
                case PacketProperty.Ack:
                // 通道传输直接交由通道处理
                case PacketProperty.Channeled:
                    if (packet.ChannelId >= _channels.Length)
                    {
                        NetManager.PoolRecycle(packet);
                        break;
                    }
                    var channel = _channels[packet.ChannelId] ?? (packet.Property == PacketProperty.Ack ? null : CreateChannel(packet.ChannelId));
                    if (channel != null)
                    {
                        if (!channel.ProcessPacket(packet))
                            NetManager.PoolRecycle(packet);
                    }
                    break;

                //Simple packet without acks
                case PacketProperty.Unreliable:
                    NetManager.CreateReceiveEvent(packet, DeliveryMethod.Unreliable, 0, NetConstants.HeaderSize, this);
                    return;

                case PacketProperty.MtuCheck:
                case PacketProperty.MtuOk:
                    ProcessMtuPacket(packet);
                    break;

                default:
                    NetDebug.WriteError("Error! Unexpected packet type: " + packet.Property);
                    break;
            }
        }

        /// <summary>
        /// 发送合并数据包
        /// </summary>
        private void SendMerged()
        {
            if (_mergeCount == 0)
                return;
            int bytesSent;
            if (_mergeCount > 1)
            {
                NetDebug.Write("[P]Send merged: " + _mergePos + ", count: " + _mergeCount);
                bytesSent = NetManager.SendRaw(_mergeData.RawData, 0, NetConstants.HeaderSize + _mergePos, this);
            }
            else
            {
                //Send without length information and merging
                bytesSent = NetManager.SendRaw(_mergeData.RawData, NetConstants.HeaderSize + 2, _mergePos - 2, this);
            }

            if (NetManager.EnableStatistics)
            {
                Statistics.IncrementPacketsSent();
                Statistics.AddBytesSent(bytesSent);
            }

            _mergePos = 0;
            _mergeCount = 0;
        }

        /// <summary>
        /// 发送packet数据包 -> 装入MergedPacket，若装入会导致大于MTU，则先发送MergedPacket，再装入
        /// </summary>
        /// <param name="packet"></param>
        internal void SendUserData(NetPacket packet)
        {
            packet.ConnectionNumber = _connectNum;
            int mergedPacketSize = NetConstants.HeaderSize + packet.Size + 2;
            const int sizeTreshold = 20;
            if (mergedPacketSize + sizeTreshold >= _mtu)
            {
                NetDebug.Write(NetLogLevel.Trace, "[P]SendingPacket: " + packet.Property);
                int bytesSent = NetManager.SendRaw(packet, this);

                if (NetManager.EnableStatistics)
                {
                    Statistics.IncrementPacketsSent();
                    Statistics.AddBytesSent(bytesSent);
                }

                return;
            }
            if (_mergePos + mergedPacketSize > _mtu)
                SendMerged();

            FastBitConverter.GetBytes(_mergeData.RawData, _mergePos + NetConstants.HeaderSize, (ushort)packet.Size);
            Buffer.BlockCopy(packet.RawData, 0, _mergeData.RawData, _mergePos + NetConstants.HeaderSize + 2, packet.Size);
            _mergePos += packet.Size + 2;
            _mergeCount++;
            //DebugWriteForce("Merged: " + _mergePos + "/" + (_mtu - 2) + ", count: " + _mergeCount);
        }

        /// <summary>
        /// 网络节点Tick -> 处理数据包发送
        /// </summary>
        /// <param name="deltaTime"></param>
        internal void Update(float deltaTime)
        {
            // 更新自最近一次包体接收经过的时间
            _timeSinceLastPacket = _timeSinceLastPacket + deltaTime;
            // 处理连接超时
            switch (_connectionState)
            {
                // 超过设置的断连时间
                case ConnectionState.Connected:
                    if (_timeSinceLastPacket > NetManager.DisconnectTimeout)
                    {
                        NetDebug.Write($"[UPDATE] Disconnect by timeout: {_timeSinceLastPacket} > {NetManager.DisconnectTimeout}");
                        NetManager.DisconnectPeerForce(this, DisconnectReason.Timeout, 0, null);
                        return;
                    }
                    break;
                // 断连请求
                case ConnectionState.ShutdownRequested:
                    if (_timeSinceLastPacket > NetManager.DisconnectTimeout)
                    {
                        _connectionState = ConnectionState.Disconnected;
                    }
                    else
                    {
                        _shutdownTimer += deltaTime;
                        if (_shutdownTimer >= ShutdownDelay)
                        {
                            _shutdownTimer = 0;
                            NetManager.SendRaw(_shutdownPacket, this);
                        }
                    }
                    return;

                case ConnectionState.Outgoing:
                    _connectTimer += deltaTime;
                    if (_connectTimer > NetManager.ReconnectDelay)
                    {
                        _connectTimer = 0;
                        _connectAttempts++;
                        if (_connectAttempts > NetManager.MaxConnectAttempts)
                        {
                            NetManager.DisconnectPeerForce(this, DisconnectReason.ConnectionFailed, 0, null);
                            return;
                        }

                        //else send connect again
                        NetManager.SendRaw(_connectRequestPacket, this);
                    }
                    return;

                case ConnectionState.Disconnected:
                    return;
            }

            // 处理Ping
            //Send ping
            _pingSendTimer += deltaTime;
            if (_pingSendTimer >= NetManager.PingInterval)
            {
                NetDebug.Write("[PP] Send ping...");
                //reset timer
                _pingSendTimer = 0;
                //send ping
                _pingPacket.Sequence++;
                //ping timeout
                if (_pingTimer.IsRunning)
                    UpdateRoundTripTime((int)_pingTimer.ElapsedMilliseconds);
                _pingTimer.Restart();
                NetManager.SendRaw(_pingPacket, this);
            }

            // 更新RTT
            //RTT - round trip time
            _rttResetTimer += deltaTime;
            if (_rttResetTimer >= NetManager.PingInterval * 3)
            {
                _rttResetTimer = 0;
                _rtt = _avgRtt;
                _rttCount = 1;
            }

            // Check MTU
            UpdateMtuLogic(deltaTime);

            // 发送通道中的数据包
            //Pending send
            int count = _channelSendQueue.Count;
            while (count-- > 0)
            {
                if (!_channelSendQueue.TryDequeue(out var channel))
                    break;
                if (channel.SendAndCheckQueue())
                {
                    // still has something to send, re-add it to the send queue
                    _channelSendQueue.Enqueue(channel);
                }
            }

            // 发送不可靠数据包 -> 装入MergePacket
            if (_unreliablePendingCount > 0)
            {
                int unreliableCount;
                lock (_unreliableChannelLock)
                {
                    (_unreliableChannel, _unreliableSecondQueue) = (_unreliableSecondQueue, _unreliableChannel);
                    unreliableCount = _unreliablePendingCount;
                    _unreliablePendingCount = 0;
                }
                for (int i = 0; i < unreliableCount; i++)
                {
                    var packet = _unreliableSecondQueue[i];
                    SendUserData(packet);
                    NetManager.PoolRecycle(packet);
                }
            }

            SendMerged();
        }

        /// <summary>
        /// For reliable channel -> 消息确认时调用
        /// 回收Packet，如果UserData不为Null，则触发DeliverEvent
        /// </summary>
        /// <param name="packet"></param>
        internal void RecycleAndDeliver(NetPacket packet)
        {
            if (packet.UserData != null)
            {
                if (packet.IsFragmented)
                {
                    _deliveredFragments.TryGetValue(packet.FragmentId, out ushort fragCount);
                    fragCount++;
                    if (fragCount == packet.FragmentsTotal)
                    {
                        NetManager.MessageDelivered(this, packet.UserData);
                        _deliveredFragments.Remove(packet.FragmentId);
                    }
                    else
                    {
                        _deliveredFragments[packet.FragmentId] = fragCount;
                    }
                }
                else
                {
                    NetManager.MessageDelivered(this, packet.UserData);
                }
                packet.UserData = null;
            }
            NetManager.PoolRecycle(packet);
        }
    }
}
