using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using LiteNetLib.Utils;

namespace LiteNetLib
{
    /// <summary>
    /// MAT地址类型
    /// </summary>
    public enum NatAddressType
    {
        /// <summary>
        /// 局域网内部NAT -> 内部私有IP
        /// </summary>
        Internal,
        /// <summary>
        /// 互联网外部NAT -> 外部公有IP
        /// </summary>
        External
    }

    /// <summary>
    /// NAT穿透监听事件接口
    /// </summary>
    public interface INatPunchListener
    {
        /// <summary>
        /// NAT穿透请求
        /// </summary>
        /// <param name="localEndPoint"></param>
        /// <param name="remoteEndPoint"></param>
        /// <param name="token">Relay Server给的token</param>
        void OnNatIntroductionRequest(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, string token);
        /// <summary>
        /// NAT穿透成功
        /// </summary>
        /// <param name="targetEndPoint">目标的实际IP</param>
        /// <param name="type">目标IP类型</param>
        /// <param name="token"></param>
        void OnNatIntroductionSuccess(IPEndPoint targetEndPoint, NatAddressType type, string token);
    }

    /// <summary>
    /// NAT穿透事件监听器
    /// </summary>
    public class EventBasedNatPunchListener : INatPunchListener
    {
        public delegate void OnNatIntroductionRequest(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, string token);
        public delegate void OnNatIntroductionSuccess(IPEndPoint targetEndPoint, NatAddressType type, string token);

        public event OnNatIntroductionRequest NatIntroductionRequest;
        public event OnNatIntroductionSuccess NatIntroductionSuccess;

        void INatPunchListener.OnNatIntroductionRequest(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, string token)
        {
            if(NatIntroductionRequest != null)
                NatIntroductionRequest(localEndPoint, remoteEndPoint, token);
        }

        void INatPunchListener.OnNatIntroductionSuccess(IPEndPoint targetEndPoint, NatAddressType type, string token)
        {
            if (NatIntroductionSuccess != null)
                NatIntroductionSuccess(targetEndPoint, type, token);
        }
    }

    /// <summary>
    /// Module for UDP NAT Hole punching operations. Can be accessed from NetManager
    /// </summary>
    public sealed class NatPunchModule
    {
        struct RequestEventData
        {
            public IPEndPoint LocalEndPoint;
            public IPEndPoint RemoteEndPoint;
            public string Token;
        }

        struct SuccessEventData
        {
            public IPEndPoint TargetEndPoint;
            public NatAddressType Type;
            public string Token;
        }

        /// <summary>
        /// NAT请求包
        /// </summary>
        class NatIntroduceRequestPacket
        {
            /// <summary>
            /// 私有IP
            /// </summary>
            public IPEndPoint Internal { [Preserve] get; [Preserve] set; }
            public string Token { [Preserve] get; [Preserve] set; }
        }

        /// <summary>
        /// NAT响应包
        /// </summary>
        class NatIntroduceResponsePacket
        {
            /// <summary>
            /// 私有IP
            /// </summary>
            public IPEndPoint Internal { [Preserve] get; [Preserve] set; }
            /// <summary>
            /// 公有IP
            /// </summary>
            public IPEndPoint External { [Preserve] get; [Preserve] set; }
            public string Token { [Preserve] get; [Preserve] set; }
        }

        /// <summary>
        /// NAT包
        /// </summary>
        class NatPunchPacket
        {
            public string Token { [Preserve] get; [Preserve] set; }
            /// <summary>
            /// 是否为公网
            /// </summary>
            public bool IsExternal { [Preserve] get; [Preserve] set; }
        }

        private readonly NetManager _socket;
        /// <summary>
        /// 待处理事件队列
        /// </summary>
        private readonly ConcurrentQueue<RequestEventData> _requestEvents = new ConcurrentQueue<RequestEventData>();
        /// <summary>
        /// 待处理事件队列
        /// </summary>
        private readonly ConcurrentQueue<SuccessEventData> _successEvents = new ConcurrentQueue<SuccessEventData>();
        private readonly NetDataReader _cacheReader = new NetDataReader();
        private readonly NetDataWriter _cacheWriter = new NetDataWriter();
        private readonly NetPacketProcessor _netPacketProcessor = new NetPacketProcessor(MaxTokenLength);
        private INatPunchListener _natPunchListener;
        public const int MaxTokenLength = 256;

        /// <summary>
        /// Events automatically will be called without PollEvents method from another thread
        /// 不进行线程同步 -> 直接进行事件处理
        /// </summary>
        public bool UnsyncedEvents = false;

        internal NatPunchModule(NetManager socket)
        {
            _socket = socket;
            // 接收到响应包
            _netPacketProcessor.SubscribeReusable<NatIntroduceResponsePacket>(OnNatIntroductionResponse);
            // 接收到请求包
            _netPacketProcessor.SubscribeReusable<NatIntroduceRequestPacket, IPEndPoint>(OnNatIntroductionRequest);
            // NAT穿透包
            _netPacketProcessor.SubscribeReusable<NatPunchPacket, IPEndPoint>(OnNatPunch);
        }

        internal void ProcessMessage(IPEndPoint senderEndPoint, NetPacket packet)
        {
            lock (_cacheReader)
            {
                _cacheReader.SetSource(packet.RawData, NetConstants.HeaderSize, packet.Size);
                _netPacketProcessor.ReadAllPackets(_cacheReader, senderEndPoint);
            }
        }

        /// <summary>
        /// 初始化设置NAT事件监听器
        /// </summary>
        /// <param name="listener"></param>
        public void Init(INatPunchListener listener)
        {
            _natPunchListener = listener;
        }

        private void Send<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(T packet, IPEndPoint target) where T : class, new()
        {
            _cacheWriter.Reset();
            // 包头 -> 数据包类型
            _cacheWriter.Put((byte)PacketProperty.NatMessage);
            // 写入数据
            _netPacketProcessor.Write(_cacheWriter, packet);
            // 发送
            _socket.SendRaw(_cacheWriter.Data, 0, _cacheWriter.Length, target);
        }

        /// <summary>
        /// Relay Server 调用，告诉服务器和客户端各自的IP
        /// </summary>
        /// <param name="hostInternal"></param>
        /// <param name="hostExternal"></param>
        /// <param name="clientInternal"></param>
        /// <param name="clientExternal"></param>
        /// <param name="additionalInfo"></param>
        public void NatIntroduce(
            IPEndPoint hostInternal,
            IPEndPoint hostExternal,
            IPEndPoint clientInternal,
            IPEndPoint clientExternal,
            string additionalInfo)
        {
            // 响应包
            var req = new NatIntroduceResponsePacket
            {
                Token = additionalInfo
            };

            //First packet (server) send to client
            // 告诉客户端 -> 服务器的IP
            req.Internal = hostInternal;
            req.External = hostExternal;
            Send(req, clientExternal);

            //Second packet (client) send to server
            // 告诉服务器 -> 客户端的IP
            req.Internal = clientInternal;
            req.External = clientExternal;
            Send(req, hostExternal);
        }


        /// <summary>
        /// 处理所有缓存的事件队列
        /// </summary>
        public void PollEvents()
        {
            if (UnsyncedEvents)
                return;

            if (_natPunchListener == null || (_successEvents.IsEmpty && _requestEvents.IsEmpty))
                return;

            while (_successEvents.TryDequeue(out var evt))
            {
                _natPunchListener.OnNatIntroductionSuccess(
                    evt.TargetEndPoint,
                    evt.Type,
                    evt.Token);
            }

            while (_requestEvents.TryDequeue(out var evt))
            {
                _natPunchListener.OnNatIntroductionRequest(evt.LocalEndPoint, evt.RemoteEndPoint, evt.Token);
            }
        }

        /// <summary>
        /// 客户端向主机发送NAT请求包
        /// </summary>
        /// <param name="host"></param>
        /// <param name="port"></param>
        /// <param name="additionalInfo"></param>
        public void SendNatIntroduceRequest(string host, int port, string additionalInfo)
        {
            SendNatIntroduceRequest(NetUtils.MakeEndPoint(host, port), additionalInfo);
        }

        public void SendNatIntroduceRequest(IPEndPoint masterServerEndPoint, string additionalInfo)
        {
            //prepare outgoing data
            string networkIp = NetUtils.GetLocalIp(LocalAddrType.IPv4);
            if (string.IsNullOrEmpty(networkIp) || masterServerEndPoint.AddressFamily == AddressFamily.InterNetworkV6)
            {
                networkIp = NetUtils.GetLocalIp(LocalAddrType.IPv6);
            }

            Send(
                new NatIntroduceRequestPacket
                {
                    Internal = NetUtils.MakeEndPoint(networkIp, _socket.LocalPort),
                    Token = additionalInfo
                },
                masterServerEndPoint);
        }

        /// <summary>
        /// We got request and must introduce
        /// 处理请求事件
        /// </summary>
        /// <param name="req"></param>
        /// <param name="senderEndPoint"></param>
        private void OnNatIntroductionRequest(NatIntroduceRequestPacket req, IPEndPoint senderEndPoint)
        {
            if (UnsyncedEvents)
            {
                _natPunchListener.OnNatIntroductionRequest(
                    req.Internal,
                    senderEndPoint,
                    req.Token);
            }
            else
            {
                _requestEvents.Enqueue(new RequestEventData
                {
                    LocalEndPoint = req.Internal,
                    RemoteEndPoint = senderEndPoint,
                    Token = req.Token
                });
            }
        }

        /// <summary>
        /// We got introduce and must punch
        /// 处理NAT响应 -> 发送NAT穿透包，告诉对方自己的IP
        /// </summary>
        /// <param name="req"></param>
        private void OnNatIntroductionResponse(NatIntroduceResponsePacket req)
        {
            NetDebug.Write(NetLogLevel.Trace, "[NAT] introduction received");

            // send internal punch
            var punchPacket = new NatPunchPacket {Token = req.Token};
            Send(punchPacket, req.Internal);
            NetDebug.Write(NetLogLevel.Trace, $"[NAT] internal punch sent to {req.Internal}");

            // hack for some routers
            _socket.Ttl = 2;
            _socket.SendRaw(new[] { (byte)PacketProperty.Empty }, 0, 1, req.External);

            // send external punch
            _socket.Ttl = NetConstants.SocketTTL;
            punchPacket.IsExternal = true;
            Send(punchPacket, req.External);
            NetDebug.Write(NetLogLevel.Trace, $"[NAT] external punch sent to {req.External}");
        }

        /// <summary>
        /// We got punch and can connect
        /// 穿透成功，可以开始进行连接
        /// </summary>
        /// <param name="req"></param>
        /// <param name="senderEndPoint"></param>
        private void OnNatPunch(NatPunchPacket req, IPEndPoint senderEndPoint)
        {
            //Read info
            NetDebug.Write(NetLogLevel.Trace, $"[NAT] punch received from {senderEndPoint} - additional info: {req.Token}");

            //Release punch success to client; enabling him to Connect() to Sender if token is ok
            if(UnsyncedEvents)
            {
                _natPunchListener.OnNatIntroductionSuccess(
                    senderEndPoint,
                    req.IsExternal ? NatAddressType.External : NatAddressType.Internal,
                    req.Token
                    );
            }
            else
            {
                _successEvents.Enqueue(new SuccessEventData
                {
                    TargetEndPoint = senderEndPoint,
                    Type = req.IsExternal ? NatAddressType.External : NatAddressType.Internal,
                    Token = req.Token
                });
            }
        }
    }
}
