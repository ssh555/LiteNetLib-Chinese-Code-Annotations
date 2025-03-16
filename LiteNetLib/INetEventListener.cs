using System.Net;
using System.Net.Sockets;
using LiteNetLib.Utils;

namespace LiteNetLib
{
    /// <summary>
    /// Type of message that you receive in OnNetworkReceiveUnconnected event
    /// 尚未建立连接时的消息类型 -> 用于正式连接请求前确认服务器的IP端口或者告诉服务器(或者其他网络节点)自己的存在
    /// </summary>
    public enum UnconnectedMessageType
    {
        BasicMessage,
        Broadcast
    }

    /// <summary>
    /// Disconnect reason that you receive in OnPeerDisconnected event
    /// </summary>
    public enum DisconnectReason
    {
        /// <summary>
        /// 连接尝试失败
        /// </summary>
        ConnectionFailed,
        /// <summary>
        /// 连接超时
        /// </summary>
        Timeout,
        /// <summary>
        /// 目标主机无法访问 -> 目标主机网络问题
        /// </summary>
        HostUnreachable,
        /// <summary>
        /// 网络无法访问 -> 自身网络问题
        /// </summary>
        NetworkUnreachable,
        /// <summary>
        /// 远程关闭
        /// </summary>
        RemoteConnectionClose,
        /// <summary>
        /// 本地关闭
        /// </summary>
        DisconnectPeerCalled,
        /// <summary>
        /// 拒绝连接请求
        /// </summary>
        ConnectionRejected,
        /// <summary>
        /// 协议不匹配
        /// </summary>
        InvalidProtocol,
        /// <summary>
        /// 无法解析主机
        /// </summary>
        UnknownHost,
        /// <summary>
        /// 重新连接
        /// </summary>
        Reconnect,
        /// <summary>
        /// P2P连接问题
        /// </summary>
        PeerToPeerConnection,
        /// <summary>
        /// 找不到目标网络节点
        /// </summary>
        PeerNotFound
    }

    /// <summary>
    /// Additional information about disconnection
    /// </summary>
    public struct DisconnectInfo
    {
        /// <summary>
        /// Additional info why peer disconnected
        /// </summary>
        public DisconnectReason Reason;

        /// <summary>
        /// Error code (if reason is SocketSendError or SocketReceiveError)
        /// </summary>
        public SocketError SocketErrorCode;

        /// <summary>
        /// Additional data that can be accessed (only if reason is RemoteConnectionClose)
        /// </summary>
        public NetPacketReader AdditionalData;
    }

    /// <summary>
    /// 网络事件监听器 -> NetManager处理网络相关事件
    /// </summary>
    public interface INetEventListener
    {
        /// <summary>
        /// New remote peer connected to host, or client connected to remote host
        /// 建立连接时触发
        /// </summary>
        /// <param name="peer">Connected peer object</param>
        void OnPeerConnected(NetPeer peer);

        /// <summary>
        /// Peer disconnected
        /// 断开连接时触发
        /// </summary>
        /// <param name="peer">disconnected peer</param>
        /// <param name="disconnectInfo">additional info about reason, errorCode or data received with disconnect message</param>
        void OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo);

        /// <summary>
        /// Network error (on send or receive)
        /// 网络收发数据包错误时触发
        /// </summary>
        /// <param name="endPoint">From endPoint (can be null)</param>
        /// <param name="socketError">Socket error</param>
        void OnNetworkError(IPEndPoint endPoint, SocketError socketError);

        /// <summary>
        /// Received some data
        /// 连接后接收到数据时触发
        /// </summary>
        /// <param name="peer">From peer</param>
        /// <param name="reader">DataReader containing all received data</param>
        /// <param name="channelNumber">Number of channel at which packet arrived</param>
        /// <param name="deliveryMethod">Type of received packet</param>
        void OnNetworkReceive(NetPeer peer, NetPacketReader reader, byte channelNumber, DeliveryMethod deliveryMethod);

        /// <summary>
        /// Received unconnected message
        /// 接收到尚未进行连接的IP端口的数据时触发
        /// </summary>
        /// <param name="remoteEndPoint">From address (IP and Port)</param>
        /// <param name="reader">Message data</param>
        /// <param name="messageType">Message type (simple, discovery request or response)</param>
        void OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, NetPacketReader reader, UnconnectedMessageType messageType);

        /// <summary>
        /// Latency information updated
        /// 网络延迟更新
        /// </summary>
        /// <param name="peer">Peer with updated latency</param>
        /// <param name="latency">latency value in milliseconds</param>
        void OnNetworkLatencyUpdate(NetPeer peer, int latency);

        /// <summary>
        /// On peer connection requested
        /// 连接请求
        /// </summary>
        /// <param name="request">Request information (EndPoint, internal id, additional data)</param>
        void OnConnectionRequest(ConnectionRequest request);
    }

    /// <summary>
    /// 消息回传确认的事件回调，即目标收到消息后发送确认数据包，接收确认后触发
    /// 仅支持ReliableChannel
    /// </summary>
    public interface IDeliveryEventListener
    {
        /// <summary>
        /// On reliable message delivered
        /// </summary>
        /// <param name="peer"></param>
        /// <param name="userData"></param>
        void OnMessageDelivered(NetPeer peer, object userData);
    }

    /// <summary>
    /// 接收NTP系统时间同步Packet时触发
    /// </summary>
    public interface INtpEventListener
    {
        /// <summary>
        /// Ntp response
        /// </summary>
        /// <param name="packet"></param>
        void OnNtpResponse(NtpPacket packet);
    }

    /// <summary>
    /// 网络节点的IP端口发生改变时触发
    /// </summary>
    public interface IPeerAddressChangedListener
    {
        /// <summary>
        /// Called when peer address changed (when AllowPeerAddressChange is enabled)
        /// </summary>
        /// <param name="peer">Peer that changed address (with new address)</param>
        /// <param name="previousAddress">previous IP</param>
        void OnPeerAddressChanged(NetPeer peer, IPEndPoint previousAddress);
    }

    /// <summary>
    /// 网络事件监听器
    /// </summary>
    public class EventBasedNetListener : INetEventListener, IDeliveryEventListener, INtpEventListener, IPeerAddressChangedListener
    {
        // 对应于上面接口中事件函数的委托
        public delegate void OnPeerConnected(NetPeer peer);
        public delegate void OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo);
        public delegate void OnNetworkError(IPEndPoint endPoint, SocketError socketError);
        public delegate void OnNetworkReceive(NetPeer peer, NetPacketReader reader, byte channel, DeliveryMethod deliveryMethod);
        public delegate void OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, NetPacketReader reader, UnconnectedMessageType messageType);
        public delegate void OnNetworkLatencyUpdate(NetPeer peer, int latency);
        public delegate void OnConnectionRequest(ConnectionRequest request);
        public delegate void OnDeliveryEvent(NetPeer peer, object userData);
        public delegate void OnNtpResponseEvent(NtpPacket packet);
        public delegate void OnPeerAddressChangedEvent(NetPeer peer, IPEndPoint previousAddress);

        // 网络事件
        public event OnPeerConnected PeerConnectedEvent;
        public event OnPeerDisconnected PeerDisconnectedEvent;
        public event OnNetworkError NetworkErrorEvent;
        public event OnNetworkReceive NetworkReceiveEvent;
        public event OnNetworkReceiveUnconnected NetworkReceiveUnconnectedEvent;
        public event OnNetworkLatencyUpdate NetworkLatencyUpdateEvent;
        public event OnConnectionRequest ConnectionRequestEvent;
        public event OnDeliveryEvent DeliveryEvent;
        public event OnNtpResponseEvent NtpResponseEvent;
        public event OnPeerAddressChangedEvent PeerAddressChangedEvent;

        public void ClearPeerConnectedEvent()
        {
            PeerConnectedEvent = null;
        }

        public void ClearPeerDisconnectedEvent()
        {
            PeerDisconnectedEvent = null;
        }

        public void ClearNetworkErrorEvent()
        {
            NetworkErrorEvent = null;
        }

        public void ClearNetworkReceiveEvent()
        {
            NetworkReceiveEvent = null;
        }

        public void ClearNetworkReceiveUnconnectedEvent()
        {
            NetworkReceiveUnconnectedEvent = null;
        }

        public void ClearNetworkLatencyUpdateEvent()
        {
            NetworkLatencyUpdateEvent = null;
        }

        public void ClearConnectionRequestEvent()
        {
            ConnectionRequestEvent = null;
        }

        public void ClearDeliveryEvent()
        {
            DeliveryEvent = null;
        }

        public void ClearNtpResponseEvent()
        {
            NtpResponseEvent = null;
        }

        public void ClearPeerAddressChangedEvent()
        {
            PeerAddressChangedEvent = null;
        }

        void INetEventListener.OnPeerConnected(NetPeer peer)
        {
            if (PeerConnectedEvent != null)
                PeerConnectedEvent(peer);
        }

        void INetEventListener.OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo)
        {
            if (PeerDisconnectedEvent != null)
                PeerDisconnectedEvent(peer, disconnectInfo);
        }

        void INetEventListener.OnNetworkError(IPEndPoint endPoint, SocketError socketErrorCode)
        {
            if (NetworkErrorEvent != null)
                NetworkErrorEvent(endPoint, socketErrorCode);
        }

        void INetEventListener.OnNetworkReceive(NetPeer peer, NetPacketReader reader, byte channelNumber, DeliveryMethod deliveryMethod)
        {
            if (NetworkReceiveEvent != null)
                NetworkReceiveEvent(peer, reader, channelNumber, deliveryMethod);
        }

        void INetEventListener.OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, NetPacketReader reader, UnconnectedMessageType messageType)
        {
            if (NetworkReceiveUnconnectedEvent != null)
                NetworkReceiveUnconnectedEvent(remoteEndPoint, reader, messageType);
        }

        void INetEventListener.OnNetworkLatencyUpdate(NetPeer peer, int latency)
        {
            if (NetworkLatencyUpdateEvent != null)
                NetworkLatencyUpdateEvent(peer, latency);
        }

        void INetEventListener.OnConnectionRequest(ConnectionRequest request)
        {
            if (ConnectionRequestEvent != null)
                ConnectionRequestEvent(request);
        }

        void IDeliveryEventListener.OnMessageDelivered(NetPeer peer, object userData)
        {
            if (DeliveryEvent != null)
                DeliveryEvent(peer, userData);
        }

        void INtpEventListener.OnNtpResponse(NtpPacket packet)
        {
            if (NtpResponseEvent != null)
                NtpResponseEvent(packet);
        }

        void IPeerAddressChangedListener.OnPeerAddressChanged(NetPeer peer, IPEndPoint previousAddress)
        {
            if (PeerAddressChangedEvent != null)
                PeerAddressChangedEvent(peer, previousAddress);
        }
    }
}
