using System;
using System.Net;
using LiteNetLib.Utils;

namespace LiteNetLib
{
    /// <summary>
    /// 连接请求数据包
    /// </summary>
    internal sealed class NetConnectRequestPacket
    {
        public const int HeaderSize = 18;
        /// <summary>
        /// 客户端连接时的系统时(客户端的系统时)
        /// </summary>
        public readonly long ConnectionTime;
        /// <summary>
        /// 客户端进行的连接次数
        /// </summary>
        public byte ConnectionNumber;
        /// <summary>
        /// 连接的客户端的IP端口
        /// </summary>
        public readonly byte[] TargetAddress;
        /// <summary>
        /// 用于连接的额外数据
        /// </summary>
        public readonly NetDataReader Data;
        /// <summary>
        /// RemoteId
        /// </summary>
        public readonly int PeerId;

        private NetConnectRequestPacket(long connectionTime, byte connectionNumber, int localId, byte[] targetAddress, NetDataReader data)
        {
            ConnectionTime = connectionTime;
            ConnectionNumber = connectionNumber;
            TargetAddress = targetAddress;
            Data = data;
            PeerId = localId;
        }

        /// <summary>
        /// 使用的协议
        /// </summary>
        /// <param name="packet"></param>
        /// <returns></returns>
        public static int GetProtocolId(NetPacket packet)
        {
            return BitConverter.ToInt32(packet.RawData, 1);
        }

        /// <summary>
        /// 解析连接请求数据包 -> 服务器处理
        /// </summary>
        /// <param name="packet"></param>
        /// <returns></returns>
        public static NetConnectRequestPacket FromData(NetPacket packet)
        {
            if (packet.ConnectionNumber >= NetConstants.MaxConnectionNumber)
                return null;

            //Getting connection time for peer
            long connectionTime = BitConverter.ToInt64(packet.RawData, 5);

            //Get peer id
            int peerId = BitConverter.ToInt32(packet.RawData, 13);

            //Get target address
            int addrSize = packet.RawData[HeaderSize-1];
            if (addrSize != 16 && addrSize != 28)
                return null;
            byte[] addressBytes = new byte[addrSize];
            Buffer.BlockCopy(packet.RawData, HeaderSize, addressBytes, 0, addrSize);

            // Read data and create request
            var reader = new NetDataReader(null, 0, 0);
            if (packet.Size > HeaderSize+addrSize)
                reader.SetSource(packet.RawData, HeaderSize + addrSize, packet.Size);

            return new NetConnectRequestPacket(connectionTime, packet.ConnectionNumber, peerId, addressBytes, reader);
        }

        /// <summary>
        /// 生成客户端发送到服务器的连接请求数据包
        /// </summary>
        /// <param name="connectData"></param>
        /// <param name="addressBytes"></param>
        /// <param name="connectTime"></param>
        /// <param name="localId"></param>
        /// <returns></returns>
        public static NetPacket Make(NetDataWriter connectData, SocketAddress addressBytes, long connectTime, int localId)
        {
            //Make initial packet
            var packet = new NetPacket(PacketProperty.ConnectRequest, connectData.Length+addressBytes.Size);

            //Add data
            FastBitConverter.GetBytes(packet.RawData, 1, NetConstants.ProtocolId);
            FastBitConverter.GetBytes(packet.RawData, 5, connectTime);
            FastBitConverter.GetBytes(packet.RawData, 13, localId);
            packet.RawData[HeaderSize-1] = (byte)addressBytes.Size;
            for (int i = 0; i < addressBytes.Size; i++)
                packet.RawData[HeaderSize + i] = addressBytes[i];
            Buffer.BlockCopy(connectData.Data, 0, packet.RawData, HeaderSize + addressBytes.Size, connectData.Length);
            return packet;
        }
    }

    /// <summary>
    /// 连接接受数据包
    /// </summary>
    internal sealed class NetConnectAcceptPacket
    {
        public const int Size = 15;
        /// <summary>
        /// 客户端自己发送时的连接时间
        /// </summary>
        public readonly long ConnectionTime;
        public readonly byte ConnectionNumber;
        /// <summary>
        /// RemoteId
        /// </summary>
        public readonly int PeerId;
        /// <summary>
        /// 远程网络端口是否改变
        /// </summary>
        public readonly bool PeerNetworkChanged;

        private NetConnectAcceptPacket(long connectionTime, byte connectionNumber, int peerId, bool peerNetworkChanged)
        {
            ConnectionTime = connectionTime;
            ConnectionNumber = connectionNumber;
            PeerId = peerId;
            PeerNetworkChanged = peerNetworkChanged;
        }

        /// <summary>
        /// 客户端解析连接接受数据包
        /// </summary>
        /// <param name="packet"></param>
        /// <returns></returns>
        public static NetConnectAcceptPacket FromData(NetPacket packet)
        {
            if (packet.Size != Size)
                return null;

            long connectionId = BitConverter.ToInt64(packet.RawData, 1);

            //check connect num
            byte connectionNumber = packet.RawData[9];
            if (connectionNumber >= NetConstants.MaxConnectionNumber)
                return null;

            //check reused flag
            byte isReused = packet.RawData[10];
            if (isReused > 1)
                return null;

            //get remote peer id
            int peerId = BitConverter.ToInt32(packet.RawData, 11);
            if (peerId < 0)
                return null;

            return new NetConnectAcceptPacket(connectionId, connectionNumber, peerId, isReused == 1);
        }

        /// <summary>
        /// 服务器生成连接接受数据包
        /// </summary>
        /// <param name="connectTime"></param>
        /// <param name="connectNum"></param>
        /// <param name="localPeerId"></param>
        /// <returns></returns>
        public static NetPacket Make(long connectTime, byte connectNum, int localPeerId)
        {
            var packet = new NetPacket(PacketProperty.ConnectAccept, 0);
            FastBitConverter.GetBytes(packet.RawData, 1, connectTime);
            packet.RawData[9] = connectNum;
            FastBitConverter.GetBytes(packet.RawData, 11, localPeerId);
            return packet;
        }

        /// <summary>
        /// 远程网络节点发生改变
        /// </summary>
        /// <param name="peer"></param>
        /// <returns></returns>
        public static NetPacket MakeNetworkChanged(NetPeer peer)
        {
            var packet = new NetPacket(PacketProperty.PeerNotFound, Size-1);
            FastBitConverter.GetBytes(packet.RawData, 1, peer.ConnectTime);
            packet.RawData[9] = peer.ConnectionNum;
            packet.RawData[10] = 1;
            FastBitConverter.GetBytes(packet.RawData, 11, peer.RemoteId);
            return packet;
        }
    }
}
