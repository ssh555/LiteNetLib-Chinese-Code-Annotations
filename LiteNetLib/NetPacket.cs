using System;
using LiteNetLib.Utils;

namespace LiteNetLib
{
    /// <summary>
    /// 包体标识
    /// </summary>
    internal enum PacketProperty : byte
    {
        /// <summary>
        /// 不可靠数据包
        /// </summary>
        Unreliable,
        /// <summary>
        /// 数据包通过一个指定的通道发送
        /// </summary>
        Channeled,
        /// <summary>
        /// 确认数据包
        /// </summary>
        Ack,
        /// <summary>
        /// 测试连接的可达性
        /// </summary>
        Ping,
        /// <summary>
        /// 响应 Ping 数据包的消息
        /// </summary>
        Pong,
        /// <summary>
        /// 连接请求数据包
        /// </summary>
        ConnectRequest,
        /// <summary>
        /// 连接接受数据包
        /// </summary>
        ConnectAccept,
        /// <summary>
        /// 断开连接的数据包
        /// </summary>
        Disconnect,
        /// <summary>
        /// 无连接消息，通常用于广播消息而不建立正式连接
        /// </summary>
        UnconnectedMessage,
        /// <summary>
        /// 检查最大传输单元（MTU）的数据包，确保网络路径上的数据包大小合适
        /// </summary>
        MtuCheck,
        /// <summary>
        /// MTU检查的响应数据包，表示MTU检查通过
        /// </summary>
        MtuOk,
        /// <summary>
        /// 广播消息
        /// </summary>
        Broadcast,
        /// <summary>
        /// 合并数据包 -> 多个小的数据包合并为一个数据包
        /// </summary>
        Merged,
        /// <summary>
        /// 关闭确认数据包
        /// </summary>
        ShutdownOk,
        /// <summary>
        /// 目标对等节点未找到的消息
        /// </summary>
        PeerNotFound,
        /// <summary>
        /// 协议无效的消息，通常表示接收到的消息不符合预期的协议格式
        /// </summary>
        InvalidProtocol,
        /// <summary>
        /// 网络地址转换（NAT）消息，用于处理NAT设备的穿越
        /// </summary>
        NatMessage,
        /// <summary>
        /// 空数据包
        /// </summary>
        Empty
    }

    /// <summary>
    /// 网络数据包
    /// </summary>
    internal sealed class NetPacket
    {
        /// <summary>
        /// 包体标识种类
        /// </summary>
        private static readonly int PropertiesCount = Enum.GetValues(typeof(PacketProperty)).Length;
        /// <summary>
        /// 各种标识包体头的尺寸
        /// </summary>
        private static readonly int[] HeaderSizes;

        /// <summary>
        /// 确定各种标识包体的包体头大小
        /// </summary>
        static NetPacket()
        {
            HeaderSizes = NetUtils.AllocatePinnedUninitializedArray<int>(PropertiesCount);
            for (int i = 0; i < HeaderSizes.Length; i++)
            {
                switch ((PacketProperty)i)
                {
                    case PacketProperty.Channeled:
                    case PacketProperty.Ack:
                        HeaderSizes[i] = NetConstants.ChanneledHeaderSize;
                        break;
                    case PacketProperty.Ping:
                        HeaderSizes[i] = NetConstants.HeaderSize + 2;
                        break;
                    case PacketProperty.ConnectRequest:
                        HeaderSizes[i] = NetConnectRequestPacket.HeaderSize;
                        break;
                    case PacketProperty.ConnectAccept:
                        HeaderSizes[i] = NetConnectAcceptPacket.Size;
                        break;
                    case PacketProperty.Disconnect:
                        HeaderSizes[i] = NetConstants.HeaderSize + 8;
                        break;
                    case PacketProperty.Pong:
                        HeaderSizes[i] = NetConstants.HeaderSize + 10;
                        break;
                    default:
                        HeaderSizes[i] = NetConstants.HeaderSize;
                        break;
                }
            }
        }

        /// <summary>
        /// Header 包体标识
        /// </summary>
        public PacketProperty Property
        {
            get => (PacketProperty)(RawData[0] & 0x1F);
            set => RawData[0] = (byte)((RawData[0] & 0xE0) | (byte)value);
        }

        /// <summary>
        /// Peer 进行的连接次数
        /// </summary>
        public byte ConnectionNumber
        {
            get => (byte)((RawData[0] & 0x60) >> 5);
            set => RawData[0] = (byte) ((RawData[0] & 0x9F) | (value << 5));
        }

        /// <summary>
        /// 数据包的序列号
        /// </summary>
        public ushort Sequence
        {
            get => BitConverter.ToUInt16(RawData, 1);
            set => FastBitConverter.GetBytes(RawData, 1, value);
        }

        /// <summary>
        /// 是否分片 -> 数据太大，分为多个数据包传输
        /// </summary>
        public bool IsFragmented => (RawData[0] & 0x80) != 0;

        /// <summary>
        /// 标记为分片数据包
        /// </summary>
        public void MarkFragmented()
        {
            RawData[0] |= 0x80; //set first bit
        }

        /// <summary>
        /// 所属的通道
        /// </summary>
        public byte ChannelId
        {
            get => RawData[3];
            set => RawData[3] = value;
        }

        /// <summary>
        /// 分配序号 -> 标识属于同一份数据的数据包
        /// </summary>
        public ushort FragmentId
        {
            get => BitConverter.ToUInt16(RawData, 4);
            set => FastBitConverter.GetBytes(RawData, 4, value);
        }

        /// <summary>
        /// 分片数据包的顺序标识
        /// </summary>
        public ushort FragmentPart
        {
            get => BitConverter.ToUInt16(RawData, 6);
            set => FastBitConverter.GetBytes(RawData, 6, value);
        }

        /// <summary>
        /// 总分片数
        /// </summary>
        public ushort FragmentsTotal
        {
            get => BitConverter.ToUInt16(RawData, 8);
            set => FastBitConverter.GetBytes(RawData, 8, value);
        }

        //Data -> 包体头+包体
        public byte[] RawData;
        public int Size;

        //Delivery -> 需要发送的包体数据
        public object UserData;

        //Pool node -> 下一个空闲数据包，对象池，避免频繁申请和释放数据包内存
        public NetPacket Next;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size">包含包体头</param>
        public NetPacket(int size)
        {
            RawData = new byte[size];
            Size = size;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="property"></param>
        /// <param name="size">不包含包体头</param>
        public NetPacket(PacketProperty property, int size)
        {
            size += GetHeaderSize(property);
            RawData = new byte[size];
            Property = property;
            Size = size;
        }

        public static int GetHeaderSize(PacketProperty property)
        {
            return HeaderSizes[(int)property];
        }

        public int GetHeaderSize()
        {
            return HeaderSizes[RawData[0] & 0x1F];
        }

        /// <summary>
        /// 数据包数据验证
        /// </summary>
        /// <returns></returns>
        public bool Verify()
        {
            byte property = (byte)(RawData[0] & 0x1F);
            if (property >= PropertiesCount)
                return false;
            int headerSize = HeaderSizes[property];
            bool fragmented = (RawData[0] & 0x80) != 0;
            return Size >= headerSize && (!fragmented || Size >= headerSize + NetConstants.FragmentHeaderSize);
        }

        #if LITENETLIB_SPANS || NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1_OR_GREATER || NETCOREAPP2_1 || NETCOREAPP3_1 || NET5_0 || NETSTANDARD2_1
        /// <summary>
        /// 隐式转换为Span<byte>不分配内存，只是对现有数据进行包装的视图
        /// </summary>
        /// <param name="p"></param>
        public static implicit operator Span<byte>(NetPacket p) => new Span<byte>(p.RawData, 0, p.Size);
        #endif
    }
}
