using System.Net;

namespace LiteNetLib.Layers
{
    /// <summary>
    /// 接收包|发送包的基础层抽象类(经过Layer才能成功接收|发送消息)
    /// </summary>
    public abstract class PacketLayerBase
    {
        /// <summary>
        /// 层所占用的额外包体大小
        /// </summary>
        public readonly int ExtraPacketSizeForLayer;

        protected PacketLayerBase(int extraPacketSizeForLayer)
        {
            ExtraPacketSizeForLayer = extraPacketSizeForLayer;
        }

        /// <summary>
        /// 处理接收的包体数据
        /// </summary>
        /// <param name="endPoint">发送的IP终端</param>
        /// <param name="data">数据</param>
        /// <param name="length">数据长度</param>
        public abstract void ProcessInboundPacket(ref IPEndPoint endPoint, ref byte[] data, ref int length);
        /// <summary>
        /// 处理发送的包体数据
        /// </summary>
        /// <param name="endPoint">目标IP终端</param>
        /// <param name="data">数据</param>
        /// <param name="offset">数据偏移: 开始处理数据的位置</param>
        /// <param name="length">长度</param>
        public abstract void ProcessOutBoundPacket(ref IPEndPoint endPoint, ref byte[] data, ref int offset, ref int length);
    }
}
