using LiteNetLib.Utils;
using System;
using System.Net;

namespace LiteNetLib.Layers
{
    /// <summary>
    /// 网络的CRC校验层 -> 保证传输的数据没有差错
    /// </summary>
    public sealed class Crc32cLayer : PacketLayerBase
    {
        /// <summary>
        /// 额外包体校验数据长度为4
        /// </summary>
        public Crc32cLayer() : base(CRC32C.ChecksumSize)
        {

        }

        public override void ProcessInboundPacket(ref IPEndPoint endPoint, ref byte[] data, ref int length)
        {
            // 数据长度小于(包体头长+校验码长度) -> 没有实际数据 -> 丢弃这个包
            if (length < NetConstants.HeaderSize + CRC32C.ChecksumSize)
            {
                NetDebug.WriteError("[NM] DataReceived size: bad!");
                //Set length to 0 to have netManager drop the packet.
                length = 0;
                return;
            }

            // 校验码开始位置
            int checksumPoint = length - CRC32C.ChecksumSize;
            // CRC校验失败
            if (CRC32C.Compute(data, 0, checksumPoint) != BitConverter.ToUInt32(data, checksumPoint))
            {
                NetDebug.Write("[NM] DataReceived checksum: bad!");
                //Set length to 0 to have netManager drop the packet.
                length = 0;
                return;
            }
            // CRC校验成功
            length -= CRC32C.ChecksumSize;
        }

        public override void ProcessOutBoundPacket(ref IPEndPoint endPoint, ref byte[] data, ref int offset, ref int length)
        {
            // 在末尾写入CRC校验码
            FastBitConverter.GetBytes(data, length, CRC32C.Compute(data, offset, length));
            length += CRC32C.ChecksumSize;
        }
    }
}
