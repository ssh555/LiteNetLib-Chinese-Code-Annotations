namespace LiteNetLib.Utils
{
    /// <summary>
    /// 网络传输的自定义序列化数据接口
    /// </summary>
    public interface INetSerializable
    {
        /// <summary>
        /// 序列化写入之后需要修改position，增加序列化数据的长度
        /// </summary>
        /// <param name="writer"></param>
        void Serialize(NetDataWriter writer);
        /// <summary>
        /// 如果不止T数据，需要更改position到下一个数据的开始位置 -> 最好每次一定修改positon
        /// </summary>
        /// <param name="reader"></param>
        void Deserialize(NetDataReader reader);
    }
}
