using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace LiteNetLib.Utils
{
    /// <summary>
    /// 网络包处理器 -> 数据序列化写入(Write) & 数据反序列化读取(Read) & 接收包体时的数据处理回调，但不负责接收和发送
    /// 包体组成: (typeHash, Data) x N
    /// typeHash用于指定数据类型，然后读取之后对应的Data，可以将多个拼接在一起进行处理
    /// </summary>
    public class NetPacketProcessor
    {
        /// <summary>
        /// 类型的Hash.Id缓存
        /// 4字节的ULong类型
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private static class HashCache<T>
        {
            public static readonly ulong Id;

            //FNV-1 64 bit hash
            static HashCache()
            {
                ulong hash = 14695981039346656037UL; //offset
                string typeName = typeof(T).ToString();
                for (var i = 0; i < typeName.Length; i++)
                {
                    hash ^= typeName[i];
                    hash *= 1099511628211UL; //prime
                }
                Id = hash;
            }
        }

        /// <summary>
        /// 数据更新(读取)时的回调
        /// 必须使用reader将对应数据类型的数据全部读取出来，不然会影响后续的包体数据读取 -> 已自动处理
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="userData"></param>
        protected delegate void SubscribeDelegate(NetDataReader reader, object userData);
        /// <summary>
        /// 托管的数据序列化器
        /// </summary>
        private readonly NetSerializer _netSerializer;
        /// <summary>
        /// 注册的读取数据回调
        /// </summary>
        private readonly Dictionary<ulong, SubscribeDelegate> _callbacks = new Dictionary<ulong, SubscribeDelegate>();

        public NetPacketProcessor()
        {
            _netSerializer = new NetSerializer();
        }

        public NetPacketProcessor(int maxStringLength)
        {
            _netSerializer = new NetSerializer(maxStringLength);
        }

        protected virtual ulong GetHash<T>()
        {
            return HashCache<T>.Id;
        }

        /// <summary>
        /// 获取reader包体中对应的数据类型的读取回调类型
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        protected virtual SubscribeDelegate GetCallbackFromData(NetDataReader reader)
        {
            ulong hash = reader.GetULong();
            if (!_callbacks.TryGetValue(hash, out var action))
            {
                throw new ParseException("Undefined packet in NetDataReader");
            }
            return action;
        }

        /// <summary>
        /// 写入类型T的Hash
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writer"></param>
        protected virtual void WriteHash<T>(NetDataWriter writer)
        {
            writer.Put(GetHash<T>());
        }

        /// <summary>
        /// Register nested property type
        /// 注册自定义的托管类型
        /// </summary>
        /// <typeparam name="T">INetSerializable structure</typeparam>
        public void RegisterNestedType<T>() where T : struct, INetSerializable
        {
            _netSerializer.RegisterNestedType<T>();
        }

        /// <summary>
        /// Register nested property type
        /// 注册自定义的托管类型
        /// </summary>
        /// <param name="writeDelegate"></param>
        /// <param name="readDelegate"></param>
        public void RegisterNestedType<T>(Action<NetDataWriter, T> writeDelegate, Func<NetDataReader, T> readDelegate)
        {
            _netSerializer.RegisterNestedType<T>(writeDelegate, readDelegate);
        }

        /// <summary>
        /// Register nested property type
        /// 注册自定义的托管类型
        /// </summary>
        /// <typeparam name="T">INetSerializable class</typeparam>
        public void RegisterNestedType<T>(Func<T> constructor) where T : class, INetSerializable
        {
            _netSerializer.RegisterNestedType(constructor);
        }

        /// <summary>
        /// Reads all available data from NetDataReader and calls OnReceive delegates
        /// 处理reader中剩余的所有没有读取的包体数据并响应对应的回调函数
        /// </summary>
        /// <param name="reader">NetDataReader with packets data</param>
        public void ReadAllPackets(NetDataReader reader)
        {
            while (reader.AvailableBytes > 0)
                ReadPacket(reader);
        }

        /// <summary>
        /// Reads all available data from NetDataReader and calls OnReceive delegates
        /// 处理reader中剩余的所有没有读取的包体数据并响应对应的回调函数
        /// 传入额外用户数据参数
        /// </summary>
        /// <param name="reader">NetDataReader with packets data</param>
        /// <param name="userData">Argument that passed to OnReceivedEvent</param>
        /// <exception cref="ParseException">Malformed packet</exception>
        public void ReadAllPackets(NetDataReader reader, object userData)
        {
            while (reader.AvailableBytes > 0)
                ReadPacket(reader, userData);
        }

        /// <summary>
        /// Reads one packet from NetDataReader and calls OnReceive delegate
        /// </summary>
        /// <param name="reader">NetDataReader with packet</param>
        /// <exception cref="ParseException">Malformed packet</exception>
        public void ReadPacket(NetDataReader reader)
        {
            ReadPacket(reader, null);
        }

        /// <summary>
        /// 序列化写入包体数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="writer"></param>
        /// <param name="packet"></param>
        public void Write<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(NetDataWriter writer, T packet) where T : class, new()
        {
            WriteHash<T>(writer);
            _netSerializer.Serialize(writer, packet);
        }

        public void WriteNetSerializable<T>(NetDataWriter writer, ref T packet) where T : INetSerializable
        {
            WriteHash<T>(writer);
            packet.Serialize(writer);
        }

        /// <summary>
        /// Reads one packet from NetDataReader and calls OnReceive delegate
        /// </summary>
        /// <param name="reader">NetDataReader with packet</param>
        /// <param name="userData">Argument that passed to OnReceivedEvent</param>
        /// <exception cref="ParseException">Malformed packet</exception>
        public void ReadPacket(NetDataReader reader, object userData)
        {
            GetCallbackFromData(reader)(reader, userData);
        }

        /// <summary>
        /// Register and subscribe to packet receive event
        /// </summary>
        /// <param name="onReceive">event that will be called when packet deserialized with ReadPacket method</param>
        /// <param name="packetConstructor">Method that constructs packet instead of slow Activator.CreateInstance</param>
        /// <exception cref="InvalidTypeException"><typeparamref name="T"/>'s fields are not supported, or it has no fields</exception>
        public void Subscribe<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(Action<T> onReceive, Func<T> packetConstructor) where T : class, new()
        {
            // 托管数据类型T
            _netSerializer.Register<T>();
            // 加入|替换读取回调
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                // 调用传入的数据载体对象引用 -> 可以是新建也可以是已有的传入
                var reference = packetConstructor();
                // 反序列化读取数据
                _netSerializer.Deserialize(reader, reference);
                // 执行传入的回调
                onReceive(reference);
            };
        }

        /// <summary>
        /// Register and subscribe to packet receive event (with userData)
        /// </summary>
        /// <param name="onReceive">event that will be called when packet deserialized with ReadPacket method</param>
        /// <param name="packetConstructor">Method that constructs packet instead of slow Activator.CreateInstance</param>
        /// <exception cref="InvalidTypeException"><typeparamref name="T"/>'s fields are not supported, or it has no fields</exception>
        public void Subscribe<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T, TUserData>(Action<T, TUserData> onReceive, Func<T> packetConstructor) where T : class, new()
        {
            _netSerializer.Register<T>();
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                var reference = packetConstructor();
                _netSerializer.Deserialize(reader, reference);
                onReceive(reference, (TUserData)userData);
            };
        }

        /// <summary>
        /// Register and subscribe to packet receive event
        /// This method will overwrite last received packet class on receive (less garbage)
        /// 每次注册都会重新new一个数据载体对象，不注册则复用之前new的对象
        /// </summary>
        /// <param name="onReceive">event that will be called when packet deserialized with ReadPacket method</param>
        /// <exception cref="InvalidTypeException"><typeparamref name="T"/>'s fields are not supported, or it has no fields</exception>
        public void SubscribeReusable<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(Action<T> onReceive) where T : class, new()
        {
            _netSerializer.Register<T>();
            var reference = new T();
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                _netSerializer.Deserialize(reader, reference);
                onReceive(reference);
            };
        }

        /// <summary>
        /// Register and subscribe to packet receive event
        /// This method will overwrite last received packet class on receive (less garbage)
        /// </summary>
        /// <param name="onReceive">event that will be called when packet deserialized with ReadPacket method</param>
        /// <exception cref="InvalidTypeException"><typeparamref name="T"/>'s fields are not supported, or it has no fields</exception>
        public void SubscribeReusable<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T, TUserData>(Action<T, TUserData> onReceive) where T : class, new()
        {
            _netSerializer.Register<T>();
            var reference = new T();
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                _netSerializer.Deserialize(reader, reference);
                onReceive(reference, (TUserData)userData);
            };
        }

        public void SubscribeNetSerializable<T, TUserData>(
            Action<T, TUserData> onReceive,
            Func<T> packetConstructor) where T : INetSerializable
        {
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                var pkt = packetConstructor();
                pkt.Deserialize(reader);
                onReceive(pkt, (TUserData)userData);
            };
        }

        public void SubscribeNetSerializable<T>(
            Action<T> onReceive,
            Func<T> packetConstructor) where T : INetSerializable
        {
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                var pkt = packetConstructor();
                pkt.Deserialize(reader);
                onReceive(pkt);
            };
        }

        public void SubscribeNetSerializable<T, TUserData>(
            Action<T, TUserData> onReceive) where T : INetSerializable, new()
        {
            var reference = new T();
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                reference.Deserialize(reader);
                onReceive(reference, (TUserData)userData);
            };
        }

        public void SubscribeNetSerializable<T>(
            Action<T> onReceive) where T : INetSerializable, new()
        {
            var reference = new T();
            _callbacks[GetHash<T>()] = (reader, userData) =>
            {
                reference.Deserialize(reader);
                onReceive(reference);
            };
        }

        /// <summary>
        /// Remove any subscriptions by type
        /// </summary>
        /// <typeparam name="T">Packet type</typeparam>
        /// <returns>true if remove is success</returns>
        public bool RemoveSubscription<T>()
        {
            return _callbacks.Remove(GetHash<T>());
        }
    }
}
