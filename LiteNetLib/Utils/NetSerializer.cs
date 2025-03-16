using System;
using System.Reflection;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Runtime.Serialization;

namespace LiteNetLib.Utils
{
    /// <summary>
    /// 无效类型异常
    /// </summary>
    public class InvalidTypeException : ArgumentException
    {
        public InvalidTypeException(string message) : base(message) { }
    }

    /// <summary>
    /// 解析异常
    /// </summary>
    public class ParseException : Exception
    {
        public ParseException(string message) : base(message) { }
    }

    /// <summary>
    /// 网络数据序列化器 -> 针对 public Property {get; set; } 的序列化器，不支持Field
    /// 直接对包体进行读写(NetDataReader|NetDataWriter)
    /// 等同于实现INetSerializable接口的数据载体类型进行的序列化和反序列化
    /// 使用反射Get|Set对数据载体和Reader.Writer进行数据序列化|反序列化
    /// </summary>
    public class NetSerializer
    {
        /// <summary>
        /// 序列化的数据类型
        /// </summary>
        private enum CallType
        {
            Basic,
            Array,
            List
        }

        #region FastCall -> 数据载体|具体数据|Reader.Writer三者之间的序列化与反序列化
        /// <summary>
        /// 数据载体|具体数据|Reader.Writer三者之间的序列化与反序列化
        /// 从inf数据载体中将数据写入Writer
        /// 从Reader中将具体数据读入inf数据载体
        /// </summary>
        /// <typeparam name="T">载体数据类型</typeparam>
        private abstract class FastCall<T>
        {
            public CallType Type;
            public virtual void Init(MethodInfo getMethod, MethodInfo setMethod, CallType type) { Type = type; }
            public abstract void Read(T inf, NetDataReader r);
            public abstract void Write(T inf, NetDataWriter w);
            public abstract void ReadArray(T inf, NetDataReader r);
            public abstract void WriteArray(T inf, NetDataWriter w);
            public abstract void ReadList(T inf, NetDataReader r);
            public abstract void WriteList(T inf, NetDataWriter w);
        }

        /// <summary>
        /// 将TProperty序列化写入TClass，或者从TClass反序列化读出TProperty
        /// </summary>
        /// <typeparam name="TClass">数据信息载体类型</typeparam>
        /// <typeparam name="TProperty">具体数据类型</typeparam>
        private abstract class FastCallSpecific<TClass, TProperty> : FastCall<TClass>
        {
            /// <summary>
            /// 从TClass读取数据
            /// </summary>
            protected Func<TClass, TProperty> Getter;
            /// <summary>
            /// 将数据写入TClass
            /// </summary>
            protected Action<TClass, TProperty> Setter;
            /// <summary>
            /// 从TClass读取数据
            /// </summary>
            protected Func<TClass, TProperty[]> GetterArr;
            /// <summary>
            /// 将数据写入TClass
            /// </summary>
            protected Action<TClass, TProperty[]> SetterArr;
            /// <summary>
            /// 从TClass读取数据
            /// </summary>
            protected Func<TClass, List<TProperty>> GetterList;
            /// <summary>
            /// 将数据写入TClass
            /// </summary>
            protected Action<TClass, List<TProperty>> SetterList;

            public override void ReadArray(TClass inf, NetDataReader r) { throw new InvalidTypeException("Unsupported type: " + typeof(TProperty) + "[]"); }
            public override void WriteArray(TClass inf, NetDataWriter w) { throw new InvalidTypeException("Unsupported type: " + typeof(TProperty) + "[]"); }
            public override void ReadList(TClass inf, NetDataReader r) { throw new InvalidTypeException("Unsupported type: List<" + typeof(TProperty) + ">"); }
            public override void WriteList(TClass inf, NetDataWriter w) { throw new InvalidTypeException("Unsupported type: List<" + typeof(TProperty) + ">"); }

            /// <summary>
            /// 从r中读取数据进入inf辅助器 -> 读取r的数组长度，并返回一个inf内部的数组引用，便于后续读出数据进入返回的数组
            /// </summary>
            /// <param name="inf"></param>
            /// <param name="r"></param>
            /// <returns></returns>
            protected TProperty[] ReadArrayHelper(TClass inf, NetDataReader r)
            {
                ushort count = r.GetUShort();
                var arr = GetterArr(inf);
                arr = arr == null || arr.Length != count ? new TProperty[count] : arr;
                SetterArr(inf, arr);
                return arr;
            }

            /// <summary>
            /// 将inf中数据写入w辅助器 -> 写入数组长度，并返回inf中的数组引用，便于后续写入数组具体数据
            /// </summary>
            /// <param name="inf"></param>
            /// <param name="w"></param>
            /// <returns></returns>
            protected TProperty[] WriteArrayHelper(TClass inf, NetDataWriter w)
            {
                var arr = GetterArr(inf);
                w.Put((ushort)arr.Length);
                return arr;
            }

            /// <summary>
            /// 从r中读取数据进入inf辅助器 -> 读取r中List的长度，并返回一个inf中的List的引用
            /// </summary>
            /// <param name="inf"></param>
            /// <param name="r"></param>
            /// <param name="len"></param>
            /// <returns></returns>
            protected List<TProperty> ReadListHelper(TClass inf, NetDataReader r, out int len)
            {
                len = r.GetUShort();
                var list = GetterList(inf);
                if (list == null)
                {
                    list = new List<TProperty>(len);
                    SetterList(inf, list);
                }
                return list;
            }

            /// <summary>
            /// 将inf中数据写入w辅助器 -> 写入List长度，并返回inf中的List引用，便于后续写入List具体数据
            /// </summary>
            /// <param name="inf"></param>
            /// <param name="w"></param>
            /// <param name="len"></param>
            /// <returns></returns>
            protected List<TProperty> WriteListHelper(TClass inf, NetDataWriter w, out int len)
            {
                var list = GetterList(inf);
                if (list == null)
                {
                    len = 0;
                    w.Put(0);
                    return null;
                }
                len = list.Count;
                w.Put((ushort)len);
                return list;
            }

            /// <summary>
            /// 反射：方法类型 -> MethodInfo.GetGetMethod|SetSetMethod自动对对应数据读写
            /// Basic: Get = Func<TClass, TProperty> | Set= Action<TClass, TProperty>
            /// Array: Get = Func<TClass, TProperty[]> | Set= Action<TClass, TProperty[]>
            /// List: Get = Func<TClass, List<TProperty>> | Set= Action<TClass, List<TProperty>>
            /// </summary>
            /// <param name="getMethod"></param>
            /// <param name="setMethod"></param>
            /// <param name="type"></param>
            public override void Init(MethodInfo getMethod, MethodInfo setMethod, CallType type)
            {
                base.Init(getMethod, setMethod, type);
                switch (type)
                {
                    case CallType.Array:
                        GetterArr = (Func<TClass, TProperty[]>)Delegate.CreateDelegate(typeof(Func<TClass, TProperty[]>), getMethod);
                        SetterArr = (Action<TClass, TProperty[]>)Delegate.CreateDelegate(typeof(Action<TClass, TProperty[]>), setMethod);
                        break;
                    case CallType.List:
                        GetterList = (Func<TClass, List<TProperty>>)Delegate.CreateDelegate(typeof(Func<TClass, List<TProperty>>), getMethod);
                        SetterList = (Action<TClass, List<TProperty>>)Delegate.CreateDelegate(typeof(Action<TClass, List<TProperty>>), setMethod);
                        break;
                    default:
                        Getter = (Func<TClass, TProperty>)Delegate.CreateDelegate(typeof(Func<TClass, TProperty>), getMethod);
                        Setter = (Action<TClass, TProperty>)Delegate.CreateDelegate(typeof(Action<TClass, TProperty>), setMethod);
                        break;
                }
            }
        }

        /// <summary>
        /// 实现了单个元素读取写入的FastCallSpecific(支持Basic|Array) -> 从Reader|Writer中反序列化|序列化出单个TProperty数据
        /// </summary>
        /// <typeparam name="TClass"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        private abstract class FastCallSpecificAuto<TClass, TProperty> : FastCallSpecific<TClass, TProperty>
        {
            /// <summary>
            /// 读取单个元素数据
            /// </summary>
            /// <param name="r"></param>
            /// <param name="prop"></param>
            protected abstract void ElementRead(NetDataReader r, out TProperty prop);
            /// <summary>
            /// 写入单个元素数据
            /// </summary>
            /// <param name="w"></param>
            /// <param name="prop"></param>
            protected abstract void ElementWrite(NetDataWriter w, ref TProperty prop);

            public override void Read(TClass inf, NetDataReader r)
            {
                ElementRead(r, out var elem);
                Setter(inf, elem);
            }

            public override void Write(TClass inf, NetDataWriter w)
            {
                var elem = Getter(inf);
                ElementWrite(w, ref elem);
            }

            public override void ReadArray(TClass inf, NetDataReader r)
            {
                var arr = ReadArrayHelper(inf, r);
                for (int i = 0; i < arr.Length; i++)
                    ElementRead(r, out arr[i]);
            }

            public override void WriteArray(TClass inf, NetDataWriter w)
            {
                var arr = WriteArrayHelper(inf, w);
                for (int i = 0; i < arr.Length; i++)
                    ElementWrite(w, ref arr[i]);
            }
        }

        /// <summary>
        /// 根据reader和writer自定义的反序列化和序列化方法读取|写入数据
        /// </summary>
        /// <typeparam name="TClass"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        private sealed class FastCallStatic<TClass, TProperty> : FastCallSpecific<TClass, TProperty>
        {
            /// <summary>
            /// NetDataWriter的数据写入方法 -> 将TProperty写入NetDataWriter
            /// </summary>
            private readonly Action<NetDataWriter, TProperty> _writer;
            /// <summary>
            /// NetDataReader的数据读取方法 -> 从NetDataReader读取TProperty
            /// </summary>
            private readonly Func<NetDataReader, TProperty> _reader;

            public FastCallStatic(Action<NetDataWriter, TProperty> write, Func<NetDataReader, TProperty> read)
            {
                _writer = write;
                _reader = read;
            }

            public override void Read(TClass inf, NetDataReader r) { Setter(inf, _reader(r)); }
            public override void Write(TClass inf, NetDataWriter w) { _writer(w, Getter(inf)); }

            public override void ReadList(TClass inf, NetDataReader r)
            {
                var list = ReadListHelper(inf, r, out int len);
                int listCount = list.Count;
                for (int i = 0; i < len; i++)
                {
                    if (i < listCount)
                        list[i] = _reader(r);
                    // 将r中多余的读出，便于后续数据的读取
                    else
                        list.Add(_reader(r));
                }
                // 移除多余的读出数据
                if (len < listCount)
                    list.RemoveRange(len, listCount - len);
            }

            public override void WriteList(TClass inf, NetDataWriter w)
            {
                var list = WriteListHelper(inf, w, out int len);
                for (int i = 0; i < len; i++)
                    _writer(w, list[i]);
            }

            public override void ReadArray(TClass inf, NetDataReader r)
            {
                var arr = ReadArrayHelper(inf, r);
                int len = arr.Length;
                for (int i = 0; i < len; i++)
                    arr[i] = _reader(r);
            }

            public override void WriteArray(TClass inf, NetDataWriter w)
            {
                var arr = WriteArrayHelper(inf, w);
                int len = arr.Length;
                for (int i = 0; i < len; i++)
                    _writer(w, arr[i]);
            }
        }

        /// <summary>
        /// 限定TProperty为结构体和INetSerializable -> 对自定义结构体整体进行操作
        /// </summary>
        /// <typeparam name="TClass"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        private sealed class FastCallStruct<TClass, TProperty> : FastCallSpecific<TClass, TProperty> where TProperty : struct, INetSerializable
        {
            private TProperty _p;

            public override void Read(TClass inf, NetDataReader r)
            {
                // 从r中反序列化数据
                _p.Deserialize(r);
                // 写入inf
                Setter(inf, _p);
            }

            public override void Write(TClass inf, NetDataWriter w)
            {
                _p = Getter(inf);
                // 序列化写入
                _p.Serialize(w);
            }

            public override void ReadList(TClass inf, NetDataReader r)
            {
                var list = ReadListHelper(inf, r, out int len);
                int listCount = list.Count;
                for (int i = 0; i < len; i++)
                {
                    var itm = default(TProperty);
                    itm.Deserialize(r);
                    if (i < listCount)
                        list[i] = itm;
                    else
                        list.Add(itm);
                }
                if (len < listCount)
                    list.RemoveRange(len, listCount - len);
            }

            public override void WriteList(TClass inf, NetDataWriter w)
            {
                var list = WriteListHelper(inf, w, out int len);
                for (int i = 0; i < len; i++)
                    list[i].Serialize(w);
            }

            public override void ReadArray(TClass inf, NetDataReader r)
            {
                var arr = ReadArrayHelper(inf, r);
                int len = arr.Length;
                for (int i = 0; i < len; i++)
                    arr[i].Deserialize(r);
            }

            public override void WriteArray(TClass inf, NetDataWriter w)
            {
                var arr = WriteArrayHelper(inf, w);
                int len = arr.Length;
                for (int i = 0; i < len; i++)
                    arr[i].Serialize(w);
            }
        }

        /// <summary>
        /// 限定TProperty为类和INetSerializable -> 对自定义类整体进行操作
        /// </summary>
        /// <typeparam name="TClass"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        private sealed class FastCallClass<TClass, TProperty> : FastCallSpecific<TClass, TProperty> where TProperty : class, INetSerializable
        {
            /// <summary>
            /// 自定义类构造器
            /// </summary>
            private readonly Func<TProperty> _constructor;
            /// <summary>
            /// 
            /// </summary>
            /// <param name="constructor">自定义类构造器</param>
            public FastCallClass(Func<TProperty> constructor) { _constructor = constructor; }

            public override void Read(TClass inf, NetDataReader r)
            {
                var p = _constructor();
                p.Deserialize(r);
                Setter(inf, p);
            }

            public override void Write(TClass inf, NetDataWriter w)
            {
                var p = Getter(inf);
                p?.Serialize(w);
            }

            public override void ReadList(TClass inf, NetDataReader r)
            {
                var list = ReadListHelper(inf, r, out int len);
                int listCount = list.Count;
                for (int i = 0; i < len; i++)
                {
                    if (i < listCount)
                    {
                        list[i].Deserialize(r);
                    }
                    else
                    {
                        var itm = _constructor();
                        itm.Deserialize(r);
                        list.Add(itm);
                    }
                }
                if (len < listCount)
                    list.RemoveRange(len, listCount - len);
            }

            public override void WriteList(TClass inf, NetDataWriter w)
            {
                var list = WriteListHelper(inf, w, out int len);
                for (int i = 0; i < len; i++)
                    list[i].Serialize(w);
            }

            public override void ReadArray(TClass inf, NetDataReader r)
            {
                var arr = ReadArrayHelper(inf, r);
                int len = arr.Length;
                for (int i = 0; i < len; i++)
                {
                    arr[i] = _constructor();
                    arr[i].Deserialize(r);
                }
            }

            public override void WriteArray(TClass inf, NetDataWriter w)
            {
                var arr = WriteArrayHelper(inf, w);
                int len = arr.Length;
                for (int i = 0; i < len; i++)
                    arr[i].Serialize(w);
            }
        }

        #endregion

        #region Serializer -> 常规数据类型的Serializer，用于类中统一进行全体的读写
        /// <summary>
        /// 载入数据T中的int数据的序列化与反序列化
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private class IntSerializer<T> : FastCallSpecific<T, int>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetInt()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetIntArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class UIntSerializer<T> : FastCallSpecific<T, uint>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetUInt()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetUIntArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class ShortSerializer<T> : FastCallSpecific<T, short>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetShort()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetShortArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class UShortSerializer<T> : FastCallSpecific<T, ushort>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetUShort()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetUShortArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class LongSerializer<T> : FastCallSpecific<T, long>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetLong()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetLongArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class ULongSerializer<T> : FastCallSpecific<T, ulong>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetULong()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetULongArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class ByteSerializer<T> : FastCallSpecific<T, byte>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetByte()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetBytesWithLength()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutBytesWithLength(GetterArr(inf)); }
        }

        private class SByteSerializer<T> : FastCallSpecific<T, sbyte>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetSByte()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetSBytesWithLength()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutSBytesWithLength(GetterArr(inf)); }
        }

        private class FloatSerializer<T> : FastCallSpecific<T, float>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetFloat()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetFloatArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class DoubleSerializer<T> : FastCallSpecific<T, double>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetDouble()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetDoubleArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class BoolSerializer<T> : FastCallSpecific<T, bool>
        {
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetBool()); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf)); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetBoolArray()); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf)); }
        }

        private class CharSerializer<T> : FastCallSpecificAuto<T, char>
        {
            protected override void ElementWrite(NetDataWriter w, ref char prop) { w.Put(prop); }
            protected override void ElementRead(NetDataReader r, out char prop) { prop = r.GetChar(); }
        }

        private class IPEndPointSerializer<T> : FastCallSpecificAuto<T, IPEndPoint>
        {
            protected override void ElementWrite(NetDataWriter w, ref IPEndPoint prop) { w.Put(prop); }
            protected override void ElementRead(NetDataReader r, out IPEndPoint prop) { prop = r.GetNetEndPoint(); }
        }

        private class GuidSerializer<T> : FastCallSpecificAuto<T, Guid>
        {
            protected override void ElementWrite(NetDataWriter w, ref Guid guid) { w.Put(guid); }
            protected override void ElementRead(NetDataReader r, out Guid guid) { guid = r.GetGuid(); }
        }

        private class StringSerializer<T> : FastCallSpecific<T, string>
        {
            private readonly int _maxLength;
            public StringSerializer(int maxLength) { _maxLength = maxLength > 0 ? maxLength : short.MaxValue; }
            public override void Read(T inf, NetDataReader r) { Setter(inf, r.GetString(_maxLength)); }
            public override void Write(T inf, NetDataWriter w) { w.Put(Getter(inf), _maxLength); }
            public override void ReadArray(T inf, NetDataReader r) { SetterArr(inf, r.GetStringArray(_maxLength)); }
            public override void WriteArray(T inf, NetDataWriter w) { w.PutArray(GetterArr(inf), _maxLength); }
        }

        /// <summary>
        /// Byte类型的Enum
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private class EnumByteSerializer<T> : FastCall<T>
        {
            /// <summary>
            /// 序列化的数据载体类型中的EnumByte的Property
            /// </summary>
            protected readonly PropertyInfo Property;
            /// <summary>
            /// EnumByte的类型
            /// </summary>
            protected readonly Type PropertyType;
            /// <summary>
            /// 
            /// </summary>
            /// <param name="property">数据载体类型中的EnumByte的PropertyInfo</param>
            /// <param name="propertyType">EnumByte的类型</param>
            public EnumByteSerializer(PropertyInfo property, Type propertyType)
            {
                Property = property;
                PropertyType = propertyType;
            }
            public override void Read(T inf, NetDataReader r) { Property.SetValue(inf, Enum.ToObject(PropertyType, r.GetByte()), null); }
            public override void Write(T inf, NetDataWriter w) { w.Put((byte)Property.GetValue(inf, null)); }
            public override void ReadArray(T inf, NetDataReader r) { throw new InvalidTypeException("Unsupported type: Enum[]"); }
            public override void WriteArray(T inf, NetDataWriter w) { throw new InvalidTypeException("Unsupported type: Enum[]"); }
            public override void ReadList(T inf, NetDataReader r) { throw new InvalidTypeException("Unsupported type: List<Enum>"); }
            public override void WriteList(T inf, NetDataWriter w) { throw new InvalidTypeException("Unsupported type: List<Enum>"); }
        }

        private class EnumIntSerializer<T> : EnumByteSerializer<T>
        {
            /// <summary>
            /// 
            /// </summary>
            /// <param name="property">数据载体类型中的EnumByte的PropertyInfo</param>
            /// <param name="propertyType">EnumByte的类型</param>
            public EnumIntSerializer(PropertyInfo property, Type propertyType) : base(property, propertyType) { }
            public override void Read(T inf, NetDataReader r) { Property.SetValue(inf, Enum.ToObject(PropertyType, r.GetInt()), null); }
            public override void Write(T inf, NetDataWriter w) { w.Put((int)Property.GetValue(inf, null)); }
        }
        #endregion

        /// <summary>
        /// 一个类的类型的数据 -> T内的所有的Property的序列化FastCall
        /// 对类的所有数据统一进行读写
        /// </summary>
        /// <typeparam name="T">数据载体类的类型</typeparam>
        private sealed class ClassInfo<T>
        {
            public static ClassInfo<T> Instance;
            /// <summary>
            /// 需要进行序列化的类的内部数据
            /// </summary>
            private readonly FastCall<T>[] _serializers;
            /// <summary>
            /// 总共需要的序列化数据成员的数量
            /// </summary>
            private readonly int _membersCount;

            public ClassInfo(List<FastCall<T>> serializers)
            {
                _membersCount = serializers.Count;
                _serializers = serializers.ToArray();
            }

            /// <summary>
            /// 统一将obj中的数据序列化写入writer
            /// </summary>
            /// <param name="obj"></param>
            /// <param name="writer"></param>
            public void Write(T obj, NetDataWriter writer)
            {
                for (int i = 0; i < _membersCount; i++)
                {
                    var s = _serializers[i];
                    if (s.Type == CallType.Basic)
                        s.Write(obj, writer);
                    else if (s.Type == CallType.Array)
                        s.WriteArray(obj, writer);
                    else
                        s.WriteList(obj, writer);
                }
            }

            /// <summary>
            /// 统一从reader中读取数据至obj
            /// </summary>
            /// <param name="obj"></param>
            /// <param name="reader"></param>
            public void Read(T obj, NetDataReader reader)
            {
                for (int i = 0; i < _membersCount; i++)
                {
                    var s = _serializers[i];
                    if (s.Type == CallType.Basic)
                        s.Read(obj, reader);
                    else if (s.Type == CallType.Array)
                        s.ReadArray(obj, reader);
                    else
                        s.ReadList(obj, reader);
                }
            }
        }

        #region CustomType -> 自定义数据类型的FastCall(Serializer)，也用于ClassInfo
        private abstract class CustomType
        {
            public abstract FastCall<T> Get<T>();
        }

        private sealed class CustomTypeStruct<TProperty> : CustomType where TProperty : struct, INetSerializable
        {
            public override FastCall<T> Get<T>() { return new FastCallStruct<T, TProperty>(); }
        }

        private sealed class CustomTypeClass<TProperty> : CustomType where TProperty : class, INetSerializable
        {
            private readonly Func<TProperty> _constructor;
            public CustomTypeClass(Func<TProperty> constructor) { _constructor = constructor; }
            public override FastCall<T> Get<T>() { return new FastCallClass<T, TProperty>(_constructor); }
        }

        private sealed class CustomTypeStatic<TProperty> : CustomType
        {
            private readonly Action<NetDataWriter, TProperty> _writer;
            private readonly Func<NetDataReader, TProperty> _reader;
            public CustomTypeStatic(Action<NetDataWriter, TProperty> writer, Func<NetDataReader, TProperty> reader)
            {
                _writer = writer;
                _reader = reader;
            }
            public override FastCall<T> Get<T>() { return new FastCallStatic<T, TProperty>(_writer, _reader); }
        }
        #endregion

        #region 自定义类型的注册
        /// <summary>
        /// Register custom property type
        /// 可网络序列化的结构体类型
        /// </summary>
        /// <typeparam name="T">INetSerializable structure</typeparam>
        public void RegisterNestedType<T>() where T : struct, INetSerializable
        {
            _registeredTypes.Add(typeof(T), new CustomTypeStruct<T>());
        }

        /// <summary>
        /// Register custom property type
        /// 可网络序列化的类类型
        /// </summary>
        /// <typeparam name="T">INetSerializable class</typeparam>
        public void RegisterNestedType<T>(Func<T> constructor) where T : class, INetSerializable
        {
            _registeredTypes.Add(typeof(T), new CustomTypeClass<T>(constructor));
        }

        /// <summary>
        /// Register custom property type
        /// 注册FastCallStatic -> 任意类型，但需要给出writer和reader
        /// </summary>
        /// <typeparam name="T">Any packet</typeparam>
        /// <param name="writer">custom type writer</param>
        /// <param name="reader">custom type reader</param>
        public void RegisterNestedType<T>(Action<NetDataWriter, T> writer, Func<NetDataReader, T> reader)
        {
            _registeredTypes.Add(typeof(T), new CustomTypeStatic<T>(writer, reader));
        }

        #endregion

        /// <summary>
        /// 用于内部将需要进行序列化的数据进行序列化写入并将byte[]数据返回
        /// </summary>
        private NetDataWriter _writer;
        /// <summary>
        /// 序列化器序列化字符串的最大长度
        /// </summary>
        private readonly int _maxStringLength;
        /// <summary>
        /// 注册可使用的自定义类型<类型，FastCall>
        /// </summary>
        private readonly Dictionary<Type, CustomType> _registeredTypes = new Dictionary<Type, CustomType>();

        public NetSerializer() : this(0)
        {
        }

        public NetSerializer(int maxStringLength)
        {
            _maxStringLength = maxStringLength;
        }

        /// <summary>
        /// 内部注册需要统一进行序列化读写的类类型 -> 没有则注册并返回，有则直接返回ClassInfo
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private ClassInfo<T> RegisterInternal<
#if NET5_0_OR_GREATER
        [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
            T>()
        {
            // 有则直接返回
            if (ClassInfo<T>.Instance != null)
                return ClassInfo<T>.Instance;

            // 必须是public Property {get; set;}而且不能是static
            var props = typeof(T).GetProperties(
                BindingFlags.Instance |
                BindingFlags.Public |
                BindingFlags.GetProperty |
                BindingFlags.SetProperty);
            // 类中的所有的可序列化读写的数据的Property的序列化器
            var serializers = new List<FastCall<T>>();

            for (int i = 0; i < props.Length; i++)
            {
                // property
                var property = props[i];
                // type
                var propertyType = property.PropertyType;

                // 元素类型
                var elementType = propertyType.IsArray ? propertyType.GetElementType() : propertyType;
                var callType = propertyType.IsArray ? CallType.Array : CallType.Basic;

                // 为List
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(List<>))
                {
                    elementType = propertyType.GetGenericArguments()[0];
                    callType = CallType.List;
                }

                // 不作为数据成员 -> 忽略数据序列化
                if (Attribute.IsDefined(property, typeof(IgnoreDataMemberAttribute)))
                    continue;

                var getMethod = property.GetGetMethod();
                var setMethod = property.GetSetMethod();
                if (getMethod == null || setMethod == null)
                    continue;

                FastCall<T> serialzer = null;
                if (propertyType.IsEnum)
                {
                    var underlyingType = Enum.GetUnderlyingType(propertyType);
                    if (underlyingType == typeof(byte))
                        serialzer = new EnumByteSerializer<T>(property, propertyType);
                    else if (underlyingType == typeof(int))
                        serialzer = new EnumIntSerializer<T>(property, propertyType);
                    else
                        throw new InvalidTypeException("Not supported enum underlying type: " + underlyingType.Name);
                }
                else if (elementType == typeof(string))
                    serialzer = new StringSerializer<T>(_maxStringLength);
                else if (elementType == typeof(bool))
                    serialzer = new BoolSerializer<T>();
                else if (elementType == typeof(byte))
                    serialzer = new ByteSerializer<T>();
                else if (elementType == typeof(sbyte))
                    serialzer = new SByteSerializer<T>();
                else if (elementType == typeof(short))
                    serialzer = new ShortSerializer<T>();
                else if (elementType == typeof(ushort))
                    serialzer = new UShortSerializer<T>();
                else if (elementType == typeof(int))
                    serialzer = new IntSerializer<T>();
                else if (elementType == typeof(uint))
                    serialzer = new UIntSerializer<T>();
                else if (elementType == typeof(long))
                    serialzer = new LongSerializer<T>();
                else if (elementType == typeof(ulong))
                    serialzer = new ULongSerializer<T>();
                else if (elementType == typeof(float))
                    serialzer = new FloatSerializer<T>();
                else if (elementType == typeof(double))
                    serialzer = new DoubleSerializer<T>();
                else if (elementType == typeof(char))
                    serialzer = new CharSerializer<T>();
                else if (elementType == typeof(IPEndPoint))
                    serialzer = new IPEndPointSerializer<T>();
                else if (elementType == typeof(Guid))
                    serialzer = new GuidSerializer<T>();
                else
                {
                    _registeredTypes.TryGetValue(elementType, out var customType);
                    if (customType != null)
                        serialzer = customType.Get<T>();
                }

                if (serialzer != null)
                {
                    serialzer.Init(getMethod, setMethod, callType);
                    serializers.Add(serialzer);
                }
                else
                {
                    throw new InvalidTypeException("Unknown property type: " + propertyType.FullName);
                }
            }

            ClassInfo<T>.Instance = new ClassInfo<T>(serializers);
            return ClassInfo<T>.Instance;
        }

        /// <exception cref="InvalidTypeException">
        /// <typeparamref name="T"/>'s fields are not supported, or it has no fields
        /// </exception>
        /// <summary>
        /// 注册统一进行整个类的数据的序列化读写的类型
        /// 必须是public Property {get; set;}而且不能是static
        /// 不支持field
        /// </summary>
        public void Register<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>()
        {
            RegisterInternal<T>();
        }

        /// <summary>
        /// Reads packet with known type
        /// </summary>
        /// <param name="reader">NetDataReader with packet</param>
        /// <returns>Returns packet if packet in reader is matched type</returns>
        /// <exception cref="InvalidTypeException"><typeparamref name="T"/>'s fields are not supported, or it has no fields</exception>
        public T Deserialize<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(NetDataReader reader) where T : class, new()
        {
            var info = RegisterInternal<T>();
            var result = new T();
            try
            {
                info.Read(result, reader);
            }
            catch
            {
                return null;
            }
            return result;
        }

        /// <summary>
        /// Reads packet with known type (non alloc variant)
        /// </summary>
        /// <param name="reader">NetDataReader with packet</param>
        /// <param name="target">Deserialization target</param>
        /// <returns>Returns true if packet in reader is matched type</returns>
        /// <exception cref="InvalidTypeException"><typeparamref name="T"/>'s fields are not supported, or it has no fields</exception>
        public bool Deserialize<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(NetDataReader reader, T target) where T : class, new()
        {
            var info = RegisterInternal<T>();
            try
            {
                info.Read(target, reader);
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Serialize object to NetDataWriter (fast)
        /// </summary>
        /// <param name="writer">Serialization target NetDataWriter</param>
        /// <param name="obj">Object to serialize</param>
        /// <exception cref="InvalidTypeException"><typeparamref name="T"/>'s fields are not supported, or it has no fields</exception>
        public void Serialize<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(NetDataWriter writer, T obj) where T : class, new()
        {
            RegisterInternal<T>().Write(obj, writer);
        }

        /// <summary>
        /// Serialize object to byte array
        /// </summary>
        /// <param name="obj">Object to serialize</param>
        /// <returns>byte array with serialized data</returns>
        public byte[] Serialize<
#if NET5_0_OR_GREATER
            [DynamicallyAccessedMembers(Trimming.SerializerMemberTypes)]
#endif
        T>(T obj) where T : class, new()
        {
            if (_writer == null)
                _writer = new NetDataWriter();
            _writer.Reset();
            Serialize(_writer, obj);
            return _writer.CopyData();
        }
    }
}
