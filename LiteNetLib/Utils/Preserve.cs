using System;

namespace LiteNetLib.Utils
{
    /// <summary>
    ///   <para>PreserveAttribute prevents byte code stripping from removing a class, method, field, or property.</para>
    ///   在编译过程中防止特定的类、方法、字段、属性等被字节码剥离（stripping）工具移除。它通常用于确保某些
    /// </summary>
    [AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Enum | AttributeTargets.Constructor | AttributeTargets.Method | AttributeTargets.Property | AttributeTargets.Field | AttributeTargets.Event | AttributeTargets.Interface | AttributeTargets.Delegate, Inherited = false)]
    public class PreserveAttribute : Attribute
    {
    }
}
