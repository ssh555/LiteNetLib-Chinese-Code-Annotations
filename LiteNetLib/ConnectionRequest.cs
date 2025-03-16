using System.Net;
using System.Threading;
using LiteNetLib.Utils;

namespace LiteNetLib
{
    /// <summary>
    /// 连接请求的结果
    /// </summary>
    internal enum ConnectionRequestResult
    {
        None,
        Accept,
        /// <summary>
        /// 拒绝连接 -> 保留NetPeer
        /// </summary>
        Reject,
        /// <summary>
        /// 强制拒绝 -> NetPeer都不保留
        /// </summary>
        RejectForce
    }

    /// <summary>
    /// 连接请求
    /// </summary>
    public class ConnectionRequest
    {
        private readonly NetManager _listener;
        /// <summary>
        /// 连接请求是否被使用
        /// </summary>
        private int _used;

        public NetDataReader Data => InternalPacket.Data;

        internal ConnectionRequestResult Result { get; private set; }
        internal NetConnectRequestPacket InternalPacket;

        public readonly IPEndPoint RemoteEndPoint;

        internal void UpdateRequest(NetConnectRequestPacket connectRequest)
        {
            //old request
            if (connectRequest.ConnectionTime < InternalPacket.ConnectionTime)
                return;

            // 同一个连接
            if (connectRequest.ConnectionTime == InternalPacket.ConnectionTime &&
                connectRequest.ConnectionNumber == InternalPacket.ConnectionNumber)
                return;

            InternalPacket = connectRequest;
        }

        /// <summary>
        /// 标记为已使用
        /// </summary>
        /// <returns></returns>
        private bool TryActivate()
        {
            return Interlocked.CompareExchange(ref _used, 1, 0) == 0;
        }

        internal ConnectionRequest(IPEndPoint remoteEndPoint, NetConnectRequestPacket requestPacket, NetManager listener)
        {
            InternalPacket = requestPacket;
            RemoteEndPoint = remoteEndPoint;
            _listener = listener;
        }

        /// <summary>
        /// 将接收的数据与Key比较，并返回连接请求的客户端NetPeer
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public NetPeer AcceptIfKey(string key)
        {
            if (!TryActivate())
                return null;
            try
            {
                if (Data.GetString() == key)
                    Result = ConnectionRequestResult.Accept;
            }
            catch
            {
                NetDebug.WriteError("[AC] Invalid incoming data");
            }
            if (Result == ConnectionRequestResult.Accept)
                return _listener.OnConnectionSolved(this, null, 0, 0);

            Result = ConnectionRequestResult.Reject;
            _listener.OnConnectionSolved(this, null, 0, 0);
            return null;
        }

        /// <summary>
        /// Accept connection and get new NetPeer as result
        /// </summary>
        /// <returns>Connected NetPeer</returns>
        public NetPeer Accept()
        {
            if (!TryActivate())
                return null;
            Result = ConnectionRequestResult.Accept;
            return _listener.OnConnectionSolved(this, null, 0, 0);
        }

        public void Reject(byte[] rejectData, int start, int length, bool force)
        {
            if (!TryActivate())
                return;
            Result = force ? ConnectionRequestResult.RejectForce : ConnectionRequestResult.Reject;
            _listener.OnConnectionSolved(this, rejectData, start, length);
        }

        public void Reject(byte[] rejectData, int start, int length)
        {
            Reject(rejectData, start, length, false);
        }


        public void RejectForce(byte[] rejectData, int start, int length)
        {
            Reject(rejectData, start, length, true);
        }

        public void RejectForce()
        {
            Reject(null, 0, 0, true);
        }

        public void RejectForce(byte[] rejectData)
        {
            Reject(rejectData, 0, rejectData.Length, true);
        }

        public void RejectForce(NetDataWriter rejectData)
        {
            Reject(rejectData.Data, 0, rejectData.Length, true);
        }

        public void Reject()
        {
            Reject(null, 0, 0, false);
        }

        public void Reject(byte[] rejectData)
        {
            Reject(rejectData, 0, rejectData.Length, false);
        }

        public void Reject(NetDataWriter rejectData)
        {
            Reject(rejectData.Data, 0, rejectData.Length, false);
        }
    }
}
