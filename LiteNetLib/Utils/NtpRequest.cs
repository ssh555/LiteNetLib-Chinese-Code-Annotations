using System.Net;
using System.Net.Sockets;

namespace LiteNetLib.Utils
{
    /// <summary>
    /// 时间同步请求
    /// 网络时间协议，用于同步网络的计算机时间 -> 用于发送NtpPacket同步网络时间
    /// TODO: ResendTimer怪怪的，并没有达到计时的效果，会一直进行SendPacket
    /// </summary>
    internal sealed class NtpRequest
    {
        private const int ResendTimer = 1000;
        private const int KillTimer = 10000;
        public const int DefaultPort = 123;
        private readonly IPEndPoint _ntpEndPoint;
        //private float _resendTime = ResendTimer;
        // Change
        private float _resendTime = ResendTimer;
        private float _killTime = 0;

        /// <summary>
        /// 目标IP终端
        /// </summary>
        /// <param name="endPoint"></param>
        public NtpRequest(IPEndPoint endPoint)
        {
            _ntpEndPoint = endPoint;
        }

        public bool NeedToKill => _killTime >= KillTimer;

        public bool Send(Socket socket, float time)
        {
            // TODO: 感觉resend时间怪怪的 -> 每次调用必定会进行send
            _resendTime += time;
            _killTime += time;
            if (_resendTime < ResendTimer)
            {
                return false;
            }
            // Change
            _resendTime = 0;
            var packet = new NtpPacket();
            try
            {
                int sendCount = socket.SendTo(packet.Bytes, 0, packet.Bytes.Length, SocketFlags.None, _ntpEndPoint);
                return sendCount == packet.Bytes.Length;
            }
            catch
            {
                return false;
            }
        }
    }
}
