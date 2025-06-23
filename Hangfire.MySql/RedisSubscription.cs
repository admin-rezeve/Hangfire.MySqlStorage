using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Server;
using StackExchange.Redis;

namespace Hangfire.MySql.Redis
{
#pragma warning disable 618
    internal class RedisSubscription : IServerComponent, IDisposable
#pragma warning restore 618
    {
        private readonly ManualResetEvent _mre = new ManualResetEvent(false);
        private readonly MySqlStorage _storage;
        private readonly ISubscriber _subscriber;
        private bool _disposed;

        public RedisSubscription([NotNull] MySqlStorage storage, [NotNull] ISubscriber subscriber)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            Channel = new RedisChannel(_storage.GetRedisKey("JobFetchChannel"), RedisChannel.PatternMode.Literal);
            _subscriber = subscriber ?? throw new ArgumentNullException(nameof(subscriber));
            
        }

        public RedisChannel Channel { get; }

        public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
        {
            _mre.Reset();
            WaitHandle.WaitAny(new[] {_mre, cancellationToken.WaitHandle}, timeout);
        }

        void IServerComponent.Execute(CancellationToken cancellationToken)
        {
            _subscriber.Subscribe(Channel, (channel, value) => { if (!_disposed) _mre.Set(); });
            cancellationToken.WaitHandle.WaitOne();

            if (cancellationToken.IsCancellationRequested)
            {
                _subscriber.Unsubscribe(Channel);
                _mre.Reset();
            }
        }

        public void Dispose()
        {
            if (_disposed) return;

            try
            {
                _subscriber.Unsubscribe(Channel);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Redis unsubscribe error: {ex.Message}");
            }

            _mre.Dispose();
            _disposed = true;
        }

        ~RedisSubscription()
        {
            _mre.Dispose();
        }
    }
}