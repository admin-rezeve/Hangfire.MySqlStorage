using System;
using System.Linq;
using System.Threading;
using Dapper;
using MySqlConnector;
using StackExchange.Redis;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class CountersAggregatorTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly CountersAggregator _sut;
        private readonly MySqlStorage _storage;
        private readonly MySqlConnection _connection;
        private readonly ConnectionMultiplexer _redis;

        public CountersAggregatorTests()
        {
            var options = new MySqlStorageOptions
            {
                CountersAggregateInterval = TimeSpan.Zero,
                RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
                RedisPrefix = "test:hangfire",
                UseRedisDistributedLock = true

            };
            _redis = ConnectionUtils.CreateRedisConnection();
            _connection = ConnectionUtils.CreateConnection();
            _storage = new MySqlStorage(_connection, _redis, options);
            _sut = new CountersAggregator(_storage, options);
        }
        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase]
        public void CountersAggregatorExecutesProperly()
        {
            const string createSql = @"
insert into Counter (`Key`, Value, ExpireAt) 
values ('key', 1, @expireAt)";

            _storage.UseConnection(connection =>
            {
                // Arrange
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddHours(1) });

                var cts = new CancellationTokenSource();
                cts.Cancel();

                // Act
                _sut.Execute(cts.Token);

                // Assert
                Assert.Equal(1, connection.Query<int>(@"select count(*) from AggregatedCounter").Single());
            });
        }
    }
}
