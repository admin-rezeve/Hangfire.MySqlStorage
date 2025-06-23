using System;
using System.Linq;
using System.Transactions;
using Dapper;
using Hangfire.MySql.JobQueue;
using MySqlConnector;
using StackExchange.Redis;
using Xunit;

namespace Hangfire.MySql.Tests.JobQueue
{
    public class MySqlJobQueueMonitoringApiTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private readonly MySqlJobQueueMonitoringApi _sut;
        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;
        private readonly MySqlConnection _connection;
        private readonly string _queue = "default";
        private readonly ConnectionMultiplexer _redis;
        private readonly IDatabase _redisDb;

        public MySqlJobQueueMonitoringApiTests()
        {
            _connection = new MySqlConnection(ConnectionUtils.GetConnectionString());
            _connection.Open();

            _storageOptions = new MySqlStorageOptions { 
                UseRedisDistributedLock = true,
                RedisConnectionString = ConnectionUtils.GetRedisConnectionString(), 
                RedisPrefix = "test:hangfire"
            };

            _redis = ConnectionMultiplexer.Connect(_storageOptions.RedisConnectionString);
            _redisDb = _redis.GetDatabase();
            _storage = new MySqlStorage(_connection, _redis, _storageOptions);
            _sut = new MySqlJobQueueMonitoringApi(_storage, _storageOptions);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedAndFetchedCount_ReturnsEqueuedCount_WhenExists()
        {
            EnqueuedAndFetchedCountDto result = null;
            if (_storageOptions.UseRedisDistributedLock)
            {
                // Arrange
                string redisQueueKey = _storage.GetRedisKey($"queue:{_queue}");
                _redisDb.ListRightPush(redisQueueKey, "1"); // Simulate enqueuing job with JobId 1

                // Act
                result = _sut.GetEnqueuedAndFetchedCount(_queue);

                // Assert
                Assert.Equal(1, result.EnqueuedCount); // Or use whatever assertion matches your case

                // Cleanup
                _redisDb.KeyDelete(redisQueueKey); // Remove test data

            }
            else
            {
                _storage.UseConnection(connection =>
                {
                    connection.Execute(
                        "insert into JobQueue (JobId, Queue) " +
                        "values (1, @queue);", new { queue = _queue });

                    result = _sut.GetEnqueuedAndFetchedCount(_queue);

                    connection.Execute("delete from JobQueue");
                });
            }
            Assert.Equal(1, result.EnqueuedCount);
        }


        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsEmptyCollection_IfQueueIsEmpty()
        {
            var result = _sut.GetEnqueuedJobIds(_queue, 5, 15);

            Assert.Empty(result);
        }

        [Fact, CleanDatabase(IsolationLevel.ReadUncommitted)]
        public void GetEnqueuedJobIds_ReturnsCorrectResult()
        {
            int[] result = null;
            if (_storageOptions.UseRedisDistributedLock)
            {
                string redisQueueKey = _storage.GetRedisKey($"queue:{_queue}");
                _redisDb.KeyDelete(redisQueueKey); // Ensure it's clean before starting
                
                for (var i = 1; i <= 10; i++)
                {
                    _redisDb.ListRightPush(redisQueueKey, i.ToString());
                }

                // Act
                result = _sut.GetEnqueuedJobIds(_queue, 3, 2).ToArray(); // should return [4, 5]

                Assert.Equal(2, result.Length);
                Assert.Equal(4, result[0]);
                Assert.Equal(5, result[1]);

                _redisDb.KeyDelete(redisQueueKey);
            }
            else
            {
                _storage.UseConnection(connection =>
                {
                    for (var i = 1; i <= 10; i++)
                    {
                        connection.Execute(
                            "insert into JobQueue (JobId, Queue) " +
                            "values (@jobId, @queue);", new { jobId = i, queue = _queue });
                    }

                    result = _sut.GetEnqueuedJobIds(_queue, 3, 2).ToArray();

                    connection.Execute("delete from JobQueue");
                });
                Assert.Equal(2, result.Length);
                Assert.Equal(4, result[0]);
                Assert.Equal(5, result[1]);
            }
        }
    }
}
