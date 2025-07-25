﻿using System;
using System.Linq;
using System.Threading;
using Dapper;
using MySqlConnector;
using StackExchange.Redis;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class ExpirationManagerTests : IClassFixture<TestDatabaseFixture>
    {
        private readonly CancellationToken _token;

        public ExpirationManagerTests()
        {
            var cts = new CancellationTokenSource();
            _token = cts.Token;
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new ExpirationManager(null, new MySqlStorageOptions { 
                                                                                        UseRedisDistributedLock = true,
                                                                                        UseRedisTransactions =true,
                                                                                        RedisConnectionString = ConnectionUtils.GetRedisConnectionString(), 
                                                                                        RedisPrefix = "test:hangfire"
            }));
        }

        [Fact, CleanDatabase]
        public void Execute_RemovesOutdatedRecords()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(-1));
                var manager = CreateManager(connection, redis);

                manager.Execute(_token);

                Assert.True(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithNoExpirationTimeSet()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                var entryId = CreateExpirationEntry(connection, null);
                var manager = CreateManager(connection, redis);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_DoesNotRemoveEntries_WithFreshExpirationTime()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                var entryId = CreateExpirationEntry(connection, DateTime.UtcNow.AddMonths(1));
                var manager = CreateManager(connection, redis);

                manager.Execute(_token);

                Assert.False(IsEntryExpired(connection, entryId));
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_AggregatedCounterTable()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                // Arrange
                connection
                    .Execute(
                        "insert into AggregatedCounter (`Key`, Value, ExpireAt) values ('key', 1, @expireAt)",
                        new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection, redis);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from Counter").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_JobTable()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into Job (InvocationData, Arguments, CreatedAt, ExpireAt) " +
                    "values ('', '', UTC_TIMESTAMP(), @expireAt)",
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection, redis);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from Job").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_ListTable()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into List (`Key`, ExpireAt) values ('key', @expireAt)",
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection, redis);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from List").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_SetTable()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                // Arrange
                connection.Execute(
                    "insert into `Set` (`Key`, Score, Value, ExpireAt) values ('key', 0, '', @expireAt)",
                    new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection, redis);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from `Set`").Single());
            }
        }

        [Fact, CleanDatabase]
        public void Execute_Processes_HashTable()
        {
            using (var connection = CreateConnection())
            using (var redis = CreateRedisConnection())
            {
                // Arrange
                const string createSql = @"
insert into Hash (`Key`, Field, Value, ExpireAt) 
values ('key1', 'field', '', @expireAt),
       ('key2', 'field', '', @expireAt)";
                connection.Execute(createSql, new { expireAt = DateTime.UtcNow.AddMonths(-1) });

                var manager = CreateManager(connection, redis);

                // Act
                manager.Execute(_token);

                // Assert
                Assert.Equal(0, connection.Query<int>(@"select count(*) from Hash").Single());
            }
        }

        private static int CreateExpirationEntry(MySqlConnection connection, DateTime? expireAt)
        {
            const string insertSql = @"
delete from AggregatedCounter;
insert into AggregatedCounter (`Key`, Value, ExpireAt)
values ('key', 1, @expireAt);
select last_insert_id() as Id";

            var id = connection.Query(insertSql, new { @expireAt = expireAt }).Single();
            var recordId = (int)id.Id;
            return recordId;
        }

        private static bool IsEntryExpired(MySqlConnection connection, int entryId)
        {
            var count = connection.Query<int>(
                    "select count(*) from AggregatedCounter where Id = @id", new { id = entryId }).Single();
            return count == 0;
        }

        private MySqlConnection CreateConnection()
        {
            return ConnectionUtils.CreateConnection();
        }

        private ConnectionMultiplexer CreateRedisConnection()
        {
            return ConnectionUtils.CreateRedisConnection();
        }

        private ExpirationManager CreateManager(MySqlConnection connection, ConnectionMultiplexer redis)
        {
            var options = new MySqlStorageOptions
            {
                JobExpirationCheckInterval = TimeSpan.Zero,
                UseRedisDistributedLock = true,
                RedisPrefix = "test:hangfire",
                UseRedisTransactions = true,
                RedisConnectionString = ConnectionUtils.GetRedisConnectionString()
            };
            var storage = new MySqlStorage(connection, redis, options);
            return new ExpirationManager(storage, options);
        }
    }
}
