using System;
using System.Linq;
using System.Transactions;
using Xunit;

namespace Hangfire.MySql.Tests
{
    public class MySqlStorageTests : IClassFixture<TestDatabaseFixture>
    {
        private readonly MySqlStorageOptions _options;

        public MySqlStorageTests()
        {
            _options = new MySqlStorageOptions
            {
                PrepareSchemaIfNecessary = false,
                RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
                RedisPrefix = "test:hangfire",
                UseRedisDistributedLock = true,
                UseRedisTransactions = true
            };
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MySqlStorage((string)null, new MySqlStorageOptions
                {
                    RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
                    RedisPrefix = "test:hangfire",
                    UseRedisDistributedLock = true,
                    UseRedisTransactions = true
                }));

            Assert.Equal("connectionString", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MySqlStorage("hello", null));

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_CanCreateSqlServerStorage_WithExistingConnection()
        {
            using (var connection = ConnectionUtils.CreateConnection())
            using (var redis = ConnectionUtils.CreateRedisConnection())
            {
                var storage = new MySqlStorage(connection, redis, _options);

                Assert.NotNull(storage);
            }
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            using (var connection = (MySqlStorageConnection)storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact, CleanDatabase]
        public void GetComponents_ReturnsAllNeededComponents()
        {
            var storage = CreateStorage();

            var components = storage.GetComponents();

            var componentTypes = components.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(ExpirationManager), componentTypes);
        }

        [Fact, CleanDatabase(isolationLevel: IsolationLevel.ReadUncommitted)]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            var storage = CreateStorage();
            var api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        private MySqlStorage CreateStorage()
        {
            return new MySqlStorage(
                ConnectionUtils.GetConnectionString(),
                _options);
        }
    }
}
