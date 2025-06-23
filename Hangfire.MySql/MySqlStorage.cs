using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.MySql.JobQueue;
using Hangfire.MySql.Monitoring;
using Hangfire.MySql.Redis;
using Hangfire.Server;
using Hangfire.Storage;
using MySqlConnector;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Transactions;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Hangfire.MySql
{
    public class MySqlStorage : JobStorage, IDisposable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlStorage));

        private readonly string _connectionString;
        private readonly MySqlConnection _existingConnection;
        private readonly ConnectionMultiplexer _existingRedisConnection;
        private readonly MySqlStorageOptions _storageOptions;
        
        private readonly Lazy<ConnectionMultiplexer> _lazyRedisConnection;
        private readonly IDatabase _redisDb;
        private readonly RedisSubscription _subscription;
        private bool _ownsRedisConnection = false;
        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public MySqlStorage(string connectionString, MySqlStorageOptions storageOptions)
        {
            if (connectionString == null) throw new ArgumentNullException("connectionString");
            if (storageOptions == null) throw new ArgumentNullException("storageOptions");

            if (IsConnectionString(connectionString))
            {
                _connectionString = connectionString;
            }
            else
            {
                throw new ArgumentException(
                    string.Format(
                        "Could not find connection string with name '{0}' in application config file",
                        connectionString));
            }
            _storageOptions = storageOptions;

            if (storageOptions.PrepareSchemaIfNecessary)
            {
                using (var connection = CreateAndOpenConnection())
                {
                    MySqlObjectsInstaller.Install(connection, storageOptions.TablesPrefix);
                }
            }

            if(_storageOptions.UseRedisDistributedLock && !string.IsNullOrEmpty(_storageOptions.RedisConnectionString))
            {
                _lazyRedisConnection = new Lazy<ConnectionMultiplexer>(() =>
                {
                    _ownsRedisConnection = true;
                    return ConnectionMultiplexer.Connect(_storageOptions.RedisConnectionString);
                });
                _subscription = new RedisSubscription(this, _lazyRedisConnection.Value.GetSubscriber());
                _redisDb = _lazyRedisConnection.Value.GetDatabase();
                
            }

            InitializeQueueProviders();
        }

        public MySqlStorage(MySqlConnection existingConnection, ConnectionMultiplexer existingRedisConnection, MySqlStorageOptions storageOptions)
        {
            if (existingConnection == null) throw new ArgumentNullException("existingConnection");

            _existingConnection = existingConnection;
            _storageOptions = storageOptions;

            if (_storageOptions.UseRedisDistributedLock && !string.IsNullOrEmpty(_storageOptions.RedisConnectionString))
            {
                _lazyRedisConnection = new Lazy<ConnectionMultiplexer>(() =>
                {
                    return existingRedisConnection;
                });
                _subscription = new RedisSubscription(this, _lazyRedisConnection.Value.GetSubscriber());
            }

            InitializeQueueProviders();
        }

        private string ApplyAllowUserVariablesProperty(string connectionString)
        {
            if (connectionString.ToLower().Contains("allow user variables"))
            {
                return connectionString;
            }

            return connectionString + ";Allow User Variables=True;";
        }

        private void InitializeQueueProviders()
        {
            QueueProviders =
                new PersistentJobQueueProviderCollection(
                    new MySqlJobQueueProvider(this, _storageOptions));
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new ExpirationManager(this, _storageOptions);
            yield return new CountersAggregator(this, _storageOptions);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.InfoFormat("    Queue poll interval: {0}.", _storageOptions.QueuePollInterval);
        }

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var parts = _connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(x => new { Key = x[0].Trim(), Value = x[1].Trim() })
                    .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

                var builder = new StringBuilder();

                foreach (var alias in new[] { "Data Source", "Server", "Address", "Addr", "Network Address" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                if (builder.Length != 0) builder.Append("@");

                foreach (var alias in new[] { "Database", "Initial Catalog" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                return builder.Length != 0
                    ? String.Format("Server: {0}", builder)
                    : canNotParseMessage;
            }
            catch (Exception ex)
            {
                Logger.ErrorException(ex.Message, ex);
                return canNotParseMessage;
            }
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new MySqlMonitoringApi(this, _storageOptions);
        }

        public override IStorageConnection GetConnection()
        {
            return new MySqlStorageConnection(this, _storageOptions);
        }

        public ConnectionMultiplexer GetRedisConnection()
        {
            if (_lazyRedisConnection?.IsValueCreated == true)
            {
                return _lazyRedisConnection.Value;
            }

            throw new InvalidOperationException("Redis connection has not been initialized.");
        }

        private bool IsConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString.Contains(";");
        }

        internal void UseTransaction([InstantHandle] Action<MySqlConnection> action)
        {
            UseTransaction(connection =>
            {
                action(connection);
                return true;
            }, null);
        }

        internal T UseTransaction<T>(
            [InstantHandle] Func<MySqlConnection, T> func, IsolationLevel? isolationLevel)
        {
            using (var tScope = new TransactionScope(TransactionScopeOption.Required,
                new TransactionOptions
                {
                    IsolationLevel = isolationLevel ?? IsolationLevel.ReadCommitted
                },
                TransactionScopeAsyncFlowOption.Enabled))
            {

                MySqlConnection connection = null;

                try
                {
                    connection = CreateAndOpenConnection();
                    T result = func(connection);
                    tScope.Complete();

                    return result;
                }
                finally
                {
                    ReleaseConnection(connection);
                }
            }
        }

        internal void UseConnection([InstantHandle] Action<MySqlConnection> action)
        {
            UseConnection(connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>([InstantHandle] Func<MySqlConnection, T> func)
        {
            MySqlConnection connection = null;

            try
            {
                connection = CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                ReleaseConnection(connection);
            }
        }

        internal MySqlConnection CreateAndOpenConnection()
        {
            if (_existingConnection != null)
            {
                return _existingConnection;
            }

            var connection = new MySqlConnection(_connectionString);
            connection.Open();

            return connection;
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection != null && !ReferenceEquals(connection, _existingConnection))
            {
                connection.Dispose();
            }
        }

        internal void ReleaseRedisConnection(ConnectionMultiplexer connection)
        {
            if (connection != null && !ReferenceEquals(connection, _existingRedisConnection))
            {
                connection.Dispose();
            }
        }

        public void Dispose()
        {
            Logger.Debug("Disposing MySqlStorage...");

            _subscription?.Dispose();

            if (_ownsRedisConnection && _lazyRedisConnection?.IsValueCreated == true)
            {
                Logger.Debug("Disposing internal Redis connection.");
                _lazyRedisConnection.Value.Dispose();
            }

            Logger.Debug("MySqlStorage disposed.");
        }

        public string GetRedisKey(string key)
        {
            return _storageOptions.UseRedisDistributedLock ? $"{_storageOptions.RedisPrefix}:{key}" : string.Empty ;
        }

        internal RedisChannel SubscriptionChannel => _subscription.Channel;

        internal bool UseRedisTransactions => _storageOptions.UseRedisTransactions;
        internal string[] LifoRedisQueues => _storageOptions.LifoRedisQueues;

    }
}
