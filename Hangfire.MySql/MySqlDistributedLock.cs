using Dapper;
using Hangfire.Logging;
using StackExchange.Redis;
using System;
using System.Data;
using System.Threading;

namespace Hangfire.MySql
{
    public class MySqlDistributedLock : IDisposable, IComparable
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlDistributedLock));

        private readonly string _resource;
        private readonly TimeSpan _timeout;
        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;
        private readonly DateTime _start;
        private readonly CancellationToken _cancellationToken;

        private readonly IDatabase _redisDb;
        private ConnectionMultiplexer _redis;
        private readonly string _lockKey;
        private readonly string _lockValue;
        private readonly bool _useRedis;

        private const int DelayBetweenPasses = 100;
        
        private readonly IDbConnection _connection;

        public MySqlDistributedLock(MySqlStorage storage, string resource, TimeSpan timeout, MySqlStorageOptions storageOptions)
        {
            _storageOptions = storageOptions;
            _resource = resource;
            _timeout = timeout;
            _cancellationToken = new CancellationToken();
            _start = DateTime.UtcNow;
            _storage = storage;

            if (_storageOptions.UseRedisDistributedLock && !string.IsNullOrWhiteSpace(_storageOptions.RedisConnectionString))
            {
                _connection = null; // Redis doesn't need a SQL connection
                _useRedis = true;

                _redis = ConnectionMultiplexer.Connect(_storageOptions.RedisConnectionString);
                _redisDb = _redis.GetDatabase();
                _lockKey = $"hangfire:lock:{_resource}";
                _lockValue = Guid.NewGuid().ToString();
            }
            else
            {
                _connection = storage.CreateAndOpenConnection();
                _useRedis = false;
            }
        }

        public MySqlDistributedLock(IDbConnection connection, string resource, TimeSpan timeout, MySqlStorageOptions storageOptions)
            : this(connection, resource, timeout, storageOptions, new CancellationToken())
        {
        }

        public MySqlDistributedLock(
            IDbConnection connection, string resource, TimeSpan timeout, MySqlStorageOptions storageOptions, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("MySqlDistributedLock resource={0}, timeout={1}", resource, timeout);

            _storageOptions = storageOptions;
            _resource = resource;
            _timeout = timeout;
            _connection = connection;
            _cancellationToken = cancellationToken;
            _start = DateTime.UtcNow;

            if (_storageOptions.UseRedisDistributedLock && !string.IsNullOrWhiteSpace(_storageOptions.RedisConnectionString))
            {
                _connection = null; // Redis doesn't need a SQL connection
                _useRedis = true;

                _redis = ConnectionMultiplexer.Connect(_storageOptions.RedisConnectionString);
                _redisDb = _redis.GetDatabase();
                _lockKey = $"hangfire:lock:{_resource}";
                _lockValue = Guid.NewGuid().ToString();
            }
            else
            {
                _useRedis = false;
            }
        }

        public string Resource {
            get { return _resource; }
        }

        private int AcquireLock(string resource, TimeSpan timeout)
        {
            if (_useRedis)
            {
                var start = DateTime.UtcNow;
                while (true)
                {
                    _cancellationToken.ThrowIfCancellationRequested();

                    bool acquired = _redisDb.StringSet(_lockKey, _lockValue, timeout, When.NotExists);

                    if (acquired)
                    {
                        return 1;
                    }

                    if (DateTime.UtcNow - start > timeout)
                    {
                        throw new MySqlDistributedLockException($"Could not acquire Redis lock on resource '{resource}' within timeout {timeout}");
                    }

                    Thread.Sleep(DelayBetweenPasses);
                }
            }
            else
            {
                return
                   _connection
                       .Execute(
                           "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; " +
                           $"INSERT INTO `{_storageOptions.TablesPrefix}DistributedLock` (Resource, CreatedAt) " +
                           "  SELECT @resource, @now " +
                           "  FROM dual " +
                           "  WHERE NOT EXISTS ( " +
                           $"  		SELECT * FROM `{_storageOptions.TablesPrefix}DistributedLock` " +
                           "     	WHERE Resource = @resource " +
                           "       AND CreatedAt > @expired);",
                           new
                           {
                               resource,
                               now = DateTime.UtcNow,
                               expired = DateTime.UtcNow.Add(timeout.Negate())
                           });
            }
        }

        public void Dispose()
        {
            Release();

            if (!_useRedis && _storage != null && _connection != null)
            {
                _storage.ReleaseConnection(_connection);
            }
        }

        internal MySqlDistributedLock Acquire()
        {
            Logger.TraceFormat("Acquiring {0} lock for resource={1}, timeout={2}", _useRedis ? "Redis" : "MySQL", _resource, _timeout);

            int insertedObjectCount;
            do
            {
                _cancellationToken.ThrowIfCancellationRequested();

                insertedObjectCount = AcquireLock(_resource, _timeout);

                if (ContinueCondition(insertedObjectCount))
                {
                    _cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    _cancellationToken.ThrowIfCancellationRequested();
                }
            } while (ContinueCondition(insertedObjectCount));

            if (insertedObjectCount == 0)
            {
                throw new MySqlDistributedLockException("cannot acquire lock");
            }
            return this;
        }

        private bool ContinueCondition(int insertedObjectCount)
        {
            return insertedObjectCount == 0 && _start.Add(_timeout) > DateTime.UtcNow;
        }

        internal void Release()
        {
            Logger.TraceFormat("Releasing {0} lock for resource={1}", _useRedis ? "Redis" : "MySQL", _resource);

            if (_useRedis)
            {
                if (!string.IsNullOrEmpty(_lockKey) && !string.IsNullOrEmpty(_lockValue) && _redisDb != null)
                {
                    const string luaScript = @"
                        if redis.call('get', KEYS[1]) == ARGV[1] then
                            return redis.call('del', KEYS[1])
                        else
                            return 0
                        end";

                    try
                    {
                        _redisDb.ScriptEvaluate(luaScript,
                            new RedisKey[] { _lockKey },
                            new RedisValue[] { _lockValue });
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorException($"Failed to release Redis lock for {_lockKey}", ex);
                    }
                }
                else
                {
                    Logger.WarnFormat("Redis lock release skipped: lockKey or redisDb not set for resource={0}", _resource);
                }
            }
            else
            {
                try
                {
                    _connection?.Execute(
                        $"DELETE FROM `{_storageOptions.TablesPrefix}DistributedLock` " +
                        "WHERE Resource = @resource",
                        new { resource = _resource });
                }
                catch (Exception ex)
                {
                    Logger.ErrorException($"Failed to release MySQL lock for {_resource}", ex);
                }
            }
        }

        public int CompareTo(object obj)
        {
            if (obj == null) return 1;

            var mySqlDistributedLock = obj as MySqlDistributedLock;
            if (mySqlDistributedLock != null)
                return string.Compare(this.Resource, mySqlDistributedLock.Resource, StringComparison.OrdinalIgnoreCase);
            
            throw new ArgumentException("Object is not a mySqlDistributedLock");
        }
    }
}