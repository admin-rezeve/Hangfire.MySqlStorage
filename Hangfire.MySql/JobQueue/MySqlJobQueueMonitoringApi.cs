using Dapper;
using Hangfire.Logging;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlJobQueueMonitoringApi));
        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);
        private readonly object _cacheLock = new object();
        private List<string> _queuesCache = new List<string>();
        private DateTime _cacheUpdated;

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;

        // redis
        private readonly bool _useRedis;
        private readonly IDatabase _redisDb;
        private readonly ConnectionMultiplexer _redis;

        public MySqlJobQueueMonitoringApi(MySqlStorage storage, MySqlStorageOptions storageOptions)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            _storage = storage;
            _storageOptions = storageOptions;
            if (_storageOptions.UseRedisDistributedLock && !string.IsNullOrEmpty(_storageOptions.RedisConnectionString))
            {
                _useRedis = true;
                _redis = _storage.GetRedisConnection();
                _redisDb = _redis.GetDatabase();
            }
        }

        public IEnumerable<string> GetQueues()
        {
            lock (_cacheLock)
            {
                if (_queuesCache.Count == 0 || _cacheUpdated.Add(QueuesCacheTimeout) < DateTime.UtcNow)
                {
                    if (_useRedis)
                    {
                        var redisDb = _storage.GetRedisConnection().GetDatabase();
                        var redisQueues = redisDb.SetMembers(_storage.GetRedisKey("queues"))
                            .Select(q => (string)q).ToList();
                        _queuesCache = redisQueues;
                    }
                    else {
                        var result = _storage.UseConnection(connection =>
                        {
                            return connection.Query($"select distinct(Queue) from `{_storageOptions.TablesPrefix}JobQueue`").Select(x => (string)x.Queue).ToList();
                        });
                        _queuesCache = result;
                    }
                    _cacheUpdated = DateTime.UtcNow;
                }

                return _queuesCache.ToList();
            }
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            if (_useRedis)
            {
                var redisDb = _storage.GetRedisConnection().GetDatabase();
                var redisList = _storage.GetRedisKey($"queue:{queue}");
                var jobIds = redisDb.ListRange(redisList, from, from + perPage - 1);
                return jobIds.Select(j => int.TryParse(j.ToString(), out var id) ? id : 0).Where(id => id > 0);
            }

            string sqlQuery = $@"
SET @rank=0;
select r.JobId from (
  select jq.JobId, @rank := @rank+1 AS 'rank' 
  from `{_storageOptions.TablesPrefix}JobQueue` jq
  where jq.Queue = @queue
  order by jq.Id
) as r
where r.rank between @start and @end;";

            return _storage.UseConnection(connection =>
                connection.Query<int>(
                    sqlQuery,
                    new {queue = queue, start = @from + 1, end = @from + perPage}));
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return Enumerable.Empty<int>();
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            if (_useRedis)
            {
                var redisDb = _storage.GetRedisConnection().GetDatabase();
                var count = redisDb.ListLength(_storage.GetRedisKey($"queue:{queue}"));
                Logger.DebugFormat("Redis queue length for {0}: {1}", queue, count);
                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = (int)count,
                };
            }

            return _storage.UseConnection(connection =>
            {
                var result = 
                    connection.Query<int>(
                        $"select count(Id) from `{_storageOptions.TablesPrefix}JobQueue` where Queue = @queue", new { queue = queue }).Single();

                return new EnqueuedAndFetchedCountDto
                {
                    EnqueuedCount = result,
                };
            });
        }
    }
}