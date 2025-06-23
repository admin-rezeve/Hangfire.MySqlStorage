using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.States;
using Hangfire.Storage;
using MySqlConnector;
using StackExchange.Redis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Hangfire.Storage.JobStorageFeatures;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlJobQueue));

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;
        private readonly IDatabase _redisDb;
        private readonly ConnectionMultiplexer _redis;
        private readonly bool _useRedis;

        public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _storage = storage;
            _options = options;

            if (options.UseRedisDistributedLock && !string.IsNullOrEmpty(options.RedisConnectionString)) { 
                _useRedis = true;
                _redis = _storage.GetRedisConnection();
                _redisDb = _redis.GetDatabase();
            }
        }


        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            if (_useRedis)
            {
                return DequeueFromRedis(queues, cancellationToken);
            }
            else
            {
                return DequeueFromMySql(queues, cancellationToken);
            }
        }

        private IFetchedJob DequeueFromRedis(string[] queues, CancellationToken cancellationToken)
        {
            RedisKey[] redisQueues = queues
                .Select(q => (RedisKey)_storage.GetRedisKey($"queue:{q}"))
                .ToArray();

            if (redisQueues.Length == 0)
                throw new ArgumentException("No Redis queues to dequeue from.");
            
            FetchedJob fetchedJob = null;
            MySqlConnection connection = null;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                foreach (var redisQueue in redisQueues)
                {
                    Logger.TraceFormat("Polling Redis Queue: {0}", redisQueue);

                    var jobId = _redisDb.ListRightPop(redisQueue);
                    if (jobId.HasValue)
                    {
                        Logger.TraceFormat("Dequeued JobId={0} from Queue={1}", jobId, redisQueue);

                        string jobIdStr = jobId.ToString();

                        // Extract queue name from Redis key, safely:
                        string redisKeyPrefix = _storage.GetRedisKey("queue:");
                        string redisQueueStr = redisQueue.ToString();
                        string queue = redisQueueStr.StartsWith(redisKeyPrefix)
                            ? redisQueueStr.Substring(redisKeyPrefix.Length)
                            : redisQueueStr;

                        // Track fetch in Redis
                        _redisDb.ListRightPush(_storage.GetRedisKey($"queue:{queue}:dequeued"), jobIdStr);
                        _redisDb.HashSet(_storage.GetRedisKey($"job:{jobIdStr}"), new HashEntry[]
                        {
                        new("Fetched", DateTime.UtcNow.ToString("o")),
                        new("Checked", DateTime.UtcNow.ToString("o"))
                        });

                        fetchedJob = new FetchedJob()
                        {
                            JobId = int.Parse(jobIdStr),
                            Queue = queue,
                            FetchedAt = DateTime.UtcNow
                        };

                        connection = _storage.CreateAndOpenConnection();
                        return new MySqlFetchedJob(_storage, connection, _redisDb, fetchedJob, _options);
                    }
                }
                // Wait with cancellation support
                cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
            }
        }

        private IFetchedJob DequeueFromMySql(string[] queues, CancellationToken cancellationToken)
        {
            FetchedJob fetchedJob = null;
            MySqlConnection connection = null;

            try
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    connection = _storage.CreateAndOpenConnection();

                    try
                    {
                        using (new MySqlDistributedLock(_storage, "JobQueue", TimeSpan.FromSeconds(30), _options))
                        using (var transaction = connection.BeginTransaction())
                        {
                            string token = Guid.NewGuid().ToString();

                            var parameters = new DynamicParameters();
                            var inClause = string.Join(", ", queues.Select((q, i) =>
                            {
                                var name = $"@q{i}";
                                parameters.Add(name, q);
                                return name;
                            }));
                            parameters.Add("fetchToken", token);

                            // Lock next available job row with SKIP LOCKED to avoid blocking other workers
                            var job = connection.QueryFirstOrDefault<FetchedJob>(
                                $@"
                        SELECT Id, JobId, Queue
                        FROM `{_options.TablesPrefix}JobQueue` FORCE INDEX (IX_HangfireJobQueue_QueueAndFetchedAt)
                        WHERE (FetchedAt IS NULL OR FetchedAt < DATE_ADD(UTC_TIMESTAMP(), INTERVAL -1800 SECOND))
                          AND Queue IN ({inClause})
                        ORDER BY Id
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED;",
                                parameters,
                                transaction: transaction);

                            if (job != null)
                            {
                                // Mark job as fetched
                                connection.Execute(
                                    $@"UPDATE `{_options.TablesPrefix}JobQueue`
                               SET FetchedAt = UTC_TIMESTAMP(), FetchToken = @fetchToken
                               WHERE Id = @Id;",
                                    new { Id = job.Id, fetchToken = token },
                                    transaction);

                                transaction.Commit();
                                fetchedJob = job;
                            }
                            else
                            {
                                transaction.Rollback();
                            }
                        }
                    }
                    catch (MySqlException ex)
                    {
                        Logger.ErrorException(ex.Message, ex);
                        // Release connection on exception before rethrowing
                        _storage.ReleaseConnection(connection);
                        connection = null;
                        throw;
                    }

                    if (fetchedJob == null)
                    {
                        _storage.ReleaseConnection(connection);
                        connection = null;

                        cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (fetchedJob == null);

                // Return job with active connection so caller can manage it
                return new MySqlFetchedJob(_storage, connection, null, fetchedJob, _options);
            }
            finally
            {
                // If we never assigned fetchedJob, make sure connection is closed
                if (fetchedJob == null && connection != null)
                {
                    _storage.ReleaseConnection(connection);
                }
            }
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);
            if(_useRedis)
            {
                // Use Redis to enqueue the job
                var queueKey = _storage.GetRedisKey($"queue:{queue}");

                _redisDb.SetAdd(_storage.GetRedisKey("queues"), queue);

                if (_storage.LifoRedisQueues != null && _storage.LifoRedisQueues.Contains(queue, StringComparer.OrdinalIgnoreCase))
                {
                    _redisDb.ListRightPush(queueKey, jobId);
                }
                else
                {
                    _redisDb.ListLeftPush(queueKey, jobId);
                }

                _redisDb.Publish(_storage.SubscriptionChannel, jobId);
            }
            else
            {
                connection.Execute($"insert into `{_options.TablesPrefix}JobQueue` (JobId, Queue) values (@jobId, @queue)", new { jobId, queue });
            }
        }
        
    }
}