using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Data;
using System.Globalization;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlFetchedJob : IFetchedJob
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlFetchedJob));

        private readonly MySqlStorage _storage;
        private readonly IDbConnection _connection;
        private readonly MySqlStorageOptions _storageOptions;
        private readonly int _id;
        private bool _removedFromQueue;
        private bool _requeued;
        private bool _disposed;
        private readonly bool _useRedis = false;
        private readonly IDatabase _redisDb;

        public MySqlFetchedJob(
                MySqlStorage storage,
                IDbConnection connection,
                IDatabase redisDb,
                FetchedJob fetchedJob,
                MySqlStorageOptions storageOptions)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (fetchedJob == null) throw new ArgumentNullException("fetchedJob");
            if (storageOptions.UseRedisDistributedLock && !string.IsNullOrEmpty(storageOptions.RedisConnectionString))
            {
                Console.WriteLine(redisDb.ToString());
                if (redisDb == null) throw new ArgumentNullException("connection");
                _useRedis = true;
                _redisDb = redisDb;
            }
            else
            {
                if (connection == null) throw new ArgumentNullException("connection");
            }

            _storage = storage;
            _connection = connection;
            _storageOptions = storageOptions;
            _id = fetchedJob.Id;
            JobId = fetchedJob.JobId.ToString(CultureInfo.InvariantCulture);
            Queue = fetchedJob.Queue;
            FetchedAt = fetchedJob.FetchedAt;
        }

        public string JobId { get; private set; }
        public string Queue { get; private set; }
        public DateTime? FetchedAt { get; }

        private DateTime? GetFetchedValue()
        {
            return JobHelper.DeserializeNullableDateTime(_redisDb.HashGet(_storage.GetRedisKey($"job:{JobId}"), "Fetched"));
        }

        public void Dispose()
        {

            if (_disposed) return;

            if (!_removedFromQueue && !_requeued)
            {
                Logger.TraceFormat("JobId={0} not removed, requeuing...", JobId);
                Requeue();
            }

            Logger.TraceFormat("Dispose {0} JobId={1}", _useRedis ? "Redis" : "MySql", JobId);
            _storage.ReleaseConnection(_connection);
            _disposed = true;
        }

        public void RemoveFromQueue()
        {
            Logger.TraceFormat("RemoveFromQueue {0} JobId={1}", _useRedis ? "Redis" : "MySql", JobId);
            if (_useRedis)
            {
                var jobState = _connection.QueryFirstOrDefault<string>(
                $"select StateName from `{_storageOptions.TablesPrefix}Job` where Id = @jobId",
                new { jobId = JobId });

                if (jobState == "Enqueued")
                {
                    Logger.WarnFormat("Skipping Redis removal. JobId={0} still in Enqueued state.", JobId);
                    return;
                }

                var fetchedAt = GetFetchedValue();
                if (_storage.UseRedisTransactions)
                {
                    var transaction = _redisDb.CreateTransaction();

                    if (fetchedAt?.ToString("s") == FetchedAt?.ToString("s"))
                    {
                        RemoveFromFetchedListAsync(transaction);
                    }
                    transaction.PublishAsync(_storage.SubscriptionChannel, JobId);
                    transaction.Execute();
                }
                else
                {
                    if (fetchedAt?.ToString("s") == FetchedAt?.ToString("s"))
                    {
                        RemoveFromFetchedList(_redisDb);
                    }

                    _redisDb.Publish(_storage.SubscriptionChannel, JobId);
                }
            }
            else
            {
                //todo: unit test
                _connection.Execute(
                        $"delete from `{_storageOptions.TablesPrefix}JobQueue` " +
                        "where Id = @id",
                        new
                        {
                            id = _id
                        });

            }
            _removedFromQueue = true;
        }

        public void Requeue()
        {
            Logger.TraceFormat("Requeue {0} JobId={1}", _useRedis ? "Redis" : "MySql", JobId);
            if (_useRedis)
            {
                var fetchedAt = GetFetchedValue();
                if (_storage.UseRedisTransactions)
                {
                    var transaction = _redisDb.CreateTransaction();
                    transaction.ListRightPushAsync(_storage.GetRedisKey($"queue:{Queue}"), JobId);

                    if (fetchedAt?.ToString("s") == FetchedAt?.ToString("s"))
                    {
                        RemoveFromFetchedList(_redisDb);
                    }

                    transaction.PublishAsync(_storage.SubscriptionChannel, JobId);
                    transaction.Execute();
                }
                else
                {
                    _redisDb.ListRightPush(_storage.GetRedisKey($"queue:{Queue}"), JobId);
                    if (fetchedAt?.ToString("s") == FetchedAt?.ToString("s"))
                    {
                        RemoveFromFetchedList(_redisDb);
                    }

                    _redisDb.Publish(_storage.SubscriptionChannel, JobId);
                }

                //using (var connection = _storage.CreateAndOpenConnection())
                //{
                //    var transaction = new MySqlWriteOnlyTransaction(_storage, _storageOptions);
                //    transaction.AddJobState(JobId, new EnqueuedState(Queue));
                //    transaction.Commit();
                //}
            }
            else
            {
                //todo: unit test
                _connection.Execute(
                    $"update `{_storageOptions.TablesPrefix}JobQueue` set FetchedAt = null " +
                    "where Id = @id",
                    new
                    {
                        id = _id
                    });
            }
            _requeued = true;
        }

        private void RemoveFromFetchedListAsync(IDatabaseAsync databaseAsync)
        {
            databaseAsync.ListRemoveAsync(_storage.GetRedisKey($"queue:{Queue}:dequeued"), JobId, -1);
            databaseAsync.HashDeleteAsync(_storage.GetRedisKey($"job:{JobId}"), ["Fetched", "Checked"]);
        }
        private void RemoveFromFetchedList(IDatabase database)
        {
            database.ListRemove(_storage.GetRedisKey($"queue:{Queue}:dequeued"), JobId, -1);
            database.HashDelete(_storage.GetRedisKey($"job:{JobId}"), ["Fetched", "Checked"]);
        }
    }
}