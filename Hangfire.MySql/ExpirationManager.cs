using System;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.Server;
using MySqlConnector;
using StackExchange.Redis;

namespace Hangfire.MySql
{
    internal class ExpirationManager : IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(ExpirationManager));

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(60);
        private const string DistributedLockKey = "expirationmanager";
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromSeconds(1);
        private const int NumberOfRecordsInSinglePass = 1000;

        private readonly string[] _processedTables;

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _storageOptions;

        // redis
        private readonly IDatabase _redisDb;
        private readonly bool _useRedis = false;

        public ExpirationManager(MySqlStorage storage, MySqlStorageOptions storageOptions)
        {
            _storage = storage ?? throw new ArgumentNullException("storage");
            _storageOptions = storageOptions ?? throw new ArgumentNullException(nameof(storageOptions));

            if(_storageOptions.UseRedisDistributedLock && !string.IsNullOrEmpty(_storageOptions.RedisConnectionString))
            {
                _useRedis = true;
                _redisDb = _storage.GetRedisConnection().GetDatabase();
            }


            _processedTables = new[]
            {
                $"{storageOptions.TablesPrefix}AggregatedCounter",
                $"{storageOptions.TablesPrefix}Job",
                $"{storageOptions.TablesPrefix}List",
                $"{storageOptions.TablesPrefix}Set",
                $"{storageOptions.TablesPrefix}Hash",
            };
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in _processedTables)
            {
                Logger.DebugFormat("Removing outdated records from table '{0}'...", table);

                int removedCount = 0;

                do
                {
                    _storage.UseConnection(connection =>
                    {
                        var redisDb = _storage.GetRedisConnection().GetDatabase();
                        try
                        {
                            // Try to acquire Redis lock manually
                            if (redisDb.LockTake(_storage.GetRedisKey(DistributedLockKey), Environment.MachineName, DefaultLockTimeout))
                            {
                                try
                                {
                                    var totalCount = connection.ExecuteScalar<long>(
                                        $"SELECT count(*) FROM `{table}` WHERE ExpireAt IS NOT NULL AND ExpireAt < @now",
                                        new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                                    if(totalCount > 0)
                                    {
                                        Logger.DebugFormat("Found {0} outdated records in '{1}' table.", totalCount, table);
                                        removedCount = connection.Execute(
                                        $"DELETE FROM `{table}` WHERE ExpireAt IS NOT NULL AND ExpireAt < @now LIMIT @count",
                                        new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                                    }
                                }
                                finally
                                {
                                    redisDb.LockRelease(_storage.GetRedisKey(DistributedLockKey), Environment.MachineName);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Logger.Error($"Failed deleting expired from {table}: {ex.Message}");
                        }

                        //try
                        //{
                        //    Logger.DebugFormat("delete from `{0}` where ExpireAt < @now limit @count;", table);

                        //    using (
                        //        new MySqlDistributedLock(
                        //            connection,
                        //            DistributedLockKey,
                        //            DefaultLockTimeout,
                        //            _storageOptions,
                        //            cancellationToken).Acquire())
                        //    {
                        //        removedCount = connection.Execute(
                        //            String.Format(
                        //                "delete from `{0}` where ExpireAt < @now limit @count;", table),
                        //            new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass });
                        //    }

                        //    Logger.DebugFormat("removed records count={0}", removedCount);
                        //}
                        //catch (MySqlException ex)
                        //{
                        //    Logger.Error(ex.ToString());
                        //}
                    });

                    if (removedCount > 0)
                    {
                        Logger.Trace(String.Format("Removed {0} outdated record(s) from '{1}' table.", removedCount,
                            table));

                        cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                } while (removedCount > 0);
            }

            cancellationToken.WaitHandle.WaitOne(_storageOptions.JobExpirationCheckInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }
    }
}
