using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Storage;
using MySqlConnector;

namespace Hangfire.MySql.JobQueue
{
    internal class MySqlJobQueue : IPersistentJobQueue
    {
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(MySqlJobQueue));

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;
        public MySqlJobQueue(MySqlStorage storage, MySqlStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");

            _storage = storage;
            _options = options;
        }

        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException("queues");
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", "queues");

            FetchedJob fetchedJob = null;
            MySqlConnection connection = null;

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
                           transaction: transaction
                       );

                        if (job != null)
                        {
                            connection.Execute(
                                $@"UPDATE `{_options.TablesPrefix}JobQueue`
                           SET FetchedAt = UTC_TIMESTAMP(), FetchToken = @fetchToken
                           WHERE Id = @Id;",
                                new
                                {
                                    Id = job.Id,
                                    fetchToken = token
                                },
                                transaction
                            );

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
                    _storage.ReleaseConnection(connection);
                    throw;
                }

                if (fetchedJob == null)
                {
                    _storage.ReleaseConnection(connection);

                    cancellationToken.WaitHandle.WaitOne(_options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (fetchedJob == null);

            return new MySqlFetchedJob(_storage, connection, fetchedJob, _options);
        }

        public void Enqueue(IDbConnection connection, string queue, string jobId)
        {
            Logger.TraceFormat("Enqueue JobId={0} Queue={1}", jobId, queue);
            connection.Execute($"insert into `{_options.TablesPrefix}JobQueue` (JobId, Queue) values (@jobId, @queue)", new {jobId, queue});
        }
    }
}