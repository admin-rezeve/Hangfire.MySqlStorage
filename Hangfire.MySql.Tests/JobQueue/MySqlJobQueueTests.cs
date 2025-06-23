using Dapper;
using Hangfire.MySql.JobQueue;
using Moq;
using MySqlConnector;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading;
using Xunit;
using static Hangfire.Storage.JobStorageFeatures;

namespace Hangfire.MySql.Tests.JobQueue
{
    public class MySqlJobQueueTests : IClassFixture<TestDatabaseFixture>, IDisposable
    {
        private static readonly string[] DefaultQueues = { "default" };
        private readonly MySqlStorage _storage;
        private readonly MySqlConnection _connection;
        private readonly ConnectionMultiplexer _redis;
        private readonly IDatabase _redisDb;
        private readonly MySqlStorageOptions _storageOptions = new MySqlStorageOptions { 
            RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
            RedisPrefix = "test:hangfire",
            UseRedisDistributedLock = true,
            UseRedisTransactions = true
        };

        public MySqlJobQueueTests()
        {
            _connection = ConnectionUtils.CreateConnection();

            _redis  = ConnectionUtils.CreateRedisConnection();
            _redisDb = _redis.GetDatabase();
            _storage = new MySqlStorage(_connection, _redis, _storageOptions);
        }

        public void Dispose()
        {
            _connection.Dispose();
            _storage.Dispose();
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MySqlJobQueue(null, new MySqlStorageOptions
                {
                    RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
                    RedisPrefix = "test:hangfire",
                    UseRedisDistributedLock = true,
                    UseRedisTransactions = true
                }));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MySqlJobQueue(_storage, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            _storage.UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection, _redis);

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            _storage.UseConnection(connection =>
            {
                var queue = CreateJobQueue(connection, _redis);

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            _storage.UseConnection(connection =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(connection, _redis);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            _storage.UseConnection(connection =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(connection, _redis);

                Assert.Throws<OperationCanceledException>(
                    () => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            if(_storageOptions.UseRedisDistributedLock)
            {
                // Arrange
                string queueName = "default";
                string redisQueueKey = _storage.GetRedisKey($"queue:{queueName}");
                string jobId = "1"; // Simulate JobId
                string jobRedisKey = _storage.GetRedisKey($"job:{jobId}");

                // Clean up Redis first
                _redisDb.KeyDelete(redisQueueKey);
                _redisDb.KeyDelete(jobRedisKey);

                // Push a job ID into the Redis queue (simulating enqueue)
                _redisDb.ListRightPush(redisQueueKey, jobId);

                // Optionally, store job data in Redis if needed
                _redisDb.HashSet(jobRedisKey, new HashEntry[]
                        {
                    new("Fetched", DateTime.UtcNow.ToString("o")),
                    new("Checked", DateTime.UtcNow.ToString("o"))
                        });

                var queue = new MySqlJobQueue(_storage, _storageOptions);

                // Act
                var payload = (MySqlFetchedJob)queue.Dequeue(
                    new[] { queueName },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(payload);

                payload.RemoveFromQueue(); // This should remove jobId from Redis queue (if still present)

                // Assert
                var remainingJobs = _redisDb.ListLength(redisQueueKey);
                Assert.Equal(0, remainingJobs); // Queue should be empty

                // Optionally clean up job data
                _redisDb.KeyDelete(redisQueueKey);
                _redisDb.KeyDelete(jobRedisKey);
            }
            else
            {
                const string arrangeSql = @"
                            insert into JobQueue (JobId, Queue)
                            values (@jobId, @queue);
                            select last_insert_id() as Id;";

                // Arrange
                _storage.UseConnection(connection =>
                {
                    var id = (int)connection.Query(
                        arrangeSql,
                        new { jobId = 1, queue = "default" }).Single().Id;
                    var queue = CreateJobQueue(connection, _redis);

                    // Act
                    var payload = (MySqlFetchedJob)queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken());

                    // Assert
                    Assert.Equal("1", payload.JobId);
                    Assert.Equal("default", payload.Queue);
                });
            }
                
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldDeleteAJob()
        {
            if (_storageOptions.UseRedisDistributedLock)
            {
                const string arrangeSql = @"
                    delete from JobQueue;
                    delete from Job;
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                    select last_insert_id();";

                // Arrange
                

                _storage.UseConnection(connection =>
                {
                    string queueName = "default";
                    string redisQueueKey = _storage.GetRedisKey($"queue:{queueName}");                   
                    // Clean up Redis first
                    _redisDb.KeyDelete(redisQueueKey);


                    var jobId = connection.ExecuteScalar<long>(
                        arrangeSql,
                        new { invocationData = "", arguments = ""});

                    string jobRedisKey = _storage.GetRedisKey($"job:{jobId}");
                    _redisDb.KeyDelete(jobRedisKey);

                    // Push a job ID into the Redis queue (simulating enqueue)
                    _redisDb.ListRightPush(redisQueueKey, jobId);

                    // Optionally, store job data in Redis if needed
                    _redisDb.HashSet(jobRedisKey, new HashEntry[]
                            {
                                new("Fetched", DateTime.UtcNow.ToString("o")),
                                new("Checked", DateTime.UtcNow.ToString("o"))
                            });

                    var queue = new MySqlJobQueue(_storage, _storageOptions); // or your equivalent queue class

                    // Act
                    var payload = queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken());

                    // Assert
                    Assert.NotNull(payload);

                    payload.RemoveFromQueue(); // This should remove jobId from Redis queue (if still present)

                    // Assert
                    var remainingJobs = _redisDb.ListLength(redisQueueKey);
                    Assert.Equal(0, remainingJobs); // Queue should be empty

                    // Optionally clean up job data
                    _redisDb.KeyDelete(redisQueueKey);
                    _redisDb.KeyDelete(jobRedisKey);
                });

               
            }
            else
            {
                const string arrangeSql = @"
                    delete from JobQueue;
                    delete from Job;
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                    insert into JobQueue (JobId, Queue)
                    values (last_insert_id(), @queue)";

                // Arrange
                _storage.UseConnection(connection =>
                {
                    connection.Execute(
                        arrangeSql,
                        new { invocationData = "", arguments = "", queue = "default" });
                    var queue = CreateJobQueue(connection, _redis);

                    // Act
                    var payload = queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken());

                    payload.RemoveFromQueue();

                    // Assert
                    Assert.NotNull(payload);

                    var jobInQueue = connection.Query("select * from JobQueue").SingleOrDefault();
                    Assert.Null(jobInQueue);
                });
            }
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            if (_storageOptions.UseRedisDistributedLock)
            {
                const string arrangeSql = @"
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                     select last_insert_id();";

                // Arrange


                _storage.UseConnection(connection =>
                {
                    string queueName = "default";
                    string redisQueueKey = _storage.GetRedisKey($"queue:{queueName}");
                    // Clean up Redis first
                    _redisDb.KeyDelete(redisQueueKey);


                    var jobId = connection.ExecuteScalar<string>(
                        arrangeSql,
                        new { invocationData = "", arguments = "" });

                    string jobRedisKey = _storage.GetRedisKey($"job:{jobId}");
                    _redisDb.KeyDelete(jobRedisKey);

                    // Push a job ID into the Redis queue (simulating enqueue)
                    _redisDb.ListRightPush(redisQueueKey, jobId);

                    // Optionally, store job data in Redis if needed
                    _redisDb.HashSet(jobRedisKey, new HashEntry[]
                            {
                                new("Fetched", DateTime.UtcNow.AddDays(-1).ToString("o")),
                                new("Checked", DateTime.UtcNow.ToString("o"))
                            });

                    var queue = new MySqlJobQueue(_storage, _storageOptions); // or your equivalent queue class

                    // Act
                    var payload = queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken());

                    // Assert
                    Assert.NotEmpty(payload.JobId);

                    _redisDb.KeyDelete(redisQueueKey);
                    _redisDb.KeyDelete(jobRedisKey);
                });
            }
            else
            {
                const string arrangeSql = @"
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                    insert into JobQueue (JobId, Queue, FetchedAt)
                    values (last_insert_id(), @queue, @fetchedAt)";

                // Arrange
                _storage.UseConnection(connection =>
                {
                    connection.Execute(
                        arrangeSql,
                        new
                        {
                            queue = "default",
                            fetchedAt = DateTime.UtcNow.AddDays(-1),
                            invocationData = "",
                            arguments = ""
                        });
                    var queue = CreateJobQueue(connection, _redis);

                    // Act
                    var payload = queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken());

                    // Assert
                    Assert.NotEmpty(payload.JobId);
                });
            }
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            if (_storageOptions.UseRedisDistributedLock)
            {
                const string arrangeSql = @"
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                     select last_insert_id();";

                // Arrange

                _storage.UseConnection(connection =>
                {
                    connection.Execute("delete from Job;");

                    var jobId1 = connection.ExecuteScalar<string>(
                       arrangeSql,
                       new { invocationData = "", arguments = "" });

                    var jobId2 = connection.ExecuteScalar<string>(
                       arrangeSql,
                       new { invocationData = "", arguments = "" });

                    string queueName = "default";
                    string redisQueueKey = _storage.GetRedisKey($"queue:{queueName}");
                    // Clean up Redis first
                    _redisDb.KeyDelete(redisQueueKey);

                    string jobRedisKey1 = _storage.GetRedisKey($"job:{jobId1}");
                    string jobRedisKey2 = _storage.GetRedisKey($"job:{jobId2}");
                    _redisDb.KeyDelete(jobRedisKey1);
                    _redisDb.KeyDelete(jobRedisKey2);

                    // Push a job ID into the Redis queue (simulating enqueue)
                    _redisDb.ListRightPush(redisQueueKey, jobId1);
                    _redisDb.ListRightPush(redisQueueKey, jobId2);

                    // Optionally, store job data in Redis if needed
                    _redisDb.HashSet(jobRedisKey1, new HashEntry[]
                            {
                                //new("Fetched", DateTime.UtcNow.ToString("o")),
                                //new("Checked", DateTime.UtcNow.ToString("o"))
                            });

                    _redisDb.HashSet(jobRedisKey2, new HashEntry[]
                            {
                                //new("Fetched", DateTime.UtcNow.ToString("o")),
                                //new("Checked", DateTime.UtcNow.ToString("o"))
                            });

                    var queue = new MySqlJobQueue(_storage, _storageOptions);

                    // Act
                    var payload = queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken());

                    // Assert
                    string otherJobKey = payload.JobId == jobId1 ? jobRedisKey2 : jobRedisKey1;

                    var fetchedAtOfOtherJob = _redisDb.HashGet(otherJobKey, "Fetched");

                    Assert.True(fetchedAtOfOtherJob.IsNullOrEmpty, "FetchedAt should be null for the other job");

                    _redisDb.KeyDelete(redisQueueKey);
                    _redisDb.KeyDelete(jobRedisKey1);
                    _redisDb.KeyDelete(jobRedisKey2);
                });
            }
            else
            {
                const string arrangeSql = @"
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                    insert into JobQueue (JobId, Queue)
                    values (last_insert_id(), @queue)";

                // Arrange
                _storage.UseConnection(connection =>
                {
                    connection.Execute("delete from JobQueue; delete from Job;");

                    connection.Execute(
                        arrangeSql,
                        new[]
                        {
                        new { queue = "default", invocationData = "", arguments = "" },
                        new { queue = "default", invocationData = "", arguments = "" }
                        });
                    var queue = CreateJobQueue(connection, _redis);

                    // Act
                    var payload = queue.Dequeue(
                        DefaultQueues,
                        CreateTimingOutCancellationToken());

                    // Assert
                    var otherJobFetchedAt = connection.Query<DateTime?>(
                        "select FetchedAt from JobQueue where JobId != @id",
                        new { id = payload.JobId }).Single();

                    Assert.Null(otherJobFetchedAt);
                });
            }
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            if (_storageOptions.UseRedisDistributedLock)
            {
                const string arrangeSql = @"
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                     select last_insert_id();";

                // Arrange

                _storage.UseConnection(connection =>
                {
                    connection.Execute("delete from Job;");

                    var jobId = connection.ExecuteScalar<string>(
                       arrangeSql,
                       new { invocationData = "", arguments = "" });

                    string queueName = "critical";
                    string redisQueueKey = _storage.GetRedisKey($"queue:{queueName}");
                    // Clean up Redis first
                    _redisDb.KeyDelete(redisQueueKey);

                    string jobRedisKey = _storage.GetRedisKey($"job:{jobId}");
                    _redisDb.KeyDelete(jobRedisKey);

                    // Push a job ID into the Redis queue (simulating enqueue)
                    _redisDb.ListRightPush(redisQueueKey, jobId);

                    // Optionally, store job data in Redis if needed
                    _redisDb.HashSet(jobRedisKey, new HashEntry[]
                            {
                                new("Fetched", DateTime.UtcNow.ToString("o")),
                                new("Checked", DateTime.UtcNow.ToString("o"))
                            });

                    var queue = new MySqlJobQueue(_storage, _storageOptions); // or your equivalent queue class

                    // Act
                    Assert.Throws<OperationCanceledException>(
                        () => queue.Dequeue(
                            DefaultQueues,
                            CreateTimingOutCancellationToken()));


                    _redisDb.KeyDelete(redisQueueKey);
                    _redisDb.KeyDelete(jobRedisKey);
                });
            }
            else
            {
                const string arrangeSql = @"
                        insert into Job (InvocationData, Arguments, CreatedAt)
                        values (@invocationData, @arguments, UTC_TIMESTAMP());
                        insert into JobQueue (JobId, Queue)
                        values (last_insert_id(), @queue)";

                _storage.UseConnection(connection =>
                {
                    connection.Execute("delete from JobQueue; delete from Job;");
                    var queue = CreateJobQueue(connection, _redis);

                    connection.Execute(
                        arrangeSql,
                        new { queue = "critical", invocationData = "", arguments = "" });

                    Assert.Throws<OperationCanceledException>(
                        () => queue.Dequeue(
                            DefaultQueues,
                            CreateTimingOutCancellationToken()));
                });
            }
        }

        [Fact, CleanDatabase]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueues()
        {
            if (_storageOptions.UseRedisDistributedLock)
            {
                const string arrangeSql = @"
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                     select last_insert_id();";

                // Arrange
                var queues = new[] { "critical", "default" };

                _storage.UseConnection(connection =>
                {

                    var jobId1 = connection.ExecuteScalar<string>(
                       arrangeSql,
                       new { invocationData = "", arguments = "" });

                    var jobId2 = connection.ExecuteScalar<string>(
                       arrangeSql,
                       new { invocationData = "", arguments = "" });

                    string redisQueueKey1 = _storage.GetRedisKey($"queue:{queues[0]}");
                    string redisQueueKey2 = _storage.GetRedisKey($"queue:{queues[1]}");
                    string jobRedisKey1 = _storage.GetRedisKey($"job:{jobId1}");
                    string jobRedisKey2 = _storage.GetRedisKey($"job:{jobId2}");
                    _redisDb.KeyDelete(jobRedisKey1);
                    _redisDb.KeyDelete(jobRedisKey2);

                    // Push a job ID into the Redis queue (simulating enqueue)
                    _redisDb.ListRightPush(redisQueueKey1, jobId1);
                    _redisDb.ListRightPush(redisQueueKey2, jobId2);

                    // Optionally, store job data in Redis if needed
                    _redisDb.HashSet(jobRedisKey1, new HashEntry[]
                            {
                                new("Fetched", DateTime.UtcNow.ToString("o")),
                                new("Checked", DateTime.UtcNow.ToString("o"))
                            });

                    _redisDb.HashSet(jobRedisKey2, new HashEntry[]
                            {
                                new("Fetched", DateTime.UtcNow.ToString("o")),
                                new("Checked", DateTime.UtcNow.ToString("o"))
                            });

                    var queue = new MySqlJobQueue(_storage, _storageOptions);

                    var firstJob = (MySqlFetchedJob)queue.Dequeue(
                        queues,
                        CreateTimingOutCancellationToken());
                    Assert.NotNull(firstJob.JobId);
                    Assert.Contains(firstJob.Queue, queues);

                    var secondJob = (MySqlFetchedJob)queue.Dequeue(
                        queues,
                        CreateTimingOutCancellationToken());
                    Assert.NotNull(secondJob.JobId);
                    Assert.Contains(secondJob.Queue, queues);

                    _redisDb.KeyDelete(redisQueueKey1);
                    _redisDb.KeyDelete(redisQueueKey2);
                    _redisDb.KeyDelete(jobRedisKey1);
                    _redisDb.KeyDelete(jobRedisKey2);
                });
            }
            else
            {
                const string arrangeSql = @"
                    insert into Job (InvocationData, Arguments, CreatedAt)
                    values (@invocationData, @arguments, UTC_TIMESTAMP());
                    insert into JobQueue (JobId, Queue)
                    values (last_insert_id(), @queue)";
                var queues = new[] { "critical", "default" };

                _storage.UseConnection(connection =>
                {
                    connection.Execute(
                        arrangeSql,
                        new[]
                        {
                        new { queue = "default", invocationData = "", arguments = "" },
                        new { queue = "critical", invocationData = "", arguments = "" }
                        });

                    var queue = CreateJobQueue(connection, _redis);

                    var firstJob = (MySqlFetchedJob)queue.Dequeue(
                        queues,
                        CreateTimingOutCancellationToken());
                    Assert.NotNull(firstJob.JobId);
                    Assert.Contains(firstJob.Queue, queues);

                    var secondJob = (MySqlFetchedJob)queue.Dequeue(
                        queues,
                        CreateTimingOutCancellationToken());
                    Assert.NotNull(secondJob.JobId);
                    Assert.Contains(secondJob.Queue, queues);
                });
            }
        }

        [Fact, CleanDatabase]
        public void Enqueue_AddsAJobToTheQueue()
        {
            if (_storageOptions.UseRedisDistributedLock)
            {
                // Arrange
                string queueName = "default";
                string jobId = "1";
                string redisQueueKey = _storage.GetRedisKey($"queue:{queueName}");
                string jobKey = _storage.GetRedisKey($"job:{jobId}");

                // Cleanup
                _redisDb.KeyDelete(redisQueueKey);
                _redisDb.KeyDelete(jobKey);

                _storage.UseConnection(connection =>
                {
                    var queue = new MySqlJobQueue(_storage, _storageOptions);
                    queue.Enqueue(connection, "default", "1");

                    // Assert
                    var redisQueueItems = _redisDb.ListRange(redisQueueKey).ToArray();
                    Assert.Contains(jobId, redisQueueItems.Select(x => x.ToString()));

                    // Make sure FetchedAt is not set
                    var fetchedAt = _redisDb.HashGet(jobKey, "Fetched");
                    Assert.True(fetchedAt.IsNullOrEmpty, "Fetched should be null or missing");

                    // Cleanup
                    _redisDb.KeyDelete(redisQueueKey);
                    _redisDb.KeyDelete(jobKey);
                });
            }
            else
            {
                _storage.UseConnection(connection =>
                {
                    connection.Execute("delete from JobQueue");

                    var queue = CreateJobQueue(connection, _redis);

                    queue.Enqueue(connection, "default", "1");

                    var record = connection.Query("select * from JobQueue").Single();
                    Assert.Equal("1", record.JobId.ToString());
                    Assert.Equal("default", record.Queue);
                    Assert.Null(record.FetchedAt);
                });
            }
                
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return source.Token;
        }

        public static void Sample(string arg1, string arg2) { }

        private MySqlJobQueue CreateJobQueue(MySqlConnection connection, ConnectionMultiplexer redis)
        {
            var storage = new MySqlStorage(connection, redis, _storageOptions);
            return new MySqlJobQueue(storage, new MySqlStorageOptions
            {
                RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
                RedisPrefix = "test:hangfire",
                UseRedisDistributedLock = true,
                UseRedisTransactions = true
            });
        }
    }
}
