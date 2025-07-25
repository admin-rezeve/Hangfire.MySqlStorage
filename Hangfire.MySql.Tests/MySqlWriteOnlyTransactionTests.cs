﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Dapper;
using Hangfire.MySql.JobQueue;
using Hangfire.States;
using Moq;
using MySqlConnector;
using Xunit;
using StackExchange.Redis;

namespace Hangfire.MySql.Tests
{
    public class MySqlWriteOnlyTransactionTests : IClassFixture<TestDatabaseFixture>
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public MySqlWriteOnlyTransactionTests()
        {
            var defaultProvider = new Mock<IPersistentJobQueueProvider>();
            defaultProvider.Setup(x => x.GetJobQueue())
                .Returns(new Mock<IPersistentJobQueue>().Object);

            _queueProviders = new PersistentJobQueueProviderCollection(defaultProvider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MySqlWriteOnlyTransaction(null, new MySqlStorageOptions
                {
                    RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
                    RedisPrefix = "test:hangfire",
                    UseRedisDistributedLock = true,
                    UseRedisTransactions = true
                }));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void ExpireJob_SetsJobExpirationData()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt)
values ('', '', UTC_TIMESTAMP());
select last_insert_id() as Id";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var jobId = sql.Query(arrangeSql).Single().Id.ToString();
                var anotherJobId = sql.Query(arrangeSql).Single().Id.ToString();

                Commit(sql, redis, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));

                var job = GetTestJob(sql, jobId);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < job.ExpireAt && job.ExpireAt <= DateTime.UtcNow.AddDays(1));

                var anotherJob = GetTestJob(sql, anotherJobId);
                Assert.Null(anotherJob.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt, ExpireAt)
values ('', '', UTC_TIMESTAMP(), UTC_TIMESTAMP());
select last_insert_id() as Id";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var jobId = sql.Query(arrangeSql).Single().Id.ToString();
                var anotherJobId = sql.Query(arrangeSql).Single().Id.ToString();

                Commit(sql, redis, x => x.PersistJob(jobId));

                var job = GetTestJob(sql, jobId);
                Assert.Null(job.ExpireAt);

                var anotherJob = GetTestJob(sql, anotherJobId);
                Assert.NotNull(anotherJob.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt)
values ('', '', UTC_TIMESTAMP());
select last_insert_id() as Id";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var jobId = sql.Query(arrangeSql).Single().Id.ToString();
                var anotherJobId = sql.Query(arrangeSql).Single().Id.ToString();

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(sql, redis, x => x.SetJobState(jobId, state.Object));

                var job = GetTestJob(sql, jobId);
                Assert.Equal("State", job.StateName);
                Assert.NotNull(job.StateId);

                var anotherJob = GetTestJob(sql, anotherJobId);
                Assert.Null(anotherJob.StateName);
                Assert.Null(anotherJob.StateId);

                var jobState = sql.Query("select * from State").Single();
                Assert.Equal((string)jobId, jobState.JobId.ToString());
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.NotNull(jobState.CreatedAt);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.Data);
            });
        }

        [Fact, CleanDatabase]
        public void AddJobState_JustAddsANewRecordInATable()
        {
            const string arrangeSql = @"
insert into Job (InvocationData, Arguments, CreatedAt)
values ('', '', UTC_TIMESTAMP());
select last_insert_id() as Id";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();

                var jobId = sql.Query(arrangeSql).Single().Id.ToString();

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData())
                    .Returns(new Dictionary<string, string> { { "Name", "Value" } });

                Commit(sql, redis, x => x.AddJobState(jobId, state.Object));

                var job = GetTestJob(sql, jobId);
                Assert.Null(job.StateName);
                Assert.Null(job.StateId);

                var jobState = sql.Query("select * from State").Single();
                Assert.Equal((string)jobId, jobState.JobId.ToString());
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.NotNull(jobState.CreatedAt);
                Assert.Equal("{\"Name\":\"Value\"}", jobState.Data);
            });
        }

        [Fact, CleanDatabase]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            var correctJobQueue = new Mock<IPersistentJobQueue>();
            var correctProvider = new Mock<IPersistentJobQueueProvider>();
            correctProvider.Setup(x => x.GetJobQueue())
                .Returns(correctJobQueue.Object);

            _queueProviders.Add(correctProvider.Object, new[] { "default" });

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x => x.AddToQueue("default", "1"));

                correctJobQueue.Verify(x => x.Enqueue(It.IsNotNull<IDbConnection>(), "default", "1"));
            });
        }

        private static dynamic GetTestJob(IDbConnection connection, string jobId)
        {
            return connection
                .Query("select * from Job where Id = @id", new { id = jobId })
                .Single();
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis,x => x.IncrementCounter("my-key"));

                var record = sql.Query("select * from Counter").Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1, record.Value);
                Assert.Equal((DateTime?)null, record.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = sql.Query("select * from Counter").Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(1, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = (DateTime)record.ExpireAt;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void IncrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                var recordCount = sql.Query<int>("select count(*) from Counter").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x => x.DecrementCounter("my-key"));

                var record = sql.Query("select * from Counter").Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1, record.Value);
                Assert.Equal((DateTime?)null, record.ExpireAt);
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                var record = sql.Query("select * from Counter").Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal(-1, record.Value);
                Assert.NotNull(record.ExpireAt);

                var expireAt = (DateTime)record.ExpireAt;

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact, CleanDatabase]
        public void DecrementCounter_WithExistingKey_AddsAnotherRecord()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.DecrementCounter("my-key");
                    x.DecrementCounter("my-key");
                });

                var recordCount = sql.Query<int>("select count(*) from Counter").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_IfThereIsNo_SuchKeyAndValue()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x => x.AddToSet("my-key", "my-value"));

                var record = sql.Query("select * from `Set`").Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(0.0, record.Score, 2);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                var recordCount = sql.Query<int>("select count(*) from `Set`").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql,redis, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                var recordCount = sql.Query<int>("select count(*) from `Set`").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x => x.AddToSet("my-key", "my-value", 3.2));

                var record = sql.Query("select * from `Set`").Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
                Assert.Equal(3.2, record.Score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                var record = sql.Query("select * from `Set`").Single();

                Assert.Equal(3.2, record.Score, 3);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = sql.Query<int>("select count(*) from `Set`").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = sql.Query<int>("select count(*) from `Set`").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = sql.Query<int>("select count(*) from `Set`").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x => x.InsertToList("my-key", "my-value"));

                var record = sql.Query("select * from List").Single();

                Assert.Equal("my-key", record.Key);
                Assert.Equal("my-value", record.Value);
            });
        }

        [Fact, CleanDatabase]
        public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_TrimsAList_ToASpecifiedRange()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                var records = sql.Query("select * from List").ToArray();

                Assert.Equal(2, records.Length);
                Assert.Equal("1", records[0].Value);
                Assert.Equal("2", records[1].Value);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecordsToEnd_IfKeepAndingAt_GreaterThanMaxElementIndex()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(2, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql, redis, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = sql.Query<int>("select count(*) from List").Single();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, redis, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql,redis, x => x.SetRangeInHash("some-hash", null)));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Commit(sql,redis, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                var result = sql.Query(
                    "select * from Hash where `Key` = @key",
                    new { key = "some-hash" })
                    .ToDictionary(x => (string)x.Field, x => (string)x.Value);

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Assert.Throws<ArgumentNullException>(
                    () => Commit(sql,redis, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveHash_RemovesAllHashRecords()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                // Arrange
                Commit(sql,redis, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                // Act
                Commit(sql,redis, x => x.RemoveHash("some-hash"));

                // Assert
                var count = sql.Query<int>("select count(*) from Hash").Single();
                Assert.Equal(0, count);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql,redis, x => x.AddRangeToSet(null, new List<string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_ThrowsAnException_WhenItemsValueIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql,redis, x => x.AddRangeToSet("my-set", null)));

                Assert.Equal("items", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void AddRangeToSet_AddsAllItems_ToAGivenSet()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var items = new List<string> { "1", "2", "3" };

                Commit(sql, redis, x => x.AddRangeToSet("my-set", items));

                var records = sql.Query<string>(@"select `Value` from `Set` where `Key` = N'my-set'");
                Assert.Equal(items, records);
            });
        }

        [Fact, CleanDatabase]
        public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                Assert.Throws<ArgumentNullException>(
                    () => Commit(sql,redis, x => x.RemoveSet(null)));
            });
        }

        [Fact, CleanDatabase]
        public void RemoveSet_RemovesASet_WithAGivenKey()
        {
            const string arrangeSql = @"
insert into `Set` (`Key`, `Value`, Score) values (@key, @value, 0.0)";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1" },
                    new { key = "set-2", value = "1" }
                });

                Commit(sql, redis, x => x.RemoveSet("set-1"));

                var record = sql.Query("select * from `Set`").Single();
                Assert.Equal("set-2", record.Key);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();

                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, redis, x => x.ExpireHash(null, TimeSpan.FromMinutes(5))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireHash_SetsExpirationTimeOnAHash_WithGivenKey()
        {
            const string arrangeSql = @"
insert into Hash (`Key`, `Field`)
values (@key, @field)";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field" },
                    new { key = "hash-2", field = "field" }
                });

                // Act
                Commit(sql, redis, x => x.ExpireHash("hash-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = sql.Query("select * from Hash").ToDictionary(x => (string)x.Key, x => (DateTime?)x.ExpireAt);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["hash-1"]);
                Assert.True(records["hash-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["hash-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql,redis, x => x.ExpireSet(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireSet_SetsExpirationTime_OnASet_WithGivenKey()
        {
            const string arrangeSql = @"
insert into `Set` (`Key`, `Value`, Score)
values (@key, @value, 0.0)";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1" },
                    new { key = "set-2", value = "1" }
                });

                // Act
                Commit(sql, redis, x => x.ExpireSet("set-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = sql.Query("select * from `Set`").ToDictionary(x => (string)x.Key, x => (DateTime?)x.ExpireAt);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["set-1"]);
                Assert.True(records["set-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["set-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, redis, x => x.ExpireList(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void ExpireList_SetsExpirationTime_OnAList_WithGivenKey()
        {
            const string arrangeSql = @"
insert into List (`Key`) values (@key)";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", value = "1" },
                    new { key = "list-2", value = "1" }
                });

                // Act
                Commit(sql,redis, x => x.ExpireList("list-1", TimeSpan.FromMinutes(60)));

                // Assert
                var records = sql.Query("select * from List").ToDictionary(x => (string)x.Key, x => (DateTime?)x.ExpireAt);
                Assert.True(DateTime.UtcNow.AddMinutes(59) < records["list-1"]);
                Assert.True(records["list-1"] < DateTime.UtcNow.AddMinutes(61));
                Assert.Null(records["list-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql,redis, x => x.PersistHash(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistHash_ClearsExpirationTime_OnAGivenHash()
        {
            const string arrangeSql = @"
insert into Hash (`Key`, `Field`, ExpireAt)
values (@key, @field, @expireAt)";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "hash-1", field = "field", expireAt = DateTime.UtcNow.AddDays(1) },
                    new { key = "hash-2", field = "field", expireAt = DateTime.UtcNow.AddDays(1) }
                });

                // Act
                Commit(sql, redis,x => x.PersistHash("hash-1"));

                // Assert
                var records = sql.Query("select * from Hash").ToDictionary(x => (string)x.Key, x => (DateTime?)x.ExpireAt);
                Assert.Null(records["hash-1"]);
                Assert.NotNull(records["hash-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, redis, x => x.PersistSet(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistSet_ClearsExpirationTime_OnAGivenHash()
        {
            const string arrangeSql = @"
insert into `Set` (`Key`, `Value`, ExpireAt, Score)
values (@key, @value, @expireAt, 0.0)";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "set-1", value = "1", expireAt = DateTime.UtcNow.AddDays(1) },
                    new { key = "set-2", value = "1", expireAt = DateTime.UtcNow.AddDays(1) }
                });

                // Act
                Commit(sql, redis, x => x.PersistSet("set-1"));

                // Assert
                var records = sql.Query("select * from `Set`").ToDictionary(x => (string)x.Key, x => (DateTime?)x.ExpireAt);
                Assert.Null(records["set-1"]);
                Assert.NotNull(records["set-2"]);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(sql, redis, x => x.PersistList(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void PersistList_ClearsExpirationTime_OnAGivenHash()
        {
            const string arrangeSql = @"
insert into List (`Key`, ExpireAt)
values (@key, @expireAt)";

            UseConnection(sql =>
            {
                var redis = ConnectionUtils.CreateRedisConnection();
                // Arrange
                sql.Execute(arrangeSql, new[]
                {
                    new { key = "list-1", expireAt = DateTime.UtcNow.AddDays(1) },
                    new { key = "list-2", expireAt = DateTime.UtcNow.AddDays(1) }
                });

                // Act
                Commit(sql, redis, x => x.PersistList("list-1"));

                // Assert
                var records = sql.Query("select * from List").ToDictionary(x => (string)x.Key, x => (DateTime?)x.ExpireAt);
                Assert.Null(records["list-1"]);
                Assert.NotNull(records["list-2"]);
            });
        }

        private static void UseConnection(Action<MySqlConnection> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }

        private static void UseRedisConnection(Action<ConnectionMultiplexer> action)
        {
            using (var connection = ConnectionUtils.CreateRedisConnection())
            {
                action(connection);
            }
        }

        private void Commit(
            MySqlConnection connection,
            ConnectionMultiplexer redisConnection,
            Action<MySqlWriteOnlyTransaction> action)
        {
            var storage = new Mock<MySqlStorage>(connection, redisConnection, new MySqlStorageOptions
            {
                RedisConnectionString = ConnectionUtils.GetRedisConnectionString(),
                RedisPrefix = "test:hangfire",
                UseRedisDistributedLock = true,
                UseRedisTransactions = true
            });
            storage.Setup(x => x.QueueProviders).Returns(_queueProviders);

            using (var transaction = new MySqlWriteOnlyTransaction(storage.Object, new MySqlStorageOptions()))
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
}
