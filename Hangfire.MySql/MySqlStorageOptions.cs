using System;
using System.Data;

namespace Hangfire.MySql
{
    public class MySqlStorageOptions
    {
        private TimeSpan _queuePollInterval;
        public static readonly string DefaultTablesPrefix = String.Empty;

        public MySqlStorageOptions()
        {
            TransactionIsolationLevel = System.Transactions.IsolationLevel.ReadCommitted;
            QueuePollInterval = TimeSpan.FromSeconds(15);
            JobExpirationCheckInterval = TimeSpan.FromHours(1);
            CountersAggregateInterval = TimeSpan.FromMinutes(5);
            PrepareSchemaIfNecessary = true;
            DashboardJobListLimit = 50000;
            TransactionTimeout = TimeSpan.FromMinutes(1);
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            UseSkipLocked = true;
            TablesPrefix = DefaultTablesPrefix;
            RedisLockTimeout = TimeSpan.FromSeconds(30);
            RedisPrefix = "hangfire:";
            UseRedisDistributedLock = true;
            LifoRedisQueues = new string[0];
            UseRedisTransactions = true;
        }

        public System.Transactions.IsolationLevel? TransactionIsolationLevel { get; set; }

        public TimeSpan QueuePollInterval
        {
            get { return _queuePollInterval; }
            set
            {
                var message = String.Format(
                    "The QueuePollInterval property value should be positive. Given: {0}.",
                    value);

                if (value == TimeSpan.Zero)
                {
                    throw new ArgumentException(message, "value");
                }
                if (value != value.Duration())
                {
                    throw new ArgumentException(message, "value");
                }

                _queuePollInterval = value;
            }
        }

        public bool PrepareSchemaIfNecessary { get; set; }

        public TimeSpan JobExpirationCheckInterval { get; set; }
        public TimeSpan CountersAggregateInterval { get; set; }

        public int? DashboardJobListLimit { get; set; }
        public TimeSpan TransactionTimeout { get; set; }
        [Obsolete("Does not make sense anymore. Background jobs re-queued instantly even after ungraceful shutdown now. Will be removed in 2.0.0.")]
        public TimeSpan InvisibilityTimeout { get; set; }

        public string TablesPrefix { get; set; }

        public bool UseSkipLocked { get; set; }

        public string RedisConnectionString { get; set; }
        public string RedisPrefix { get; set; }
        public TimeSpan RedisLockTimeout { get; set; }
        public bool UseRedisDistributedLock { get; set; }
        public string[] LifoRedisQueues { get; set; }
        public bool UseRedisTransactions { get; set; }
    }

}