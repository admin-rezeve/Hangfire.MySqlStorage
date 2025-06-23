using System;
using MySqlConnector;
using StackExchange.Redis;

namespace Hangfire.MySql.Tests
{
    public static class ConnectionUtils
    {
        private const string DatabaseVariable = "Hangfire_SqlServer_DatabaseName";
        private const string ConnectionStringTemplateVariable 
            = "Hangfire_SqlServer_ConnectionStringTemplate";

        private const string MasterDatabaseName = "mysql";
        private const string DefaultDatabaseName = @"Hangfire.MySql.Tests";
        private const string DefaultConnectionStringTemplate
            = "server=127.0.0.1;uid=root;pwd=generalkitty;database={0};port=3309;Allow User Variables=True";

        private const string DefaultRedisConnectionStringTemplate
            = "localhost:6379,abortConnect=false,ssl=false";

        public static string GetDatabaseName()
        {
            return Environment.GetEnvironmentVariable(DatabaseVariable) ?? DefaultDatabaseName;
        }

        public static string GetMasterConnectionString()
        {
            return String.Format(GetConnectionStringTemplate(), MasterDatabaseName);
        }

        public static string GetConnectionString()
        {
            return String.Format(GetConnectionStringTemplate(), GetDatabaseName());
        }

        public static string GetRedisConnectionString()
        {
            return DefaultRedisConnectionStringTemplate;
        }

        private static string GetConnectionStringTemplate()
        {
            return Environment.GetEnvironmentVariable(ConnectionStringTemplateVariable)
                   ?? DefaultConnectionStringTemplate;
        }

        public static ConnectionMultiplexer CreateRedisConnection()
        {
            return ConnectionMultiplexer.Connect(GetRedisConnectionString());
        }

        public static IDatabase GetRedisDatabase()
        {
            return ConnectionMultiplexer.Connect(GetRedisConnectionString()).GetDatabase();
        }

        public static MySqlConnection CreateConnection()
        {
            var connection = new MySqlConnection(GetConnectionString());
            connection.Open();

            return connection;
        }
    }
}
