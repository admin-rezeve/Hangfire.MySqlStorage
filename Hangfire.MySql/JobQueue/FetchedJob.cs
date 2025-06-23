using System;

namespace Hangfire.MySql.JobQueue
{
    internal class FetchedJob
    {
        public int Id { get; set; }
        public int JobId { get; set; }
        public string Queue { get; set; }
        public DateTime? FetchedAt { get; set; }
    }
}
