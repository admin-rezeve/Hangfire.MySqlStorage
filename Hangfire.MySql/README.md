ALTER TABLE `HangfireDistributedLock` 
ADD INDEX `IX_resource` (`Resource` ASC) VISIBLE;


ALTER TABLE `HangfireJobQueue` 
ADD INDEX `idx_fetch_token` (`FetchToken` ASC) VISIBLE;
