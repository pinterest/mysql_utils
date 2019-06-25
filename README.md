**Note:** This project is no longer actively maintained by Pinterest.

---

# Pinterest MySQL Management Tools

## NOTE: THESE TOOLS WILL NOT JUST WORK!!!
You will need to add a fair amount of glue in order to make these tools work:
  - **Service discovery**
  - **A CMDB**
  - **A bunch of company specific information in the environment specifics lib**

It is hoped that this code will be useful to you as an example of working
implementations of DB tools.

## Basics of MySQL at Pinterest

Pinterest has historically used MySQL to store some of our most important data:
  - **Pins**
  - **Boards**
  - **Image metadata**
  - **Usernames and passwords**

Recently, we have added additional use cases:
  - **Pinlater**
Thanks in part to kernel optimizations, MySQL is now replacing Redis and
becoming the only supported backend for our asynchronous job execution engine.
  - **Zen**
MySQL has joined HBase as a supported backend for our Graph Storage Engine.

In all of the environments that MySQL supports at Pinterest, the environment is
identical from an administrative perspective:
  - **A single master with one or two slaves**
Historically MySQL was used with multiple writable instances in a replica set.
This topology is error prone and has been simplified to a single master with
one or more slaves.
  - **Zookeeper provides service discovery**
The contract between the administrative tools and the MySQL applications is
Zookeeper.  With few exceptions, Zookeeper provides clients with database
hostnames, usernames and passwords.

## The Lifecycle of a MySQL Instance in the Cloud

MySQL servers at Pinterest are launched, live, and die with only the rarest of 
configuration changes. Upgrading kernels, MySQL versions, and any other changes
that would require a restart of the database are never done in-place.  Instead,
these actions are always performed through server replacements and 
failovers/slave promotions as needed. This choice has greatly simplified our
automation by removing the need to manage intermediate state. 

One of our most important scripts is launch_replacement_db_host.py. In the
simplest case, the only required argument to launch_replacement_db_host.py is 
the hostname of a failed slave. The existing instance is examined, all required
parameters for a new server are computed, and then the new server is launched.
For other changes, such as MySQL upgrades, hardware upgrades/downgrades, and
datacenter migrations, there are optional arguments.

After the new server has booted and received its initial base configuration
from our provisioning system, a cron job will notice that the data directory is
empty and run mysql_restore.py. Based on service discovery, this 
script will attempt to find a database backup, restore it, setup replication,
and then add the new MySQL instance to service discovery.  Like 
launch_replacement_db_host.py,  mysql_restore.py accepts many 
optional arguments for non-standard uses.

If a MySQL master server requires replacement, the mysql_failover.py script
must be run to promote the primary slaves to master. This script deals with
either either living or dead initial masters, modifies MySQL replication
topology, and then updates service discovery.

After a server has been removed from service discovery, it will be subject to a
retirement queue system. This system has several steps that lead to eventual
termination of a server:
  - Based on service discovery, servers that are not in use will have several
status counters reset.
  - After a day, the servers will be inspected to see if any activity has
caused the status counters to increment. If the counters have incremented, the
retirement process is aborted. If the counters have not incremented, the MySQL
instance is sent a shutdown command.
  - After another day, the server is subject to termination if its database has
not been restarted.

## Utilities

A list of included utilities:
  - **archive_mysql_binlogs.py**
This script backs up MySQL replication logs in order to be able to perform
point-in-time recoveries in the case where all servers in a replica set are
lost. All logs up to the current log being written to are uploaded to S3.
  - **backup_tester.py**
Replace some number of replica servers in order to test backups.
  - **binlog_rotator.py**
If the current binlog has been in use longer than a predefined limit, rotate
it.
  - **check_mysql_replication.py**
This script displays replication status of an instance in terms of sql/io
thread status and bytes behind, a computed seconds behind master based on
pt-heartbeat. If a watch argument is supplied, normal output is suppess and
instead only a computed seconds behind master is displayed along with a
guestimate for replication catchup.
  - **find_shard_mismatches.py**
This script examines production servers and find any incorrectly located
shards.
  - **find_unused_db_servers.py**
This script finds unused servers based on service discovery and optionally add
the instances to the retirement queue.
  - **fix_orphaned_shards.py**
This script picks up where find_shard_mismatches.py left off and rename and then
eventually drop the unused shards.
  - **get_recent_checksums.py**
Use data populated by mysql_checksum.py to display current replication
consistency data.
  - **kill_backups.py**
Kill any running backups. This is run by cron on master servers.
  - **launch_amazon_mysql_server.py**
This script is generally called by launch_replacement_db_host.py and provides
an easy interface to correctly launch a new server in aws.
  - **launch_replacement_db_host.py**
This script accepts a hostname of an existing replica and then pulls a variety
of data from our cmdb and attempts to launch a server to replace the supplied
instance with a similar configuration. Optional additional arguments can
override the configuration in a variety of ways.
  - **modify_mysql_zk**
This script will modify our service discovery system in a variety of ways.
  - **mysql_backup_status.py**
This script checks that backups have run across all replica sets and optionally
display the created backup files. This script only checks xtrabackup backups
  - **mysql_backup.py**
This script is the entry point for backups for MySQL. It can perform logical
and xtrabackup backups.
  - **mysql_backup_csv.py**
This script backups up data to S3 in CSV format in a manner that can be
queried by Hive like systems. It is **very** much multiprocess and
multithreaded and doubles as a stress testing utility.
  - **mysql_backup_logical.py**
This script is basically shorthand for running a logical backup through
mysql_backup.py.
  - **mysql_backup_xtrabackup.py**
This script is basically shorthand for running a xtrabackups backup through
mysql_backup.py.
  - **mysql_checksum.py**
This script is run every day, and runs a pt-checksum against a subset of the
shards in order to verify that master and slave are not out of sync and if so,
by how much. The mysql_checksum.py script runs the checksums and stores the
results.
  - **mysql_cli.py**
This script attempts to remove the need for users to know hostnames,
usernames and passwords in order to use the mysql cli. The script accepts
a replica set name or in some cases a shard name and will determine the
current hosts in productions and then launches a mysql cli in the shell
using a read only username and password. It can also use a variety of
privileges such as a read write connection or a admin connections. It can also
accept a hostname and just figure out usernames and passwords.
  - **mysql_cnf_builder.py**
This script builds MySQL configuration files based on global defaults, and then
overrides for workload type, hardware and MySQL version. Several example
configuration files are included.
  - **mysql_grants.py**
This script manages our database users. It is one of our oldest bits of
automation and one of our most limited. It fulfills our needs for the time
being but sooner or later will need to be significantly expanded.
  - **mysql_failover.py**
This script attempts to safely run a failover on a MySQL replica set, updating
replication topology and service discovery.
  - **mysql_grants.py**
This script provides an interface to check and correct db user configuration.
  - **mysql_init_server.py**
This script takes a server with mysql binaries installed and sets up an empty
mysql instance and then imports users.
  - **mysql_record_table_size.py*
Record the size of all innodb tables.
  - **mysql_replica_mappings.py**
This script provides administrators a quick view of what is in production in a
format that is easy to use for shell scripting. The script can also pull in
hardware, availability zone, etc...
  - **restart_daemons.py**
Restart pt daemons, if needed.
  - **mysql_restore.py**
This script finds a backup, restores it, sets up replication and
then adds the new instance to service discovery based on data recorded by
launch_replacement_db_host.py.
  - **mysql_shard_status.py**
This script displays the status in service discovery of an instance. Primarily
used for gating cron jobs.
  - **other_slave_running_etl.py**
Checks is another slave server is running a csv backup. Useful for gating cron.
  - **retirement_queue.py**
This script ensures that a server which is no longer in service discovery
is no longer in use and then terminates the instance.
  - **safe_uploader.py**
This module provides our canonical way to upload data into S3. Either the 
processes feeding in data all succeed or the upload is not finalized.
  - **schema_verifier.py**
This script ensures that schema is in sync across sharded data sets.

## Some examples

Find the pinlater test servers
```
$ ./mysql_replica_mappings.py | grep pinlatertest
pinlatertestdb002               master      pinlatertestdb-2-3:3306
pinlatertestdb002               slave       pinlatertestdb-2-4:3306
```

Promote the slave to master
```
$ ./mysql_failover.py pinlatertestdb-2-3
I18:15:10 [__main__] Master to demote is pinlatertestdb-2-3:3306
I18:15:10 [__main__] Replica set is detected as pinlatertestdb002
I18:15:10 [__main__] Taking promotion lock on replica set
I18:15:10 [__main__] Promotion lock identifier is 48f8ee80-d97e-47b4-bf2a-75fd5d985b20
I18:15:10 [__main__] Releasing any expired locks
I18:15:10 [__main__] UPDATE mysqlops.promotion_locks SET lock_active = NULL WHERE expires < now()
I18:15:10 [__main__] Checking existing locks
I18:15:10 [__main__] Taking lock against replica set: pinlatertestdb002
I18:15:10 [__main__] INSERT INTO mysqlops.promotion_locks SET lock_identifier = '48f8ee80-d97e-47b4-bf2a-75fd5d985b20', lock_active = 'active', created_at = NOW(), expires = NOW() + INTERVAL 12 HOUR, released = NULL, replica_set = 'pinlatertestdb002', promoting_host = 'devops001', promoting_user = 'rwultsch'
I18:15:10 [__main__] Slave/new master is detected as pinlatertestdb-2-4:3306
I18:15:10 [__main__] DR slave is detected as None
I18:15:10 [__main__] Replica pinlatertestdb-2-4:3306 is replicating from expected master server pinlatertestdb-2-3:3306
I18:15:10 [__main__] Testing to see if Slave/new master is setup to write replication logs
I18:15:10 [__main__] Slave/new master is setup to write replication logs
I18:15:10 [__main__] Master is considered alive
I18:15:10 [__main__] Lag on pinlatertestdb-2-4:3306 is 0 is <= limit of 60
I18:15:10 [__main__] Preliminary sanity checks complete, starting promotion
I18:15:10 [__main__] Setting read_only on master
I18:15:10 [lib.mysql_lib] Confirming that long running transactions have gone away
I18:15:10 [lib.mysql_lib] All long trx are now dead
I18:15:10 [lib.mysql_lib] SET GLOBAL read_only = 1
I18:15:10 [__main__] Confirming no writes to old master
I18:15:10 [lib.mysql_lib] FLUSH TABLE_STATISTICS
I18:15:10 [lib.mysql_lib] FLUSH USER_STATISTICS
I18:15:10 [__main__] Waiting 10 seconds to confirm instance is no longer accepting writes
I18:15:20 [__main__] No writes after sleep, looks like we are good to go
I18:15:20 [__main__] Waiting for replicas to be caught up
I18:15:20 [__main__] pinlatertestdb-2-4:3306 is in sync with the master
I18:15:20 [__main__] Setting up replication from old master (pinlatertestdb-2-3:3306)to new master (pinlatertestdb-2-4:3306)
I18:15:20 [lib.mysql_lib] Setting pinlatertestdb-2-3:3306 as a replica of new master pinlatertestdb-2-4:3306
I18:15:20 [lib.mysql_lib] Confirming that long running transactions have gone away
I18:15:20 [lib.mysql_lib] All long trx are now dead
I18:15:20 [lib.mysql_lib] SET GLOBAL read_only = 1
I18:15:20 [lib.mysql_lib] CHANGE MASTER TO MASTER_USER='REDACTED', MASTER_PASSWORD='REDACTED', MASTER_HOST='pinlatertestdb-2-4', MASTER_PORT=3306, MASTER_LOG_FILE='pinlatertestdb-2-4-bin.000665', MASTER_LOG_POS=90715916
I18:15:20 [lib.mysql_lib] START SLAVE
I18:15:21 [__main__] Updating zk
I18:15:21 [modify_mysql_zk] Instance is pinlatertestdb-2-4:3306
I18:15:21 [modify_mysql_zk] Detected replica_set as pinlatertestdb002
I18:15:21 [kazoo_utils] Underlying zookeeper connection is healthy.
I18:15:21 [modify_mysql_zk] Replica set pinlatertestdb002 is held in zk_node /config/services/generaldb/mysql_databases
I18:15:21 [modify_mysql_zk] Existing config:
I18:15:21 [modify_mysql_zk] {'master': {'host': 'pinlatertestdb-2-3', 'port': 3306},
 'passwd': 'REDACTED',
 'slave': {'host': 'pinlatertestdb-2-4', 'port': 3306},
 'user': 'REDACTED'}
I18:15:21 [modify_mysql_zk] New config:
I18:15:21 [modify_mysql_zk] {'master': {'host': 'pinlatertestdb-2-4', 'port': 3306},
 'passwd': 'REDACTED',
 'slave': {'host': 'pinlatertestdb-2-3', 'port': 3306},
 'user': 'REDACTED'}
I18:15:21 [modify_mysql_zk] Pushing new configuration for pinlatertestdb002:
I18:15:21 [__main__] Removing read_only from new master
I18:15:21 [lib.mysql_lib] SET GLOBAL read_only = 0
I18:15:21 [__main__] Removing replication configuration from new master
I18:15:21 [lib.mysql_lib] ('Previous replication settings:', {'Replicate_Wild_Do_Table': '', 'Retrieved_Gtid_Set': '', 'Master_SSL_CA_Path': '', 'Last_Error': '', 'Until_Log_File': '', 'SQL_Delay': 0L, 'Seconds_Behind_Master': 0L, 'Master_User': 'REDACTED', 'Master_Port': 3306L, 'Master_Retry_Count': 86400L, 'Until_Log_Pos': 0L, 'Master_Log_File': 'pinlatertestdb-2-3-bin.000715', 'Read_Master_Log_Pos': 41406489L, 'Replicate_Do_DB': '', 'Master_SSL_Verify_Server_Cert': 'No', 'Exec_Master_Log_Pos': 41406489L, 'Replicate_Ignore_Server_Ids': '', 'Replicate_Ignore_Table': '', 'Master_Server_Id': 167842827L, 'Relay_Log_Space': 41406892L, 'Last_SQL_Error': '', 'SQL_Remaining_Delay': None, 'Relay_Master_Log_File': 'pinlatertestdb-2-3-bin.000715', 'Master_SSL_Allowed': 'No', 'Master_SSL_CA_File': '', 'Slave_IO_State': 'Waiting for master to send event', 'Last_SQL_Error_Timestamp': '', 'Relay_Log_File': 'mysqld_3306-relay-bin.002138', 'Replicate_Ignore_DB': '', 'Last_IO_Error': '', 'Until_Condition': 'None', 'Slave_SQL_Running_State': 'Slave has read all relay log; waiting for the slave I/O thread to update it', 'Replicate_Do_Table': '', 'Last_Errno': 0L, 'Master_Host': 'pinlatertestdb-2-3', 'Master_Info_File': '/raid0/mysql/3306/data/master.info', 'Master_SSL_Key': '', 'Executed_Gtid_Set': '', 'Master_Bind': '', 'Skip_Counter': 0L, 'Slave_SQL_Running': 'Yes', 'Relay_Log_Pos': 41406661L, 'Master_SSL_Cert': '', 'Last_IO_Errno': 0L, 'Slave_IO_Running': 'Yes', 'Connect_Retry': 60L, 'Last_SQL_Errno': 0L, 'Last_IO_Error_Timestamp': '', 'Replicate_Wild_Ignore_Table': '', 'Master_UUID': 'f38ce3e5-1609-11e5-9d3c-0e36038ac59d', 'Auto_Position': 0L, 'Master_SSL_Crl': '', 'Master_SSL_Cipher': '', 'Master_SSL_Crlpath': ''})
I18:15:21 [lib.mysql_lib] STOP SLAVE
I18:15:21 [lib.mysql_lib] RESET SLAVE ALL
I18:15:21 [__main__] Releasing promotion lock
I18:15:21 [__main__] UPDATE mysqlops.promotion_locks SET lock_active = NULL AND released = NOW() WHERE lock_identifier = '48f8ee80-d97e-47b4-bf2a-75fd5d985b20'
I18:15:21 [__main__] Failover complete
```

Replace the old master/new slave
```
$ ./launch_replacement_db_host.py pinlatertestdb-2-3 --reason kicks_and_giggles
I18:19:28 [__main__] Trying to launch a replacement for host pinlatertestdb-2-3 which is part of replica set is pinlatertestdb002
I18:19:28 [__main__] Data from cmdb: {u'config.instance_type': u'i2.2xlarge', u'region': u'us-east-1', u'cloud.aws.vpc_id': u'REDACTED', u'cloud.aws.subnet_id': u'REDACED', u'location': u'us-east-1a', u'id': u'REDACTED', u'security_group_ids': u'REDACTED', u'config.name': u'pinlatertestdb-2-3', u'security_groups': u'REDACTED'}
I18:19:29 [__main__] Reason for launch: kicks_and_giggles
I18:19:29 [launch_amazon_mysql_server] Requested hostname = pinlatertestdb-2-7
I18:19:29 [launch_amazon_mysql_server] Requested instance_type = i2.2xlarge
I18:19:29 [launch_amazon_mysql_server] Requested vpc_security_group = REDACTED
I18:19:29 [launch_amazon_mysql_server] Requested classic_security_group = None
I18:19:29 [launch_amazon_mysql_server] Requested availability_zone = us-east-1a
I18:19:29 [launch_amazon_mysql_server] Requested mysql_major_version = 5.6
I18:19:29 [launch_amazon_mysql_server] Requested mysql_minor_version = stable
I18:19:29 [launch_amazon_mysql_server] Requested dry_run = False
I18:19:29 [launch_amazon_mysql_server] Requested skip_name_check = True
I18:19:29 [launch_amazon_mysql_server] Will use subnet "REDACTED" in "REDACTED" based upon security group REDACTED and availibility zone us-east-1a
I18:19:29 [launch_amazon_mysql_server] Config for new server:
..Tons of Pinterest stuff redacted here..
I18:19:30 [launch_amazon_mysql_server] Launched instance i-1231234
```

 Check replication on the old master/new slave
```
./check_mysql_replication.py pinlatertestdb-2-3
Heartbeat_seconds_behind: 0
Slave_IO_Running: Yes
IO_lag_bytes: 2068
IO_lag_binlogs: 0
Slave_SQL_Running: Yes
SQL_lag_bytes: 2068
SQL_lag_binlogs: 0
```

Check grants on the old master/new slave
```
$ ./mysql_grants.py -i pinlatertestdb-2-3 -a check
$ echo $?
0
```

## Not a Panacea
These tools are tightly integrated into our service discovery mechanism and
would likely require moderate modification of the code that reads and writes
from service discovery. There are also some significant legacy limitations to
these utilities, such as the lack of support for more than two slaves. It is
our hope that these tools are useful to other that wish to create automation
for their MySQL infrastructure.
