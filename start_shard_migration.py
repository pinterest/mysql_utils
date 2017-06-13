#!/usr/bin/env python
import argparse
import logging
import time
import uuid

from lib import mysql_lib
from lib import host_utils
from lib import backup
from lib import environment_specific
import find_shard_mismatches
import mysql_record_table_size
import mysql_restore

# Do not allow migrate so the destination would be above DISK_LIMIT
# percent full
DISK_LIMIT = .65
# Terrible escaping... Desired outcome is of the form (wrapped for clarity)
# ssh root@testmodsharddb-1-86 "df /raid0/mysql/3306/data/ | 
# awk \"{print (0.65*\\\$2 - \\\$3)/1024}\" | tail -n1"

MIGRATION_SPACE_CMD = ('ssh root@{hostname} "df {datadir} | awk \\\"{{print '
                       '({disk_limit}*\\\\\$2 - \\\\\$3)/1024}}\\\" | '
                       'tail -n1"')
STATUS_ABORTED = 'ABORTED'
STATUS_EXPORT_FAILED = 'Export failed'
STATUS_FAILOVER_READY = 'Ready for failover'
STATUS_FINISHED = 'Finished'
STATUS_IMPORTING = 'Importing'
log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source_replica_set',
                        help='Which replica set to move shards *FROM*')
    parser.add_argument('destination_replica_set',
                        help='Which replica set to move shards *TO*')
    parser.add_argument('databases',
                        help=('Which databases to move from source_replica_set'
                              'to destination_replica_set'),
                        nargs='+')
    parser.add_argument('--dry_run',
                        help=('Do not actually run a migration, just run '
                              'safety checks, etc...'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()

    start_shard_migration(args.source_replica_set,
                          args.destination_replica_set,
                          set(args.databases))


def start_shard_migration(source_replica_set, destination_replica_set,
                          mig_dbs):
    """ Move shards from one replica set to another

    Args:
    source_replica_set - Which replica set to take the shards from
    destination_replica_set - Which replica set to put the shards on
    mig_dbs - A set of databases to be migrated
    """
    # In 2017Q1 shardb and modsharddb will learn how to deal with shard
    # migrations. We will block them for now.
    if source_replica_set.startswith('db') or \
            source_replica_set.startswith('moddb'):
        raise Exception('Sharddb and modsharddb migrations are not yet '
                        'supported')

    if source_replica_set == destination_replica_set:
        raise Exception('Source and destination can not be the same!')
    # Dealing with failures, potentially due to failovers seems scary
    # here. We are intentionally not catching exception as this seems racy
    # and it would be far better for the entire process to fail than to mess
    # with replication during a failover.
    log.info('Requested to migrate from {s} to {d} databases: {db}'
             ''.format(s=source_replica_set,
                       d=destination_replica_set,
                       db=', '.join(mig_dbs)))

    zk = host_utils.MysqlZookeeper()
    source_master = zk.get_mysql_instance_from_replica_set(source_replica_set)
    source_slave = zk.get_mysql_instance_from_replica_set(
        source_replica_set, host_utils.REPLICA_ROLE_DR_SLAVE)

    if not source_slave:
        source_slave = zk.get_mysql_instance_from_replica_set(
            source_replica_set, host_utils.REPLICA_ROLE_SLAVE)
    log.info('Source host for dumping data {}'.format(source_slave))
    destination_master = zk.get_mysql_instance_from_replica_set(
            destination_replica_set)
    log.info('Destination host for restoring data {}'
             ''.format(destination_master))

    expected_dbs_on_source = zk.get_sharded_dbs_by_replica_set()[source_replica_set]
    non_mig_dbs = mysql_lib.get_dbs(source_slave).difference(mig_dbs)
    unexpected_dbs = mig_dbs.difference(expected_dbs_on_source)
    if unexpected_dbs:
        raise Exception('Unexpected database supplied for migraton: {}'
                        ''.format(unexpected_dbs))

    # Make sure there are no missing or extra shards
    precheck_schema(source_master)
    precheck_schema(destination_master)

    # Check disk space
    required_disk_space = get_required_disk_space(mig_dbs, source_master)
    available_disk_space = disk_space_available_for_migration(destination_master)
    if available_disk_space < required_disk_space:
        raise Exception('Insufficent disk space to migrate, '
                        'available {a}MB, '
                        'requred {r}MB'
                        ''.format(a=available_disk_space,
                                  r=required_disk_space))
    else:
        log.info('Disk space looks ok: '
                 'available {a}MB, '
                 'requred {r}MB'
                 ''.format(a=available_disk_space,
                           r=required_disk_space))

    # Let's take out a lock to make sure we don't have multiple migrations
    # running on the same replica sets (either source or destination).
    lock_id = take_migration_lock(source_replica_set, destination_replica_set,
                                  mig_dbs, non_mig_dbs)
    try:
        if(non_mig_dbs):
            # First we will dump the schema for the shards that are not moving
            log.info('Backing up non-migrating schema: {}'.format(non_mig_dbs))
            no_mig_backup = backup.logical_backup_instance(
                                            source_slave, time.localtime(),
                                            blackhole=True, databases=non_mig_dbs)

        time.sleep(1)
        # And next the metadata db
        log.info('Backing up metadata db: {}'.format(mysql_lib.METADATA_DB))
        metadata_backup = backup.logical_backup_instance(
                                        source_slave, time.localtime(),
                                        databases=[mysql_lib.METADATA_DB])

        time.sleep(1)
        # Next we will backup the data for the shards that are moving
        log.info('Backing up migrating schema data: {}'.format(mig_dbs))
        mig_backup = backup.logical_backup_instance(
                                       source_slave, time.localtime(),
                                       databases=mig_dbs)
    except:
        finish_migration_log(lock_id, STATUS_EXPORT_FAILED)
        raise

    if(non_mig_dbs):
        # Finally import the backups
        log.info('Importing all the blackhole tables')
        mysql_restore.logical_restore(no_mig_backup, destination_master)

    log.info('Import metadata')
    mysql_restore.logical_restore(metadata_backup, destination_master)

    log.info('Setting up replicaiton')
    mysql_lib.change_master(destination_master, source_master,
                            'BOGUS', 0, no_start=True, skip_set_readonly=True)
    mysql_restore.logical_restore(mig_backup, destination_master)

    # add start slave, catchup
    mysql_lib.start_replication(destination_master)
    mysql_lib.wait_for_catch_up(destination_master, migration=True)

    # And update the log/locks
    update_migration_status(lock_id, STATUS_FAILOVER_READY)
    log.info('The migration is ready to be finished by running:')
    log.info('/usr/local/bin/mysql_utils/finish_shard_migration.py {src}'
             ''.format(src=source_replica_set))


def take_migration_lock(source_replica_set, destination_replica_set,
                        mig_dbs, non_mig_dbs):
    """ Take a migration lock to ensure no other migration are run concurrenly

    Args:
    source_replica_set - Which replica set to take the shards from
    destination_replica_set - Which replica set to put the shards on
    mig_dbs - The names of the databases which map to the shards which
              are being migrated
    non_mig_dbs - The names of the databases which are created with blackhole
                  tables for replication to function.

    Returns: a lock identifier
    """
    conn = mysql_lib.get_mysqlops_connections()
    cursor = conn.cursor()
    lock_identifier = str(uuid.uuid4())
    log.info('Migration lock identifier is {}'.format(lock_identifier))

    log.info('Checking existing locks')
    existing_lock = check_migration_lock(source_replica_set)
    if not existing_lock:
        existing_lock = check_migration_lock(destination_replica_set)
    if existing_lock:
        log.error('Lock is already held by {}'.format(existing_lock))
        log.error('You can abort this migration by running:')
        log.error('/usr/local/bin/mysql_utils/clean_up_unfinished_migration.py {}'
                  ''.format(existing_lock['source_replica_set']))
        raise Exception('Can not take migration lock')

    params = {'lock': lock_identifier,
              'source_replica_set': source_replica_set,
              'destination_replica_set': destination_replica_set,
              'mig_dbs': ', '.join(mig_dbs),
              'non_mig_dbs': ', '.join(non_mig_dbs),
              'status': STATUS_IMPORTING}

    # Todo: turn on locking checking, swich to INSERT
    sql = ("INSERT INTO mysqlops.mysql_migration_locks "
           "SET "
           "lock_identifier = %(lock)s, "
           "lock_active = 'active', "
           "created_at = NOW(), "
           "released = NULL, "
           "source_replica_set = %(source_replica_set)s, "
           "destination_replica_set = %(destination_replica_set)s, "
           "mig_databases = %(mig_dbs)s, "
           "non_mig_databases = %(non_mig_dbs)s, "
           "status = %(status)s ")
    cursor.execute(sql, params)
    conn.commit()
    log.info(cursor._executed)
    return lock_identifier


def update_migration_status(lock_identifier, status):
    """ Update the migration lock table

    Args:
    lock_identifier - a lock id as returned by take_migration_lock
    status - The new status
    """
    conn = mysql_lib.get_mysqlops_connections()
    cursor = conn.cursor()

    params = {'lock': lock_identifier,
              'status': status}
    sql = ("UPDATE mysqlops.mysql_migration_locks "
           "SET "
           "status = %(status)s "
           "WHERE "
           "lock_identifier = %(lock)s ")
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()
    log.info(cursor._executed)

def finish_migration_log(lock_identifier, status):
    """ Update the migration lock table and release the lock

    Args:
    lock_identifier - a lock id as returned by take_migration_lock
    status - The new status
    """
    conn = mysql_lib.get_mysqlops_connections()
    cursor = conn.cursor()

    params = {'lock': lock_identifier,
              'status': status}
    sql = ("UPDATE mysqlops.mysql_migration_locks "
           "SET "
           "status = %(status)s, "
           "lock_active = NULL "
           "WHERE "
           "lock_identifier = %(lock)s ")
    cursor = conn.cursor()
    cursor.execute(sql, params)
    conn.commit()
    log.info(cursor._executed)


def check_migration_lock(replica_set):
    """ Confirm there are no active locks that would block taking a
        migration lock

    Args:
    replica_set - A name of a replica set
    """
    conn = mysql_lib.get_mysqlops_connections()
    cursor = conn.cursor()
    params = {'replica_set': replica_set}
    sql = ('SELECT lock_identifier, '
           '       source_replica_set, '
           '       destination_replica_set, '
           '       mig_databases, '
           '       non_mig_databases, '
           '       status '
           'FROM mysqlops.mysql_migration_locks '
           "WHERE lock_active = 'active' AND "
           "( source_replica_set = %(replica_set)s OR"
           "  destination_replica_set = %(replica_set)s )")
    cursor.execute(sql, params)
    row = cursor.fetchone()
    log.info(cursor._executed)
    return row


def precheck_schema(instance):
    """ Make sure the existing state is sane

    Args:
    instance - a hostAddr instance
    """
    orphaned, orphaned_but_used, missing = \
        find_shard_mismatches.find_shard_mismatches(instance)
    if (orphaned or orphaned_but_used):
        raise Exception('Unexpected shards are on {inst}. You can try to '
                        'clean them up using:  '
                        '/usr/local/bin/mysql_utils/fix_orphaned_shards.py '
                        '-a rename -i {inst}'
                        ''.format(inst=instance))
    if missing:
        raise Exception('Shards are missing on {}. This is really weird '
                        'and needs to be debugged'.format(instance))


def disk_space_available_for_migration(instance):
    """ Check the disk space available for migrations on the data dir mount

    Args:
    instance - A hostaddr object

    Returns: The number of MB available
    """
    datadir = mysql_lib.get_global_variables(instance)['datadir']
    cmd = MIGRATION_SPACE_CMD.format(hostname=instance.hostname,
                                     datadir=datadir,
                                     disk_limit=DISK_LIMIT)
    log.info(cmd)
    out, err, ret = host_utils.shell_exec(cmd)
    return float(out.strip())


def get_required_disk_space(databases, instance):
    """ Determine how much disk space is needed for a migration

    Args:
    databases - A list of databases to be migrated
    instance - A hostaddr object

    Returns - The number of MB needed for the migration
    """
    required_disk_space = 0
    for db in databases:
        try:
            required_disk_space += mysql_record_table_size.get_db_size_from_log(
                instance, db)
        except:
            log.info('Exact table size is unavailable for {}, using estimate'
                     ''.format(db))
            required_disk_space += mysql_lib.get_approx_schema_size(instance, db)
    return required_disk_space


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
