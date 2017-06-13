#!/usr/bin/env python
import argparse
import logging
import os
import time

import fix_orphaned_shards
import mysql_shard_config
import mysql_failover
import start_shard_migration
from lib import mysql_lib
from lib import host_utils
from lib import environment_specific

log = logging.getLogger(__name__)

REPL_SYNC_MAX_SECONDS = 30
ROW_ESTIMATE_MAX_DIFF = .3
ROW_ACTUAL_MAX_DIFF = .001

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source_replica_set',
                        help='Which replica set to move shards *FROM*')
    parser.add_argument('--dry_run',
                        help=('Do not actually run a migration, just run '
                              'safety checks, etc...'),
                        default=False,
                        action='store_true')
    parser.add_argument('--confirm_estimated_row_counts',
                        help=('If set, do not confirm row counts, just confirm '
                              'estimate.'),
                        default=True,
                        dest='confirm_row_counts',
                        action='store_false')
    args = parser.parse_args()

    finish_shard_migration(args.source_replica_set,
                           args.dry_run,
                           args.confirm_row_counts)


def finish_shard_migration(source_replica_set,
                           dry_run=False, confirm_row_counts=True):
    """ Run a partial failover to finish a migration

    Args:
    source_replica_set -  The source replica set for the migration
    dry_run - Just run verifications, do not failover shards
    confirm_row_counts - If true, confirm row counts of slaves, otherwise
                         check estimated row counts
    """
    if host_utils.get_security_role() not in environment_specific.ROLE_TO_MIGRATE:
        raise Exception(environment_specific.ROLE_ERROR_MSG)
    # Figure out what we are working on based on the lock present on the
    # source_replica_set
    migration = start_shard_migration.check_migration_lock(source_replica_set)
    if migration['status'] != start_shard_migration.STATUS_FAILOVER_READY:
        raise Exception('Migration is not ready to run')

    destination_replica_set = migration['destination_replica_set']
    source_replica_set = migration['source_replica_set']
    mig_databases = set(migration['mig_databases'].split(', '))
    non_mig_databases = set(migration['non_mig_databases'].split(', '))
    mig_lock_identifier = migration['lock_identifier']

    # Figure out some more data based on the lock
    zk = host_utils.MysqlZookeeper()
    source_master = zk.get_mysql_instance_from_replica_set(source_replica_set)
    destination_master = zk.get_mysql_instance_from_replica_set(destination_replica_set)
    log.info('Will migrate {dbs} from {s} to {d}'
             ''.format(dbs=mig_databases,
                       s=source_replica_set,
                       d=destination_replica_set))
    try:
        dest_lock_identifier = None
        src_lock_identifier = None
        read_only_set = False

        if not dry_run:
            log.info('Taking promotion locks')
            dest_lock_identifier = mysql_failover.get_promotion_lock(destination_replica_set)
            src_lock_identifier = mysql_failover.get_promotion_lock(source_replica_set)

        # Check replication
        log.info('Checking replication')
        check_replication_for_migration(source_replica_set, destination_replica_set)

        # Check schema and data
        log.info('Checking schema and row counts')
        verify_schema_for_migration(source_replica_set, destination_replica_set,
                                    mig_databases, confirm_row_counts)

        log.info('Confirming that no real tables exists on blackhole dbs')
        verify_blackhole_dbs(destination_master, non_mig_databases)

        if dry_run:
            log.info('In dry_run mode, existing now')
            os._exit(environment_specific.DRY_RUN_EXIT_CODE)

        log.info('Preliminary sanity checks complete, starting migration')

        log.info('Setting read_only on master servers')
        read_only_set = True
        mysql_lib.set_global_variable(source_master, 'read_only', True)

        # We don't strictly need to set read_only on the destination_master
        # but it seems sane out of paranoia
        mysql_lib.set_global_variable(destination_master, 'read_only', True)

        log.info('Confirming no writes to old master')
        mysql_failover.confirm_no_writes(source_master)

        log.info('Waiting for replicas to be caught up')
        wait_for_repl_sync(destination_master)

        log.info('Breaking replication from source to destination')
        mysql_lib.reset_slave(destination_master)

        log.info('Renaming migrated dbs on source master')
        fix_orphaned_shards.rename_db_to_drop(source_master, mig_databases,
                                              skip_check=True)

        log.info('Modifying shard mapping')
        migrate_shard_mapping(source_replica_set,
                              destination_replica_set,
                              mig_databases)

        log.info('Dropping blackhole dbs on destination')
        for db in non_mig_databases:
            mysql_lib.drop_db(destination_master, db)

        start_shard_migration.finish_migration_log(mig_lock_identifier,
                                                   start_shard_migration.STATUS_FINISHED)
    except:
        raise

    finally:
        if read_only_set:
            log.info('Remove read_only settings')
            mysql_lib.set_global_variable(source_master, 'read_only', False)
            mysql_lib.set_global_variable(destination_master, 'read_only', False)

        if dest_lock_identifier:
            log.info('Releasing destination promotion lock')
            mysql_failover.release_promotion_lock(dest_lock_identifier)

        if src_lock_identifier:
            log.info('Releasing source promotion lock')
            mysql_failover.release_promotion_lock(src_lock_identifier)

    # No exception got reraised in the the except, things must have worked
    log.info('To drop migrated dbs on source, wait a bit and then run:')
    log.info('/usr/local/bin/mysql_utils/fix_orphaned_shards.py -a '
             'drop -i {}'.format(source_master))


def verify_schema_for_migration(source_replica_set,
                                destination_replica_set,
                                databases,
                                confirm_row_counts):
    """ Confirm that source and destination have schema and row counts in sync

    Args:
    source - A hostaddr instance for the source
    destination -A hostaddr instance for the destination
    dbs - A set of database to check
    confirm_row_counts - If True, check that row counts are very close to
                         synchronized, otherwise do a very cursory check
    """
    zk = host_utils.MysqlZookeeper()
    source_master = zk.get_mysql_instance_from_replica_set(source_replica_set)
    destination_master = zk.get_mysql_instance_from_replica_set(destination_replica_set)
    source_slave = zk.get_mysql_instance_from_replica_set(source_replica_set,
                                                          host_utils.REPLICA_ROLE_SLAVE)
    destination_slave = zk.get_mysql_instance_from_replica_set(destination_replica_set,
                                                          host_utils.REPLICA_ROLE_SLAVE)
    problems = list()
    for db in databases:
        source_tables = mysql_lib.get_tables(source_master, db)
        destination_tables = mysql_lib.get_tables(destination_master, db)

        differences = source_tables.symmetric_difference(destination_tables)
        if differences:
            problems.append('Found table existence mismatch in db {db}: {dif}'
                            ''.format(db=db, dif=differences))

        for table in source_tables:
            if table not in destination_tables:
                pass
            source_def = mysql_lib.show_create_table(source_master, db, table,
                                                     standardize=True)
            
            destination_def = mysql_lib.show_create_table(destination_master,
                                                          db,
                                                          table,
                                                          standardize=True)

            if source_def != destination_def:
                problems.append('Table definition mismatch db {db} '
                                'table {table}'
                                ''.format(db=db,
                                          table=table))

            cnt_problem = check_row_counts(source_slave, destination_slave,
                                           db, table, exact=confirm_row_counts)
            if cnt_problem:
                problems.append(cnt_problem)

    if problems:
        raise Exception('. '.join(problems))

    log.info('Schema and data appear to be in *NSYNC')


def check_row_counts(source, destination, db, table, exact):
    """ Confirm that source and destination have the same or nearly the
        same row counts

    Args:
    source - A hostaddr instance for the source
    destination -A hostaddr instance for the destination
    db - The database to check
    table - The table to check
    exact - how closely do we want to check this?

    Returns - A message if there is a problem
    """

    if exact:
        source_cnt = mysql_lib.get_row_count(source, db, table)
        destination_cnt = mysql_lib.get_row_count(destination, db, table)
        max_cnt = source_cnt * (1 + ROW_ACTUAL_MAX_DIFF)
        min_cnt = source_cnt * (1 - ROW_ACTUAL_MAX_DIFF)
    else:
        source_cnt = mysql_lib.get_row_estimate(source, db, table)
        destination_cnt = mysql_lib.get_row_estimate(destination, db, table)
        max_cnt = source_cnt * (1 + ROW_ESTIMATE_MAX_DIFF)
        min_cnt = source_cnt * (1 - ROW_ESTIMATE_MAX_DIFF)

    if max_cnt < destination_cnt:
        return ('Table {db}.{table} appears to be too large. '
                'Row count is {cnt} > but max size is {max_cnt} '
                ''.format(db=db,
                          table=table,
                          cnt=destination_cnt,
                          max_cnt=max_cnt))

    if min_cnt > destination_cnt:
        return ('Table {db}.{table} appears to be too small. '
                'Row count is {cnt} < min size {min_cnt} '
                ''.format(db=db,
                          table=table,
                          cnt=destination_cnt,
                          min_cnt=min_cnt))
    return None


def verify_blackhole_dbs(destination, non_mig_databases):
    """ Confirm that non migrated tables have no non-blackhole tables

    Args:
    destination - A hostaddr object
    non_mig_databases - A set of dbs to check
    """
    conn = mysql_lib.connect_mysql(destination)
    cursor = conn.cursor()
    query = ("SELECT COUNT(*) AS 'tbls' "
             "FROM information_schema.tables "
             "WHERE ENGINE !='BLACKHOLE'"
             " AND TABLE_SCHEMA=%(db)s")

    for db in non_mig_databases:
        cursor.execute(query, {'db': db})
        check = cursor.fetchone()
        if check['tbls']:
            raise Exception('Blackhole db {db} has non blackhole table on '
                            'instance {i}'
                            ''.format(db=db,
                                      i=destination))


def check_replication_for_migration(source_replica_set,
                                    destination_replica_set):
    """ Confirm that replication is sane for finishing a shard migration

    Args:
    source_replica_set - Where shards are coming from
    destination_replica_set - Where shards are being sent
    """
    zk = host_utils.MysqlZookeeper()
    source_master = zk.get_mysql_instance_from_replica_set(source_replica_set)
    destination_master = zk.get_mysql_instance_from_replica_set(destination_replica_set)
    source_slave = zk.get_mysql_instance_from_replica_set(source_replica_set,
                                                          host_utils.REPLICA_ROLE_SLAVE)
    destination_slave = zk.get_mysql_instance_from_replica_set(destination_replica_set,
                                                          host_utils.REPLICA_ROLE_SLAVE)

    # First we will confirm that the slave of the source is caught up
    # this is important for row count comparisons
    mysql_lib.assert_replication_unlagged(source_slave,
                                          mysql_lib.REPLICATION_TOLERANCE_NORMAL)

    # Next, the slave of the destination replica set for the same reason
    mysql_lib.assert_replication_unlagged(destination_slave,
                                          mysql_lib.REPLICATION_TOLERANCE_NORMAL)

    # Next, the destination master is relatively caught up to the source master
    mysql_lib.assert_replication_unlagged(destination_master,
                                          mysql_lib.REPLICATION_TOLERANCE_NORMAL)

    # We will also verify that the source master is not replicating. A scary
    # scenario is if the there is some sort of ring replication going and db
    # drops of blackhole db's would propegate to the source db.
    try:
        source_slave_status = mysql_lib.get_slave_status(source_master)
    except mysql_lib.ReplicationError:
        source_slave_status = None

    if source_slave_status:
        raise Exception('Source master is setup for replication '
                        'this is super dangerous!')

    # We will also verify that the destination master is replicating from the
    # source master
    slave_status = mysql_lib.get_slave_status(destination_master)
    master_of_destination_master = host_utils.HostAddr(
            ':'.join((slave_status['Master_Host'],
                      str(slave_status['Master_Port']))))
    if source_master != master_of_destination_master:
            raise Exception('Master of destination {d} is {actual} rather than '
                            'expected {expected} '
                            ''.format(d=destination_master,
                                      actual=master_of_destination_master,
                                      expected=destination_master))
    log.info('Replication looks ok for migration')


def wait_for_repl_sync(instance):
    """ Wait for replication to become synced

    args:
    instance - A hostaddr instance
    """
    start = time.time()
    while True:
        acceptable = True
        try:
            mysql_lib.assert_replication_unlagged(instance,
                                                  mysql_lib.REPLICATION_TOLERANCE_NONE)
        except Exception as e:
            log.warning(e)
            acceptable = False

        if acceptable:
            return
        elif (time.time() - start) > REPL_SYNC_MAX_SECONDS:
            raise Exception('Replication is not in an acceptable state on '
                            'replica {}'.format(instance))
        else:
            log.info('Sleeping for 5 second to allow replication to catch up')
            time.sleep(5)


def migrate_shard_mapping(source_replica_set, destination_replica_set,
                          databases):
    """ Update the shard mapping for a migration

    source_replica_set - The source replica set
    destination_replica_set - The desintation replica set
    databases - A set of databases on the source replica set to
                be migrated
    """
    zk = host_utils.MysqlZookeeper()
    shard_config = mysql_shard_config.MySqlShardConfig(use_test_config=False)
    for db in databases:
        (service, namespace, shard) = zk.find_shard(source_replica_set, db)
        shard_config.migrate_shard(service, namespace, shard,
                                   source_replica_set,
                                   destination_replica_set)
    shard_config.push_config()


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
