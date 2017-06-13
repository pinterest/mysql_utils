#!/usr/bin/env python
import argparse
import logging

import fix_orphaned_shards
import find_shard_mismatches
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
                        help='Which replica set shards were being '
                             'moved *FROM*')
    args = parser.parse_args()

    clean_up_migration(args.source_replica_set)


def clean_up_migration(source_replica_set):
    migration = start_shard_migration.check_migration_lock(source_replica_set)
    destination_replica_set = migration['destination_replica_set']
    mig_lock_identifier = migration['lock_identifier']
    zk = host_utils.MysqlZookeeper()
    destination_master = zk.get_mysql_instance_from_replica_set(destination_replica_set)

    try:
        mysql_lib.get_slave_status(destination_master)
        reset_repl = True
    except:
        reset_repl = False

    if reset_repl:
        log.info('Taking promotion locks')
        dest_lock_identifier = mysql_failover.get_promotion_lock(destination_replica_set)
        log.info('Removing replication from destination master {}'
                 ''.format(destination_master))
        try:
            mysql_lib.reset_slave(destination_master)
        except:
            raise
        finally:
            mysql_failover.release_promotion_lock(dest_lock_identifier)

    (orphans_tmp, orphaned_but_used_tmp, _) = \
            find_shard_mismatches.find_shard_mismatches(destination_master)

    orphans = orphans_tmp[destination_master] if \
            destination_master in orphans_tmp else []
    orphaned_but_used = orphaned_but_used_tmp[destination_master] if \
            destination_master in orphaned_but_used_tmp else []

    if orphaned_but_used:
        log.info('Orphaned but used dbs: {}'.format(', '.join(orphaned_but_used)))
        raise Exception('Cowardly refusing to do anything')

    if orphans:
        log.info('Orphaned dbs: {}'.format(', '.join(orphans)))
        fix_orphaned_shards.rename_db_to_drop(destination_master, orphans)

    start_shard_migration.finish_migration_log(mig_lock_identifier,
                                               start_shard_migration.STATUS_ABORTED)


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
