#!/usr/bin/env python
import argparse
import logging

from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

log = logging.getLogger(__name__)


def main():
    description = ("MySQL orpahned shard detector\n\n"
                   "This utility will attempt to find orphaned databases "
                   "across sharded MySQL systems")

    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-i',
                        '--instance',
                        help='Check a single instance rather than all',
                        default=False)
    args = parser.parse_args()

    if args.instance:
        instance = host_utils.HostAddr(args.instance)
    else:
        instance = False

    orphaned, orphaned_but_used, missing = find_shard_mismatches(instance)

    for o in orphaned:
        log.info('Orphan dbs: {host} {dbs}'.format(
            host=o, dbs=','.join(orphaned[o])))

    for obu in orphaned_but_used:
        log.info('Orphan, but still used, dbs: {host} {dbs}'.format(
            host=obu, dbs=','.join(orphaned_but_used[obu])))

    for m in missing:
        log.info('Missing dbs:{host} {dbs}'.format(
            host=m, dbs=','.join(missing[m])))

    if not (orphaned or orphaned_but_used or missing):
        log.info('No problems found!')


def find_shard_mismatches(instance=False):
    """ Find shards that are missing or unexpected in a sharded dataset

    Args:
        instance - If supplied, only check this instance.

    Returns:
        orphaned - A dict of unexpected and (according to table statistics)
                   unused dbs. Key is master instance, value is a set.
        orphaned_but_used - A dict of unexpected and but used dbs.
                            Data structure is the same as orphaned.
        missing - A dict of expected but missing dbs.
                  Data structure is the same as orphaned.
    """
    orphaned = dict()
    orphaned_but_used = dict()
    missing_dbs = dict()

    zk = host_utils.MysqlZookeeper()
    rs_dbs_map = zk.get_sharded_dbs_by_replica_set()

    if instance:
        rs = zk.get_replica_set_from_instance(instance)
        rs_dbs_map = {rs: rs_dbs_map[rs]}

    for rs in rs_dbs_map:
        # non-sharded replica sets
        if not len(rs_dbs_map[rs]):
            continue

        expected_dbs = rs_dbs_map[rs]
        instance = zk.get_mysql_instance_from_replica_set(rs)

        activity = mysql_lib.get_dbs_activity(instance)
        actual_dbs = mysql_lib.get_dbs(instance)
        unexpected_dbs = actual_dbs.difference(expected_dbs)
        missing = expected_dbs.difference(actual_dbs)
        if missing:
            missing_dbs[instance] = expected_dbs.difference(actual_dbs)

        for db in unexpected_dbs:
            if activity[db]['ROWS_CHANGED'] != 0:
                if instance not in orphaned_but_used:
                    orphaned_but_used[instance] = set()
                orphaned_but_used[instance].add(db)
            else:
                if instance not in orphaned:
                    orphaned[instance] = set()
                orphaned[instance].add(db)

    return orphaned, orphaned_but_used, missing_dbs


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
