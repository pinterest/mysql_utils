#!/usr/bin/env python
import argparse
from lib import host_utils
from lib import mysql_lib


def main():
    description = ("MySQL orpahned shard detector\n\n"
                   "This utility will attempt to find orphaned databases "
                   "across sharddb and modsharddb")

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

    for orphan in orphaned:
        print 'Orphan dbs {host} {dbs}'.format(host=orphan,
                                               dbs=','.join(orphaned[orphan]))
    for orphan in orphaned_but_used:
        print 'Orphan but still used dbs {host} {dbs}'.format(host=orphan,
                                                              dbs=','.join(orphaned_but_used[orphan]))
    for orphan in missing:
        print 'Missing dbs {host} {dbs}'.format(host=orphan,
                                                dbs=','.join(missing[orphan]))

    if not orphaned and not orphaned_but_used and not missing:
        print "No problems found"


def find_shard_mismatches(instance=False):
    """ Find shards that are missing or unexpected in modhsarddb and sharddb

    Args:
    instance - If supplied, only check this instance.

    Returns:
    orphaned - A dict of unexpected and (according to table statistics)
               unused shards. Key is master instance, value is a set.
    orphaned_but_used - A dict of unexpected and but used shards.
                        Data strucutre is the same as orphaned.
    missing - A dict of expected but missing shards.
              Data strucutre is the same as orphaned.

    """
    orphaned = dict()
    orphaned_but_used = dict()
    missing_shards = dict()

    zk = host_utils.MysqlZookeeper()
    host_shard_map = zk.get_host_shard_map()

    if instance:
        new_host_shard_map = dict()
        new_host_shard_map[instance.__str__()] = host_shard_map[instance.__str__()]
        host_shard_map = new_host_shard_map

    for master in host_shard_map:
        expected_shards = host_shard_map[master]
        instance = host_utils.HostAddr(master)
        conn = mysql_lib.connect_mysql(instance)
        activity = mysql_lib.get_dbs_activity(conn)
        actual_shards = mysql_lib.get_dbs(conn)
        unexpected_shards = actual_shards.difference(expected_shards)
        missing = expected_shards.difference(actual_shards)
        if missing:
            missing_shards[master] = expected_shards.difference(actual_shards)

        for db in unexpected_shards:
            if activity[db]['ROWS_CHANGED'] != 0:
                if master not in orphaned_but_used:
                    orphaned_but_used[master] = set()
                orphaned_but_used[master].add(db)
            else:
                if master not in orphaned:
                    orphaned[master] = set()
                orphaned[master].add(db)

    return orphaned, orphaned_but_used, missing_shards


if __name__ == "__main__":
    main()
