#!/usr/bin/python
"""
    MySQL backup verification utility based on current list of replica servers
    in ZooKeeper and what is present in s3. If a discrepancy is found,
    exits 1 and shows errors.
"""

__author__ = ("rwultsch@pinterest.com (Rob Wultsch)")


import argparse
import time
import sys

from lib import backup
from lib import host_utils


BACKUP_OK_RETURN = 0
BACKUP_MISSING_RETURN = 1
BACKUP_NOT_IN_ZK_RETURN = 127


def find_mysql_backup(replica_set, date):
    """ Check whether or not a given replica set has a backup in S3

    Args:
        replica_set: The replica set we're checking for.
        date: The date to search for.

    Returns:
        location: The location of the backup for this replica set.
                  Returns None if not found.
    """
    zk = host_utils.MysqlZookeeper()
    for repl_type in host_utils.REPLICA_TYPES:
        instance = zk.get_mysql_instance_from_replica_set(replica_set,
                                                          repl_type)
        if instance:
            try:
                backup_file = backup.get_s3_backup(instance, date)
                if backup_file:
                    return backup_file
                break
            except:
                # we'll get a 404 if there was no s3 backup, but that's OK,
                # so we can just move on to the next one.
                pass
    return None


def main():
    parser = argparse.ArgumentParser(description='MySQL backup reporting')
    parser.add_argument("-d",
                        "--date",
                        default=time.strftime('%Y-%m-%d'),
                        help="Backup date. Ex: 2013-12-12")
    parser.add_argument("-f",
                        "--show_found",
                        default=False,
                        action='store_true',
                        help="Display found backups")
    parser.add_argument("-i",
                        "--instance",
                        default=host_utils.HOSTNAME,
                        help=("Check backup status for this instance if "
                              "not the default (localhost:3306)"))
    parser.add_argument("-a",
                        "--all",
                        action='store_true',
                        help="Check all replica sets")

    args = parser.parse_args()
    zk = host_utils.MysqlZookeeper()

    if args.all:
        replica_sets = zk.get_all_mysql_replica_sets()
    else:
        instance = host_utils.HostAddr(args.instance)

        # if we aren't in ZK, we will exit with a special return code
        # that can be picked up by the nagios check.
        try:
            (replica_set, _) = zk.get_replica_set_from_instance(instance)
            replica_sets = set([replica_set])
        except Exception as e:
            print "Nothing known about backups for {i}: {e}".format(i=instance, e=e)
            sys.exit(BACKUP_NOT_IN_ZK_RETURN)

    return_code = BACKUP_OK_RETURN
    for replica_set in replica_sets:
        found_backup = find_mysql_backup(replica_set, args.date)
        if found_backup is not None:
            if args.show_found:
                print "{file}".format(file=found_backup)
        else:
            print "Backup not found for replica set {rs}".format(rs=replica_set)
            return_code = BACKUP_MISSING_RETURN
    sys.exit(return_code)


if __name__ == '__main__':
    main()
