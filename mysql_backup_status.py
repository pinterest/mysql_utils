#!/usr/bin/python
"""
    MySQL backup verification utility based on current list of replica servers
    in ZooKeeper and what is present in s3. If a discrepancy is found,
    exits 1 and shows errors.
"""

__author__ = ("rwultsch@pinterest.com (Rob Wultsch)")


import argparse
import datetime
import os
import sys
import pprint

import boto

import mysql_backup_csv
from lib import backup
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib


BACKUP_OK_RETURN = 0
BACKUP_MISSING_RETURN = 1
BACKUP_NOT_IN_ZK_RETURN = 127
CSV_STARTUP = datetime.time(0, 15)
CSV_COMPLETION_TIME = datetime.time(2, 30)
MISSING_BACKUP_VERBOSE_LIMIT = 20


def find_mysql_backup(replica_set, date, backup_type):
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
                backup_file = backup.get_s3_backup(instance, date, backup_type)
                if backup_file:
                    return backup_file
                break
            except:
                # we'll get a 404 if there was no s3 backup, but that's OK,
                # so we can just move on to the next one.
                pass
    return None


def verify_sharded_csv_backup(shard_type, date, instance=None):
    """ Verify that a sharded data set has been backed up to hive

    Args:
    shard_type -  i.e. 'sharddb', etc
    date - The date to search for
    instance - Restrict the search to problem on a single instnace

    Returns True for no problems found, False otherwise.
    """

    if instance:
        # if we are running the check on a specific instance, we will add in
        # some additional logic to not cause false alarms during the backup
        # process.
        if (date == (datetime.datetime.utcnow().date() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")):
            # For todays date, we give CSV_STARTUP minutes before checking anything.
            if datetime.datetime.utcnow().time() < CSV_STARTUP:
                print 'Backup startup time has not yet passed'
                return True

            if datetime.datetime.utcnow().time() < CSV_COMPLETION_TIME:
                # For todays date, until after CSV_COMPLETION_TIME it is good enough
                # to check if backups are running. If they are running, everything
                # is ok. If they are not running, we will do all the normal checks.
                if csv_backups_running(instance):
                    print 'Backup running on {i}'.format(i=instance)
                    return True

    schema_db = environment_specific.SHARDED_DBS_PREFIX_MAP[shard_type]['example_schema']
    zk = host_utils.MysqlZookeeper()
    schema_host = zk.shard_to_instance(schema_db, repl_type=host_utils.REPLICA_ROLE_SLAVE)
    (success, tables) = \
        verify_csv_schema_upload(shard_type, date, schema_host, schema_db,
                                 mysql_backup_csv.PATH_DAILY_BACKUP_SHARDED_SCHEMA)

    if not success:
        # problem with schema, don't bother verifying data
        return False

    if instance:
        host_shard_map = zk.get_host_shard_map()
        (replica_set, replica_type) = zk.get_replica_set_from_instance(instance)
        master = zk.get_mysql_instance_from_replica_set(replica_set, host_utils.REPLICA_ROLE_MASTER)
        shards = host_shard_map[master.__str__()]
    else:
        shards = zk.get_shards_by_shard_type(shard_type)

    missing_uploads = set()
    for table in tables:
        expected_s3_keys = set()
        prefix = None
        for shard in shards:
            key = mysql_backup_csv.PATH_DAILY_BACKUP.format(table=table,
                                                            hostname_prefix=shard_type,
                                                            date=date,
                                                            db_name=environment_specific.convert_shard_to_db(shard))
            expected_s3_keys.add(key)
            if not prefix:
                prefix = os.path.dirname(key)

        boto_conn = boto.connect_s3()
        bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
        uploaded_keys = set()
        for key in bucket.list(prefix=prefix):
            uploaded_keys.add(key.name)
        missing = expected_s3_keys.difference(uploaded_keys)
        if missing and table not in environment_specific.IGNORABLE_MISSING_TABLES:
            missing_uploads.update(missing)

    if missing_uploads:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print 'Missing uploads: {uploads}'.format(uploads=missing_uploads)
        else:
            print 'Missing {num} uploads'.format(num=len(missing_uploads))
        return False
    else:
        if not instance:
            # we have checked all shards, all are good, create success files
            for table in tables:
                key_name = mysql_backup_csv.PATH_DAILY_SUCCESS.format(table=table,
                                                                      hostname_prefix=shard_type,
                                                                      date=date)
                if bucket.get_key(key_name):
                    print 'Key already exists {key}'.format(key=key)
                else:
                    print 'Creating success key {key}'.format(key=key)
                    key = bucket.new_key(key_name)
                    key.set_contents_from_string('')

        return True


def verify_unsharded_csv_backup(shard_type, date, instance):
    """ Verify that a non-sharded db has been backed up to hive

    Args:
    shard_type - In this case, a hostname prefix
    date - The date to search for
    instance - The actual instance to inspect for backups being done

    Returns True for no problems found, False otherwise.
    """
    if (date == (datetime.datetime.utcnow().date() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")):
        if datetime.datetime.utcnow().time() < CSV_STARTUP:
            print 'Backup startup time has not yet passed'
            # For todays date, we give CSV_STARTUP minutes before checking anything.
            return True

        if datetime.datetime.utcnow().time() < CSV_COMPLETION_TIME:
            # For todays date, until after CSV_COMPLETION_TIME it is good enough
            # to check if backups are running. If they are running, everything
            # is ok. If they are not running, we will do all the normal checks.
            if csv_backups_running(instance):
                print 'Backup running on {i}'.format(i=instance)
                return True

    return_status = True
    for db in mysql_lib.get_dbs(instance):
        (success, _) = \
            verify_csv_schema_upload(shard_type, date, instance, db,
                                     mysql_backup_csv.PATH_DAILY_BACKUP_NONSHARDED_SCHEMA)
        if not success:
            return_status = False

    if not return_status:
        print 'missing schema file'
        # problem with schema, don't bother verifying data
        return return_status

    boto_conn = boto.connect_s3()
    bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
    missing_uploads = set()
    for db in mysql_lib.get_dbs(instance):
        for table in mysql_lib.get_tables(instance, db, skip_views=True):
            key = mysql_backup_csv.PATH_DAILY_BACKUP.format(table=table,
                                                            hostname_prefix=shard_type,
                                                            date=date,
                                                            db_name=db)
            if not bucket.get_key(key):
                missing_uploads.add(key)
            else:
                # we still need to create a success file for the data
                # team for this table, even if something else is AWOL
                # later in the backup.
                key_name = mysql_backup_csv.PATH_DAILY_SUCCESS.format(
                        table=table, hostname_prefix=shard_type, date=date)
                if bucket.get_key(key_name):
                    print 'Key already exists {key}'.format(key=key_name)
                else:
                    print 'Creating success key {key}'.format(key=key_name)
                    key = bucket.new_key(key_name)
                    key.set_contents_from_string('')

    if missing_uploads:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print 'Missing uploads: {uploads}'.format(uploads=missing_uploads)
        else:
            print 'Missing {num} uploads'.format(num=len(missing_uploads))
    else:
        return True


def csv_backups_running(instance):
    """ Check to see if csv dumps are running

    Args:
    instance - we will use this to determine the replica set

    Returns:
    True if backups are running, False otherwise
    """
    (dump_user, _) = mysql_lib.get_mysql_user_for_role(backup.USER_ROLE_MYSQLDUMP)
    replica_set = instance.get_zk_replica_set()[0]
    zk = host_utils.MysqlZookeeper()

    for slave_role in [host_utils.REPLICA_ROLE_DR_SLAVE, host_utils.REPLICA_ROLE_SLAVE]:
        slave_instance = zk.get_mysql_instance_from_replica_set(replica_set, slave_role)
        if not slave_instance:
            continue

        if dump_user in mysql_lib.get_connected_users(slave_instance):
            return True

    return False


def verify_csv_schema_upload(shard_type, date, schema_host, schema_db,
                             schema_upload_path_raw):
    """ Confirm that schema files are uploaded

    Args:
    shard_type - In this case, a hostname or shard type (generally
                 one in the same)
    date - The date to search for
    schema_host - A for to examine to find which tables should exist
    schema_db - Which db to inxpect on schema_host
    schema_upload_path_raw - A string that can be format'ed in order to create
                             a S3 key path

    Returns True for no problems found, False otherwise.
    """
    boto_conn = boto.connect_s3()
    bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
    tables = mysql_lib.get_tables(schema_host,
                                  environment_specific.convert_shard_to_db(schema_db),
                                  skip_views=True)
    return_status = True
    for table in tables:
        path = schema_upload_path_raw.format(table=table,
                                             hostname_prefix=shard_type,
                                             date=date,
                                             db_name=schema_db)
        if not bucket.get_key(path):
            print 'Expected key {key} is missing'.format(key=path)
            return_status = False
    return return_status, tables


def main():
    parser = argparse.ArgumentParser(description='MySQL backup reporting')
    parser.add_argument('-t',
                        '--backup_type',
                        default=backup.BACKUP_TYPE_XBSTREAM,
                        choices=backup.BACKUP_TYPES)
    parser.add_argument("-d",
                        "--date",
                        default=datetime.datetime.utcnow().strftime('%Y-%m-%d'),
                        help="Backup date. Ex: 2013-12-12. With csv backups, "
                             "a day is subtracted. IE a backup done on Jan 2, "
                             "will be placed in the file for Jan 1.")
    parser.add_argument("-f",
                        "--show_found",
                        default=False,
                        action='store_true',
                        help="Display found backups")
    parser.add_argument("-i",
                        "--instance",
                        default=None,
                        help=("Check backup status for this instance if "
                              "not the default (localhost:3306)"))
    parser.add_argument('-s',
                        '--shard_type',
                        default=None,
                        help='Used for csv backups for success file creation',
                        choices=environment_specific.SHARDED_DBS_PREFIX_MAP)
    parser.add_argument("-a",
                        "--all",
                        action='store_true',
                        help=("Check all replica sets for xbstream or sql.gz "
                              "and all shard types (but not unsharded) for "
                              "csv backups"))
    args = parser.parse_args()

    zk = host_utils.MysqlZookeeper()
    return_code = BACKUP_OK_RETURN
    backups = []
    if (args.backup_type == backup.BACKUP_TYPE_XBSTREAM or
            args.backup_type == backup.BACKUP_TYPE_LOGICAL):
        if args.all:
            replica_sets = zk.get_all_mysql_replica_sets()
        else:
            # if we aren't in ZK, we will exit with a special return code
            # that can be picked up by the nagios check.
            if args.instance:
                instance = host_utils.HostAddr(args.instance)
            else:
                instance = host_utils.HostAddr(host_utils.HOSTNAME)

            try:
                (replica_set, _) = zk.get_replica_set_from_instance(instance)
                replica_sets = set([replica_set])
            except Exception as e:
                print "Nothing known about backups for {i}: {e}".format(i=instance, e=e)
                sys.exit(BACKUP_NOT_IN_ZK_RETURN)

        for replica_set in replica_sets:
            backup_file = find_mysql_backup(replica_set, args.date, args.backup_type)
            if backup_file:
                backups.append(backup_file)
            else:
                return_code = BACKUP_MISSING_RETURN
                if args.show_found:
                    print ("Backup not found for replica set {rs}"
                           "".format(rs=replica_set))
    elif args.backup_type == backup.BACKUP_TYPE_CSV:
        if args.instance:
            instance = host_utils.HostAddr(args.instance)
        else:
            instance = None

        # by convention, the csv backup files use the previous days date
        split_date = args.date.split('-')
        date = (datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
                - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        if args.all:
            for shard_type in environment_specific.SHARDED_DBS_PREFIX_MAP:
                print "checking {shard_type}".format(shard_type=shard_type)
                if not verify_sharded_csv_backup(shard_type, date, instance):
                    return_code = BACKUP_MISSING_RETURN
        else:
            if not args.shard_type and not instance:
                raise Exception('For CSV backup verification, either a shard type '
                                'or an instance is required')

            if args.shard_type:
                shard_type = args.shard_type
            else:
                shard_type = instance.replica_type

            # Make sure the schema files are uploaded
            if shard_type in environment_specific.SHARDED_DBS_PREFIX_MAP:
                if not verify_sharded_csv_backup(shard_type, date, instance):
                    return_code = BACKUP_MISSING_RETURN
            else:
                if not verify_unsharded_csv_backup(shard_type, date, instance):
                    return_code = BACKUP_MISSING_RETURN
    else:
        raise Exception('Backup type unsupported')

    if args.show_found and backups:
        pprint.pprint(backups)
    sys.exit(return_code)


if __name__ == '__main__':
    main()
