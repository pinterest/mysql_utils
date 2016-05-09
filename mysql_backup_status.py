#!/usr/bin/python
import argparse
import copy
import datetime
import multiprocessing
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
CSV_CHECK_PROCESSES = 8
CSV_STARTUP = datetime.time(0, 15)
CSV_COMPLETION_TIME = datetime.time(2, 30)
MISSING_BACKUP_VERBOSE_LIMIT = 20
CSV_BACKUP_LOG_TABLE_DEFINITION = """CREATE TABLE {db}.{tbl} (
 `backup_date` date NOT NULL,
 `completion` datetime DEFAULT NULL,
 PRIMARY KEY (`backup_date`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 """


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


def verify_csv_backup(shard_type, date, instance=None):
    """ Verify csv backup(s)

    Args:
    shard_type - Type of mysql db
    date - date as a string
    instance - (optional) HostAddr instance

    Returns:
    True if backup is verified, false otherwise
    """
    if instance and csv_backup_success_logged(instance, date):
        print ('Per csv backup success log, backup has already been '
               'verified')
        return True

    if instance and early_verification(date, instance):
        print 'Backups are currently running'
        return True

    if shard_type in environment_specific.SHARDED_DBS_PREFIX_MAP:
        ret = verify_sharded_csv_backup(shard_type, date, instance)
    elif shard_type in environment_specific.FLEXSHARD_DBS:
        ret = verify_flexsharded_csv_backup(shard_type, date, instance)
    else:
        ret = verify_unsharded_csv_backup(shard_type, date, instance)

    if instance and ret:
        log_csv_backup_success(instance, date)

    return ret


def verify_flexsharded_csv_backup(shard_type, date, instance=None):
    """ Verify that a flexsharded data set has been backed up to hive

    Args:
    shard_type -  i.e. 'commercefeeddb', etc
    date - The date to search for
    instance - Restrict the search to problem on a single instnace

    Returns True for no problems found, False otherwise.
    """
    success = True
    replica_sets = set()
    zk = host_utils.MysqlZookeeper()
    if instance:
        replica_sets.add(zk.get_replica_set_from_instance(instance)[0])
    else:
        for replica_set in zk.get_all_mysql_replica_sets():
            if replica_set.startswith(environment_specific.FLEXSHARD_DBS[shard_type]['zk_prefix']):
                replica_sets.add(replica_set)

    schema_host = zk.get_mysql_instance_from_replica_set(
            environment_specific.FLEXSHARD_DBS[shard_type]['example_shard_replica_set'],
            repl_type=host_utils.REPLICA_ROLE_SLAVE)

    boto_conn = boto.connect_s3()
    bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
    missing_uploads = set()

    for db in mysql_lib.get_dbs(schema_host):
        for table in mysql_backup_csv.mysql_backup_csv(schema_host).get_tables_to_backup(db):
            if not verify_csv_schema_upload(shard_type, date, schema_host, db, [table]):
                success = False
                continue

            table_missing_uploads = set()
            for replica_set in replica_sets:
                chk_instance = zk.get_mysql_instance_from_replica_set(replica_set)
                (_, data_path, success_path) = environment_specific.get_csv_backup_paths(
                                                   date, db, table, chk_instance.replica_type,
                                                   chk_instance.get_zk_replica_set()[0])
                if not bucket.get_key(data_path):
                    table_missing_uploads.add(data_path)
                    success = False

            if not table_missing_uploads and not instance:
                if not bucket.get_key(success_path):
                    print 'Creating success key {key}'.format(key=success_path)
                    key = bucket.new_key(success_path)
                    key.set_contents_from_string('')

            missing_uploads.update(table_missing_uploads)

    if missing_uploads:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print ('Shard type {shard_type} is missing uploads:'
                   ''.format(shard_type=shard_type))
            pprint.pprint(missing_uploads)
        else:
            print ('Shard type {shard_type} is missing {num} uploads'
                   ''.format(num=len(missing_uploads),
                             shard_type=shard_type))

    if not missing_uploads and not instance and success:
        print 'Shard type {shard_type} is backed up'.format(shard_type=shard_type)

    return success


def verify_sharded_csv_backup(shard_type, date, instance=None):
    """ Verify that a sharded data set has been backed up to hive

    Args:
    shard_type -  i.e. 'sharddb', etc
    date - The date to search for
    instance - Restrict the search to problem on a single instnace

    Returns True for no problems found, False otherwise.
    """
    zk = host_utils.MysqlZookeeper()
    example_shard = environment_specific.SHARDED_DBS_PREFIX_MAP[shard_type]['example_shard']
    schema_host = zk.shard_to_instance(example_shard, repl_type=host_utils.REPLICA_ROLE_SLAVE)
    tables = mysql_backup_csv.mysql_backup_csv(schema_host).get_tables_to_backup(environment_specific.convert_shard_to_db(example_shard))
    success = verify_csv_schema_upload(shard_type, date, schema_host,
                                       environment_specific.convert_shard_to_db(example_shard), tables)
    if instance:
        host_shard_map = zk.get_host_shard_map()
        (replica_set, replica_type) = zk.get_replica_set_from_instance(instance)
        master = zk.get_mysql_instance_from_replica_set(replica_set, host_utils.REPLICA_ROLE_MASTER)
        shards = host_shard_map[master.__str__()]
    else:
        shards = zk.get_shards_by_shard_type(shard_type)

    pool = multiprocessing.Pool(processes=CSV_CHECK_PROCESSES)
    pool_args = list()
    if not tables:
        raise Exception('No tables will be checked for backups')
    if not shards:
        raise Exception('No shards will be checked for backups')

    for table in tables:
        pool_args.append((table, shard_type, date, shards))
    results = pool.map(get_missing_uploads, pool_args)
    missing_uploads = set()
    for result in results:
        missing_uploads.update(result)

    if missing_uploads or not success:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print ('Shard type {shard_type} is missing uploads:'
                   ''.format(shard_type=shard_type))
            pprint.pprint(missing_uploads)
        else:
            print ('Shard type {shard_type} is missing {num} uploads'
                   ''.format(num=len(missing_uploads),
                             shard_type=shard_type))
        return False
    else:
        if instance:
            print 'Instance {instance} is backed up'.format(instance=instance)
        else:
            # we have checked all shards, all are good, create success files
            boto_conn = boto.connect_s3()
            bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
            for table in tables:
                (_, _, success_path) = environment_specific.get_csv_backup_paths(date,
                                                                                 environment_specific.convert_shard_to_db(example_shard),
                                                                                 table, shard_type)
                if not bucket.get_key(success_path):
                    print 'Creating success key {key}'.format(key=success_path)
                    key = bucket.new_key(success_path)
                    key.set_contents_from_string('')
            print 'Shard type {shard_type} is backed up'.format(shard_type=shard_type)

        return True


def get_missing_uploads(args):
    """ Check to see if all backups are present

    Args: A tuple which can be expanded to:
    table - table name
    shard_type -  sharddb, etc
    shards -  a set of shards

    Returns: a set of shards which are not backed up
    """
    (table, shard_type, date, shards) = args
    expected_s3_keys = set()
    prefix = None

    for shard in shards:
        (_, data_path, _) = environment_specific.get_csv_backup_paths(
                                date, environment_specific.convert_shard_to_db(shard),
                                table, shard_type)
        expected_s3_keys.add(data_path)
        if not prefix:
            prefix = os.path.dirname(data_path)

    boto_conn = boto.connect_s3()
    bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
    uploaded_keys = set()
    for key in bucket.list(prefix=prefix):
        uploaded_keys.add(key.name)

    missing_uploads = expected_s3_keys.difference(uploaded_keys)

    for entry in copy.copy(missing_uploads):
        # the list api occassionally has issues, so we will recheck any missing
        # entries. If any are actually missing we will quit checking because
        # there is definitely work that needs to be done
        if bucket.get_key(entry):
            print 'List method erronious did not return data for key:{entry}'.format(entry=entry)
            missing_uploads.discard(entry)
        else:
            return missing_uploads

    return missing_uploads


def verify_unsharded_csv_backup(shard_type, date, instance):
    """ Verify that a non-sharded db has been backed up to hive

    Args:
    shard_type - In this case, a hostname prefix
    date - The date to search for
    instance - The actual instance to inspect for backups being done

    Returns True for no problems found, False otherwise.
    """
    return_status = True
    boto_conn = boto.connect_s3()
    bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
    missing_uploads = set()
    for db in mysql_lib.get_dbs(instance):
        tables = mysql_backup_csv.mysql_backup_csv(instance).get_tables_to_backup(db)
        for table in tables:
            if not verify_csv_schema_upload(shard_type, date, instance, db,
                                            set([table])):
                return_status = False
                print 'Missing schema for {db}.{table}'.format(db=db,
                                                               table=table)
                continue

            (_, data_path, success_path) = \
                environment_specific.get_csv_backup_paths(date, db, table,
                                                          instance.replica_type,
                                                          instance.get_zk_replica_set()[0])
            if not bucket.get_key(data_path):
                missing_uploads.add(data_path)
            else:
                # we still need to create a success file for the data
                # team for this table, even if something else is AWOL
                # later in the backup.
                if bucket.get_key(success_path):
                    print 'Key already exists {key}'.format(key=success_path)
                else:
                    print 'Creating success key {key}'.format(key=success_path)
                    key = bucket.new_key(success_path)
                    key.set_contents_from_string('')

    if missing_uploads:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print 'Missing uploads: {uploads}'.format(uploads=missing_uploads)
        else:
            print 'Missing {num} uploads'.format(num=len(missing_uploads))
        return_status = False

    return return_status


def early_verification(date, instance):
    """ Just after UTC midnight we don't care about backups. For a bit after
        that we just care that backups are running

    Args:
    date - The backup date to check
    instance - What instance is being checked

    Returns:
    True if backups are running or it is too early, False otherwise
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


def log_csv_backup_success(instance, date):
    """ The CSV backup check can be expensive, so let's log that it is done

    Args:
    instance - A hostaddr object
    date - a string for the date
    """
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)[0]
    master = zk.get_mysql_instance_from_replica_set(replica_set)
    conn = mysql_lib.connect_mysql(master, 'scriptrw')
    cursor = conn.cursor()

    if not mysql_lib.does_table_exist(master, mysql_lib.METADATA_DB,
                                      environment_specific.CSV_BACKUP_LOG_TABLE):
            print 'Creating missing metadata table'
            cursor.execute(CSV_BACKUP_LOG_TABLE_DEFINITION.format(
                               db=mysql_lib.METADATA_DB,
                               tbl=environment_specific.CSV_BACKUP_LOG_TABLE))

    sql = ('INSERT IGNORE INTO {METADATA_DB}.{CSV_BACKUP_LOG_TABLE} '
           'SET backup_date = %(date)s, '
           'completion = NOW()'
           ''.format(METADATA_DB=mysql_lib.METADATA_DB,
                     CSV_BACKUP_LOG_TABLE=environment_specific.CSV_BACKUP_LOG_TABLE))
    cursor.execute(sql, {'date': date})
    conn.commit()


def csv_backup_success_logged(instance, date):
    """ Check for log entries created by log_csv_backup_success

    Args:
    instance - A hostaddr object
    date - a string for the date

    Returns:
    True if already backed up, False otherwise
    """
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)[0]
    master = zk.get_mysql_instance_from_replica_set(replica_set)
    conn = mysql_lib.connect_mysql(master, 'scriptrw')
    cursor = conn.cursor()

    if not mysql_lib.does_table_exist(master, mysql_lib.METADATA_DB,
                                      environment_specific.CSV_BACKUP_LOG_TABLE):
        return False

    sql = ('SELECT COUNT(*) as "cnt" '
           'FROM {METADATA_DB}.{CSV_BACKUP_LOG_TABLE} '
           'WHERE backup_date = %(date)s '
           ''.format(METADATA_DB=mysql_lib.METADATA_DB,
                     CSV_BACKUP_LOG_TABLE=environment_specific.CSV_BACKUP_LOG_TABLE))
    cursor.execute(sql, {'date': date})
    if cursor.fetchone()["cnt"]:
        return True
    else:
        return False


def verify_csv_schema_upload(shard_type, date, instance, schema_db,
                             tables):
    """ Confirm that schema files are uploaded

    Args:
    shard_type - In this case, a hostname or shard type (generally
                 one in the same)
    date - The date to search for
    schema_host - A host to examine to find which tables should exist
    schema_db - Which db to inxpect on schema_host
    tables - A set of which tables to check in schema_db for schema upload

    Returns True for no problems found, False otherwise.
    """
    return_status = True
    missing = set()
    boto_conn = boto.connect_s3()
    bucket = boto_conn.get_bucket(environment_specific.S3_CSV_BUCKET, validate=False)
    for table in tables:
        (path, _, _) = environment_specific.get_csv_backup_paths(
                           date, schema_db, table,
                           instance.replica_type,
                           instance.get_zk_replica_set()[0])
        if not bucket.get_key(path):
            missing.add(path)
            return_status = False

    if missing:
        print 'Expected schema files are missing: {missing}'.format(missing=missing)
    return return_status


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
                        choices=(environment_specific.SHARDED_DBS_PREFIX_MAP.keys() +
                                 environment_specific.FLEXSHARD_DBS.keys()))
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

        if args.all:
            if instance:
                raise Exception('Arguments all and instance can not be '
                                'used together')
            shard_types = (environment_specific.SHARDED_DBS_PREFIX_MAP.keys() +
                           environment_specific.FLEXSHARD_DBS.keys())
        else:
            if args.shard_type:
                shard_types = [args.shard_type]
            else:
                shard_types = [instance.replica_type]

        if not shard_types and not instance:
            raise Exception('For CSV backup verification, either a shard type '
                            'or an instance is required')

        # by convention, the csv backup files use the previous days date
        split_date = args.date.split('-')
        date = (datetime.date(int(split_date[0]),
                              int(split_date[1]),
                              int(split_date[2])) -
                datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        for shard_type in shard_types:
            if not verify_csv_backup(shard_type, date, instance):
                return_code = BACKUP_MISSING_RETURN

    else:
        raise Exception('Backup type unsupported')

    if args.show_found and backups:
        pprint.pprint(backups)
    sys.exit(return_code)


if __name__ == '__main__':
    main()
