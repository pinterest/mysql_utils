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
CSV_BACKUP_LOG_TABLE_DEFINITION = """CREATE TABLE IF NOT EXISTS {db}.{tbl} (
 `backup_date` date NOT NULL,
 `completion` datetime DEFAULT NULL,
 `dev_bucket` TINYINT NOT NULL DEFAULT 0,
 PRIMARY KEY (`backup_date`, `dev_bucket`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 """


def is_sharded_but_not_sharded(replica_set):
    """ We have a few systems which look like they're sharded but they
        aren't - i.e., this means they are in the shard config but there
        is only one of them, so we have to check them as if they were
        unsharded.  There are only 3 of these.

    Args:
        replica_set: What replica set is this
    Returns:
        True or False - default is to False
    """
    if replica_set in ('interest', 'metadata', 'generalmysql001'):
        return True
    return False


def find_mysql_backup(replica_set, date, backup_type):
    """ Check whether or not a given replica set has a logical or physical
        backup in S3

    Args:
        replica_set: The replica set we're checking for.
        date: The date to search for.

    Returns:
        location: The location of the backup for this replica set.
                  Returns None if not found.
    """
    zk = host_utils.MysqlZookeeper()
    for repl_type in host_utils.REPLICA_TYPES:
        instance = zk.get_mysql_instance_from_replica_set(replica_set, repl_type)
        if instance:
            try:
                backup_file = backup.get_s3_backup(instance, date, backup_type)
                if backup_file:
                    return backup_file
                break
            except boto.exception.S3ResponseError:
                raise
            except Exception as e:
                if backup.NO_BACKUP not in e[0]:
                    raise
    return None


def verify_csv_full_sharded_systems_backup(shard_type, date, dev_bucket=False):
    """ Verify csv backup(s)

    Args:
        shard_type - Type of mysql db
        date - date as a string
        dev_bucket - Check the dev bucket?

    Returns:
        True if backup is verified, false otherwise
    """
    zk = host_utils.MysqlZookeeper()
    if shard_type in zk.get_sharded_types():
        ret = verify_sharded_csv_backup_by_shard_type(shard_type, date, dev_bucket)
    elif shard_type in environment_specific.FLEXSHARD_DBS:
        ret = verify_flexsharded_csv_backup(shard_type, date, dev_bucket)
    else:
        raise Exception('Unsupported shard type {}'.format(shard_type))

    return ret


def verify_flexsharded_csv_backup(shard_type, date, dev_bucket=False):
    """ Verify that a flexsharded data set has been backed up to hive

    Args:
        shard_type -  i.e. 'commercefeeddb', etc
        date - The date to search for
        dev_bucket - Look in the dev bucket?

    Returns:
        True for no problems found, False otherwise.
    """
    success = True
    replica_sets = set()
    zk = host_utils.MysqlZookeeper()

    # Figure out what replica sets to check based on a prefix
    for replica_set in zk.get_all_mysql_replica_sets():
        if replica_set.startswith(environment_specific.FLEXSHARD_DBS[shard_type]['zk_prefix']):
            replica_sets.add(replica_set)

    # Example schema host
    schema_host = zk.get_mysql_instance_from_replica_set(
            environment_specific.FLEXSHARD_DBS[shard_type]['example_shard_replica_set'],
            repl_type=host_utils.REPLICA_ROLE_SLAVE)

    boto_conn = boto.connect_s3()
    bucket_name = environment_specific.S3_CSV_BUCKET_DEV if dev_bucket \
                    else environment_specific.S3_CSV_BUCKET
    bucket = boto_conn.get_bucket(bucket_name, validate=False)
    missing_uploads = set()

    for db in mysql_lib.get_dbs(schema_host):
        for table in mysql_backup_csv.mysql_backup_csv(schema_host).get_tables_to_backup(db):
            try:
                verify_csv_schema_upload(schema_host, db, [table],
                                         date, dev_bucket)
            except:
                continue

            table_missing_uploads = set()
            for replica_set in replica_sets:
                chk_instance = zk.get_mysql_instance_from_replica_set(replica_set)
                (_, data_path, success_path) = backup.get_csv_backup_paths(
                                                   chk_instance, db, table, date)

                k = bucket.get_key(data_path)
                if k is None:
                    table_missing_uploads.add(data_path)
                    success = False
                elif k.size == 0:
                    # we should not have zero-length files, because even if
                    # we send zero bytes to lzop, there's a 55-byte header.
                    # so, if this actually happened, it probably means that
                    # something is wrong.  delete the key and add it to the
                    # missing_uploads list so that we'll try again.
                    k.delete()
                    table_missing_uploads.add(data_path)
                    success = False

            if not table_missing_uploads and not bucket.get_key(success_path):
                print 'Creating success key {b}/{k}'.format(b=bucket_name,
                                                            k=success_path)
                key = bucket.new_key(success_path)
                key.set_contents_from_string(' ')

            missing_uploads.update(table_missing_uploads)

    if missing_uploads:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print ('Shard type {} is missing uploads:'.format(shard_type))
            pprint.pprint(missing_uploads)
        else:
            print ('Shard type {shard_type} is missing {num} uploads'
                   ''.format(num=len(missing_uploads),
                             shard_type=shard_type))

    if not missing_uploads and success:
        print 'Shard type {} is backed up'.format(shard_type)

    return success


def verify_sharded_csv_backup_by_shard_type(shard_type, date,
                                            dev_bucket=False):
    """ Verify that a sharded data set has been backed up to hive

    Args:
        shard_type -  i.e. 'sharddb', etc
        date - The date to search for
        dev_bucket - Look in the dev bucket

    Returns:
        True for no problems found, False otherwise.
    """
    zk = host_utils.MysqlZookeeper()
    (replica_set, db) = zk.get_example_db_and_replica_set_for_shard_type(shard_type)
    schema_host = zk.get_mysql_instance_from_replica_set(replica_set,
                      repl_type=host_utils.REPLICA_ROLE_SLAVE)

    tables = mysql_backup_csv.mysql_backup_csv(schema_host).get_tables_to_backup(db)
    if not tables:
        raise Exception('No tables will be checked for backups')

    verify_csv_schema_upload(schema_host, db, tables, date, dev_bucket)

    shards = zk.get_shards_by_shard_type(shard_type)
    if not shards:
        raise Exception('No shards will be checked for backups')

    missing_uploads = verify_sharded_csv_backup_by_shards(shards, tables,
                                                          date, dev_bucket)
    if missing_uploads:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print ('Shard type {} is missing uploads:'.format(shard_type))
            pprint.pprint(missing_uploads)
        else:
            print ('Shard type {shard_type} is missing {num} uploads'
                   ''.format(num=len(missing_uploads),
                             shard_type=shard_type))
        return False
    else:
        # we have checked all shards, all are good, create success files
        boto_conn = boto.connect_s3()
        bucket_name = environment_specific.S3_CSV_BUCKET_DEV \
            if dev_bucket else environment_specific.S3_CSV_BUCKET
        bucket = boto_conn.get_bucket(bucket_name, validate=False)

        for table in tables:
            (_, _, success_path) = backup.get_csv_backup_paths(
                                       schema_host, db, table, date)
            if not bucket.get_key(success_path):
                print 'Creating success key {b}/{k}'.format(b=bucket_name,
                                                            k=success_path)
                key = bucket.new_key(success_path)
                key.set_contents_from_string(' ')
        print 'Shard type {} is backed up'.format(shard_type)

    return True


def verify_sharded_csv_backup_by_shards(shards, tables, date,
                                        dev_bucket=False):
    """ Verify that shards has been backed up to hive

    Args:
        shards - A set of shard names to check
        tables - A set of table names to check
        date - The date to search for
        dev_bucket - Use the dev bucket?

    Returns:
        True for no problems found, False otherwise.
    """
    pool = multiprocessing.Pool(processes=CSV_CHECK_PROCESSES)
    pool_args = list()
    for table in tables:
        pool_args.append((table, date, shards, dev_bucket))

    results = pool.map(get_sharded_db_missing_uploads, pool_args)
    missing_uploads = set()
    for result in results:
        missing_uploads.update(result)

    return missing_uploads


def get_sharded_db_missing_uploads(args):
    """ Check to see if all backups are present

    Args: A tuple which can be expanded to:
        table - table name
        shard_type - sharddb, etc
        shards -  a set of shards
        dev_bucket - check the dev bucket instead of the prod bucket?

    Returns: a set of shards which are not backed up
    """
    (table, date, shards, dev_bucket) = args
    zk = host_utils.MysqlZookeeper()
    expected_s3_keys = set()
    prefix = None

    for shard in shards:
        (replica_set, db) = zk.map_shard_to_replica_and_db(shard)
        instance = zk.get_mysql_instance_from_replica_set(replica_set,
                          repl_type=host_utils.REPLICA_ROLE_SLAVE)
        (_, data_path, _) = backup.get_csv_backup_paths(
                                instance, db, table, date)
        expected_s3_keys.add(data_path)
        if not prefix:
            prefix = os.path.dirname(data_path)

    boto_conn = boto.connect_s3()
    bucket_name = environment_specific.S3_CSV_BUCKET_DEV if dev_bucket \
                    else environment_specific.S3_CSV_BUCKET
    bucket = boto_conn.get_bucket(bucket_name, validate=False)
    uploaded_keys = set()
    for key in bucket.list(prefix=prefix):
        if key.size > 0:
            uploaded_keys.add(key.name)
        elif key.name.split('/')[-1][0] != '_':
            # If we have a zero-length file that doesn't start with
            # an underscore, it shouldn't be here.
            key.delete()

    missing_uploads = expected_s3_keys.difference(uploaded_keys)

    for entry in copy.copy(missing_uploads):
        # The list API occassionally has issues, so we will recheck any missing
        # entries. If any are actually missing we will quit checking because
        # there is definitely work that needs to be done
        k = bucket.get_key(entry)
        if k and k.size > 0:
            print 'List method did not return data for key:{}'.format(entry)
            missing_uploads.discard(entry)
        else:
            return missing_uploads

    return missing_uploads


def verify_csv_instance_backup(instance, date, dev_bucket=False):
    """ Verify that an instance has been backed up to hive

    Args:
        instance - The instance to inspect for backups being done
        date - The date to search for
        dev_bucket - Check the dev bucket?

    Returns:
        True for no problems found, False otherwise.
    """
    return_status = True
    missing_uploads = set()

    if csv_backup_success_logged(instance, date, dev_bucket):
        print ('Per csv backup success log, backup has already been '
               'verified')
        return True

    if early_verification(date, instance):
        return True

    # We might be looking at an instance that is part of a sharded system; if
    # so we will only look at what DBs are supposed to exist on the instance
    # otherwise, we will check all DBs. Note, we only set the success flag
    # for unsharded systems.
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)
    shards = zk.get_shards_by_replica_set()[replica_set]

    if shards and not is_sharded_but_not_sharded(replica_set):
        instance_shard_type_mapping = dict()
        missing_uploads = set()
        for shard in shards:
            (s, ns, _) = environment_specific.deconstruct_shard_name(shard)
            shard_type = ''.join([s, ns])
            if shard_type not in instance_shard_type_mapping:
                instance_shard_type_mapping[shard_type] = set()
            instance_shard_type_mapping[shard_type].add(shard)

        for shard_type in instance_shard_type_mapping:
            example_shard = list(instance_shard_type_mapping[shard_type])[0]
            (_, db) = zk.map_shard_to_replica_and_db(example_shard)
            tables = mysql_backup_csv.mysql_backup_csv(instance).get_tables_to_backup(db)
            missing_uploads.update(verify_sharded_csv_backup_by_shards(
                instance_shard_type_mapping[shard_type], tables, date,
                dev_bucket))

        if missing_uploads:
            if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
                print ('Instance {} is missing uploads:'.format(instance))
                pprint.pprint(missing_uploads)
            else:
                print ('Instance {instance} is missing {num} uploads'
                       ''.format(num=len(missing_uploads),
                                 instance=instance))
            return_status = False
    else:
        return_status = verify_unsharded_csv_backups(instance, date,
                                                     dev_bucket)

    if return_status:
        print ('Instance {} is backed up'.format(instance))
    return return_status


def verify_unsharded_csv_backups(instance, date, dev_bucket=False):
    """ Verify csv backups for an instance which is not part of a sharded
        system

    Args:
        instance - The instance to inspect for backups being done
        date - The date to search for
        dev_bucket - Use the dev bucket?

    Returns:
        True for no problems found, False otherwise.
    """
    return_status = True
    boto_conn = boto.connect_s3()
    bucket_name = environment_specific.S3_CSV_BUCKET_DEV if dev_bucket \
                    else environment_specific.S3_CSV_BUCKET
    bucket = boto_conn.get_bucket(bucket_name, validate=False)
    missing_uploads = set()
    for db in mysql_lib.get_dbs(instance):
        tables = mysql_backup_csv.mysql_backup_csv(instance).get_tables_to_backup(db)
        try:
            verify_csv_schema_upload(instance, db, tables, date,
                                     dev_bucket)
        except Exception as e:
            print e
            return_status = False
            continue

        for table in tables:
            (_, data_path, success_path) = \
                backup.get_csv_backup_paths(instance, db, table, date)
            k = bucket.get_key(data_path)
            if k is None:
                missing_uploads.add(data_path)
            elif k.size == 0:
                # we should not have zero-length files, because even if
                # we send zero bytes to lzop, there's a 55-byte header.
                # so, if this actually happened, it probably means that
                # something is wrong.  delete the key and add it to the
                # missing_uploads list so that we'll try again.
                k.delete()
                missing_uploads.add(data_path)
            else:
                # We still need to create a success file for the data
                # team for this table, even if something else is AWOL
                # later in the backup.
                if bucket.get_key(success_path):
                    print 'Success key {b}/{k} exists'.format(b=bucket_name,
                                                              k=success_path)
                else:
                    print 'Creating success key {b}/{k}'.format(b=bucket_name,
                                                                k=success_path)
                    key = bucket.new_key(success_path)
                    key.set_contents_from_string(' ')

    if missing_uploads:
        if len(missing_uploads) < MISSING_BACKUP_VERBOSE_LIMIT:
            print 'Missing uploads: {}'.format(missing_uploads)
        else:
            print 'Missing {} uploads'.format(len(missing_uploads))
        return_status = False

    if return_status:
        log_csv_backup_success(instance, date, dev_bucket)
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
    if (date == (datetime.datetime.utcnow().date() - \
        datetime.timedelta(days=1)).strftime("%Y-%m-%d")):

        if datetime.datetime.utcnow().time() < CSV_STARTUP:
            print 'Backup startup time has not yet passed'
            # For today's date, we give CSV_STARTUP minutes before
            # checking anything.
            return True

        if datetime.datetime.utcnow().time() < CSV_COMPLETION_TIME:
            # For today's date, until after CSV_COMPLETION_TIME it is good enough
            # to check if backups are running. If they are running, everything
            # is ok. If they are not running, we will do all the normal checks.
            if csv_backups_running(instance):
                print 'Backup running on {}'.format(instance)
                return True


def csv_backups_running(instance):
    """ Check to see if csv dumps are running

    Args:
    instance - we will use this to determine the replica set

    Returns:
    True if backups are running, False otherwise
    """
    (dump_user,
     _) = mysql_lib.get_mysql_user_for_role(backup.USER_ROLE_MYSQLDUMP)
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)

    for slave_role in [host_utils.REPLICA_ROLE_DR_SLAVE, host_utils.REPLICA_ROLE_SLAVE]:
        slave_instance = zk.get_mysql_instance_from_replica_set(replica_set,
                            slave_role)
        if not slave_instance:
            continue

        if dump_user in mysql_lib.get_connected_users(slave_instance):
            return True

    return False


def log_csv_backup_success(instance, date, dev_bucket=False):
    """ The CSV backup check can be expensive, so let's log that it is done

    Args:
        instance - A hostaddr object
        date - a string for the date
        dev_bucket - are we operating on a dev bucket backup?
    """
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)
    master = zk.get_mysql_instance_from_replica_set(replica_set)
    conn = mysql_lib.connect_mysql(master, 'dbascript')
    cursor = conn.cursor()

    if not mysql_lib.does_table_exist(master, mysql_lib.METADATA_DB,
                                      environment_specific.CSV_BACKUP_LOG_TABLE):
            print 'Creating missing metadata table'
            cursor.execute(CSV_BACKUP_LOG_TABLE_DEFINITION.format(
                               db=mysql_lib.METADATA_DB,
                               tbl=environment_specific.CSV_BACKUP_LOG_TABLE))

    sql = ('INSERT IGNORE INTO {METADATA_DB}.{CSV_BACKUP_LOG_TABLE} '
           'SET backup_date = %(date)s, '
           'completion = NOW(), '
           'dev_bucket = %(dev)s'
           ''.format(METADATA_DB=mysql_lib.METADATA_DB,
                     CSV_BACKUP_LOG_TABLE=environment_specific.CSV_BACKUP_LOG_TABLE))
    cursor.execute(sql, {'date': date, 'dev': int(dev_bucket)})
    conn.commit()


def csv_backup_success_logged(instance, date, dev_bucket=False):
    """ Check for log entries created by log_csv_backup_success

    Args:
        instance - A hostaddr object
        date - a string for the date
        dev_bucket - are we looking for dev or prod?
    Returns:
        True if already backed up, False otherwise
    """
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)
    master = zk.get_mysql_instance_from_replica_set(replica_set)
    conn = mysql_lib.connect_mysql(master, 'dbascript')
    cursor = conn.cursor()

    if not mysql_lib.does_table_exist(master, mysql_lib.METADATA_DB,
                                      environment_specific.CSV_BACKUP_LOG_TABLE):
        return False

    sql = ('SELECT COUNT(*) as "cnt" '
           'FROM {METADATA_DB}.{CSV_BACKUP_LOG_TABLE} '
           'WHERE backup_date = %(date)s '
           'AND dev_bucket=%(dev)s'
           ''.format(METADATA_DB=mysql_lib.METADATA_DB,
                     CSV_BACKUP_LOG_TABLE=environment_specific.CSV_BACKUP_LOG_TABLE))
    cursor.execute(sql, {'date': date, 'dev': int(dev_bucket)})
    if cursor.fetchone()["cnt"]:
        return True
    else:
        return False


def verify_csv_schema_upload(instance, db, tables, date, dev_bucket=False):
    """ Confirm that schema files are uploaded

    Args:
        instance - A hostaddr object
        db - which db to inspect
        tables - A set of tables to look for
        date - The date to search for
        dev_bucket - Are we checking the dev bucket or not?

    Returns:
        True for no problems found, False otherwise.

    """
    return_status = True
    missing = set()
    boto_conn = boto.connect_s3()
    bucket_name = environment_specific.S3_CSV_BUCKET_DEV if dev_bucket \
                    else environment_specific.S3_CSV_BUCKET
    bucket = boto_conn.get_bucket(bucket_name, validate=False)
    for table in tables:
        (path, _, _) = backup.get_csv_backup_paths(instance, db, table, date)
        if not bucket.get_key(path):
            missing.add(path)
            return_status = False

    if missing:
        raise Exception('Expected schema files missing: {}'.format(missing))
    return return_status


def main():
    zk = host_utils.MysqlZookeeper()
    all_sharded_systems = (list(zk.get_sharded_types()) +
                           environment_specific.FLEXSHARD_DBS.keys())

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
                        help="Check backup status for this instance if "
                             "not the default (localhost:3306)")
    parser.add_argument('-s',
                        '--shard_type',
                        default=None,
                        help='Used for csv backups for success file creation',
                        choices=all_sharded_systems)
    parser.add_argument("-a",
                        "--all",
                        action='store_true',
                        help="Check all replica sets for xbstream or sql.gz "
                             "and all shard types (but not unsharded) for "
                             "csv backups")
    parser.add_argument('--dev_bucket',
                        default=False,
                        action='store_true',
                        help='Check the dev bucket, useful for testing, but '
                             'only produces meaningful output for CSV backups.')

    args = parser.parse_args()
    instance = host_utils.HostAddr(args.instance) if args.instance else None
    return_code = BACKUP_OK_RETURN
    backups = []
    if (args.backup_type == backup.BACKUP_TYPE_XBSTREAM or
            args.backup_type == backup.BACKUP_TYPE_LOGICAL):
        if args.all:
            replica_sets = zk.get_all_mysql_replica_sets()
        else:
            # if we aren't in ZK, we will exit with a special return code
            # that can be picked up by the nagios check.
            if not instance:
                instance = host_utils.HostAddr(host_utils.HOSTNAME)

            try:
                replica_set = zk.get_replica_set_from_instance(instance)
                replica_sets = set([replica_set])
            except Exception as e:
                print "Nothing on backups for {i}: {e}".format(i=instance,
                                                               e=e)
                sys.exit(BACKUP_NOT_IN_ZK_RETURN)

        for replica_set in replica_sets:
            backup_file = find_mysql_backup(replica_set, args.date,
                                            args.backup_type)
            if backup_file:
                backups.append(backup_file)
            else:
                return_code = BACKUP_MISSING_RETURN
                if args.show_found:
                    print ("Backup not found for replica set {rs}"
                           "".format(rs=replica_set))
    elif args.backup_type == backup.BACKUP_TYPE_CSV:
        # by convention, the csv backup files use the previous days date
        split_date = args.date.split('-')
        date = (datetime.date(int(split_date[0]),
                              int(split_date[1]),
                              int(split_date[2])) -
                datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        if args.all:
            if instance or args.shard_type:
                raise Exception("Arguments 'all' and 'instance/shard_type' "
                                "can not be used together")

            for shard_type in all_sharded_systems:
                # If there are any problems, we return BACKUP_MISSING_RETURN
                # don't stop here but continue moving on another check
                try:
                    if not verify_csv_full_sharded_systems_backup(shard_type,
                                                              date,
                                                              args.dev_bucket):
                        return_code = BACKUP_MISSING_RETURN
                except Exception as e:
                    print e
                    continue
        elif args.shard_type:
            # Same as above, but only one specified shard_type
            if not verify_csv_full_sharded_systems_backup(args.shard_type,
                                                          date,
                                                          args.dev_bucket):
                return_code = BACKUP_MISSING_RETURN
        elif instance:
            if not verify_csv_instance_backup(instance, date, args.dev_bucket):
                return_code = BACKUP_MISSING_RETURN
        else:
            raise Exception('For CSV backup verification, either a shard type '
                            'or an instance is required')
    else:
        raise Exception('Backup type unsupported')

    if args.show_found and backups:
        pprint.pprint(backups)

    sys.exit(return_code)


if __name__ == '__main__':
    main()
