#!/usr/bin/env python
import argparse
import boto
import datetime
import logging
import subprocess
import time

import modify_mysql_zk
import mysql_backup
import mysql_init_server
from lib import backup
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib


SCARY_TIMEOUT = 20

log = logging.getLogger(__name__)


def main():
    description = 'Utility to download and restore MySQL xbstream backups'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-b',
                        '--backup_type',
                        help='Type of backup to restore. Default: xtrabackup',
                        default=backup.BACKUP_TYPE_XBSTREAM,
                        choices=(backup.BACKUP_TYPE_LOGICAL,
                                 backup.BACKUP_TYPE_XBSTREAM))
    parser.add_argument('-s',
                        '--source_instance',
                        help=('Which instances backups to restore. Default is '
                              'a best guess based on the hostname.'),
                        default=None)
    parser.add_argument('-d',
                        '--date',
                        help='attempt to restore from a specific date')
    parser.add_argument('-p',
                        '--destination_port',
                        help='Port on localhost on to restore. Default 3306.',
                        default='3306')
    parser.add_argument('--no_repl',
                        help='Setup replication but do not run START SLAVE',
                        default='REQ',
                        action='store_const',
                        const='SKIP')
    parser.add_argument('--add_to_zk',
                        help=('By default the instance will not be added to '
                              'zk. This option will attempt to add the '
                              'instance to zk.'),
                        default='SKIP',
                        action='store_const',
                        const='REQ')
    parser.add_argument('--skip_production_check',
                        help=('DANGEROUS! Skip check of whether the instance '
                              'to be built is already in use'),
                        default=False,
                        action='store_true')

    args = parser.parse_args()
    if args.source_instance:
        source = host_utils.HostAddr(args.source_instance)
    else:
        source = None

    destination = host_utils.HostAddr(':'.join((host_utils.HOSTNAME,
                                               args.destination_port)))

    restore_instance(backup_type=args.backup_type,
                     restore_source=source,
                     destination=destination,
                     no_repl=args.no_repl,
                     date=args.date,
                     add_to_zk=args.add_to_zk,
                     skip_production_check=args.skip_production_check)


def restore_instance(backup_type, restore_source, destination,
                     no_repl, date,
                     add_to_zk, skip_production_check):
    """ Restore a MySQL backup on to localhost

    Args:
    backup_type - Type of backup to restore
    restore_source - A hostaddr object for where to pull a backup from
    destination -  A hostaddr object for where to restore the backup
    no_repl - Should  replication be not started. It will always be setup.
    date - What date should the backup be from
    add_to_zk - Should the instnace be added to zk. If so, the log from the
                host being launched will be consulted.
    skip_production_check - Do not check if the host is already in zk for
                            production use.
    """
    log.info('Supplied source is {source}'.format(source=restore_source))
    log.info('Supplied destination is {dest}'.format(dest=destination))
    log.info('Desired date of restore {date}'.format(date=date))
    zk = host_utils.MysqlZookeeper()

    # Try to prevent unintentional destruction of prod servers
    log.info('Confirming no prod instances running on destination')
    prod_check(destination, skip_production_check)

    # Take a lock to prevent multiple restores from running concurrently
    log.info('Taking a lock to block another restore from starting')
    lock_handle = host_utils.bind_lock_socket(backup.BACKUP_LOCK_SOCKET)

    log.info('Looking for a backup to restore')
    if restore_source:
        possible_sources = [restore_source]
    else:
        possible_sources = get_possible_sources(destination, backup_type)
    backup_key = find_a_backup_to_restore(possible_sources, destination,
                                          backup_type, date)

    # Figure out what what we use to as the master when we setup replication
    (restore_source, _) = backup.get_metadata_from_backup_file(backup_key.name)
    try:
        replica_set = restore_source.get_zk_replica_set()
        master = zk.get_mysql_instance_from_replica_set(replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
    except:
        # ZK has no idea what this replica set is, probably a new replica set.
        master = restore_source

    # Start logging
    row_id = backup.start_restore_log(master, {
                'restore_source': restore_source,
                'restore_port': destination.port,
                'restore_file': backup_key.name,
                'source_instance': destination.hostname,
                'restore_date': date,
                'replication': no_repl,
                'zookeeper': add_to_zk})

    # Giant try to allow logging if anything goes wrong.
    try:
        # If we hit an exception, this status will be used. If not, it will
        # be overwritten
        restore_log_update = {'restore_status': 'BAD'}

        # This also ensures that all needed directories exist
        log.info('Rebuilding local mysql instance')
        lock_handle = mysql_init_server.mysql_init_server(
                        destination,
                        skip_production_check=True,
                        skip_backup=True,
                        lock_handle=lock_handle)

        if backup_type == backup.BACKUP_TYPE_XBSTREAM:
            xbstream_restore(backup_key, destination.port)
            if master == restore_source:
                log.info('Pulling replication info from restore to '
                         'backup source')
                (binlog_file,
                 binlog_pos) = backup.parse_xtrabackup_binlog_info(
                                destination.port)
            else:
                log.info('Pulling replication info from restore to '
                         'master of backup source')
                (binlog_file,
                 binlog_pos) = backup.parse_xtrabackup_slave_info(
                                destination.port)
        elif backup_type == backup.BACKUP_TYPE_LOGICAL:
            log.info('Preparing replication')
            # We are importing a mysqldump which was created with
            # --master-data or --dump-slave so there will be a CHANGE MASTER
            # statement at the start of the dump. MySQL will basically just
            # ignore a CHANGE MASTER command if master_host is not already
            # setup. So we are setting master_host, username and password
            # here. We use BOGUS for master_log_file so that the IO thread is
            # intentionally broken.  With no argument for master_log_file,
            # the IO thread would start downloading the first bin log and
            # the SQL thread would start executing...
            mysql_lib.change_master(destination, master, 'BOGUS', 0,
                                    no_start=True)
            logical_restore(backup_key, destination)
            host_utils.stop_mysql(destination.port)

        log.info('Running MySQL upgrade')
        host_utils.upgrade_auth_tables(destination.port)

        log.info('Starting MySQL')
        host_utils.start_mysql(
            destination.port,
            options=host_utils.DEFAULTS_FILE_EXTRA_ARG.format(
            defaults_file=host_utils.MYSQL_NOREPL_CNF_FILE))

        # Since we haven't started the slave yet, make sure we've got these
        # plugins installed, whether we use them or not.
        mysql_lib.setup_semisync_plugins(destination)
        restore_log_update = {'restore_status': 'OK'}

        # Try to configure replication.
        log.info('Setting up MySQL replication')
        restore_log_update['replication'] = 'FAIL'
        if backup_type == backup.BACKUP_TYPE_XBSTREAM:
            mysql_lib.change_master(destination,
                                    master,
                                    binlog_file,
                                    binlog_pos,
                                    no_start=(no_repl == 'SKIP'))
        elif backup_type == backup.BACKUP_TYPE_LOGICAL:
            if no_repl == 'SKIP':
                log.info('As requested, not starting replication.')
            else:
                mysql_lib.restart_replication(destination)
        if no_repl == 'REQ':
            mysql_lib.wait_for_catch_up(destination)
        restore_log_update['replication'] = 'OK'

        host_utils.restart_pt_daemons(destination.port)
        mysql_lib.setup_response_time_metrics(destination)

    except Exception as e:
        log.error(e)
        if row_id is not None:
            restore_log_update['status_message'] = e
            restore_log_update['finished_at'] = True
        raise
    finally:
        # As with mysql_init_server, we have to do one more restart to
        # clear out lock ownership, but here we have to also do it with
        # the proper config file.
        if lock_handle:
            log.info('Releasing lock and restarting MySQL')
            host_utils.stop_mysql(destination.port)
            time.sleep(5)
            host_utils.release_lock_socket(lock_handle)
            if no_repl == 'SKIP':
                host_utils.start_mysql(
                    destination.port,
                    options=host_utils.DEFAULTS_FILE_EXTRA_ARG.format(
                    defaults_file=host_utils.MYSQL_NOREPL_CNF_FILE))
            else:
                host_utils.start_mysql(destination.port)

        backup.update_restore_log(master, row_id, restore_log_update)

    try:
        if add_to_zk == 'REQ':
            if no_repl == 'REQ':
                log.info('Waiting for replication again, as it may have '
                         'drifted due to restart.')
                mysql_lib.wait_for_catch_up(destination)
                log.info('Waiting for IO lag in case it is still too '
                         'far even wait for resync ')
                mysql_lib.wait_for_catch_up(destination, io=True)
            log.info('Adding instance to zk.')
            modify_mysql_zk.auto_add_instance_to_zk(destination.port,
                                                    dry_run=False)
            backup.update_restore_log(master, row_id, {'zookeeper': 'OK'})
        else:
            log.info('add_to_zk is not set, therefore not adding to zk')
    except Exception as e:
        log.warning("An exception occurred: {}".format(e))
        log.warning("If this is a DB issue, that's fine. "
                    "Otherwise, you should check ZK.")
    backup.update_restore_log(master, row_id, {'finished_at': True})

    if no_repl == 'REQ':
        log.info('Starting a new backup')
        mysql_backup.mysql_backup(destination, initial_build=True)


def prod_check(destination, skip_production_check):
    """ Confirm it is ok to overwrite the destination instance

    Args:
    destination - Hostaddr object for where to restore the backup
    skip_production_check - If set, it is ok to run on slabes
    """
    zk = host_utils.MysqlZookeeper()
    try:
        replica_type = zk.get_replica_type_from_instance(destination)
    except:
        # instance is not in production
        replica_type = None
    if replica_type == host_utils.REPLICA_ROLE_MASTER:
        # If the instance, we will refuse to run. No ifs, ands, or buts/
        raise Exception('Restore script must never run on a master')
    if replica_type:
        if skip_production_check:
            log.info('Ignoring production check. We hope you know what you '
                     'are doing and we will try to take a backup in case '
                     'you are wrong.')
            try:
                mysql_backup.mysql_backup(destination)
            except Exception as e:
                log.error(e)
                log.warning('Unable to take a backup. We will give you {time} '
                            'seconds to change your mind and ^c.'
                            ''.format(time=SCARY_TIMEOUT))
                time.sleep(SCARY_TIMEOUT)
        else:
            raise Exception("It appears {instance} is in use. This is"
                            " very dangerous!".format(instance=destination))


def get_possible_sources(destination, backup_type):
    """ Get a possible sources to restore a backup from. This is required due
        to mysqldump 5.5 not being able to use both --master_data and
        --slave_data

    Args:
    destination - A hostAddr object
    backup_type - backup.BACKUP_TYPE_LOGICAL or backup.BACKUP_TYPE_XTRABACKUP

    Returns A list of hostAddr objects
    """
    zk = host_utils.MysqlZookeeper()
    replica_set = destination.guess_zk_replica_set()
    possible_sources = []
    for role in host_utils.REPLICA_TYPES:
        if (role == host_utils.REPLICA_ROLE_MASTER and
                backup_type == backup.BACKUP_TYPE_LOGICAL):
            continue
        else:
            instance = zk.get_mysql_instance_from_replica_set(replica_set,
                                                              role)
            if instance:
                possible_sources.append(instance)

    return possible_sources


def find_a_backup_to_restore(possible_sources, destination,
                             backup_type, date=None):
    """ Based on supplied constains, try to find a backup to restore

    Args:
    source - A hostaddr object for where to pull a backup from
    destination -  A hostaddr object for where to restore the backup
    backup_type - What sort of backup to restore
    date - What date should the backup be from

    Returns:
    restore_source - Where the backup was taken
    retore_file - Where the file exists on whichever storage
    restore_size - What is the size of the backup in bytes
    """
    log.info('Possible source hosts:{}'.format(possible_sources))

    if date:
        dates = [date]
    else:
        dates = []
        for days in range(0, backup.DEFAULT_MAX_RESTORE_AGE):
            dates.append(datetime.date.today() - datetime.timedelta(days=days))

    # Find a backup file with a preference for newer
    possible_keys = []
    for restore_date in dates:
        if possible_keys:
            # we are looping to older dates, if we already found some keys, we
            # quit looking
            continue
        log.info('Looking for a backup for {}'.format(restore_date))
        for possible_source in possible_sources:
            try:
                possible_keys.extend(backup.get_s3_backup(possible_source,
                                                          str(restore_date),
                                                          backup_type))
            except boto.exception.S3ResponseError:
                raise

            except Exception as e:
                if backup.NO_BACKUP not in e[0]:
                    raise

                log.info('No backup found on in s3 for host {source} '
                         'on date {date}'
                         ''.format(source=possible_source,
                                   date=restore_date))

    if not possible_keys:
        raise Exception('Could not find a backup to restore')

    most_recent = None
    for key in possible_keys:
        if not most_recent:
            most_recent = key
        elif most_recent.last_modified < key.last_modified:
            most_recent = key

    log.info('Found a backup: {}'.format(key))
    return most_recent


def xbstream_restore(xbstream, port):
    """ Restore an xtrabackup file

    xbstream - An xbstream file in S3
    port - The port on which to act on on localhost
    """
    datadir = host_utils.get_cnf_setting('datadir', port)

    log.info('Shutting down MySQL')
    host_utils.stop_mysql(port)

    log.info('Removing any existing MySQL data')
    mysql_init_server.delete_mysql_data(port)

    log.info('Downloading and unpacking backup')
    backup.xbstream_unpack(xbstream, datadir)

    log.info('Decompressing compressed ibd files')
    backup.innobackup_decompress(datadir)

    log.info('Applying logs')
    backup.apply_log(datadir)

    log.info('Removing old innodb redo logs')
    mysql_init_server.delete_innodb_log_files(port)

    log.info('Setting permissions for MySQL on {dir}'.format(dir=datadir))
    host_utils.change_owner(datadir, 'mysql', 'mysql')


def logical_restore(dump, destination):
    """ Restore a compressed mysqldump file from s3 to localhost, port 3306

    Args:
    dump - a mysqldump file in s3
    destination -  a hostaddr object for where the data should be loaded on
                   localhost
    """
    (user, password) = mysql_lib.get_mysql_user_for_role('admin')
    if dump.name.startswith(backup.BACKUP_TYPE_PARTIAL_LOGICAL):
        # TODO: check if db is empty before applying rate limit
        rate_limit = backup.MAX_TRANSFER_RATE
    else:
        log.info('Restarting MySQL to turn off enforce_storage_engine')
        host_utils.stop_mysql(destination.port)
        host_utils.start_mysql(destination.port,
                           host_utils.DEFAULTS_FILE_ARG.format(
                           defaults_file=host_utils.MYSQL_UPGRADE_CNF_FILE))
        rate_limit = None

    log.info('Downloading, decompressing and importing backup')
    procs = dict()
    procs['s3_download'] = backup.create_s3_download_proc(dump)
    procs['pv'] = backup.create_pv_proc(procs['s3_download'].stdout,
                                        size=dump.size,
                                        rate_limit=rate_limit)
    log.info('zcat |')
    procs['zcat'] = subprocess.Popen(['zcat'],
                                     stdin=procs['pv'].stdout,
                                     stdout=subprocess.PIPE)
    mysql_cmd = ['mysql', 
                 '--port={}'.format(str(destination.port)),
                 '--host={}'.format(destination.hostname),
                 '--user={}'.format(user),
                 '--password={}'.format(password)]
    log.info(' '.join(mysql_cmd))
    procs['mysql'] = subprocess.Popen(mysql_cmd,
                                      stdin=procs['zcat'].stdout)
    while(not host_utils.check_dict_of_procs(procs)):
        time.sleep(.5)

if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
