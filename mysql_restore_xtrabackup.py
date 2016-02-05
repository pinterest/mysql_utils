#!/usr/bin/env python
import argparse
import datetime
import time

import modify_mysql_zk
import mysql_backup
import mysql_cnf_builder
import mysql_init_server
from lib import backup
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib


# It might be better to eventually puppetize this, but
# for now, this will do.
TEST_BASE_PATH = "/backup/mysql_restore/data"
# By default, ignore backups older than DEFAULT_MAX_RESTORE_AGE days
DEFAULT_MAX_RESTORE_AGE = 5
SCARY_TIMEOUT = 20


def main():
    description = 'Utility to download and restore MySQL xbstream backups'
    parser = argparse.ArgumentParser(description=description)
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

    restore_instance(restore_source=source,
                     destination=destination,
                     no_repl=args.no_repl,
                     date=args.date,
                     add_to_zk=args.add_to_zk,
                     skip_production_check=args.skip_production_check)


def restore_instance(restore_source, destination,
                     no_repl, date,
                     add_to_zk, skip_production_check):
    """ Restore a MySQL backup on to localhost

    Args:
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

    # Try to prevent unintentional destruction of prod servers
    zk = host_utils.MysqlZookeeper()
    try:
        (_, replica_type) = zk.get_replica_set_from_instance(destination)
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

    # Take a lock to prevent multiple restores from running concurrently
    log.info('Taking a flock to block another restore from starting')
    lock_handle = host_utils.take_flock_lock(backup.BACKUP_LOCK_FILE)

    log.info('Rebuilding cnf files just in case')
    mysql_cnf_builder.build_cnf()

    mysql_init_server.create_and_chown_dirs(destination.port)

    # load some data from the mysql conf file
    datadir = host_utils.get_cnf_setting('datadir', destination.port)

    (restore_source,
     restore_file, restore_size) = find_a_backup_to_restore(restore_source,
                                                            destination, date)
    if restore_source.get_zk_replica_set():
        replica_set = restore_source.get_zk_replica_set()[0]
        master = zk.get_mysql_instance_from_replica_set(replica_set, host_utils.REPLICA_ROLE_MASTER)
    else:
        # ZK has no idea what this replica set is, probably a new replica set.
        master = restore_source

    # Start logging
    row_id = backup.start_restore_log(master, {'restore_source': restore_source,
                                               'restore_port': destination.port,
                                               'restore_file': restore_file,
                                               'source_instance': destination.hostname,
                                               'restore_date': date,
                                               'replication': no_repl,
                                               'zookeeper': add_to_zk})
    # Giant try to allow logging if anything goes wrong.
    try:
        # If we hit an exception, this status will be used. If not, it will
        # be overwritten
        restore_log_update = {'restore_status': 'BAD'}
        log.info('Quick sanity check')
        mysql_init_server.basic_host_sanity()

        log.info('Shutting down MySQL')
        host_utils.stop_mysql(destination.port)

        log.info('Removing any existing MySQL data')
        mysql_init_server.delete_mysql_data(destination.port)

        log.info('Unpacking {rfile} into {ddir}'.format(rfile=restore_file,
                                                        ddir=datadir))
        backup.xbstream_unpack(restore_file, destination.port,
                               restore_source, restore_size)

        log.info('Decompressing files in {path}'.format(path=datadir))
        backup.innobackup_decompress(destination.port)

        log.info('Applying logs')
        backup.apply_log(destination.port, memory='10G')

        log.info('Removing old innodb redo logs')
        mysql_init_server.delete_innodb_log_files(destination.port)

        log.info('Setting permissions for MySQL on {dir}'.format(dir=datadir))
        host_utils.change_owner(datadir, 'mysql', 'mysql')

        log.info('Starting MySQL')
        host_utils.upgrade_auth_tables(destination.port)
        restore_log_update = {'restore_status': 'OK'}

        log.info('Running MySQL upgrade')
        host_utils.start_mysql(destination.port,
                               options=host_utils.DEFAULTS_FILE_EXTRA_ARG.format(defaults_file=host_utils.MYSQL_NOREPL_CNF_FILE))

        if master == backup.get_metadata_from_backup_file(restore_file)[0]:
            log.info('Pulling replication info from restore to backup source')
            (binlog_file, binlog_pos) = backup.parse_xtrabackup_binlog_info(datadir)
        else:
            log.info('Pulling replication info from restore to '
                     'master of backup source')
            (binlog_file, binlog_pos) = backup.parse_xtrabackup_slave_info(datadir)

        log.info('Setting up MySQL replication')
        restore_log_update['replication'] = 'FAIL'

        # Since we haven't started the slave yet, make sure we've got these
        # plugins installed, whether we use them or not.
        mysql_lib.setup_semisync_plugins(destination)

        # Try to configure replication.
        mysql_lib.change_master(destination,
                                master,
                                binlog_file,
                                binlog_pos,
                                no_start=(no_repl == 'SKIP'))
        mysql_lib.wait_replication_catch_up(destination)
        host_utils.restart_pt_daemons(destination.port)

        restore_log_update['replication'] = 'OK'

        mysql_lib.setup_response_time_metrics(destination)

    except Exception as e:
        log.error(e)
        if row_id is not None:
            restore_log_update['status_message'] = e
            restore_log_update['finished_at'] = True
        raise
    finally:
        if lock_handle:
            log.info('Releasing lock')
            host_utils.release_flock_lock(lock_handle)
        backup.update_restore_log(master, row_id, restore_log_update)

    try:
        if add_to_zk == 'REQ':
            log.info('Adding instance to zk')
            modify_mysql_zk.auto_add_instance_to_zk(destination, dry_run=False)
            backup.update_restore_log(master, row_id, {'zookeeper': 'OK'})
        else:
            log.info('add_to_zk is not set, therefore not adding to zk')
    except Exception as e:
        log.warning("An exception occurred: {e}".format(e=e))
        log.warning("If this is a DB issue, that's fine. "
                    "Otherwise, you should check ZK.")

    backup.update_restore_log(master, row_id, {'finished_at': True})
    log.info('Starting a new backup')
    mysql_backup.mysql_backup(destination)


def find_a_backup_to_restore(source, destination, date):
    """ Based on supplied constains, try to find a backup to restore

    Args:
    source - A hostaddr object for where to pull a backup from
    destination -  A hostaddr object for where to restore the backup
    date - What date should the backup be from

    Returns:
    restore_source - Where the backup was taken
    retore_file - Where the file exists on whichever storage
    restore_size - What is the size of the backup in bytes
    """
    zk = host_utils.MysqlZookeeper()
    possible_sources = list()
    if source:
        # the souce may not be in zk because it is a new replica set
        possible_sources.append(source)
        if source.get_zk_replica_set():
            replica_set = source.get_zk_replica_set()[0]
        else:
            replica_set = None
    else:
        replica_set = destination.get_zk_replica_set()[0]
        for role in host_utils.REPLICA_TYPES:
            possible_sources.append(zk.get_mysql_instance_from_replica_set(replica_set, role))
    log.info('Replica set detected as {replica_set}'.format(replica_set=replica_set))
    log.info('Possible source hosts:{possible_sources}'.format(possible_sources=possible_sources))

    if date:
        dates = [date]
    else:
        dates = []
        for days in range(0, DEFAULT_MAX_RESTORE_AGE):
            dates.append(datetime.date.today() - datetime.timedelta(days=days))

    # Find a backup file, preferably newer and less strong preferece on the master server
    restore_file = None
    for restore_date in dates:
        if restore_file:
            break

        log.info('Looking for a backup for {restore_date}'.format(restore_date=restore_date))
        for possible_source in possible_sources:
            try:
                (restore_file, restore_size) = backup.get_s3_backup(possible_source, str(restore_date))
                restore_source = possible_source
                break
            except:
                log.info('No backup found on in s3 for host {source} '
                         ' on date {date}'
                         ''.format(source=possible_source,
                                   date=restore_date))

    if not restore_file:
        raise Exception('Could not find a backup to restore')

    log.info('Found a backup: {restore_file}'
             ''.format(restore_file=restore_file))

    return restore_source, restore_file, restore_size


if __name__ == "__main__":
    log = environment_specific.setup_logging_defaults(__name__)
    main()
