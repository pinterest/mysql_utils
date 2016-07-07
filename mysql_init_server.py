#!/usr/bin/env python
import argparse
import ConfigParser
import glob
import os

import mysql_backup
import mysql_cnf_builder
import mysql_grants
from lib import backup
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

DIRS_TO_CLEAR = ['log_bin', 'datadir', 'tmpdir']
DIRS_TO_CREATE = ['datadir', 'log_bin', 'log_error',
                  'slow_query_log_file', 'tmpdir']
# in MySQL 5.5+, log_slow_queries is deprecated in favor of
# slow_query_log_file
FILES_TO_CLEAR = ['log_slow_queries', 'log_error', 'slow_query_log_file']

# If MySQL 5.7+, don't use mysql_install_db
MYSQL_INSTALL_DB = '/usr/bin/mysql_install_db'
MYSQL_INITIALIZE = '/usr/sbin/mysqld --initialize-insecure'
log = environment_specific.setup_logging_defaults(__name__)


def main():
    description = 'Initialize a MySQL serer'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-p',
                        '--port',
                        help='Port to act on, default is 3306',
                        default='3306')
    parser.add_argument('--skip_production_check',
                        help=('DANGEROUS! Skip check of whether the instance '
                              'to be initialized is already in use'),
                        default=False,
                        action='store_true')
    parser.add_argument('--skip_backup',
                        help=('Do not run a backup once the instance is '
                              'setup'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()

    instance = host_utils.HostAddr(':'.join((host_utils.HOSTNAME,
                                             args.port)))
    mysql_init_server(instance,
                      args.skip_production_check,
                      skip_backup=args.skip_backup)


def mysql_init_server(instance,
                      skip_production_check=False, skip_locking=False,
                      skip_backup=True):
    """ Remove any data and initialize a MySQL instance

    Args:
    instance - A hostaddr object pointing towards localhost to act upon
    skip_production_check - Dangerous! will not run safety checks to protect
                            production data
    skip_locking - Do not take a lock on localhost. Useful when the caller has
                   already has taken the lock (ie mysql_restore_xtrabackup)
    skip_backup - Don't run a backup after the instance is setup
    """
    lock_handle = None
    if not skip_locking:
        # Take a lock to prevent multiple restores from running concurrently
        log.info('Taking a flock to block race conditions')
        lock_handle = host_utils.take_flock_lock(backup.BACKUP_LOCK_FILE)

    try:
        # sanity check
        zk = host_utils.MysqlZookeeper()
        if (not skip_production_check and
                instance in zk.get_all_mysql_instances()):
            raise Exception("It appears {instance} is in use. This is"
                            " very dangerous!".format(instance=instance))

        log.info('Checking host for mounts, etc...')
        basic_host_sanity()

        log.info('(re)Generating MySQL cnf files')
        mysql_cnf_builder.build_cnf()

        log.info('Creating any missing directories')
        create_and_chown_dirs(instance.port)

        log.info('Shutting down MySQL (if applicable)')
        host_utils.stop_mysql(instance.port)

        log.info('Deleting existing MySQL data')
        delete_mysql_data(instance.port)

        log.info('Creating MySQL privileges tables')
        init_privileges_tables(instance.port)

        log.info('Clearing innodb log files')
        delete_innodb_log_files(instance.port)

        log.info('Starting up instance')
        host_utils.start_mysql(instance.port)

        log.info('Importing MySQL users')
        mysql_grants.manage_mysql_grants(instance, 'nuke_then_import')

        log.info('Creating test database')
        mysql_lib.create_db(instance, 'test')

        log.info('Setting up query response time plugins')
        mysql_lib.setup_response_time_metrics(instance)

        log.info('Setting up semi-sync replication plugins')
        mysql_lib.setup_semisync_plugins(instance)

        log.info('Restarting pt daemons')
        host_utils.restart_pt_daemons(instance.port)

        log.info('MySQL initalization complete')

    finally:
        if not skip_locking and lock_handle:
            log.info('Releasing lock')
            host_utils.release_flock_lock(lock_handle)

    if not skip_backup:
        log.info('Taking a backup')
        mysql_backup.mysql_backup(instance, initial_build=True)


def basic_host_sanity():
    """ Confirm basic sanity (mounts, etc) on localhost """
    if host_utils.get_pinfo_cloud() != host_utils.TESTING_PINFO_CLOUD:
        for path in host_utils.REQUIRED_MOUNTS:
            found = False
            for choice in path.split(':'):
                if os.path.ismount(choice):
                    found = True
                    break
            if not found:
                raise Exception('No acceptable options for {path} '
                                'are mounted'.format(path=path))

    for path in host_utils.ZK_CACHE:
        if not os.path.isfile(path):
            raise Exception('ZK updater path {path} '
                            'is not present'.format(path=path))

    if not os.path.isfile(MYSQL_INSTALL_DB):
        raise Exception('MySQL install script {script} is not present'
                        ''.format(script=mysql_init_server.MYSQL_INSTALL_DB))


def create_and_chown_dirs(port):
    """ Create and chown any missing directories needed for mysql """
    for variable in DIRS_TO_CREATE:
        try:
            path = os.path.dirname(host_utils.get_cnf_setting(variable, port))
        except ConfigParser.NoOptionError:
            # Not defined, so must not matter
            return
        if not os.path.isdir(path):
            log.info('Creating and chowning {path}'.format(path=path))
            os.makedirs(path)
            host_utils.change_owner(path, 'mysql', 'mysql')


def delete_mysql_data(port):
    """ Purge all data on disk for a MySQL instance

    Args:
    port - The port on which to act upon on localhost
    """
    for dir_key in DIRS_TO_CLEAR:
        directory = host_utils.get_cnf_setting(dir_key, port)
        if not os.path.isdir(directory):
            directory = os.path.dirname(directory)
        log.info('Removing contents of {dir}'.format(dir=directory))
        host_utils.clean_directory(directory)

    # This should not bomb if one of the files to truncate
    # isn't specified in the config file.
    for file_keys in FILES_TO_CLEAR:
        try:
            del_file = host_utils.get_cnf_setting(file_keys, port)
            log.info('Truncating {del_file}'.format(del_file=del_file))
            open(del_file, 'w').close()
            host_utils.change_owner(del_file, 'mysql', 'mysql')
        except Exception:
            log.warning('Option {f} not specified '
                        'in my.cnf - continuing.'.format(f=file_keys))


def delete_innodb_log_files(port):
    """ Purge ib_log files

    Args:
    port - the port on which to act on localhost
    """
    try:
        ib_logs_dir = host_utils.get_cnf_setting('innodb_log_group_home_dir',
                                                 port)
    except ConfigParser.NoOptionError:
        ib_logs_dir = host_utils.get_cnf_setting('datadir',
                                                 port)
    glob_path = os.path.join(ib_logs_dir, 'ib_logfile')
    final_glob = ''.join((glob_path, '*'))
    for del_file in glob.glob(final_glob):
        log.info('Clearing {del_file}'.format(del_file=del_file))
        os.remove(del_file)


def init_privileges_tables(port):
    """ Bootstap a MySQL instance

    Args:
    port - the port on which to act upon on localhost
    """
    version = mysql_lib.get_installed_mysqld_version()
    if version[0:3] < '5.7':
        install_command = MYSQL_INSTALL_DB
    else:
        install_command = MYSQL_INITIALIZE

    datadir = host_utils.get_cnf_setting('datadir', port)
    cmd = ('{MYSQL_INSTALL_DB} --datadir={datadir}'
           ' --user=mysql'.format(MYSQL_INSTALL_DB=install_command,
                                  datadir=datadir))
    log.info(cmd)
    (std_out, std_err, return_code) = host_utils.shell_exec(cmd)
    if return_code:
        raise Exception("Return {return_code} != 0 \n"
                        "std_err:{std_err}\n"
                        "std_out:{std_out}".format(return_code=return_code,
                                                   std_err=std_err,
                                                   std_out=std_out))


if __name__ == "__main__":
    main()
