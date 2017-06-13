#!/usr/bin/env python
import argparse
import os
import socket

import ConfigParser

from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

HOSTNAME_TAG = '__HOSTNAME__'
ROOTVOL_TAG = '__ROOT__'
CNF_DEFAULTS = 'default_my.cnf'
CONFIG_SUB_DIR = 'mysql_cnf_config'
RELATIVE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            CONFIG_SUB_DIR)
ROOT_CNF = '/root/.my.cnf'
LOG_ROTATE_CONF_FILE = '/etc/logrotate.d/mysql_3306'
LOG_ROTATE_FILES = ['slow_query_log_file', 'log_error', 'general_log_file']
LOG_ROTATE_SETTINGS = ('size 1G',
                       'rotate 10',
                       'missingok',
                       'copytruncate',
                       'compress')
LOG_ROTATE_TEMPLATE = '\n'.join(("{files} {{",
                                 "\t{settings}",
                                 "}}"))
MYSQLD_SECTION = 'mysqld3306'
PT_HEARTBEAT_TEMPLATE = 'pt_heartbeat.template'
PT_HEARTBEAT_CONF_FILE = '/etc/pt-heartbeat-3306.conf'
PT_KILL_BUSY_TIME = 10
PT_KILL_TEMPLATE = 'pt_kill.template'
PT_KILL_CONF_FILE = '/etc/pt-kill.conf'
PT_KILL_IGNORE_USERS = ['admin', 'etl', 'longqueryro', 'longqueryrw',
                        'pbuser', 'mysqldump', 'xtrabackup', 'ptchecksum',
                        'dbascript', 'maxwell']

MAXWELL_TEMPLATE = 'maxwell.template'
MAXWELL_CONF_FILE = '/etc/mysql/maxwell-3306.conf'

REMOVE_SETTING_PREFIX = 'remove_'
READ_ONLY_OFF = 'OFF'
READ_ONLY_ON = 'ON'
SAFE_UPDATE_PREFIXES = set(['sharddb', 'modsharddb'])
SAFE_UPDATES_SQL = 'set global sql_safe_updates=on;'
TOUCH_FOR_NO_CONFIG_OVERWRITE = '/etc/mysql/no_write_config'
TOUCH_FOR_WRITABLE_IF_NOT_IN_ZK = '/etc/mysql/make_non_zk_server_writeable'
UPGRADE_OVERRIDE_SETTINGS = {'skip_slave_start': None,
                             'skip_networking': None,
                             'innodb_fast_shutdown': '0'}
UPGRADE_REMOVAL_SETTINGS = set(['enforce_storage_engine',
                                'init_file',
                                'disabled_storage_engines'])


log = environment_specific.setup_logging_defaults(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--override_dir',
                        help='Useful for testing. Instead of writing files '
                             'to where they should exist for production use, '
                             'instead write all to this directory.')
    parser.add_argument('--override_hostname',
                        help='Useful for testing. Instead of configing '
                             'based on localhost hostname, use supplied '
                             'hostname.  This will also override the MySQL '
                             'version so you can run it on hosts w/o MySQL '
                             'installed or running.')
    parser.add_argument('--override_mysql_version',
                        help='Useful for testing.  Set the MySQL version to '
                             'build the configs for.  Allows running this '
                             'script on hosts without MySQL installed.',
                        choices=environment_specific.SUPPORTED_MYSQL_MAJOR_VERSIONS)
    args = parser.parse_args()
    if args.override_hostname:
        override_host = host_utils.HostAddr(args.override_hostname)
    else:
        override_host = None
    build_cnf(override_host, args.override_dir, args.override_mysql_version)


def build_cnf(host=None,
              override_dir=None,
              override_mysql_version=None):
    # There are situations where we don't want to overwrite the
    # existing config file, because we are testing, etc...
    if os.path.isfile(TOUCH_FOR_NO_CONFIG_OVERWRITE):
        log.info('Found {}.  Will not overwrite anything.\n'
                 'Exiting now.'.format(TOUCH_FOR_NO_CONFIG_OVERWRITE))
        return

    if not host:
        host = host_utils.HostAddr(host_utils.HOSTNAME)

    if override_mysql_version:
        major_version = override_mysql_version
    else:
        major_version = mysql_lib.get_installed_mysqld_version()[:3]

    if major_version not in environment_specific.SUPPORTED_MYSQL_MAJOR_VERSIONS:
        log.info('CNF building is not supported in '
                 '{}'.format(major_version))
        return

    config_files = list()
    parser = ConfigParser.RawConfigParser(allow_no_value=True)

    # Always use the local config files for the executing script
    RELATIVE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                CONFIG_SUB_DIR)
    config_files.append(os.path.join(RELATIVE_DIR,
                                     CNF_DEFAULTS))

    log.info('MySQL major version detected as {major_version}'
             ''.format(major_version=major_version))
    config_files.append(os.path.join(RELATIVE_DIR,
                                     major_version))

    instance_type = host_utils.get_instance_type()
    log.info('Hardware detected as {instance_type}'
             ''.format(instance_type=instance_type))
    config_files.append(os.path.join(RELATIVE_DIR,
                                     instance_type))

    log.info('Hostname "{hostname}" results in hostname prefix "{prefix}"'
             ''.format(hostname=host.hostname,
                       prefix=host.hostname_prefix))
    config_files.append(os.path.join(RELATIVE_DIR, host.replica_type))

    # Using the config files, setup a config file parser
    log.info('Using config files {}'.format(config_files))
    parser.read(config_files)

    # Set the server server_id based upon hostname
    server_id = hostname_to_server_id(host.hostname)
    log.info('Setting server_id to {}'.format(server_id))
    parser.set(MYSQLD_SECTION, 'server_id', server_id)

    ro_status = config_read_only(host)
    parser.set(MYSQLD_SECTION, 'read_only', ro_status)

    # If needed, turn on safe updates via an init_file
    create_init_sql(host.hostname_prefix, parser, override_dir)

    # Set the hostname and root volume through the config
    replace_config_tag(parser, HOSTNAME_TAG, host.hostname)
    replace_config_tag(parser, ROOTVOL_TAG, host_utils.find_root_volume())

    # Remove config elements as directed
    remove_config_by_override(parser)

    # Write out the mysql cnf files
    create_mysql_cnf_files(parser, override_dir)

    # Create log rotate conf for MySQL
    create_log_rotate_conf(parser, override_dir)

    # Create .my.cnf to set username/password defaults for local usage
    create_root_cnf(parser, override_dir)

    # Create pt heartbeat conf in order to be able to calculate replication lag
    create_pt_heartbeat_conf(override_dir)

    # Create pt kill conf in order to kill long running queries
    create_pt_kill_conf(override_dir)

    # We don't create the maxwell config file here; we let the
    # daemon-restarter do it, because we need MySQL to be running
    # to know some of the settings.


def replace_config_tag(parser, tag, replace_value):
    """ Replace a tag in the config with some other value.
        Used, for example, to fill in the hostname or the root volume.

    Args:
    parser: A configParser object
    tag: The string (tag) to replace
    replace_value: The string to replace it with
    """
    for section in parser.sections():
        for item in parser.items(section):
            if type(item[1]) is str and tag in item[1]:
                parser.set(section, item[0],
                           item[1].replace(tag, replace_value))


def hostname_to_server_id(hostname):
    """ Convert a hostname to a MySQL server_id using its ip address

    Args:
    hostname - A string

    Returns:
    An integer to be used for the server_id
    """
    ip = socket.gethostbyname(hostname)
    parts = ip.split('.')
    return ((int(parts[0]) << 24) + (int(parts[1]) << 16) +
            (int(parts[2]) << 8) + int(parts[3]))


def config_read_only(host):
    """ Determine how read_only should be set in the cnf file

    Args:
        host - a hostaddr object

    Returns:
        The string value of READ_ONLY_OFF or READ_ONLY_ON.
    """
    zk = host_utils.MysqlZookeeper()
    try:
        replica_type = zk.get_replica_type_from_instance(host)
    except:
        # If it is not in zk OR there is any other error, the safest thing is
        # to treat it as if it was not in zk and therefore read_only set to ON
        replica_type = None
    if replica_type == host_utils.REPLICA_ROLE_MASTER:
        log.info('Server is considered a master, therefore read_only '
                 'should be OFF')
        return READ_ONLY_OFF
    elif replica_type in (host_utils.REPLICA_ROLE_DR_SLAVE,
                          host_utils.REPLICA_ROLE_SLAVE):
        log.info('Server is considered a replica, therefore read_only '
                 'should be ON')
        return READ_ONLY_ON
    elif os.path.isfile(TOUCH_FOR_WRITABLE_IF_NOT_IN_ZK):
        log.info('Server is not in zk and {path} exists, therefore read_only '
                 'should be OFF'
                 ''.format(path=TOUCH_FOR_WRITABLE_IF_NOT_IN_ZK))
        return READ_ONLY_OFF
    else:
        log.info('Server is not in zk and {path} does not exist, therefore '
                 'read_only should be ON'
                 ''.format(path=TOUCH_FOR_WRITABLE_IF_NOT_IN_ZK))
        return READ_ONLY_ON


def remove_config_by_override(parser):
    """ Slightly ugly hack to allow removal of config entries.

    Args:
    parser - A ConfigParser object
    """
    for option in parser.options(MYSQLD_SECTION):
        if option.startswith(REMOVE_SETTING_PREFIX):
            option_to_remove = option[len(REMOVE_SETTING_PREFIX):]
            # first, get rid of the option we actually want to remove.
            if parser.has_option(MYSQLD_SECTION, option_to_remove):
                parser.remove_option(MYSQLD_SECTION, option_to_remove)

            # and then get rid of the remove flag for it.
            parser.remove_option(MYSQLD_SECTION, option)


def create_skip_replication_cnf(override_dir=None):
    """ Create a secondary cnf file that will allow for mysql to skip
        replication start. Useful for running mysql upgrade, etc...

    Args:
    override_dir - Write to this directory rather than CNF_DIR
    """
    skip_replication_parser = ConfigParser.RawConfigParser(allow_no_value=True)
    skip_replication_parser.add_section(MYSQLD_SECTION)
    skip_replication_parser.set(MYSQLD_SECTION, 'skip_slave_start', None)
    if override_dir:
        skip_slave_path = os.path.join(override_dir,
                                       os.path.basename(host_utils.MYSQL_NOREPL_CNF_FILE))
    else:
        skip_slave_path = host_utils.MYSQL_NOREPL_CNF_FILE
    log.info('Writing file {skip_slave_path}'
             ''.format(skip_slave_path=skip_slave_path))
    with open(skip_slave_path, "w") as skip_slave_handle:
            skip_replication_parser.write(skip_slave_handle)


def create_log_rotate_conf(parser, override_dir=None):
    """ Create log rotate conf for MySQL

    Args:
    override_dir - Write to this directory rather than default
    parser - A ConfigParser object of mysqld settings
    """
    files_to_rotate = ''
    for rotate_file in LOG_ROTATE_FILES:
        files_to_rotate = ' '.join((files_to_rotate,
                                    parser.get(MYSQLD_SECTION, rotate_file)))
    log_rotate_values = '\n\t'.join(LOG_ROTATE_SETTINGS)
    log_rotate_settings = LOG_ROTATE_TEMPLATE.format(files=files_to_rotate,
                                                     settings=log_rotate_values)
    if override_dir:
        log_rotate_conf_file = os.path.join(override_dir,
                                            os.path.basename(LOG_ROTATE_CONF_FILE))
    else:
        log_rotate_conf_file = LOG_ROTATE_CONF_FILE

    log.info('Writing log rotate config {path}'.format(path=log_rotate_conf_file))
    with open(log_rotate_conf_file, "w") as log_rotate_conf_file_handle:
        log_rotate_conf_file_handle.write(log_rotate_settings)


def create_maxwell_config(client_id, instance, exclude_dbs=None, 
                          target='kafka', gtid_mode='true'):
    """ Create the maxwell config file.

    Args:
        client_id = The server_uuid
        instance = What instance is this?
        exclude_dbs = Exclude these databases (in addition to mysql and test)
        target = Output to kafka or a file (which will be /dev/null)
        gtid_mode = True if this is a GTID cluster, false otherwise
    Returns:
        Nothing
    """
    template_path = os.path.join(RELATIVE_DIR, MAXWELL_TEMPLATE)
    with open(template_path, 'r') as f:
        template = f.read()

    (username, password) = mysql_lib.get_mysql_user_for_role('maxwell')
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)
    hostname_prefix = instance.hostname_prefix
    if hostname_prefix in environment_specific.FLEXSHARD_DBS or hostname_prefix in environment_specific.SHARDED_DBS_PREFIX:
        namespace = hostname_prefix
    else:
        namespace = replica_set
    master = zk.get_mysql_instance_from_replica_set(replica_set,
                                                    host_utils.REPLICA_ROLE_MASTER)
    log.info('Writing file {}'.format(MAXWELL_CONF_FILE))
    excluded = ','.join(['mysql', 'test', exclude_dbs]) if exclude_dbs \
                else 'mysql,test'

    target_map = environment_specific.MAXWELL_TARGET_MAP[master.hostname_prefix]
    with open(MAXWELL_CONF_FILE, "w") as f:
        f.write(template.format(master_host=master.hostname,
                                master_port=master.port,
                                instance_host=instance.hostname,
                                instance_port=instance.port,
                                username=username,
                                password=password,
                                kafka_topic=target_map['kafka_topic'],
                                kafka_servers=target_map['kafka_servers'],
                                generator=target_map['generator'],
                                zen_service=target_map['zen_service'],
                                client_id=client_id,
                                output=target,
                                excludes=excluded,
                                gtid_mode=gtid_mode,
                                namespace=namespace))


def create_mysql_cnf_files(parser, override_dir=None):
    """ Write out various mysql cnf files

    Args:
    parser - A ConfigParser object of mysqld settings
    override_dir - Write to this directory rather than default
    """
    if override_dir:
        cnf_path = os.path.join(override_dir, os.path.basename(host_utils.MYSQL_CNF_FILE))
        upgrade_cnf_path = os.path.join(override_dir,
                                        os.path.basename(host_utils.MYSQL_UPGRADE_CNF_FILE))
    else:
        cnf_path = host_utils.MYSQL_CNF_FILE
        upgrade_cnf_path = host_utils.MYSQL_UPGRADE_CNF_FILE
    log.info('Writing file {cnf_path}'.format(cnf_path=cnf_path))
    with open(cnf_path, "w") as cnf_handle:
        parser.write(cnf_handle)

    # Next create the cnf used for version upgrads
    for option in UPGRADE_OVERRIDE_SETTINGS.keys():
        parser.set(MYSQLD_SECTION, option, UPGRADE_OVERRIDE_SETTINGS[option])

    for option in UPGRADE_REMOVAL_SETTINGS:
        parser.remove_option(MYSQLD_SECTION, option)

    log.info('Writing file {upgrade_cnf_path}'
             ''.format(upgrade_cnf_path=upgrade_cnf_path))
    with open(upgrade_cnf_path, "w") as upgrade_cnf_handle:
        parser.write(upgrade_cnf_handle)

    create_skip_replication_cnf(override_dir)


def create_init_sql(replica_set_type, parser, override_dir):
    """ Create a init.sql file if needed

    Args:
    replica_type - Hostname prefix of the db host to be configured
    parser -  A ConfigParser object of mysqld settings
    override_dir - Write to this directory rather than default
    """
    if replica_set_type in SAFE_UPDATE_PREFIXES:
        log.info('Turning on safe updates')
        if override_dir:
            init_file_path = os.path.join(override_dir,
                                          os.path.basename(host_utils.MYSQL_INIT_FILE))
        else:
            init_file_path = host_utils.MYSQL_INIT_FILE
        parser.set(MYSQLD_SECTION, 'init_file', init_file_path)
        with open(init_file_path, "w") as init_file_handle:
            init_file_handle.write(SAFE_UPDATES_SQL)


def create_root_cnf(cnf_parser, override_dir):
    """ Create a .my.cnf file to setup defaults for username/password

    Args:
    cnf_parser - A ConfigParser object of mysqld settings
    override_dir - Write to this directory rather than default
    """
    admin_user, admin_password = mysql_lib.get_mysql_user_for_role('admin')
    dump_user, dump_password = mysql_lib.get_mysql_user_for_role('mysqldump')
    parser = ConfigParser.RawConfigParser(allow_no_value=True)
    parser.add_section('mysql')
    parser.set('mysql', 'user', admin_user)
    parser.set('mysql', 'password', admin_password)
    parser.set('mysql', 'socket', cnf_parser.get(MYSQLD_SECTION, 'socket'))
    parser.add_section('mysqladmin')
    parser.set('mysqladmin', 'user', admin_user)
    parser.set('mysqladmin', 'password', admin_password)
    parser.set('mysqladmin', 'socket', cnf_parser.get(MYSQLD_SECTION, 'socket'))
    parser.add_section('mysqldump')
    parser.set('mysqldump', 'user', dump_user)
    parser.set('mysqldump', 'password', dump_password)
    parser.set('mysqldump', 'socket', cnf_parser.get(MYSQLD_SECTION, 'socket'))

    if override_dir:
        root_cnf_path = os.path.join(override_dir, os.path.basename(ROOT_CNF))
    else:
        root_cnf_path = ROOT_CNF
    log.info('Writing file {root_cnf_path}'
             ''.format(root_cnf_path=root_cnf_path))
    with open(root_cnf_path, "w") as root_cnf_handle:
        parser.write(root_cnf_handle)


def create_pt_heartbeat_conf(override_dir):
    """ Create the config file for pt-hearbeat

    Args:
    override_dir - Write to this directory rather than default
    """
    template_path = os.path.join(RELATIVE_DIR, PT_HEARTBEAT_TEMPLATE)
    with open(template_path, 'r') as f:
        template = f.read()

    heartbeat_user, heartbeat_password = mysql_lib.get_mysql_user_for_role('ptheartbeat')

    if override_dir:
        heartbeat_cnf_path = os.path.join(override_dir, os.path.basename(PT_HEARTBEAT_CONF_FILE))
    else:
        heartbeat_cnf_path = PT_HEARTBEAT_CONF_FILE
    log.info('Writing file {heartbeat_cnf_path}'
             ''.format(heartbeat_cnf_path=heartbeat_cnf_path))
    with open(heartbeat_cnf_path, "w") as heartbeat_cnf_handle:
        heartbeat_cnf_handle.write(template.format(defaults_file=host_utils.MYSQL_CNF_FILE,
                                                   username=heartbeat_user,
                                                   password=heartbeat_password,
                                                   metadata_db=mysql_lib.METADATA_DB))


def create_pt_kill_conf(override_dir):
    """ Create the config file for pt-kill

    Args:
    override_dir - Write to this directory rather than default
    """
    template_path = os.path.join(RELATIVE_DIR, PT_KILL_TEMPLATE)
    with open(template_path, 'r') as f:
        template = f.read()

    kill_user, kill_password = mysql_lib.get_mysql_user_for_role('ptkill')

    if override_dir:
        kill_cnf_path = os.path.join(override_dir, os.path.basename(PT_KILL_CONF_FILE))
    else:
        kill_cnf_path = PT_KILL_CONF_FILE
    log.info('Writing file {kill_cnf_path}'
             ''.format(kill_cnf_path=kill_cnf_path))
    with open(kill_cnf_path, "w") as kill_cnf_handle:
        kill_cnf_handle.write(template.format(username=kill_user,
                                              password=kill_password,
                                              busy_time=PT_KILL_BUSY_TIME,
                                              ignore_users='|'.join(PT_KILL_IGNORE_USERS)))


if __name__ == "__main__":
    main()
