#!/usr/bin/env python
import argparse

import MySQLdb
import _mysql_exceptions

import launch_amazon_mysql_server
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

SUPPORTED_MYSQL_MAJOR_VERSIONS = {'5.5': '55', '5.6': '56'}
DEFAULT_MYSQL_MAJOR_VERSION = '5.6'
SUPPORTED_MYSQL_MINOR_VERSIONS = set(('stable', 'staging', 'latest'))
DEFAULT_MYSQL_MINOR_VERSION = 'stable'

log = environment_specific.setup_logging_defaults(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('server',
                        help='The server to be replaced')
    parser.add_argument('--reason',
                        help=('The reason why a server is being replaced. '
                              'The script will classify MySQL down as '
                              'hardware_failure, and this argument is not '
                              'required, otherwise this argument is required. '
                              'Suggested values: hardware_failure, '
                              'hardware_upgrade, mysql_upgrade, '
                              'performance_degradation'),
                        default='')
    parser.add_argument('--replace_again',
                        help=('Launch a new replacement even if a replacement '
                              'has already been ordered'),
                        default=False,
                        action='store_true')
    parser.add_argument('--dry_run',
                        help=('Do not actually launch an instance, just show '
                              'the intended configuration'),
                        default=False,
                        action='store_true')
    parser.add_argument('--not_a_replacement',
                        help=("Just create a new replica, don't replace the "
                              "existing instance"),
                        default=False,
                        action='store_true')
    parser.add_argument('--override_az',
                        help=('Do not replace with an instance in the same '
                              'availability zone, instead use the supplied '
                              'availability zone.'),
                        choices=environment_specific.SUPPORTED_AZ,
                        default=None)
    parser.add_argument('--override_hostname',
                        help=('Do not derive a hostname, use supplied '
                              'hostname instead.'),
                        default=None)
    parser.add_argument('--override_hw',
                        help=('Do not replace with an instance of the same '
                              'instance type, instead use the supplied '
                              'instance type.'),
                        choices=sorted(environment_specific.SUPPORTED_HARDWARE,
                                       reverse=True),
                        default=None)
    parser.add_argument('--override_mysql_major_version',
                        help=('Do not replace with an instance of the same '
                              'version as the master db, instead use the '
                              'supplied version.'),
                        choices=SUPPORTED_MYSQL_MAJOR_VERSIONS.keys(),
                        default=None)
    parser.add_argument('--override_mysql_minor_version',
                        help=('Which "branch" of the MySQL major version'
                              'to be used. Default is "stable".'),
                        choices=SUPPORTED_MYSQL_MINOR_VERSIONS,
                        # default is set in the underlying function
                        default=None)
    parser.add_argument('--override_classic_security',
                        help=('Do not replace with an instance in the same '
                              'security group, instead use the supplied '
                              'security group in AWS CLASSIC.'),
                        default=None)
    parser.add_argument('--override_vpc_security',
                        help=('Do not replace with an instance in the same '
                              'security group, instead use the supplied '
                              'security group in AWS VPC.'),
                        default=None)

    args = parser.parse_args()
    overrides = {'availability_zone': args.override_az,
                 'classic_security_group': args.override_classic_security,
                 'hostname': args.override_hostname,
                 'instance_type': args.override_hw,
                 'mysql_major_version': args.override_mysql_major_version,
                 'mysql_minor_version': args.override_mysql_minor_version,
                 'vpc_security_group': args.override_vpc_security}

    launch_replacement_db_host(original_server=host_utils.HostAddr(args.server),
                               dry_run=args.dry_run,
                               not_a_replacement=args.not_a_replacement,
                               overrides=overrides,
                               reason=args.reason,
                               replace_again=args.replace_again)


def launch_replacement_db_host(original_server,
                               dry_run=False,
                               not_a_replacement=False,
                               overrides=dict(),
                               reason='',
                               replace_again=False):
    """ Launch a replacement db server

    Args:
    original_server - A hostAddr object for the server to be replaced
    dry_run - If True, do not actually launch a replacement
    not_a_replacement - If set, don't log the replacement, therefore
                        automation won't put it into prod use.
    overrides - A dict of overrides. Availible keys are
                'mysql_minor_version', 'hostname', 'vpc_security_group',
                'availability_zone', 'classic_security_group',
                'instance_type', and 'mysql_major_version'.
    reason - A description of why the host is being replaced. If the instance
             is still accessible and reason is not supply an exception will be
             thrown.
    replace_again - If True, ignore already existing replacements.
    """
    reasons = set()
    if reason:
        reasons.add(reason)

    log.info('Trying to launch a replacement for host {host} which is part '
             'of replica set is {replica_set}'.format(host=original_server.hostname,
                                                      replica_set=original_server.get_zk_replica_set()[0]))

    zk = host_utils.MysqlZookeeper()
    try:
        (_, replica_type) = zk.get_replica_set_from_instance(original_server)
    except:
        raise Exception('Can not replace an instance which is not in zk')
    if replica_type == host_utils.REPLICA_ROLE_MASTER:
        # If the instance, we will refuse to run. No ifs, ands, or buts/
        raise Exception('Can not replace an instance which is a master in zk')

    # Open a connection to MySQL Ops and check if a replacement has already
    # been requested
    reporting_conn = mysql_lib.get_mysqlops_connections()
    existing_replacement = find_existing_replacements(reporting_conn,
                                                      original_server)
    if existing_replacement and not not_a_replacement:
        if replace_again:
            log.info('A replacement has already been requested: '
                     '{new_host}'.format(new_host=existing_replacement))
        else:
            raise Exception('A replacement already exists, but '
                            'replace_again is not True')

    # Pull some information from cmdb.
    cmdb_data = environment_specific.get_server_metadata(original_server.hostname)
    if not cmdb_data:
        raise Exception('Could not find information about server to be '
                        'replaced in the cmdb')

    log.info('Data from cmdb: {cmdb_data}'.format(cmdb_data=cmdb_data))
    replacement_config = {'availability_zone': cmdb_data['location'],
                          'hostname': find_unused_server_name(original_server.get_standardized_replica_set(),
                                                              reporting_conn, dry_run),
                          'instance_type': cmdb_data['config.instance_type'],
                          'mysql_major_version': get_master_mysql_major_version(original_server),
                          'mysql_minor_version': DEFAULT_MYSQL_MINOR_VERSION,
                          'dry_run': dry_run,
                          'skip_name_check': True}

    if cmdb_data.pop('cloud.aws.vpc_id', None):
        # Existing server is in VPC
        replacement_config['classic_security_group'] = None
        replacement_config['vpc_security_group'] = cmdb_data['security_groups']
    else:
        # Existing server is in Classic
        replacement_config['classic_security_group'] = cmdb_data['security_groups']
        replacement_config['vpc_security_group'] = None

    # At this point, all our defaults should be good to go
    config_overridden = False
    if replacement_config['classic_security_group'] and overrides['vpc_security_group']:
        # a VPC migration
        vpc_migration(replacement_config, overrides)
        reasons.add('vpc migration')
        config_overridden = True

    # All other overrides
    for key in overrides.keys():
        if key not in replacement_config:
            raise Exception('Invalid override {key}'.format(key=key))

        if overrides[key]:
            if replacement_config[key] == overrides[key]:
                log.info('Override for key {key} does not modify '
                         'configuration'.format(key=key))
            else:
                log.info('Overriding {key} to value {new} from {old}'
                         ''.format(key=key,
                                   old=replacement_config[key],
                                   new=overrides[key]))
                replacement_config[key] = overrides[key]
                reasons.add('changing {key} from {old} to '
                            '{old}'.format(key=key,
                                           old=replacement_config[key],
                                           new=overrides[key]))
                config_overridden = True

    if config_overridden:
        log.info('Configuration after overrides: {replacement_config}'
                 ''.format(replacement_config=replacement_config))

    # Check to see if MySQL is up on the host
    try:
        # This is not multi instance compatible. If we move to multiple
        # instances this will need to be updated
        conn = mysql_lib.connect_mysql(original_server)
        conn.close()
        dead_server = False
    except MySQLdb.OperationalError as detail:
        dead_server = True
        (error_code, msg) = detail.args
        if error_code != mysql_lib.MYSQL_ERROR_CONN_HOST_ERROR:
            raise
        log.info('MySQL is down, assuming hardware failure')
        reasons.add('hardware failure')

    if not dead_server:
        slave_status = mysql_lib.calc_slave_lag(original_server)
        if slave_status['ss']['Slave_SQL_Running'] != 'Yes':
            reasons.add('sql replication thread broken')

        if slave_status['ss']['Slave_IO_Running'] != 'Yes':
            reasons.add('io replication thread broken')

    # If we get to here and there is no reason, bail out
    if not reasons and not replacement_config['dry_run']:
        raise Exception(('MySQL appears to be up and no reason for '
                         'replacement is supplied'))
    reason = ', '.join(reasons)
    log.info('Reason for launch: {reason}'.format(reason=reason))

    new_instance_id = launch_amazon_mysql_server.launch_amazon_mysql_server(**replacement_config)
    if not (replacement_config['dry_run'] or not_a_replacement):
        log_replacement_host(reporting_conn, cmdb_data, new_instance_id,
                             replace_again, replacement_config, reason)


def find_unused_server_name(replica_set, conn, dry_run):
    """ Increment a db servers hostname

    The current naming convention for db servers is:
    {Shard Type}-{Shard number}-{Server number}


    Note: The current naming convention for db servers is:
    {Shard Type}{Shard number}{Server letter}

    The purpose of this function is to find the next server letter
    that is not used.

    Args:
    replica_set - The replica of the host to be replaced
    conn -  A mysql connection to the reporting server
    dry_run - don't log that a hostname will be used
    """
    cmdb_servers = environment_specific.get_all_replica_set_servers(replica_set)
    next_host_num = 1
    for server in cmdb_servers:
        host = host_utils.HostAddr(server['config.name'])

        # We should be able to iterate over everything that came back from the
        # cmdb and find out the greatest host number in use for a replica set
        if not host.host_identifier:
            # unparsable, probably not previously under dba management
            continue

        if (len(host.host_identifier) == 1 and
                ord(host.host_identifier) in range(ord('a'), ord('z'))):
            # old style hostname
            continue

        if int(host.host_identifier) >= next_host_num:
            next_host_num = int(host.host_identifier) + 1
    new_hostname = '-'.join((replica_set, str(next_host_num)))

    while True:
        if is_hostname_new(new_hostname, conn):
            if not dry_run:
                log_new_hostname(new_hostname, conn)
            return new_hostname

        log.info('Hostname {hostname} has been logged to be in use but is not '
                 'in brood or dns'.format(hostname=new_hostname))
        next_host_num = next_host_num + 1
        new_hostname = '-'.join((replica_set, str(next_host_num)))


def is_hostname_new(hostname, conn):
    """ Determine if a hostname has ever been used

    Args:
    hostname - a hostname
    conn -  a mysql connection to the reporting server

    Returns:
    True if the hostname is availible for new use, False otherwise
    """
    cursor = conn.cursor()

    sql = ("SELECT count(*) as cnt "
           "FROM mysqlops.unique_hostname_index "
           "WHERE hostname = %(hostname)s ")
    params = {'hostname': hostname}
    cursor.execute(sql, params)
    ret = cursor.fetchone()

    if ret['cnt'] == 0:
        return True
    else:
        return False


def log_new_hostname(hostname, conn):
    """ Determine if a hostname has ever been used

    Args:
    hostname - a hostname
    conn -  a mysql connection to the reporting server

    Returns:
    True if the hostname is availible for new use, False otherwise
    """
    cursor = conn.cursor()

    sql = ("INSERT INTO mysqlops.unique_hostname_index "
           "SET hostname = %(hostname)s ")
    params = {'hostname': hostname}
    cursor.execute(sql, params)
    conn.commit()


def find_existing_replacements(reporting_conn, old_host):
    """ Determine if a request has already been requested

    Args:
    reporting_conn - A MySQL connect to the reporting server
    old_host - The hostname for the host to be replaced

    Returns:
    If a replacement has been requested, a dict with the following elements:
        new_host - The hostname of the new server
        new_instance - The instance id of the new server
        created_at - When the request was created

    If a replacement has not been requested, then return None.
    """
    cursor = reporting_conn.cursor()

    sql = ("SELECT new_host, new_instance, created_at "
           "FROM mysqlops.host_replacement_log "
           "WHERE old_host = %(old_host)s ")
    params = {'old_host': old_host.hostname}
    cursor.execute(sql, params)
    ret = cursor.fetchone()

    if ret:
        new_host = {'new_host': ret['new_host'],
                    'new_instance': ret['new_instance'],
                    'created_at': ret['created_at']}
        return new_host
    else:
        return None


def log_replacement_host(reporting_conn, original_server_data, new_instance_id,
                         replace_again, replacement_config, reason):
    """ Log to a central db the server being replaced and why

    Args:
    reporting_conn - A connection to MySQL Ops reporting server
    original_server_data - A dict of information regarding the server to be
                           replaced
    new_instance_id - The instance id of the replacement server
    replace_again - If set, replace an existing log entry for the replacement
    replacement_config - A dict of information regarding the replacement server
    reason - A string explaining why the server is being replaced
    """
    cursor = reporting_conn.cursor()

    sql = ("INSERT INTO mysqlops.host_replacement_log "
           "SET "
           "old_host = %(old_host)s, "
           "old_instance = %(old_instance)s, "
           "old_az = %(old_az)s, "
           "old_hw_type = %(old_hw_type)s, "
           "new_host = %(new_host)s, "
           "new_instance = %(new_instance)s, "
           "new_az = %(new_az)s, "
           "new_hw_type = %(new_hw_type)s, "
           "reason = %(reason)s ")

    if replace_again:
        sql = sql.replace('INSERT INTO', 'REPLACE INTO')

    params = {'old_host': original_server_data['config.name'],
              'old_instance': original_server_data['id'],
              'old_az': original_server_data['location'],
              'old_hw_type': original_server_data['config.instance_type'],
              'new_host': replacement_config['hostname'],
              'new_instance': new_instance_id,
              'new_az': replacement_config['availability_zone'],
              'new_hw_type': replacement_config['instance_type'],
              'reason': reason}
    try:
        cursor.execute(sql, params)
    except _mysql_exceptions.IntegrityError:
        raise Exception('A replacement has already been requested')
    reporting_conn.commit()


def get_master_mysql_major_version(instance):
    """ Given an instance, determine the mysql major version for the master
        of the replica set.

    Args:
    instance - a hostaddr object

    Returns - A string similar to '5.5' or '5.6'
   """
    zk = host_utils.MysqlZookeeper()
    master = zk.get_mysql_instance_from_replica_set(instance.get_zk_replica_set()[0],
                                                    repl_type=host_utils.REPLICA_ROLE_MASTER)
    master_conn = mysql_lib.connect_mysql(master)
    mysql_version = mysql_lib.get_global_variables(master_conn)['version'][:3]
    return mysql_version


def vpc_migration(replacement_config, overrides):
    """ Figure out if a replacement is valid, and then update the
        replacement_config as needed

    Args:
    replacement_config - A dict of the default configuration for launching
                         a new server
    overrides - A dict of overrides for the default configuration
    """
    if overrides['vpc_security_group'] in \
            environment_specific.VPC_MIGRATION_MAP[replacement_config['classic_security_group']]:
        log.info('VPC migration: {classic} -> {vpc_sg}'.format(classic=replacement_config['classic_security_group'],
                                                               vpc_sg=overrides['vpc_security_group']))
        replacement_config['vpc_security_group'] = overrides['vpc_security_group']
        overrides['vpc_security_group'] = None
        replacement_config['classic_security_group'] = None
    else:
        raise Exception('VPC security group {vpc_sg} is not a valid replacement '
                        'for classic security group {classic_sg}. Valid options are:'
                        '{options}'.format(vpc_sg=overrides['vpc_security_group'],
                                           classic_sg=replacement_config['classic_security_group'],
                                           options=environment_specific.VPC_MIGRATION_MAP[replacement_config['classic_security_group']]))


if __name__ == "__main__":
    main()
