#!/usr/bin/env python
import argparse
import pprint

import boto.ec2
import MySQLdb
import MySQLdb.cursors

from lib import host_utils
from lib import mysql_lib
from lib import timeout
from lib import environment_specific


MIN_CMDB_RESULTS = 100
RESET_STATS = 'Reset statistics'
SHUTDOWN_MYSQL = 'Shutdown MySQL'
TERMINATE_INSTANCE = 'Terminate instance'
IGNORABLE_USERS = set(["admin", "ptkill", "monit",
                       "#mysql_system#", 'ptchecksum',
                       "replicant", "root", "heartbeat", "system user"])

log = environment_specific.setup_logging_defaults(__name__)
chat_handler = environment_specific.BufferingChatHandler()
log.addHandler(chat_handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action',
                        help='What action to take. The first step is '
                             'add_to_queue which add a server to the queue '
                             'and resets some status. The "shutdown_mysql" '
                             'runs against all servers in the queue for more '
                             'than a day, and it checks statistics and if ok, '
                             'shutsdown MySQL. The "terminate_instance" will '
                             'terminate instances a day after "shutdown_mysql" '
                             'has run. If (un)protect_instance" then the '
                             'supplied host will be (un)exempted from '
                             'retirement/termination. get_protected_hosts '
                             'will display what is protected from retirement',
                        choices=['add_to_queue', 'process_mysql_shutdown',
                                 'terminate_instances', 'protect_instance',
                                 'unprotect_instance', 'get_protected_hosts'])
    parser.add_argument('--dry_run',
                        help="Don't change any state",
                        action='store_true')
    parser.add_argument('--reason',
                        help="Only for action protect_instance. Specify "
                             "why the server should not be shutdown.")
    parser.add_argument('--hostname',
                        help=('The server to be acted upon, required for '
                              "add_to_queue and(un)protect_instance and "
                              "optional otherwise"),
                        default=None)
    args = parser.parse_args()

    if args.dry_run:
        log.removeHandler(chat_handler)

    if args.dry_run and (args.action == 'protect_instance' or
                         args.action == 'unprotect_instance'):
        raise Exception('Dry run is not supported for this action')

    log.info('action is {action}'.format(action=args.action))

    if args.action == 'add_to_queue':
        if args.hostname is None:
            raise Exception('Arg --hostname is required for action '
                            'add_to_queue')
        add_to_queue(args.hostname, args.dry_run)
    elif args.action == 'process_mysql_shutdown':
        process_mysql_shutdown(args.hostname, args.dry_run)
    elif args.action == 'terminate_instances':
        terminate_instances(args.hostname, args.dry_run)
    elif args.action == 'protect_instance':
        protect_host(args.hostname, args.reason)
    elif args.action == 'unprotect_instance':
        unprotect_host(args.hostname)
    elif args.action == 'get_protected_hosts':
        entries = get_protected_hosts()
        if entries:
            pprint.pprint(entries)

    else:
        raise Exception('Unexpected action '
                        '{action}'.format(action=args.action))


def add_to_queue(hostname, dry_run):
    """ Add an instance to the retirement queue

    Args:
    hostname - The hostname of the instance to add to the retirement queue
    """
    log.info('Adding server {hostname} to retirement '
             'queue'.format(hostname=hostname))

    if hostname in get_protected_hosts('set'):
        raise Exception('Host {hostname} is protected from '
                        'retirement'.format(hostname=hostname))

    # basic sanity check
    zk = host_utils.MysqlZookeeper()
    for instance in zk.get_all_mysql_instances():
        if instance.hostname == hostname:
            raise Exception("It appears {instance} is in zk. This is "
                            "very dangerous!".format(instance=instance))
    all_servers = environment_specific.get_all_server_metadata()
    if not hostname in all_servers:
        raise Exception('Host {hostname} is not cmdb'.format(hostname=hostname))

    instance_metadata = all_servers[hostname]
    log.info(instance_metadata)
    username, password = mysql_lib.get_mysql_user_for_role('admin')

    try:
        log.info('Trying to reset user_statistics on ip '
                 '{ip}'.format(ip=instance_metadata['internal_ip']))
        with timeout.timeout(3):
            conn = MySQLdb.connect(host=instance_metadata['internal_ip'],
                                   user=username,
                                   passwd=password,
                                   cursorclass=MySQLdb.cursors.DictCursor)
        if not conn:
            raise Exception('timeout')
        mysql_lib.enable_and_flush_activity_statistics(conn)
        activity = RESET_STATS
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != mysql_lib.MYSQL_ERROR_CONN_HOST_ERROR:
            raise
        log.info('Could not connect to '
                 '{ip}'.format(ip=instance_metadata['internal_ip']))
        activity = SHUTDOWN_MYSQL

    log_to_retirement_queue(hostname, instance_metadata['instance_id'],
                            activity)


def process_mysql_shutdown(hostname=None, dry_run=False):
    """ Check stats, and shutdown MySQL instances"""
    zk = host_utils.MysqlZookeeper()
    username, password = mysql_lib.get_mysql_user_for_role('admin')
    shutdown_instances = get_retirement_queue_servers(SHUTDOWN_MYSQL)

    if hostname:
        if hostname in shutdown_instances:
            log.info('Only acting on {hostname}'.format(hostname=hostname))
            shutdown_instances = {hostname: shutdown_instances[hostname]}
        else:
            log.info('Supplied host {hostname} is not ready '
                     'for shutdown'.format(hostname=hostname))
            return

    for instance in shutdown_instances:
        if instance in get_protected_hosts('set'):
            log.warning('Host {hostname} is protected from '
                        'retirement'.format(hostname=hostname))
            remove_from_retirement_queue(hostname)
            continue
        for active_instance in zk.get_all_mysql_instances():
            if active_instance.hostname == instance:
                log.warning("It appears {instance} is in zk. This is "
                            "very dangerous!".format(instance=instance))
                remove_from_retirement_queue(instance)
                continue
        log.info('Checking activity on {instance}'.format(instance=instance))
        # check mysql activity
        with timeout.timeout(3):
            conn = MySQLdb.connect(host=shutdown_instances[instance]['internal_ip'],
                                   user=username,
                                   passwd=password,
                                   cursorclass=MySQLdb.cursors.DictCursor)
        if not conn:
            raise Exception('Could not connect to {ip}'
                            ''.format(ip=shutdown_instances[instance]['internal_ip']))

        activity = mysql_lib.get_user_activity(conn)
        unexpected = set(activity.keys()).difference(IGNORABLE_USERS)
        if unexpected:
            log.error('Unexpected acitivty on {instance} by user(s):'
                      '{unexpected}'.format(instance=instance,
                                            unexpected=','.join(unexpected)))
            continue

        log.info('Checking current connections on '
                 '{instance}'.format(instance=instance))
        connected_users = mysql_lib.get_connected_users(conn)
        unexpected = connected_users.difference(IGNORABLE_USERS)
        if unexpected:
            log.error('Unexpected connection on {instance} by user(s):'
                      '{unexpected}'.format(instance=instance,
                                            unexpected=','.join(unexpected)))
            continue

        # joining on a blank string as password must not have a space between
        # the flag and the arg
        if dry_run:
            log.info('In dry_run mode, not changing state')
        else:
            log.info('Shuting down mysql on {instance}'.format(instance=instance))
            mysql_lib.shutdown_mysql(host_utils.HostAddr(instance))
            log_to_retirement_queue(instance,
                                    shutdown_instances[instance]['instance_id'],
                                    SHUTDOWN_MYSQL)


def terminate_instances(hostname=None, dry_run=False):
    zk = host_utils.MysqlZookeeper()
    username, password = mysql_lib.get_mysql_user_for_role('admin')
    terminate_instances = get_retirement_queue_servers(TERMINATE_INSTANCE)
    conn = boto.ec2.connect_to_region('us-east-1')

    if hostname:
        if hostname in terminate_instances:
            log.info('Only acting on {hostname}'.format(hostname=hostname))
            terminate_instances = {hostname: terminate_instances[hostname]}
        else:
            log.info('Supplied host {hostname} is not ready '
                     'for termination'.format(hostname=hostname))
            return

    for hostname in terminate_instances:
        if hostname in get_protected_hosts('set'):
            log.warning('Host {hostname} is protected from '
                        'retirement'.format(hostname=hostname))
            remove_from_retirement_queue(hostname)
            continue
        for instance in zk.get_all_mysql_instances():
            if instance.hostname == hostname:
                log.warning("It appears {instance} is in zk. This is "
                            "very dangerous!".format(instance=instance))
                remove_from_retirement_queue(hostname)
                continue

        log.info('Confirming mysql is down on '
                 '{hostname}'.format(hostname=hostname))

        try:
            with timeout.timeout(3):
                conn = MySQLdb.connect(host=terminate_instances[hostname]['internal_ip'],
                                       user=username,
                                       passwd=password,
                                       cursorclass=MySQLdb.cursors.DictCursor)
            log.error('Did not get MYSQL_ERROR_CONN_HOST_ERROR')
            continue
        except MySQLdb.OperationalError as detail:
            (error_code, msg) = detail.args
            if error_code != mysql_lib.MYSQL_ERROR_CONN_HOST_ERROR:
                raise
            log.info('MySQL is down')
        log.info('Terminating instance '
                 '{instance}'.format(instance=terminate_instances[hostname]['instance_id']))
        if dry_run:
            log.info('In dry_run mode, not changing state')
        else:
            conn.terminate_instances(instance_ids=[terminate_instances[hostname]['instance_id']])
            log_to_retirement_queue(hostname,
                                    terminate_instances[hostname]['instance_id'],
                                    TERMINATE_INSTANCE)


def unprotect_host(hostname):
    """ Cause an host to able to be acted on by the retirement queue

    Args:
    hostname - The hostname to remove from protection
    """
    reporting_conn = mysql_lib.get_mysqlops_connections()
    cursor = reporting_conn.cursor()
    sql = ("DELETE FROM mysqlops.retirement_protection "
           "WHERE hostname = %(hostname)s")
    cursor.execute(sql, {'hostname': hostname})
    reporting_conn.commit()
    log.info(cursor._executed)


def protect_host(hostname, reason):
    """ Cause an host to not be acted on by the retirement queue

    Args:
    hostname - The hostname to protect
    reason -  An explanation for why this host should not be retired
    dry_run - If set, don't modify state
    """
    protecting_user = host_utils.get_user()
    if protecting_user == 'root':
        raise Exception('Can not modify retirement protection as root')

    reporting_conn = mysql_lib.get_mysqlops_connections()
    cursor = reporting_conn.cursor()
    sql = ("INSERT INTO mysqlops.retirement_protection "
           "SET "
           "hostname = %(hostname)s, "
           "reason = %(reason)s, "
           "protecting_user = %(protecting_user)s")
    cursor.execute(sql, {'hostname': hostname,
                         'reason': reason,
                         'protecting_user': protecting_user})
    reporting_conn.commit()
    log.info(cursor._executed)


def get_protected_hosts(return_type='tuple'):
    """ Get data on all protected hosts

    Args:
    return_type - Options are:
                              'set'- return a set of protected hosts
                              'tuple' - returns all data regarding protected hosts

    Returns:
    A tuple which may be empty, with entries similar to:
    ({'protecting_user': 'rwultsch', 'reason': 'because', 'hostname': 'sharddb-14-4'},
     {'protecting_user': 'rwultsch', 'reason': 'because reasons', 'hostname': 'sharddb-14-5'})
    """
    if return_type != 'tuple' and return_type != 'set':
        raise Exception('Unsupported return_type '
                        '{return_type}'.format(return_type=return_type))

    reporting_conn = mysql_lib.get_mysqlops_connections()
    cursor = reporting_conn.cursor()
    sql = "SELECT * FROM mysqlops.retirement_protection"
    cursor.execute(sql)
    results = cursor.fetchall()

    if return_type == 'tuple':
        return results
    elif return_type == 'set':
        results_set = set()
        for entry in results:
            results_set.add(entry['hostname'])

        return results_set


def get_retirement_queue_servers(next_state):
    """ Pull instances in queue ready for termination

    Args:
    next_state - The desired next state of a server. Options are constants
                 SHUTDOWN_MYSQL and TERMINATE_INSTANCE.

    Returns:
    A dict of the same form as what is returned from the cmdbs
    """
    if next_state == SHUTDOWN_MYSQL:
        server_state = {'previous_state': RESET_STATS,
                        'next_state': SHUTDOWN_MYSQL}
    elif next_state == TERMINATE_INSTANCE:
        server_state = {'previous_state': SHUTDOWN_MYSQL,
                        'next_state': TERMINATE_INSTANCE}
    else:
        raise Exception('Invalid state param '
                        '"{next_state}"'.format(next_state=next_state))

    reporting_conn = mysql_lib.get_mysqlops_connections()
    cursor = reporting_conn.cursor()
    sql = ("SELECT t1.hostname, t1.instance_id "
           "FROM ( "
           "    SELECT hostname, instance_id "
           "    FROM mysqlops.retirement_queue "
           "    WHERE activity = %(previous_state)s "
           "    AND happened > now() - INTERVAL 3 WEEK "
           "    AND happened < now() - INTERVAL 1 DAY) t1 "
           "LEFT JOIN mysqlops.retirement_queue t2 on t1.instance_id = t2.instance_id "
           "AND t2.activity=%(next_state)s "
           "WHERE t2.hostname IS NULL;")
    cursor.execute(sql, server_state)
    instances = cursor.fetchall()

    all_servers = environment_specific.get_all_server_metadata()
    if len(all_servers) < MIN_CMDB_RESULTS:
        raise Exception('CMDB returned too few results')

    ret = dict()
    for instance in instances:
        if instance['hostname'] not in all_servers:
            log.error('Something killed {instance}, cleaning up '
                      'retirement queue now'.format(instance=instance))
            remove_from_retirement_queue(instance['hostname'])
        elif instance['instance_id'] != all_servers[instance['hostname']]['instance_id']:
            log.error('Possibly duplicate hostname for '
                      '{hostname}!'.format(hostname=instance['hostname']))
        else:
            ret[instance['hostname']] = all_servers[instance['hostname']]

    return ret


def log_to_retirement_queue(hostname, instance_id, activity):
    """ Add a record to the retirement queue log

    Args:
    hostname - The hostname of the server to be acted upon
    instance_id - The aws instance id
    activity - What is the state to log

    """
    reporting_conn = mysql_lib.get_mysqlops_connections()
    cursor = reporting_conn.cursor()

    # we are using a replace if we need to restart the process. That will
    # restart the clock on the replacement
    sql = ('REPLACE INTO mysqlops.retirement_queue '
           'SET '
           'hostname = %(hostname)s ,'
           'instance_id = %(instance_id)s, '
           'activity = %(activity)s, '
           'happened = now() ')
    cursor.execute(sql, {'hostname': hostname,
                         'instance_id': instance_id,
                         'activity': activity})
    log.info(cursor._executed)
    reporting_conn.commit()


def remove_from_retirement_queue(hostname):
    """ Remove an host from the retirement queue

    Args:
    hostname - the hostname to remove from the queue
    """
    reporting_conn = mysql_lib.get_mysqlops_connections()
    cursor = reporting_conn.cursor()

    sql = ('DELETE FROM mysqlops.retirement_queue '
           'WHERE hostname = %(hostname)s')
    cursor.execute(sql, {'hostname': hostname})
    log.info(cursor._executed)
    reporting_conn.commit()


if __name__ == "__main__":
    main()
