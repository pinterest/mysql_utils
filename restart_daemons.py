#!/usr/bin/python

import argparse
import logging
import psutil
from lib import mysql_lib
from lib import host_utils
from lib import environment_specific

import mysql_cnf_builder

log = logging.getLogger(__name__)


def restart_maxwell_if_not_exists(instance):
    """ Start Maxwell if it isn't currently running.
    Args:
        instance: (host_utils.HostAddr): host to check
    Returns:
        none
    """
    zk = host_utils.MysqlZookeeper()
    replica_type = zk.get_replica_type_from_instance(instance)
    gvars = mysql_lib.get_global_variables(instance)

    client_id = gvars['server_uuid']
    gtid_mode = True if gvars.get('gtid_mode') == 'ON' else False 
    (username, _) = mysql_lib.get_mysql_user_for_role('maxwell')

    output_target = 'file'

    # master writes to kafka, everything else writes to /dev/null,
    # at least for now.
    if instance.hostname_prefix in environment_specific.MAXWELL_TARGET_MAP \
            and replica_type == host_utils.REPLICA_ROLE_MASTER:
        output_target = 'kafka'

    # we need to rewrite the config each time, because something may
    # have changed - i.e., a failover.  this is just a stopgap solution
    # pending resolution of LP-809
    mysql_cnf_builder.create_maxwell_config(client_id, instance,
                                            None, output_target,
                                            gtid_mode)

    # Check for the Maxwell PID file and then see if it belongs to Maxwell.
    maxwell_running = False
    try:
        with open(environment_specific.MAXWELL_PID, "r") as f:
            pid = f.read()
            
        proc = psutil.Process(int(pid))
        cmdline = proc.cmdline()

        if 'java' in cmdline and 'com.zendesk.maxwell.Maxwell' in cmdline:
            maxwell_running = True

    except (IOError, psutil.NoSuchProcess, psutil.ZombieProcess):
        # No PID file or no process matching said PID, so maxwell is definitely
        # not running. If maxwell is a zombie then it's not running either.
        pass
    
    if maxwell_running:
        log.debug('Maxwell is already running')
        return

    if instance.hostname_prefix in environment_specific.MAXWELL_TARGET_MAP:
        host_utils.restart_maxwell(instance.port)
        log.info('Started Maxwell process')


def restart_pt_kill_if_not_exists(instance):
    """
    Restarts ptkill if it isn't currently running

    Args:
        instance (host_utils.HostAddr): host to check for ptkill

    Returns:
        None
    """
    connected_users = mysql_lib.get_connected_users(instance)
    ptkill_user, ptkill_pass = mysql_lib.get_mysql_user_for_role('ptkill')
    if ptkill_user not in connected_users:
        host_utils.restart_pt_kill(instance.port)
        log.info('Started Processes ptkill')


def restart_pt_heartbeat_if_not_exists(instance):
    """
    Restarts ptheartbeat if it isn't currently running and the
    replica role type is master

    Args:
        instance (host_utils.HostAddr): host to check for ptheartbeat

    Returns:
        None
    """
    connected_users = mysql_lib.get_connected_users(instance)
    zk = host_utils.MysqlZookeeper()
    try:
        replica_type = zk.get_replica_type_from_instance(instance)
    except:
        replica_type = None
    pthb_user, pthb_pass = mysql_lib.get_mysql_user_for_role('ptheartbeat')
    if replica_type in (host_utils.REPLICA_ROLE_MASTER, None) and \
            pthb_user not in connected_users:
        host_utils.restart_pt_heartbeat(instance.port)
        log.info('Started Processes ptheartbeat')


def main():
    parser = argparse.ArgumentParser(
                description='Restarts ptkill and ptheartbeat '
                            'if they aren\'t running under '
                            'the right conditions'
                )
    parser.add_argument('action',
                        help='Action to take',
                        default='all',
                        nargs='?',
                        choices=['ptkill', 'ptheartbeat', 'maxwell', 'all'])
    args = parser.parse_args()

    instance = host_utils.HostAddr(host_utils.HOSTNAME)

    if args.action == 'all' or args.action == 'ptkill':
        restart_pt_kill_if_not_exists(instance)

    if args.action == 'all' or args.action == 'ptheartbeat':
        restart_pt_heartbeat_if_not_exists(instance)

    if args.action == 'all' or args.action == 'maxwell':
        restart_maxwell_if_not_exists(instance)


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
