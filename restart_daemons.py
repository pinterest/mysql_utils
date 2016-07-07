#!/usr/bin/python
import argparse

from lib import mysql_lib
from lib import host_utils
from lib import environment_specific


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
        (_, replica_type) = zk.get_replica_set_from_instance(instance)
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
                        choices=['ptkill', 'ptheartbeat', 'all'])
    args = parser.parse_args()

    instance = host_utils.HostAddr(host_utils.HOSTNAME)

    if args.action == 'all' or args.action == 'ptkill':
        restart_pt_kill_if_not_exists(instance)

    if args.action == 'all' or args.action == 'ptheartbeat':
        restart_pt_heartbeat_if_not_exists(instance)


if __name__ == "__main__":
    log = environment_specific.setup_logging_defaults(__name__)
    main()
