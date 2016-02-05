#!/usr/bin/python
import argparse
import sys

import mysql_backup_status
from lib import host_utils

OTHER_SLAVE_RUNNING_ETL = 0
OTHER_SLAVE_NOT_RUNNING_ETL = 1
ERROR = 3


def main():
    parser = argparse.ArgumentParser(description="Is ETL running on a "
                                                 "different instance?")
    parser.add_argument('instance',
                        nargs='?',
                        help="Instance to inspect, default is localhost:3306",
                        default=''.join((host_utils.HOSTNAME, ':3306')))
    args = parser.parse_args()
    instance = host_utils.HostAddr(args.instance)

    zk = host_utils.MysqlZookeeper()
    (replica_set, replica_type) = zk.get_replica_set_from_instance(instance)

    if replica_type == host_utils.REPLICA_ROLE_DR_SLAVE:
        inst = zk.get_mysql_instance_from_replica_set(replica_set,
                                                      host_utils.REPLICA_ROLE_SLAVE)
    elif replica_type == host_utils.REPLICA_ROLE_SLAVE:
        inst = zk.get_mysql_instance_from_replica_set(replica_set,
                                                      host_utils.REPLICA_ROLE_DR_SLAVE)
    else:
        exit_unknown_error()

    if not inst:
        # if there is not another slave in zk, there is not possibility
        # it is ok
        exit_other_slave_not_running_etl()
    try:
        running = mysql_backup_status.csv_backups_running(instance)
    except:
        exit_other_slave_not_running_etl()

    if not running:
        exit_other_slave_not_running_etl()

    exit_other_slave_running_etl()


def exit_other_slave_not_running_etl():
    print "OTHER_SLAVE_NOT_RUNNING_ETL"
    sys.exit(OTHER_SLAVE_NOT_RUNNING_ETL)


def exit_other_slave_running_etl():
    print "OTHER_SLAVE_RUNNING_ETL"
    sys.exit(OTHER_SLAVE_RUNNING_ETL)


def exit_unknown_error():
    print "UNKNOWN"
    sys.exit(ERROR)


if __name__ == '__main__':
    main()
