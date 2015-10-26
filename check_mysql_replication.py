#!/usr/bin/env python
import argparse

from lib import host_utils
from lib import mysql_lib


def main():
    parser = argparse.ArgumentParser(description='MySQL replication checker')
    parser.add_argument('replica',
                        help='Replica MySQL instance to sanity check '
                        'hostname[:port]')

    parser.add_argument('-w',
                        '--watch_for_catch_up',
                        help='Watch replication for catch up ',
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    slave_hostaddr = host_utils.HostAddr(args.replica)

    if args.watch_for_catch_up:
        mysql_lib.wait_replication_catch_up(slave_hostaddr)
    else:
        ret = mysql_lib.calc_slave_lag(slave_hostaddr)
        print "Heartbeat_seconds_behind: {sbm}".format(sbm=ret['sbm'])
        print "Slave_IO_Running: {Slave_IO_Running} ".format(Slave_IO_Running=ret['ss']['Slave_IO_Running'])
        print "IO_lag_bytes: {io_bytes}".format(io_bytes=ret['io_bytes'])
        print "IO_lag_binlogs: {io_binlogs}".format(io_binlogs=ret['io_binlogs'])
        print "Slave_SQL_Running: {Slave_IO_Running} ".format(Slave_IO_Running=ret['ss']['Slave_SQL_Running'])
        print "SQL_lag_bytes: {sql_bytes}".format(sql_bytes=ret['sql_bytes'])
        print "SQL_lag_binlogs: {sql_binlogs}".format(sql_binlogs=ret['sql_binlogs'])


if __name__ == "__main__":
    main()
