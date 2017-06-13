#!/usr/bin/env python
import argparse
import re
import datetime

import boto.utils

from lib import environment_specific
from lib import host_utils
import retirement_queue


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a',
                        '--add_retirement_queue',
                        help="Add the servers to the retirement queue",
                        action='store_true')
    args = parser.parse_args()

    hosts_not_in_zk = find_unused_db_servers()
    for host in sorted(hosts_not_in_zk):
        if args.add_retirement_queue:
            retirement_queue.add_to_queue(hostname=host, dry_run=False)
        else:
            print host


def get_db_host_prefix(hostname):
    """ This function finds the host prefix for a db host

    Argument:
    hostname - a hostname

    Returns:
    a prefix of the hostname
    """
    prefix_match = re.match('(.+db)', hostname)
    if prefix_match is None:
        prefix_match = re.match('([a-z]+)', hostname)
    if prefix_match is None:
        prefix = None
    else:
        prefix = prefix_match.group(0)
    return prefix


def find_unused_db_servers():
    """ Compare zk and AWS to determine which servers are likely not in use

    Returns:
    A set of hosts that appear to not be in use
    """

    # First find out what servers we know about from zk, and make a
    # of hostname prefixes that we think we own.
    zk = host_utils.MysqlZookeeper()
    config = zk.get_all_mysql_instances()
    zk_servers = set()
    zk_prefixes = set()
    mysql_aws_hosts = set()
    for db in config:
        host = db.hostname
        zk_servers.add(host)
        prefix = get_db_host_prefix(host)
        zk_prefixes.add(prefix)

    cmdb_servers = environment_specific.get_all_server_metadata()
    for host in cmdb_servers:
        match = False
        for prefix in zk_prefixes:
            if host.startswith(prefix):
                match = True
        if not match:
            continue

        # We need to give servers a chance to build and then add themselves
        # to zk, so we will ignore server for a week.
        creation = boto.utils.parse_ts(cmdb_servers[host]['launch_time'])
        if creation < datetime.datetime.now()-datetime.timedelta(weeks=1):
            mysql_aws_hosts.add(host)

    hosts_not_in_zk = mysql_aws_hosts.difference(zk_servers)
    hosts_not_protected = hosts_not_in_zk.difference(retirement_queue.get_protected_hosts('set'))
    return hosts_not_protected

if __name__ == "__main__":
    main()
