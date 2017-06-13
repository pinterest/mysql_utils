#!/usr/bin/python
import argparse
from lib import host_utils


def main():
    parser = argparse.ArgumentParser(description="Print out the replica role "
                                                 "for an instance")
    parser.add_argument('instance',
                        nargs='?',
                        help="Instance to inspect, default is localhost:3306",
                        default=''.join((host_utils.HOSTNAME, ':3306')))
    args = parser.parse_args()
    instance = host_utils.HostAddr(args.instance)

    zk = host_utils.MysqlZookeeper()
    replica_type = zk.get_replica_type_from_instance(instance)
    print replica_type


if __name__ == '__main__':
    main()
