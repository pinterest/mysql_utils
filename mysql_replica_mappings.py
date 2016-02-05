#!/usr/bin/env python
import argparse

from lib import environment_specific
from lib import host_utils

OUTPUT_FORMAT = ('{replica_set:<RS}'
                 '{replica_type:<12}'
                 '{hostport}')

OUTPUT_FORMAT_EXTENDED = ('{replica_set:<RS}'
                          '{replica_type:<12}'
                          '{hostport:<HP}'
                          '{hw}\t'
                          '{az}\t'
                          '{sg:<SGL}'
                          '{id}')


def main():
    parser = argparse.ArgumentParser(description='MySQL production viewer')
    parser.add_argument('-e',
                        '--extended',
                        help='Include information about hardware, az, etc',
                        default=False,
                        action='store_true')
    args = parser.parse_args()

    zk = host_utils.MysqlZookeeper()
    config = zk.get_all_mysql_config()
    if args.extended:
        servers = environment_specific.get_all_server_metadata()

    output = list()
    max_rs_length = 10
    max_sg_length = 0

    # iterate through and find the longest replica set name and
    # security group, then use that to set the spacing
    for replica_set in config:
        for rtype in host_utils.REPLICA_TYPES:
            if rtype in config[replica_set]:
                inst = config[replica_set][rtype]
                if len(replica_set) > max_rs_length:
                    max_rs_length = len(replica_set)
                if args.extended and inst['host'] in servers:
                    sg = ','.join(servers[inst['host']].get('security_groups',
                                                            'N/A'))
                    if len(sg) > max_sg_length:
                        max_sg_length = len(sg)

    max_rs_length += 4
    max_sg_length += 4
    hostport_length = max_rs_length + 6

    # dynamically generate padding
    format_str = OUTPUT_FORMAT.replace(
        'RS', str(max_rs_length)).replace(
        'HP', str(hostport_length)).replace(
        'SGL', str(max_sg_length))
    format_str_extended = OUTPUT_FORMAT_EXTENDED.replace(
        'RS', str(max_rs_length)).replace(
        'HP', str(hostport_length)).replace(
        'SGL', str(max_sg_length))

    for replica_set in config:
        for rtype in host_utils.REPLICA_TYPES:
            if rtype in config[replica_set]:
                inst = config[replica_set][rtype]

                if args.extended and inst['host'] in servers:
                    az = servers[inst['host']]['zone']
                    id = servers[inst['host']]['instance_id']
                    hw = servers[inst['host']]['instance_type']
                    try:
                        sg = ','.join(servers[inst['host']]['security_groups'])
                    except KeyError:
                        sg = '??VPC??'

                    output.append(format_str_extended.format(
                        replica_set=replica_set,
                        replica_type=rtype,
                        hostport=':'.join([inst['host'], str(inst['port'])]),
                        az=az,
                        hw=hw,
                        sg=sg,
                        id=id))
                else:
                    output.append(format_str.format(
                        replica_set=replica_set,
                        replica_type=rtype,
                        hostport=':'.join([inst['host'], str(inst['port'])])))

    output.sort()
    print '\n'.join(output)


if __name__ == "__main__":
    main()
