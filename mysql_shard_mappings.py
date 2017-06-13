#!/usr/bin/env python
import argparse
from lib import host_utils

DEFAULT = 'default'
OUTPUT_FORMAT = ('{replica_set:<RS}'
                 '{replica_type:<12}'
                 '{hostport}')
SHARD_CONFIG = "/var/config/config.services.generaldb.mysql_shards"
TEST_SHARD_CONFIG = "/var/config/config.services.generaldb.test_mysql_shards"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_test',
                        help=('Pull data from the test shard config node'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    # todo: add z padding, sort
    zk = host_utils.MysqlZookeeper()
    raw_mapping = zk.get_zk_mysql_shard_map(args.use_test)['services']
    for s in raw_mapping:
        for ns in raw_mapping[s]['namespaces']:
            usable_name = DEFAULT if ns == '' else ns
            for shard in raw_mapping[s]['namespaces'][ns]['shards']:
                print ('{service} {namespace} {shard} '
                       '{mysqldb} {replica_set}'
                       ''.format(service=s,
                                 namespace=usable_name,
                                 shard=shard,
                                 mysqldb=raw_mapping[s]['namespaces'][ns]['shards'][shard]['mysqldb'],
                                 replica_set=raw_mapping[s]['namespaces'][ns]['shards'][shard]['replica_set']))


if __name__ == "__main__":
    main()
