#!/usr/bin/env python
import argparse
import json

from lib import host_utils
from lib import mysql_lib

from lib.environment_specific import TEST_MYSQL_SHARDS_CONFIG_LOCAL_PATH
from lib.environment_specific import MYSQL_SHARDS_CONFIG_LOCAL_PATH

DEFAULT = 'default'
OUTPUT_FORMAT = ('{replica_set:<RS}'
                 '{replica_type:<12}'
                 '{hostport}')

def get_shard_map(localpath):
    """ Query for Data Services MySQL shard mappings.

    Returns:
    A dict of all Data Services MySQL replication configuration.

    """
    with open(localpath) as f:
        mapping = json.loads(f.read())

    return mapping['services']

# if raw_mapping is available, uses it. Otherwise gets mapping from file at localpath.
def get_db_on_replica_set(localpath, raw_mapping):
    if raw_mapping is None:
        raw_mapping = get_shard_map(localpath)
    replica_sets = dict()
    for service in raw_mapping:
        for namespace in raw_mapping[service]['namespaces']:
            for shard in raw_mapping[service]['namespaces'][namespace]['shards']:
                replica_set = raw_mapping[service]['namespaces'][namespace]['shards'][shard]['replica_set']
                db = raw_mapping[service]['namespaces'][namespace]['shards'][shard]['mysqldb']
                if replica_set not in replica_sets:
                    replica_sets[replica_set] = set()
                replica_sets[replica_set].add(db)
    return replica_sets


# if raw_mapping is available, uses it. Otherwise gets mapping from file at localpath.
def get_problem_replicasets(localpath, raw_mapping):
    problem_replica_sets = dict()
    zk = host_utils.MysqlZookeeper()
    replica_set_dbs = get_db_on_replica_set(localpath, raw_mapping)
    for replica_set in replica_set_dbs:
        master = zk.get_mysql_instance_from_replica_set(replica_set=replica_set,
                                                        repl_type=host_utils.REPLICA_ROLE_MASTER)
        dbs = mysql_lib.get_dbs(master)
        missing = replica_set_dbs[replica_set].difference(dbs)
        if missing:
            problem_replica_sets[replica_set] = missing
    return problem_replica_sets

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_test',
                        default=False,
                        action='store_true',
                        help='Use test shard config')
    args = parser.parse_args()
    localpath = TEST_MYSQL_SHARDS_CONFIG_LOCAL_PATH if args.use_test else MYSQL_SHARDS_CONFIG_LOCAL_PATH

    problem_replica_sets = get_problem_replicasets(localpath, raw_mapping=None)
    if problem_replica_sets:
        raise Exception('Problem in shard mapping {}'.format(problem_replica_sets))

    print 'Everything looks good :)!'


if __name__ == "__main__":
    main()
