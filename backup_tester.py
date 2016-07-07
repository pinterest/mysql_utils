#!/usr/bin/env python
import argparse
import datetime
import logging
import multiprocessing

import MySQLdb

from lib import backup
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib
import launch_replacement_db_host

log = logging.getLogger(__name__)

AGE_START_TESTING = 55
AGE_ALARM = 60
MAX_LAUNCHED = 10


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry_run',
                        help=('Do not actually launch servers'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    launch_restores_as_needed(dry_run=args.dry_run)


def launch_restores_as_needed(dry_run=True):
    """ Launch a bunch of hosts to test restore process

    Args:
    dry_run - Don't actully launch hosts
    """
    zk = host_utils.MysqlZookeeper()
    launched = 0
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    results = pool.map(backup.get_age_last_restore,
                       zk.get_all_mysql_replica_sets())
    restore_age = dict()
    for result in results:
        if not result:
            continue
        if result[0] not in restore_age:
            restore_age[result[0]] = set()
        restore_age[result[0]].add(result[1])

    launched = 0
    min_launches = min_test_launches()
    for days in sorted(restore_age.keys(), reverse=True):
        for replica_set in restore_age[days]:
            launch = False
            if launched > MAX_LAUNCHED:
                raise Exception('Cowardly refusing to consider launching '
                                'servers as we have launched {launched} which '
                                'is greater than the limit of {max_launched}'
                                ''.format(launched=launched,
                                          max_launched=MAX_LAUNCHED))
            elif days > AGE_START_TESTING:
                launch = True
                log.info('Will replace a host in {rs} as days since last restore '
                         'is {days} days and we will always launch after '
                         '{always} days'
                         ''.format(rs=replica_set,
                                   days=days,
                                   always=AGE_START_TESTING))
            elif launched < min_launches:
                launch = True
                log.info('Will replace a host in {rs} as launched '
                         '{launched} < min {min}'
                         ''.format(rs=replica_set,
                                   launched=launched,
                                   min=min_launches))

            if launch:
                launched = launched + 1
                if not dry_run:
                    try:
                        launch_a_slave_replacement(replica_set)
                    except Exception as e:
                        log.error('Could not launch replacement due to error: '
                                  '{e}'.format(e=e))


def launch_a_slave_replacement(replica_set):
    """ Choose a slave to replace and launch it

    Args:
    replica - A MySQL replica set
    """
    zk = host_utils.MysqlZookeeper()
    instance = zk.get_mysql_instance_from_replica_set(replica_set,
                                                      host_utils.REPLICA_ROLE_DR_SLAVE)
    if not instance:
        instance = zk.get_mysql_instance_from_replica_set(replica_set,
                                                          host_utils.REPLICA_ROLE_SLAVE)
    launch_replacement_db_host.launch_replacement_db_host(instance,
                                                          reason='restore age',
                                                          replace_again=True)


def min_test_launches():
    """ Figure out what is the least number of test launches we should run

    Returns an int of the most test launches we should run
    """
    zk = host_utils.MysqlZookeeper()
    # So the idea here is that often an upgrade will cause a large burst of
    # replacements which will then potentially cause not many servers to be
    # launched for a while. This will smooth out the number of services launch.
    return len(zk.get_all_mysql_replica_sets()) / AGE_ALARM

if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
