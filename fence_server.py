#!/usr/bin/env python

import argparse
import os
import logging
import boto.ec2

from lib import environment_specific
from lib import host_utils

SG_DB_FENCE_ID = environment_specific.VPC_SECURITY_GROUPS[
    environment_specific.VPC_FENCE_DB_GROUP]

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('server', help='The server to be fenced')
    parser.add_argument('--dry_run', help=('Do not actually fence the host, '
                                           'just show the intended '
                                           'configuration'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    add_fence_to_host(hostname=host_utils.HostAddr(args.server),
                      dry_run=args.dry_run)


def add_fence_to_host(hostname, dry_run, force=False):
    """ Add a host to fence SG group

        Args:
          hostname: A hostaddr object for the instance
          dry_run: Really do it or not?
          force: Force it, even if a master in ZK
    """
    zk = host_utils.MysqlZookeeper()
    try:
        replica_type = zk.get_replica_type_from_instance(hostname)
    except:
        log.info("{} is not in zk ".format(hostname))
        replica_type = None

    # We generally don't allow fencing a master, but there could be
    # cases where a failover has occurred and ZK is having issues,
    # so we do permit forcing it.
    if replica_type == host_utils.REPLICA_ROLE_MASTER and not force:
        raise Exception('Can not fence an instance which is a Master in zk')

    conn = boto.ec2.connect_to_region(environment_specific.EC2_REGION)
    instance_id = environment_specific.get_server_metadata(
        hostname.hostname)['id']
    log.info("{hostname} with instance id {id} will be fenced ".format(
        hostname=hostname, id=instance_id))

    if dry_run:
        log.info("Do not actually run, just exit now")
        os._exit(environment_specific.DRY_RUN_EXIT_CODE)
    conn.modify_instance_attribute(instance_id, 'groupSet', [SG_DB_FENCE_ID])
    log.info("Done.")


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
