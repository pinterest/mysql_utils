#!/usr/bin/env python
import argparse
import os
import logging
from lib import environment_specific
from lib import host_utils

TOUCH_FOR_NO_STOP_MYSQL_FENCING = '/etc/mysql/no_stop_msql_for_fencing'

log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port',
                        help='Port on localhost. Default 3306',
                        default='3306')
    parser.add_argument('--dry_run', help=('Do not actually shutdown fenced '
                                           'host just show the intended '
                                           'configuration'),
                        default=False,
                        action='store_true')

    args = parser.parse_args()
    fence_shutdown_mysql(port=args.port,
                         dry_run=args.dry_run)


def fence_shutdown_mysql(port, dry_run):
    """ Shutdown fenced box

        Args:
         A port for mysql instance
    """
    log.info('checking security group and stop_mysql_fencing file exist')
    if host_utils.get_security_group() == \
            environment_specific.VPC_FENCE_DB_GROUP \
            and not os.path.exists(TOUCH_FOR_NO_STOP_MYSQL_FENCING):
        if dry_run:
            log.info("In dry_run mode: Do Not actually shutdown "
                     "mysql_in_fence, exiting now")
            os._exit(environment_specific.DRY_RUN_EXIT_CODE)
        host_utils.stop_mysql(port)
    else:
        return


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
