#!/usr/bin/env python
import argparse
import datetime
import logging
import os

from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

MAX_AGE = 600

log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Binlog rotator')
    parser.add_argument('-p',
                        '--port',
                        help='Port of instance to backup. Default is 3306',
                        default=3306)
    parser.add_argument('--dry_run',
                        help='Do not upload binlogs, just display output',
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    rotate_binlogs_if_needed(args.port, args.dry_run)


def rotate_binlogs_if_needed(port, dry_run):
    instance = host_utils.HostAddr(':'.join((host_utils.HOSTNAME,
                                             str(port))))
    log_bin_dir = host_utils.get_cnf_setting('log_bin', port)
    binlog = os.path.join(os.path.dirname(log_bin_dir),
                          mysql_lib.get_master_status(instance)['File'])
    # We don't update access time, so this is creation time.
    creation = datetime.datetime.fromtimestamp(os.stat(binlog).st_atime)
    age = (datetime.datetime.utcnow() - creation).seconds
    if age > MAX_AGE:
        log.info('Age of current binlog is {age} which is greater than '
                 'MAX_AGE ({MAX_AGE})'.format(age=age,
                                               MAX_AGE=MAX_AGE))
        if not dry_run:
            log.info('Flushing bin log')
            mysql_lib.flush_master_log(instance)
    else:
        log.info('Age of current binlog is {age} which is less than '
                 'MAX_AGE ({MAX_AGE})'.format(age=age,
                                               MAX_AGE=MAX_AGE))


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
