#!/usr/bin/env python
import argparse
import os
import time

import purge_mysql_backups
from lib import backup
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

log = environment_specific.setup_logging_defaults(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--port',
                        help='Port to backup on localhost (default: 3306)',
                        default='3306')
    parser.add_argument('-b',
                        '--backup_type',
                        help='Type of backup to run.',
                        default=backup.BACKUP_TYPE_XBSTREAM,
                        choices=backup.BACKUP_TYPES)
    args = parser.parse_args()
    instance = host_utils.HostAddr(':'.join((host_utils.HOSTNAME, args.port)))
    mysql_backup(instance, args.backup_type)


def mysql_backup(instance, backup_type=backup.BACKUP_TYPE_XBSTREAM):
    """ Run a file based backup on a supplied local instance

    Args:
    instance - A hostaddr object
    """
    log.info('Logging initial status to mysqlops')
    start_timestamp = time.localtime()
    lock_handle = None
    backup_id = mysql_lib.start_backup_log(instance, backup_type,
                                           start_timestamp)

    # Take a lock to prevent multiple backups from running concurrently
    try:
        log.info('Taking backup lock')
        lock_handle = host_utils.take_flock_lock(backup.BACKUP_LOCK_FILE)

        log.info('Cleaning up old backups')
        purge_mysql_backups.purge_mysql_backups(instance, skip_lock=True)

        # Actually run the backup
        log.info('Running backup')
        if backup_type == backup.BACKUP_TYPE_XBSTREAM:
            backup_file = backup.xtrabackup_instance(instance, start_timestamp)
        elif backup_type == backup.BACKUP_TYPE_LOGICAL:
            backup_file = backup.logical_backup_instance(instance, start_timestamp)
        else:
            raise Exception('Unsupported backup type {backup_type}'
                            ''.format(backup_type=backup_type))

        # Upload file to s3
        log.info('Uploading file to s3')
        backup.s3_upload(backup_file)

    finally:
        if lock_handle:
            log.info('Releasing lock')
            host_utils.release_flock_lock(lock_handle)

    # Update database with additional info now that backup is done.
    if backup_id:
        log.info("Updating database log entry with final backup info")
        mysql_lib.finalize_backup_log(backup_id, backup_file,
                                      size=os.stat(backup_file).st_size)
    else:
        log.info("The backup is complete, but we were not able to "
                 "write to the central log DB.")

    # Running purge again
    log.info('Purging backups again')
    purge_mysql_backups.purge_mysql_backups(instance)


if __name__ == "__main__":
    main()
