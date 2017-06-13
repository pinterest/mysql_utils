#!/usr/bin/env python
import argparse
import os
import time

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
                        choices=(backup.BACKUP_TYPE_LOGICAL,
                                 backup.BACKUP_TYPE_XBSTREAM))
    args = parser.parse_args()
    instance = host_utils.HostAddr(':'.join((host_utils.HOSTNAME, args.port)))
    mysql_backup(instance, args.backup_type)


def mysql_backup(instance, backup_type=backup.BACKUP_TYPE_XBSTREAM,
                 initial_build=False, lock_handle=None):
    """ Run a file based backup on a supplied local instance

    Args:
    instance - A hostaddr object
    backup_type - backup.BACKUP_TYPE_LOGICAL or backup.BACKUP_TYPE_XBSTREAM
    initial_build - Boolean, if this is being created right after the server
                    was built
    lock_handle - A lock handle, if we have one from the caller.
    """

    if backup_type == backup.BACKUP_TYPE_XBSTREAM and \
            os.path.isfile(backup.XTRABACKUP_SKIP_FILE):
        log.info('Found {}. Skipping xtrabackup '
                 'run.'.format(backup.XTRABACKUP_SKIP_FILE))
        return

    log.info('Confirming sanity of replication (if applicable)')
    zk = host_utils.MysqlZookeeper()
    try:
        replica_type = zk.get_replica_type_from_instance(instance)
    except:
        # instance is not in production
        replica_type = None

    if replica_type and replica_type != host_utils.REPLICA_ROLE_MASTER:
        mysql_lib.assert_replication_sanity(instance)

    log.info('Logging initial status to mysqlops')
    start_timestamp = time.localtime()
    backup_id = mysql_lib.start_backup_log(instance, backup_type,
                                           start_timestamp)

    # Take a lock to prevent multiple backups from running concurrently
    # unless we already have a lock from the caller.  This means we
    # also don't have to release the lock at the end; either we
    # exit the script entirely, and it gets cleaned up or the caller
    # maintains it.
    if lock_handle is None:
        log.info('Taking backup lock')
        lock_handle = host_utils.bind_lock_socket(backup.STD_BACKUP_LOCK_SOCKET)
    else:
        log.info('Not acquiring backup lock, we already have one.')

    # Actually run the backup
    log.info('Running backup')
    if backup_type == backup.BACKUP_TYPE_XBSTREAM:
        backup_file = backup.xtrabackup_instance(instance, start_timestamp,
                                                 initial_build)
    elif backup_type == backup.BACKUP_TYPE_LOGICAL:
        # We don't need a backup-skip file here since this isn't
        # regularly scheduled.
        backup_file = backup.logical_backup_instance(instance, start_timestamp,
                                                     initial_build)
    else:
        raise Exception('Unsupported backup type {}'.format(backup_type))

    # Update database with additional info now that backup is done.
    if backup_id:
        log.info("Updating database log entry with final backup info")
        mysql_lib.finalize_backup_log(backup_id, backup_file)
    else:
        log.info("The backup is complete, but we were not able to "
                 "write to the central log DB.")


if __name__ == "__main__":
    main()
