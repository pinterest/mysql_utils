#!/usr/bin/env python
import argparse
import socket
from lib import environment_specific
from lib import host_utils
from lib import backup

KEEP_OLD_XTRABACKUP = 4
KEEP_OLD_LOGICAL = 2


def main():
    parser = argparse.ArgumentParser(description='cleanup and chmod backups')
    parser.add_argument('-p',
                        '--port',
                        help='Port to backup on localhost (default: 3306)',
                        default='3306')
    args = parser.parse_args()
    instance = host_utils.HostAddr(''.join((socket.getfqdn(),
                                            ':',
                                            args.port)))
    purge_mysql_backups(instance)


def purge_mysql_backups(instance, skip_lock=False):
    """ Run a file based backup on a supplied local instance

    Args:
    instance - A hostaddr object
    skip_lock - Don't take out a lock against other backup related functions
                running
    """
    lock_handle = None
    try:
        if not skip_lock:
            log.info('Taking backup lock')
            lock_handle = host_utils.take_flock_lock(backup.BACKUP_LOCK_FILE)

        (temp_path, target_path) = backup.get_paths(str(instance.port))

        log.info("Cleaning up any partial backups")
        backup.remove_backups(temp_path,
                              keep_newest=0,
                              extension=('.xbstream', '.sql.gz'))

        log.info("Purge xtrabackup backups to the "
                 "last {cnt}".format(cnt=KEEP_OLD_XTRABACKUP))
        backup.remove_backups(target_path,
                              keep_newest=KEEP_OLD_XTRABACKUP,
                              extension=('.xbstream'))

        log.info("Purge logical backup backups to the "
                 "last {cnt}".format(cnt=KEEP_OLD_LOGICAL))
        backup.remove_backups(target_path,
                              keep_newest=KEEP_OLD_LOGICAL,
                              extension=('.sql.gz'))

        log.info("Chmod'ing {target}".format(target=backup.TARGET_DIR))
        host_utils.change_perms(backup.TARGET_DIR, 777)

        log.info("Chmod'ing {temp}".format(temp=backup.TEMP_DIR))
        host_utils.change_perms(backup.TEMP_DIR, 777)

    finally:
        if not skip_lock and lock_handle:
            log.info('Releasing lock')
            host_utils.release_flock_lock(lock_handle)


log = environment_specific.setup_logging_defaults(__name__)
if __name__ == "__main__":
    main()
