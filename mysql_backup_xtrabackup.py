#!/usr/bin/env python
import argparse
import socket
import os
import time
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib
from lib import backup
import purge_mysql_backups

log = environment_specific.setup_logging_defaults(__name__)


def main():
    parser = argparse.ArgumentParser(description='xtrabackup wrapper')
    parser.add_argument('-p',
                        '--port',
                        help='Port to backup on localhost (default: 3306)',
                        default='3306')
    args = parser.parse_args()
    instance = host_utils.HostAddr(':'.join((socket.getfqdn(), args.port)))
    xtrabackup_backup_instance(instance)


def xtrabackup_backup_instance(instance):
    """ Run a file based backup on a supplied local instance

    Args:
    instance - A hostaddr object
    """
    starttime_sql = time.strftime('%Y-%m-%d %H:%M:%S')

    log.info('Logging initial status to mysqlops')
    row_id = None
    lock_handle = None
    try:
        reporting_conn = mysql_lib.get_mysqlops_connections()
        cursor = reporting_conn.cursor()

        sql = ("INSERT INTO mysqlops.mysql_backups "
               "SET "
               "hostname = %(hostname)s, "
               "port = %(port)s, "
               "started = %(started)s, "
               "backup_type = 'xbstream' ")

        metadata = {'hostname': instance.hostname,
                    'port': instance.port,
                    'started': starttime_sql}

        cursor.execute(sql, metadata)
        row_id = cursor.lastrowid
        reporting_conn.commit()
    except Exception as e:
        log.warning("Unable to write log entry to "
                    "mysqlopsdb001: {e}".format(e=e))
        log.warning("However, we will attempt to continue with the backup.")

    # Take a lock to prevent multiple backups from running concurrently
    try:
        log.info('Taking backup lock')
        lock_handle = host_utils.take_flock_lock(backup.BACKUP_LOCK_FILE)

        log.info('Cleaning up old backups')
        purge_mysql_backups.purge_mysql_backups(instance, skip_lock=True)

        # Actually run the backup
        log.info('Running backup')
        backup_file = backup.xtrabackup_instance(instance)
        finished = time.strftime('%Y-%m-%d %H:%M:%S')

        # Upload file to s3
        log.info('Uploading file to s3')
        backup.s3_upload(backup_file)

        # Update database with additional info now that backup is done.
        if row_id is None:
            log.info("The backup is complete, but we were not able to "
                     "write to the central log DB.")
        else:
            log.info("Updating database log entry with final backup info")
            try:
                sql = ("UPDATE mysqlops.mysql_backups "
                       "SET "
                       "filename = %(filename)s, "
                       "finished = %(finished)s, "
                       "size = %(size)s "
                       "WHERE id = %(id)s")
                metadata = {'filename': backup_file,
                            'finished': finished,
                            'size': os.stat(backup_file).st_size,
                            'id': row_id}

                cursor.execute(sql, metadata)
                reporting_conn.commit()
                reporting_conn.close()
            except Exception as e:
                log.warning("Unable to update mysqlopsdb with "
                            "backup status: {e}".format(e=e))

            # Running purge again most for the chmod
        purge_mysql_backups.purge_mysql_backups(instance, skip_lock=True)
    finally:
        if lock_handle:
            log.info('Releasing lock')
            host_utils.release_flock_lock(lock_handle)


if __name__ == "__main__":
    main()
