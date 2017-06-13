#!/usr/bin/env python
import argparse
import datetime
import os
import boto
import boto.s3.key
import logging
import subprocess
import time
import traceback

import binlog_rotator
import safe_uploader
from lib import host_utils
from lib import mysql_lib
from lib import environment_specific

BINLOG_ARCHIVING_TABLE = """CREATE TABLE IF NOT EXISTS {db}.{tbl} (
  `hostname` varchar(90) NOT NULL,
  `port` int(11) NOT NULL,
  `binlog` varchar(90) NOT NULL,
  `binlog_creation` datetime NULL,
  `uploaded` datetime NOT NULL,
  PRIMARY KEY (`binlog`),
  INDEX `instance` (`hostname`, `port`),
  INDEX `uploaded` (`uploaded`),
  INDEX `binlog_creation` (`binlog_creation`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1"""
BINLOG_S3_BASE_DIR = 'binlogs'
STANDARD_RETENTION_BINLOG_S3_DIR = 'standard_retention'
BINLOG_LOCK_SOCKET = 'archivebinlogs'
BINLOG_INFINITE_REPEATER_TERM_FILE = '/tmp/archive_mysql_binlogs_infinite.die'
MAX_ERRORS = 5
TMP_DIR = '/tmp/'

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Upload binlogs to s3')
    parser.add_argument('-p',
                        '--port',
                        help='Port of instance to backup. Default is 3306',
                        default=3306)
    parser.add_argument('--dry_run',
                        help='Do not upload binlogs, just display output',
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    archive_mysql_binlogs(args.port, args.dry_run)


def archive_mysql_binlogs(port, dry_run):
    """ Flush logs and upload all binary logs that don't exist to s3

    Arguments:
    port - Port of the MySQL instance on which to act
    dry_run - Display output but do not uplad
    """
    binlog_rotator.rotate_binlogs_if_needed(port, dry_run)
    zk = host_utils.MysqlZookeeper()
    instance = host_utils.HostAddr(':'.join((host_utils.HOSTNAME,
                                             str(port))))

    if zk.get_replica_set_from_instance(instance) is None:
        log.info('Instance is not in production, exiting')
        return

    ensure_binlog_archiving_table_sanity(instance)
    log.info('Taking binlog archiver lock')
    lock_handle = host_utils.bind_lock_socket(BINLOG_LOCK_SOCKET)
    log_bin_dir = host_utils.get_cnf_setting('log_bin', port)
    bin_logs = mysql_lib.get_master_logs(instance)
    logged_uploads = get_logged_binlog_uploads(instance)
    for binlog in bin_logs[:-1]:
        err_count = 0
        local_file = os.path.join(os.path.dirname(log_bin_dir),
                                  binlog['Log_name'])
        if already_uploaded(instance, local_file, logged_uploads):
            continue
        success = False
        while not success:
            try:
                upload_binlog(instance, local_file, dry_run)
                success = True
            except:
                if err_count > MAX_ERRORS:
                    log.error('Error count in thread > MAX_THREAD_ERROR. '
                              'Aborting :(')
                    raise

                log.error('error: {e}'.format(e=traceback.format_exc()))
                err_count = err_count + 1
                time.sleep(err_count * 2)

    host_utils.release_lock_socket(lock_handle)
    log.info('Archiving complete')


def already_uploaded(instance, binlog, logged_uploads):
    """ Check to see if a binlog has already been uploaded

    Args:
    instance - a hostAddr object
    binlog - the full path to the binlog file
    logged_uploads - a set of all uploaded binlogs for this instance

    Returns True if already uploaded, False otherwise.
    """
    if os.path.basename(binlog) in logged_uploads:
        log.debug('Binlog already logged as uploaded')
        return True

    # we should hit this code rarely, only when uploads have not been logged
    boto_conn = boto.connect_s3()
    bucket = boto_conn.get_bucket(environment_specific.BACKUP_BUCKET_UPLOAD_MAP[host_utils.get_iam_role()],
                                  validate=False)
    if bucket.get_key(s3_binlog_path(instance, os.path.basename((binlog)))):
        log.debug("Binlog already uploaded but not logged {b}".format(b=binlog))
        log_binlog_upload(instance, binlog)
        return True

    return False


def upload_binlog(instance, binlog, dry_run):
    """ Upload a binlog file to s3

    Args:
    instance - a hostAddr object
    binlog - the full path to the binlog file
    dry_run - if set, do not actually upload a binlog
    """
    s3_upload_path = s3_binlog_path(instance, binlog)
    bucket = environment_specific.BACKUP_BUCKET_UPLOAD_MAP[host_utils.get_iam_role()]

    if dry_run:
        log.info('In dry_run mode, skipping compression and upload')
        return

    procs = dict()
    procs['lzop'] = subprocess.Popen(['lzop', binlog, '--to-stdout'],
                                     stdout=subprocess.PIPE)
    safe_uploader.safe_upload(precursor_procs=procs,
                              stdin=procs['lzop'].stdout,
                              bucket=bucket,
                              key=s3_upload_path,
                              verbose=True)
    log_binlog_upload(instance, binlog)


def log_binlog_upload(instance, binlog):
    """ Log to the master that a binlog has been uploaded

    Args:
    instance - a hostAddr object
    binlog - the full path to the binlog file
    """
    zk = host_utils.MysqlZookeeper()
    binlog_creation = datetime.datetime.fromtimestamp(os.stat(binlog).st_atime)
    replica_set = zk.get_replica_set_from_instance(instance)
    master = zk.get_mysql_instance_from_replica_set(replica_set)
    conn = mysql_lib.connect_mysql(master, 'dbascript')
    cursor = conn.cursor()
    sql = ("REPLACE INTO {metadata_db}.{tbl} "
           "SET hostname = %(hostname)s, "
           "    port = %(port)s, "
           "    binlog = %(binlog)s, "
           "    binlog_creation = %(binlog_creation)s, "
           "    uploaded = NOW() ").format(metadata_db=mysql_lib.METADATA_DB,
                                           tbl=environment_specific.BINLOG_ARCHIVING_TABLE_NAME)
    metadata = {'hostname': instance.hostname,
                'port': str(instance.port),
                'binlog': os.path.basename(binlog),
                'binlog_creation': binlog_creation}
    cursor.execute(sql, metadata)
    conn.commit()


def get_logged_binlog_uploads(instance):
    """ Get all binlogs that have been logged as uploaded

    Args:
    instance - a hostAddr object to run against and check

    Returns:
    A set of binlog file names
    """
    conn = mysql_lib.connect_mysql(instance, 'dbascript')
    cursor = conn.cursor()
    sql = ("SELECT binlog "
           "FROM {metadata_db}.{tbl} "
           "WHERE hostname = %(hostname)s AND "
           "      port = %(port)s "
           "".format(metadata_db=mysql_lib.METADATA_DB,
                     tbl=environment_specific.BINLOG_ARCHIVING_TABLE_NAME))
    cursor.execute(sql, {'hostname': instance.hostname,
                         'port': str(instance.port)})
    ret = set()
    for binlog in cursor.fetchall():
        ret.add(binlog['binlog'])

    return ret


def ensure_binlog_archiving_table_sanity(instance):
    """ Create binlog archiving log table if missing, purge old data

    Args:
    instance - A hostAddr object. Note: this function will find the master of
               the instance if the instance is not a master
    """
    zk = host_utils.MysqlZookeeper()
    replica_set = zk.get_replica_set_from_instance(instance)
    master = zk.get_mysql_instance_from_replica_set(replica_set)
    conn = mysql_lib.connect_mysql(master, 'dbascript')
    cursor = conn.cursor()
    if not mysql_lib.does_table_exist(master, mysql_lib.METADATA_DB,
            environment_specific.BINLOG_ARCHIVING_TABLE_NAME):
        log.debug('Creating missing metadata table')
        cursor.execute(BINLOG_ARCHIVING_TABLE.format(
            db=mysql_lib.METADATA_DB,
            tbl=environment_specific.BINLOG_ARCHIVING_TABLE_NAME))
    sql = ("DELETE FROM {metadata_db}.{tbl} "
           "WHERE binlog_creation < now() - INTERVAL {d} DAY"
           "").format(metadata_db=mysql_lib.METADATA_DB,
                      tbl=environment_specific.BINLOG_ARCHIVING_TABLE_NAME,
                      d=(environment_specific.S3_BINLOG_RETENTION+1))
    log.info(sql)
    cursor.execute(sql)
    conn.commit()


def s3_binlog_path(instance, binlog):
    """ Determine the path in s3 for a binlog

    Args:
    instance - A hostAddr instance
    binlog - A binlog filename

    Returns:
    A path in S3 where the file should be stored.
    """
    # At some point in the near future we will probably use reduced
    # retention for pinlater
    return os.path.join(BINLOG_S3_BASE_DIR,
                        STANDARD_RETENTION_BINLOG_S3_DIR,
                        instance.hostname_prefix,
                        instance.hostname,
                        str(instance.port),
                        ''.join((os.path.basename(binlog),
                                 '.lzo')))


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
