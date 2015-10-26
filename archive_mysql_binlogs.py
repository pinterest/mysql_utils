#!/usr/bin/env python
import argparse
import os
import boto
import boto.s3.key
import gzip
from lib import host_utils
from lib import mysql_lib
from lib import environment_specific

BINLOG_S3_DIR = 'binlogs'
BINLOG_LOCK_FILE = '/tmp/archive_mysql_binlogs.lock'
TMP_DIR = '/tmp/'

log = environment_specific.setup_logging_defaults(__name__)


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
    lock_handle = None
    try:
        log.info('Taking binlog archiver lock')
        lock_handle = host_utils.take_flock_lock(BINLOG_LOCK_FILE)

        log_bin_dir = host_utils.get_cnf_setting('log_bin', port)
        instance = host_utils.HostAddr(':'.join((host_utils.HOSTNAME,
                                                 str(port))))
        s3_conn = boto.connect_s3()
        bucket = s3_conn.get_bucket(environment_specific.S3_BUCKET, validate=False)

        mysql_conn = mysql_lib.connect_mysql(instance)
        bin_logs = mysql_lib.get_master_logs(mysql_conn)
        prefix = os.path.join(BINLOG_S3_DIR,
                              instance.hostname,
                              str(instance.port))
        uploaded_binlogs = bucket.get_all_keys(prefix=prefix)

        for binlog in bin_logs[:-1]:
            compressed_file = ''.join((binlog['Log_name'], '.gz'))
            local_file = os.path.join(os.path.dirname(log_bin_dir),
                                      binlog['Log_name'])
            local_file_gz = os.path.join(TMP_DIR, compressed_file)
            remote_path = os.path.join(BINLOG_S3_DIR,
                                       instance.hostname,
                                       str(instance.port),
                                       compressed_file)
            log.info('Local file {local_file} will compress to {local_file_gz} '
                     'and upload to {remote_path}'.format(local_file=local_file,
                                                          local_file_gz=local_file_gz,
                                                          remote_path=remote_path))

            new_key = boto.s3.key.Key(bucket)
            new_key.key = remote_path
            if already_uploaded(remote_path, uploaded_binlogs):
                log.info('Binlog has already been uploaded')
                continue

            if dry_run:
                log.info('In dry_run mode, skipping compression and upload')
                continue

            log.info('Compressing file')
            f_in = open(local_file, 'r')
            f_out = gzip.open(local_file_gz, 'w', compresslevel=2)
            f_out.writelines(f_in)
            f_out.close()
            f_in.close()

            log.info('Uploading file')
            new_key.set_contents_from_filename(local_file_gz)
            log.info('Deleting local compressed file')
            os.remove(local_file_gz)
        log.info('Archiving complete')
    finally:
        if lock_handle:
            log.info('Releasing lock')
            host_utils.release_flock_lock(lock_handle)


def already_uploaded(path, existing_uploads):
    for entry in existing_uploads:
        if entry.name == path:
            return True
    return False


if __name__ == "__main__":
    main()
