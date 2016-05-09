import boto
import datetime
import os
import re
import resource
import subprocess
import sys
import time
import urllib

import safe_uploader
import mysql_lib
import host_utils
from lib import environment_specific


BACKUP_FILE = 'mysql-{hostname}-{port}-{timestamp}.{backup_type}'
BACKUP_LOCK_FILE = '/tmp/backup_mysql.lock'
BACKUP_TYPE_LOGICAL = 'sql.gz'
BACKUP_TYPE_CSV = 'csv'
BACKUP_TYPE_XBSTREAM = 'xbstream'
BACKUP_TYPES = set([BACKUP_TYPE_LOGICAL, BACKUP_TYPE_XBSTREAM,
                    BACKUP_TYPE_CSV])
INNOBACKUPEX = '/usr/bin/innobackupex'
INNOBACKUP_OK = 'completed OK!'
MYSQLDUMP = '/usr/bin/mysqldump'
MYSQLDUMP_CMD = ' '.join((MYSQLDUMP,
                          '--master-data',
                          '--single-transaction',
                          '--events',
                          '--all-databases',
                          '--routines',
                          '--user={dump_user}',
                          '--password={dump_pass}',
                          '--host={host}',
                          '--port={port}'))
PIGZ = ['/usr/bin/pigz', '-p', '2']
PV = '/usr/bin/pv -peafbt'
S3_SCRIPT = '/usr/local/bin/gof3r'
USER_ROLE_MYSQLDUMP = 'mysqldump'
USER_ROLE_XTRABACKUP = 'xtrabackup'
XB_RESTORE_STATUS = ("CREATE TABLE IF NOT EXISTS test.xb_restore_status ("
                     "id                INT UNSIGNED NOT NULL AUTO_INCREMENT, "
                     "restore_source    VARCHAR(64), "
                     "restore_type      ENUM('s3', 'remote_server', "
                     "                       'local_file') NOT NULL, "
                     "test_restore      ENUM('normal', 'test') NOT NULL, "
                     "restore_destination   VARCHAR(64), "
                     "restore_date      DATE, "
                     "restore_port      SMALLINT UNSIGNED NOT NULL "
                     "                  DEFAULT 3306, "
                     "restore_file      VARCHAR(255), "
                     "replication       ENUM('SKIP', 'REQ', 'OK', 'FAIL'), "
                     "zookeeper         ENUM('SKIP', 'REQ', 'OK', 'FAIL'), "
                     "started_at        DATETIME NOT NULL, "
                     "finished_at       DATETIME, "
                     "restore_status    ENUM('OK', 'IPR', 'BAD') "
                     "                  DEFAULT 'IPR', "
                     "status_message    TEXT, "
                     "PRIMARY KEY(id), "
                     "INDEX (restore_type, started_at), "
                     "INDEX (restore_type, restore_status, "
                     "       started_at) )")

XTRABACKUP_CMD = ' '.join((INNOBACKUPEX,
                           '{datadir}',
                           '--slave-info',
                           '--safe-slave-backup',
                           '--parallel=8',
                           '--stream=xbstream',
                           '--no-timestamp',
                           '--compress',
                           '--compress-threads=8',
                           '--kill-long-queries-timeout=10',
                           '--user={xtra_user}',
                           '--password={xtra_pass}',
                           '--defaults-file={cnf}',
                           '--defaults-group={cnf_group}',
                           '--port={port}'))
MINIMUM_VALID_BACKUP_SIZE_BYTES = 1024 * 1024

log = environment_specific.setup_logging_defaults(__name__)


def parse_xtrabackup_slave_info(datadir):
    """ Pull master_log and master_log_pos from a xtrabackup_slave_info file
    NOTE: This file has its data as a CHANGE MASTER command. Example:
    CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.006233', MASTER_LOG_POS=863

    Args:
    datadir - the path to the restored datadir

    Returns:
    binlog_file - Binlog file to start reading from
    binlog_pos - Position in binlog_file to start reading
    """
    file_path = os.path.join(datadir, 'xtrabackup_slave_info')
    with open(file_path) as f:
        data = f.read()

    file_pattern = ".*MASTER_LOG_FILE='([a-z0-9-.]+)'.*"
    pos_pattern = ".*MASTER_LOG_POS=([0-9]+).*"
    res = re.match(file_pattern, data)
    binlog_file = res.group(1)
    res = re.match(pos_pattern, data)
    binlog_pos = int(res.group(1))

    log.info('Master info: binlog_file: {binlog_file},'
             ' binlog_pos: {binlog_pos}'.format(binlog_file=binlog_file,
                                                binlog_pos=binlog_pos))
    return (binlog_file, binlog_pos)


def parse_xtrabackup_binlog_info(datadir):
    """ Pull master_log and master_log_pos from a xtrabackup_slave_info file
    Note: This file stores its data as two strings in a file
          deliminted by a tab. Example: "mysql-bin.006231\t1619"

    Args:
    datadir - the path to the restored datadir

    Returns:
    binlog_file - Binlog file to start reading from
    binlog_pos - Position in binlog_file to start reading
    """
    file_path = os.path.join(datadir, 'xtrabackup_binlog_info')
    with open(file_path) as f:
        data = f.read()

    fields = data.strip().split("\t")
    if len(fields) != 2:
        raise Exception(('Error: Invalid format in '
                         'file {file_path}').format(file_path=file_path))
    binlog_file = fields[0].strip()
    binlog_pos = int(fields[1].strip())

    log.info('Master info: binlog_file: {binlog_file},'
             ' binlog_pos: {binlog_pos}'.format(binlog_file=binlog_file,
                                                binlog_pos=binlog_pos))
    return (binlog_file, binlog_pos)


def get_metadata_from_backup_file(full_path):
    """ Parse the filename of a backup to determine the source of a backup

    Note: there is a strong assumption that the port number matches 330[0-9]

    Args:
    full_path - Path to a backup file.
                Example: /backup/tmp/mysql-legaldb001c-3306-2014-06-06.xbstream
                Example: /backup/tmp/mysql-legaldb-1-1-3306-2014-06-06.xbstream

    Returns:
    host - A hostaddr object
    creation - a datetime object describing creation date
    extension - file extension
    """
    filename = os.path.basename(full_path)
    pattern = 'mysql-([a-z0-9-]+)-(330[0-9])-(\d{4})-(\d{2})-(\d{2}).*\.(.+)'
    res = re.match(pattern, filename)
    host = host_utils.HostAddr(':'.join((res.group(1), res.group(2))))
    creation = datetime.date(int(res.group(3)), int(res.group(4)),
                             int(res.group(5)))
    extension = res.group(6)
    return host, creation, extension


def logical_backup_instance(instance, timestamp):
    """ Take a compressed mysqldump backup

    Args:
    instance - A hostaddr instance
    timestamp - A timestamp which will be used to create the backup filename

    Returns:
    A string of the path to the finished backup
    """
    dump_file = BACKUP_FILE.format(hostname=instance.hostname,
                                   port=instance.port,
                                   timestamp=time.strftime('%Y-%m-%d-%H:%M:%S',
                                                           timestamp),
                                   backup_type=BACKUP_TYPE_LOGICAL)
    (dump_user,
     dump_pass) = mysql_lib.get_mysql_user_for_role(USER_ROLE_MYSQLDUMP)
    dump_cmd = MYSQLDUMP_CMD.format(dump_user=dump_user,
                                    dump_pass=dump_pass,
                                    host=instance.hostname,
                                    port=instance.port)
    procs = dict()
    try:
        procs['mysqldump'] = subprocess.Popen(dump_cmd.split(),
                                              stdout=subprocess.PIPE)
        procs['pigz'] = subprocess.Popen(PIGZ,
                                         stdin=procs['mysqldump'].stdout,
                                         stdout=subprocess.PIPE)
        log.info('Uploading backup to {buk}/{key}'
                 ''.format(buk=environment_specific.S3_BUCKET,
                           key=dump_file))
        safe_uploader.safe_upload(precursor_procs=procs,
                                  stdin=procs['pigz'].stdout,
                                  bucket=environment_specific.S3_BUCKET,
                                  key=dump_file)
        log.info('mysqldump was successful')
    except:
        safe_uploader.kill_precursor_procs(procs)
        raise


def xtrabackup_instance(instance, timestamp):
    """ Take a compressed mysql backup

    Args:
    instance - A hostaddr instance
    timestamp - A timestamp which will be used to create the backup filename

    Returns:
    A string of the path to the finished backup
    """
    # Prevent issues with too many open files
    resource.setrlimit(resource.RLIMIT_NOFILE, (131072, 131072))
    backup_file = BACKUP_FILE.format(
            hostname=instance.hostname,
            port=instance.port,
            timestamp=time.strftime('%Y-%m-%d-%H:%M:%S', timestamp),
            backup_type=BACKUP_TYPE_XBSTREAM)

    tmp_log = os.path.join(environment_specific.RAID_MOUNT,
                           'log', 'xtrabackup_{ts}.log'.format(
                            ts=time.strftime('%Y-%m-%d-%H:%M:%S', timestamp)))
    tmp_log_handle = open(tmp_log, "w")
    procs = dict()
    try:
        procs['xtrabackup'] = subprocess.Popen(
            create_xtrabackup_command(instance, timestamp, tmp_log),
            stdout=subprocess.PIPE, stderr=tmp_log_handle)
        log.info('Uploading backup to {buk}/{loc}'
                 ''.format(buk=environment_specific.S3_BUCKET,
                           loc=backup_file))
        safe_uploader.safe_upload(precursor_procs=procs,
                                  stdin=procs['xtrabackup'].stdout,
                                  bucket=environment_specific.S3_BUCKET,
                                  key=backup_file,
                                  check_func=check_xtrabackup_log,
                                  check_arg=tmp_log)
        log.info('Xtrabackup was successful')
    except:
        safe_uploader.kill_precursor_procs(procs)
        raise


def check_xtrabackup_log(tmp_log):
    """ Confirm that a xtrabackup backup did not have problems

    Args:
    tmp_log - The path of the log file
    """
    with open(tmp_log, 'r') as log_file:
        xtra_log = log_file.readlines()
        if INNOBACKUP_OK not in xtra_log[-1]:
            raise Exception('innobackupex failed. '
                            'log_file: {tmp_log}'.format(tmp_log=tmp_log))


def create_xtrabackup_command(instance, timestamp, tmp_log):
    """ Create a xtrabackup command

    Args:
    instance - A hostAddr object
    timestamp - A timestamp
    tmp_log - A path to where xtrabackup should log

    Returns:
    a list that can be easily ingested by subprocess
    """
    if host_utils.get_hiera_role() in host_utils.MASTERFUL_PUPPET_ROLES:
        cnf = host_utils.OLD_CONF_ROOT.format(port=instance.port)
        cnf_group = 'mysqld'
    else:
        cnf = host_utils.MYSQL_CNF_FILE
        cnf_group = 'mysqld{port}'.format(port=instance.port)
    datadir = host_utils.get_cnf_setting('datadir', instance.port)
    (xtra_user,
     xtra_pass) = mysql_lib.get_mysql_user_for_role(USER_ROLE_XTRABACKUP)
    return XTRABACKUP_CMD.format(datadir=datadir,
                                 xtra_user=xtra_user,
                                 xtra_pass=xtra_pass,
                                 cnf=cnf,
                                 cnf_group=cnf_group,
                                 port=instance.port,
                                 tmp_log=tmp_log).split()


def xbstream_unpack(xbstream, port, restore_source, size=None):
    """ Decompress an xbstream filename into a directory.

    Args:
    xbstream - A string which is the path to the xbstream file
    port - The port on which to act on on localhost
    host - A string which is a hostname if the xbstream exists on a remote host
    size - An int for the size in bytes for remote unpacks for a progress bar
    """
    datadir = host_utils.get_cnf_setting('datadir', port)

    cmd = ('{s3_script} get --no-md5 -b {bucket} -k {xbstream} '
           '2>/dev/null ').format(s3_script=S3_SCRIPT,
                                  bucket=environment_specific.S3_BUCKET,
                                  xbstream=urllib.quote_plus(xbstream))
    if size:
        cmd = ' | '.join((cmd, '{pv} -s {size}'.format(pv=PV,
                                                       size=str(size))))
    # And finally pipe everything into xbstream to unpack it
    cmd = ' | '.join((cmd, '/usr/bin/xbstream -x -C {}'.format(datadir)))
    log.info(cmd)

    extract = subprocess.Popen(cmd, shell=True)
    if extract.wait() != 0:
        raise Exception("Error: Xbstream decompress did not succeed, aborting")


def innobackup_decompress(port, threads=8):
    """ Decompress an unpacked backup compressed with xbstream.

    Args:
    port - The port of the instance on which to act
    threads - A int which signifies how the amount of parallelism. Default is 8
    """
    datadir = host_utils.get_cnf_setting('datadir', port)

    cmd = ' '.join(('/usr/bin/innobackupex',
                    '--parallel={threads}',
                    '--decompress',
                    datadir)).format(threads=threads)

    err_log = os.path.join(datadir, 'xtrabackup-decompress.err')
    out_log = os.path.join(datadir, 'xtrabackup-decompress.log')

    with open(err_log, 'w+') as err_handle, open(out_log, 'w') as out_handle:
        verbose = '{cmd} 2>{err_log} >{out_log}'.format(cmd=cmd,
                                                        err_log=err_log,
                                                        out_log=out_log)
        log.info(verbose)
        decompress = subprocess.Popen(cmd,
                                      shell=True,
                                      stdout=out_handle,
                                      stderr=err_handle)
        if decompress.wait() != 0:
            raise Exception('Fatal error: innobackupex decompress '
                            'did not return 0')

        err_handle.seek(0)
        log_data = err_handle.readlines()
        if INNOBACKUP_OK not in log_data[-1]:
            msg = ('Fatal error: innobackupex decompress did not end with '
                   '"{}"'.format(INNOBACKUP_OK))
            raise Exception(msg)


def apply_log(port, memory='10G'):
    """ Apply redo logs for an unpacked and uncompressed instance

    Args:
    path - The port of the instance on which to act
    memory - A string of how much memory can be used to apply logs. Default 10G
    """
    datadir = host_utils.get_cnf_setting('datadir', port)
    cmd = ' '.join(('/usr/bin/innobackupex',
                    '--apply-log',
                    '--use-memory={memory}',
                    datadir)).format(memory=memory)

    log_file = os.path.join(datadir, 'xtrabackup-apply-logs.log')
    with open(log_file, 'w+') as log_handle:
        verbose = '{cmd} >{log_file}'.format(cmd=cmd,
                                             log_file=log_file)
        log.info(verbose)
        apply_logs = subprocess.Popen(cmd,
                                      shell=True,
                                      stderr=log_handle)
        if apply_logs.wait() != 0:
            raise Exception('Fatal error: innobackupex apply-logs did not '
                            'return return 0')

        log_handle.seek(0)
        log_data = log_handle.readlines()
        if INNOBACKUP_OK not in log_data[-1]:
            msg = ('Fatal error: innobackupex apply-log did not end with '
                   '"{}"'.format(INNOBACKUP_OK))
            raise Exception(msg)


def get_s3_backup(hostaddr, date=None, backup_type=BACKUP_TYPE_XBSTREAM):
    """ Find the most recent xbstream file for an instance on s3

    Args:
    hostaddr - A hostaddr object for the desired instance
    date - Desired date of restore file

    Returns:
    filename - The path to the most recent backup file
    """
    prefix = 'mysql-{host}-{port}'.format(host=hostaddr.hostname,
                                          port=hostaddr.port)
    if date:
        prefix = ''.join((prefix, '-', date))
    log.debug('looking for backup with prefix {prefix}'.format(prefix=prefix))
    conn = boto.connect_s3()
    bucket = conn.get_bucket(environment_specific.S3_BUCKET, validate=False)
    bucket_items = bucket.list(prefix=prefix)

    latest_backup = None
    for elem in bucket_items:
        if elem.name.endswith(backup_type):
            # xbstream files need to be larger than
            # MINIMUM_VALID_BACKUP_SIZE_BYTES
            if (backup_type != BACKUP_TYPE_XBSTREAM) or\
               (elem.size > MINIMUM_VALID_BACKUP_SIZE_BYTES):
                latest_backup = elem
    if not latest_backup:
        msg = ('Unable to find a valid backup for '
               '{instance}').format(instance=hostaddr)
        raise Exception(msg)
    log.debug('Found a s3 backup {s3_path} with a size of '
              '{size}'.format(s3_path=latest_backup.name,
                              size=latest_backup.size))
    return (latest_backup.name, latest_backup.size)


def restore_logical(s3_key, size):
    """ Restore a compressed mysqldump file from s3 to localhost, port 3306

    Args:
    s3_key - A string which is the path to the compressed dump
    port - The port on which to act on on localhost
    size - An int for the size in bytes for remote unpacks for a progress bar
    """
    cmd = ('{s3_script} get --no-md5 -b {bucket} -k {s3_key} 2>/dev/null'
           '| {pv} -s {size}'
           '| zcat '
           '| mysql ').format(s3_script=S3_SCRIPT,
                              bucket=environment_specific.S3_BUCKET,
                              s3_key=urllib.quote_plus(s3_key),
                              pv=PV,
                              size=size)
    log.info(cmd)
    import_proc = subprocess.Popen(cmd, shell=True)
    if import_proc.wait() != 0:
        raise Exception("Error: Import failed")


def start_restore_log(instance, params):
    """ Create a record in xb_restore_status at the start of a restore
    """
    try:
        conn = mysql_lib.connect_mysql(instance)
    except Exception as e:
        log.warning("Unable to connect to master to log "
                    "our progress: {e}.  Attempting to "
                    "continue with restore anyway.".format(e=e))
        return None

    if not mysql_lib.does_table_exist(instance, 'test', 'xb_restore_status'):
        create_status_table(conn)
    sql = ("REPLACE INTO test.xb_restore_status "
           "SET "
           "restore_source = %(restore_source)s, "
           "restore_type = 's3', "
           "restore_file = %(restore_file)s, "
           "restore_destination = %(source_instance)s, "
           "restore_date = %(restore_date)s, "
           "restore_port = %(restore_port)s, "
           "replication = %(replication)s, "
           "zookeeper = %(zookeeper)s, "
           "started_at = NOW()")
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        log.info(cursor._executed)
        row_id = cursor.lastrowid
    except Exception as e:
        log.warning("Unable to log restore_status: {e}".format(e=e))
        row_id = None

    cursor.close()
    conn.commit()
    conn.close()
    return row_id


def update_restore_log(instance, row_id, params):
    try:
        conn = mysql_lib.connect_mysql(instance)
    except Exception as e:
        log.warning("Unable to connect to master to log "
                    "our progress: {e}.  Attempting to "
                    "continue with restore anyway.".format(e=e))
        return

    updates_fields = []

    if 'finished_at' in params:
        updates_fields.append('finished_at=NOW()')
    if 'restore_status' in params:
        updates_fields.append('restore_status=%(restore_status)s')
    if 'status_message' in params:
        updates_fields.append('status_message=%(status_message)s')
    if 'replication' in params:
        updates_fields.append('replication=%(replication)s')
    if 'zookeeper' in params:
        updates_fields.append('zookeeper=%(zookeeper)s')
    if 'finished_at' in params:
        updates_fields.append('finished_at=NOW()')

    sql = ("UPDATE test.xb_restore_status SET "
           "{} WHERE id=%(row_id)s".format(', '.join(updates_fields)))
    params['row_id'] = row_id
    cursor = conn.cursor()
    cursor.execute(sql, params)
    log.info(cursor._executed)
    cursor.close()
    conn.commit()
    conn.close()


def get_most_recent_restore(instance):
    conn = mysql_lib.connect_mysql(instance)
    cursor = conn.cursor()
    sql = ("SELECT * "
           "FROM test.xb_restore_status "
           "WHERE restore_status='OK' ")
    try:
        cursor.execute(sql)
    except Exception as e:
        print ("UNKNOWN: Cannot query restore status table: {e}".format(e=e))
        sys.exit(3)
    return cursor.fetchall()


def create_status_table(conn):
    """ Create the restoration status table if it isn't already there.

        Args:
            conn: A connection to the master server for this replica set.

        Returns:
            nothing
    """
    try:
        cursor = conn.cursor()
        cursor.execute(XB_RESTORE_STATUS)
        cursor.close()
    except Exception as e:
        log.error("Unable to create replication status table "
                  "on master: {e}".format(e=e))
        log.error("We will attempt to continue anyway.")


def quick_test_replication(instance):
    mysql_lib.start_replication(instance)
    mysql_lib.assert_replication_sanity(instance)
