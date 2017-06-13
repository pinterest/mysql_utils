#!/usr/bin/env python
import argparse
import datetime
import json
import logging
import multiprocessing
import os
import subprocess
import threading
import time
import traceback
import uuid

import boto
import _mysql_exceptions
import psutil

import safe_uploader
import mysql_backup_status
from lib import backup
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

ACTIVE = 'active'
CSV_BACKUP_LOCK_TABLE_NAME = 'backup_locks'
CSV_BACKUP_LOCK_TABLE = """CREATE TABLE IF NOT EXISTS {db}.{tbl} (
  `lock_identifier` varchar(36) NOT NULL,
  `lock_active` enum('active') DEFAULT 'active',
  `created_at` datetime NOT NULL,
  `expires` datetime DEFAULT NULL,
  `released` datetime DEFAULT NULL,
  `table_name` varchar(255) NOT NULL,
  `partition_number` INT UNSIGNED NOT NULL DEFAULT 0,
  `hostname` varchar(90) NOT NULL DEFAULT '',
  `port` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`lock_identifier`),
  UNIQUE KEY `lock_active` (`table_name`,`partition_number`,`lock_active`),
  INDEX `backup_location` (`hostname`, `port`),
  INDEX `expires` (`expires`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1"""
MAX_THREAD_ERROR = 5
LOCKS_HELD_TIME = '5 MINUTE'
# How long locks are held and updated
LOCK_EXTEND_FREQUENCY = 10
# LOCK_EXTEND_FREQUENCY in seconds

PATH_PITR_DATA = 'pitr/{replica_set}/{db_name}/{table}/{date}'
SUCCESS_ENTRY = 'YAY_IT_WORKED'

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',
                        default=None,
                        help='DB to export, default is all databases.')
    parser.add_argument('--force_table',
                        default=None,
                        help='Table to export, default is all tables.')
    parser.add_argument('--force_reupload',
                        default=False,
                        action='store_true',
                        help='Ignore existing uploads, reupload everyting')
    parser.add_argument('--loglevel',
                        default='INFO',
                        help='Change logging verbosity',
                        choices=set(['INFO', 'DEBUG']))
    parser.add_argument('--dev_bucket',
                        default=False,
                        action='store_true',
                        help='Use the dev bucket, useful for testing')
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.loglevel.upper(), None))

    # Nope, don't even start.
    if os.path.isfile(backup.CSV_BACKUP_SKIP_FILE):
        log.info('Found {}. Skipping CSV backup '
                 'run.'.format(backup.CSV_BACKUP_SKIP_FILE))
        return

    # If we ever want to run multi instance, this wil need to be updated
    backup_obj = mysql_backup_csv(host_utils.HostAddr(host_utils.HOSTNAME),
                                  args.db, args.force_table,
                                  args.force_reupload, args.dev_bucket)
    backup_obj.backup_instance()


class mysql_backup_csv:

    def __init__(self, instance,
                 db=None, force_table=None,
                 force_reupload=False, dev_bucket=False):
        """ Init function for backup, takes all args

        Args:
        instance - A hostAddr obect of the instance to be baced up
        db - (option) backup only specified db
        force_table - (option) backup only specified table
        force_reupload - (optional) force reupload of backup
        """
        self.instance = instance
        self.session_id = None
        self.timestamp = datetime.datetime.utcnow()
        # datestamp is for s3 files which are by convention -1 day
        self.datestamp = (self.timestamp -
                          datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        self.tables_to_backup = multiprocessing.Queue()
        self.tables_to_retry = multiprocessing.Queue()
        if db:
            table_list = ['{}.{}'.format(db, x) for x in mysql_lib.get_tables(instance, db, True)]
        else:
            table_list = mysql_lib.get_all_tables_by_instance(instance)

        for t in backup.filter_tables_to_csv_backup(instance, table_list):
            self.tables_to_backup.put(t)

        self.dev_bucket = dev_bucket
        self.force_table = force_table
        self.force_reupload = force_reupload
        self.table_count = 0
        self.upload_bucket = environment_specific.S3_CSV_BUCKET_DEV \
            if dev_bucket else environment_specific.S3_CSV_BUCKET

    def backup_instance(self):
        """ Back up a replica instance to s3 in csv """

        log.info('Backup for instance {i} started at {t}'
                 ''.format(t=str(self.timestamp),
                           i=self.instance))
        log.info('Checking heartbeat to make sure replication is not too '
                 'lagged.')
        self.check_replication_for_backup()

        log.info('Taking host backup lock')
        host_lock = host_utils.bind_lock_socket(backup.CSV_BACKUP_LOCK_SOCKET)

        log.info('Setting up export directory structure')
        self.setup_and_get_tmp_path()
        log.info('Will temporarily dump inside of {path}'
                 ''.format(path=self.dump_base_path))

        log.info('Releasing any invalid shard backup locks')
        self.ensure_backup_locks_sanity()

        log.info('Deleting old expired locks')
        self.purge_old_expired_locks()

        log.info('Stopping replication SQL thread to get a snapshot')
        mysql_lib.stop_replication(self.instance,
                                   mysql_lib.REPLICATION_THREAD_SQL)

        # starting a consistent snapshot here and retrieving the thread ID
        conn = mysql_lib.connect_mysql(self.instance,
                                       backup.USER_ROLE_MYSQLDUMP)
        mysql_lib.start_consistent_snapshot(conn, read_only=True)
        cursor = conn.cursor()
        cursor.execute('SET SESSION wait_timeout=28800')
        cursor.execute("SELECT VARIABLE_VALUE AS conn_id FROM "
                       "INFORMATION_SCHEMA.SESSION_VARIABLES "
                       "WHERE VARIABLE_NAME='pseudo_thread_id'")
        self.session_id = cursor.fetchone()['conn_id']

        workers = []
        for _ in range(multiprocessing.cpu_count() / 2):
            proc = multiprocessing.Process(target=self.mysql_backup_csv_tables)
            proc.daemon = True
            proc.start()
            workers.append(proc)

        # throw in a sleep to make sure all threads have started dumps
        time.sleep(2)
        log.info('Restarting replication')
        mysql_lib.start_replication(self.instance,
                                    mysql_lib.REPLICATION_THREAD_SQL)

        for worker in workers:
            worker.join()

        if not (self.tables_to_backup.empty() and self.tables_to_retry.empty()):
            raise Exception('All worker processes have completed, but '
                            'work remains in the queue')

        log.info('CSV backup is complete, will run a check')
        self.release_expired_locks()
        mysql_backup_status.verify_csv_instance_backup(
            self.instance,
            self.datestamp,
            self.dev_bucket)
        host_utils.release_lock_socket(host_lock)

    def mysql_backup_csv_tables(self):
        """ Worker for backing up a queue of tables """
        proc_id = multiprocessing.current_process().name
        conn = mysql_lib.connect_mysql(self.instance,
                    backup.USER_ROLE_MYSQLDUMP)
        mysql_lib.start_consistent_snapshot(conn, read_only=True,
                                            session_id=self.session_id)
        pitr_data = mysql_lib.get_pitr_data(self.instance)
        err_count = 0
        while not (self.tables_to_backup.empty() and self.tables_to_retry.empty()):
            table_tuple = self.tables_to_retry.get() if not self.tables_to_retry.empty() \
                else self.tables_to_backup.get()
            try:
                # if this is a partitioned table, and it is already
                # being backed up on some other host, we do not want to attempt
                # to back it up here.
                # 
                if table_tuple[1] and self.partition_lock_exists(table_tuple):
                    log.debug('Partitioned table {} is already being '
                              'backed up elsewhere, so we cannot do it '
                              'here.'.format(table_tuple[0]))
                else:
                    self.mysql_backup_csv_table_wrapper(table_tuple, conn, pitr_data)

                self.table_count = self.table_count + 1
                if (self.table_count % 50) == 0:
                    self.release_expired_locks()
            except:
                self.tables_to_retry.put(table_tuple)
                log.error('{proc_id}: Could not dump {tbl}, partition {p} - '
                          'error: {e}'.format(tbl=table_tuple[0], p=table_tuple[2],
                                              e=traceback.format_exc(),
                                              proc_id=proc_id))
                err_count = err_count + 1
                if err_count > MAX_THREAD_ERROR:
                    log.error('{}: Error count in thread > MAX_THREAD_ERROR. '
                              'Aborting :('.format(proc_id))
                    return

    def mysql_backup_csv_table_wrapper(self, table_tuple, conn, pitr_data):
        """ Back up a single table or partition

        Args:
        table_tuple - A tuple containing the fully-qualified table name,
                      the partition name, and the partition number
        conn - a connection the the mysql instance
        pitr_data - data describing the position of the db data in replication
        """
        proc_id = multiprocessing.current_process().name
        if not self.force_reupload and self.already_backed_up(table_tuple):
            log.info('{proc_id}: {tbl} partition {p} is already backed up, '
                     'skipping'.format(proc_id=proc_id,
                                       tbl=table_tuple[0],
                                       p=table_tuple[2]))
            return

        # attempt to take lock by writing a lock to the master
        tmp_dir_db = None
        lock_identifier = None
        extend_lock_thread = None
        try:
            self.release_expired_locks()
            lock_identifier = self.take_backup_lock(table_tuple)
            extend_lock_stop_event = threading.Event()
            extend_lock_thread = threading.Thread(
                    target=self.extend_backup_lock,
                    args=(lock_identifier, extend_lock_stop_event))
            extend_lock_thread.daemon = True
            extend_lock_thread.start()
            if not lock_identifier:
                return

            log.info('{proc_id}: {tbl} table, partition {p} backup start'
                     ''.format(tbl=table_tuple[0], p=table_tuple[2],
                               proc_id=proc_id))

            tmp_dir_db = os.path.join(self.dump_base_path, table_tuple[0].split('.')[0])
            if not os.path.exists(tmp_dir_db):
                os.makedirs(tmp_dir_db)
            host_utils.change_owner(tmp_dir_db, 'mysql', 'mysql')

            self.upload_pitr_data(*table_tuple[0].split('.'), pitr_data=pitr_data)
            self.mysql_backup_one_partition(table_tuple, tmp_dir_db, conn)

            log.info('{proc_id}: {tbl} table, partition {p} backup complete'
                     ''.format(tbl=table_tuple[0], p=table_tuple[2],
                               proc_id=proc_id))
        finally:
            if extend_lock_thread:
                extend_lock_stop_event.set()
                log.debug('{proc_id}: {tbl} table, partition {p} waiting for '
                          'lock expiry thread to end'.format(tbl=table_tuple[0],
                                                             p=table_tuple[2],
                                                             proc_id=proc_id))
                extend_lock_thread.join()
            if lock_identifier:
                log.debug('{proc_id}: {tbl} table, partition {p} releasing lock'
                          ''.format(tbl=table_tuple[0], p=table_tuple[2],
                                    proc_id=proc_id))
                self.release_table_backup_lock(lock_identifier)

    def mysql_backup_one_partition(self, table_tuple, tmp_dir_db, conn):
        """ Back up a single partition of a single table

        Args:
            table_tuple - the table_tuple (db, partition name, partition number)
                          to be backed up
            tmp_dir_db - temporary storage used for all tables in the db
            conn - a connection the the mysql instance
        """
        proc_id = multiprocessing.current_process().name
        (_, data_path, _) = backup.get_csv_backup_paths(self.instance,
                                *table_tuple[0].split('.'),
                                date=self.datestamp,
                                partition_number=table_tuple[2])
        log.debug('{proc_id}: {tbl} partition {p} dump to {path} started'
                  ''.format(proc_id=proc_id,
                            tbl=table_tuple[0],
                            p=table_tuple[2],
                            path=data_path))
        self.upload_schema(*table_tuple[0].split('.'), tmp_dir_db=tmp_dir_db)
        fifo = os.path.join(tmp_dir_db,
                            '{tbl}{part}'.format(tbl=table_tuple[0].split('.')[1],
                                                 part=table_tuple[2]))
        procs = dict()
        try:
            # giant try so we can try to clean things up in case of errors
            self.create_fifo(fifo)

            # Start creating processes
            procs['cat'] = subprocess.Popen(['cat', fifo],
                                            stdout=subprocess.PIPE)
            procs['nullescape'] = subprocess.Popen(['nullescape'],
                                                   stdin=procs['cat'].stdout,
                                                   stdout=subprocess.PIPE)
            procs['lzop'] = subprocess.Popen(['lzop'],
                                             stdin=procs['nullescape'].stdout,
                                             stdout=subprocess.PIPE)

            # Start dump query
            return_value = set()
            query_thread = threading.Thread(target=self.run_dump_query,
                                            args=(table_tuple, fifo, conn,
                                                  procs['cat'], return_value))
            query_thread.daemon = True
            query_thread.start()

            # And run the upload
            safe_uploader.safe_upload(precursor_procs=procs,
                                      stdin=procs['lzop'].stdout,
                                      bucket=self.upload_bucket,
                                      key=data_path,
                                      check_func=self.check_dump_success,
                                      check_arg=return_value)
            os.remove(fifo)
            log.debug('{proc_id}: {tbl} partition {p} clean up complete'
                      ''.format(proc_id=proc_id,
                                tbl=table_tuple[0],
                                p=table_tuple[2]))
        except:
            log.debug('{}: in exception handling for failed table '
                      'upload'.format(proc_id))

            if os.path.exists(fifo):
                self.cleanup_fifo(fifo)
            raise


    def create_fifo(self, fifo):
        """ Create a fifo to be used for dumping a mysql table

        Args:
        fifo - The path to the fifo
        """
        if os.path.exists(fifo):
            self.cleanup_fifo(fifo)

        log.debug('{proc_id}: creating fifo {fifo}'
                  ''.format(proc_id=multiprocessing.current_process().name,
                            fifo=fifo))
        os.mkfifo(fifo)
        # Could not get os.mkfifo(fifo, 0777) to work due to umask
        host_utils.change_owner(fifo, 'mysql', 'mysql')

    def cleanup_fifo(self, fifo):
        """ Safely cleanup a fifo that is an unknown state

        Args:
        fifo - The path to the fifo
        """
        log.debug('{proc_id}: Cleanup of {fifo} started'
                  ''.format(proc_id=multiprocessing.current_process().name,
                            fifo=fifo))
        cat_proc = subprocess.Popen('timeout 5 cat {} >/dev/null'.format(fifo),
                                    shell=True)
        cat_proc.wait()
        os.remove(fifo)
        log.debug('{proc_id}: Cleanup of {fifo} complete'
                  ''.format(proc_id=multiprocessing.current_process().name,
                            fifo=fifo))

    def run_dump_query(self, table_tuple, fifo, conn, cat_proc, return_value):
        """ Run a SELECT INTO OUTFILE into a fifo

        Args:
        table_tuple - A tuple of (table_name, partition_name, partition_number)
        fifo - The fifo to dump the table.db into
        conn - The connection to MySQL
        cat_proc - The process reading from the fifo
        return_value - A set to be used to populated the return status. This is
                       a semi-ugly hack that is required because of the use of
                       threads not being able to return data, however being
                       able to modify objects (like a set).
        """
        log.debug('{proc_id}: {tbl} partition {p} dump started'
                  ''.format(proc_id=multiprocessing.current_process().name,
                            tbl=table_tuple[0],
                            p=table_tuple[2]))
        extra = '' if not table_tuple[1] else " PARTITION ({})".format(table_tuple[1])
        (db, tbl) = table_tuple[0].split('.')
        sql = ("SELECT * "
               "INTO OUTFILE '{fifo}' "
               "FROM {db}.`{tbl}` {extra} "
               "").format(fifo=fifo,
                          db=db,
                          tbl=tbl,
                          extra=extra)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        except Exception as detail:
            # if we have not output any data, then the cat proc will never
            # receive an EOF, so we will be stuck
            if psutil.pid_exists(cat_proc.pid):
                cat_proc.kill()
            log.error('{proc_id}: dump query encountered an error: {er}'
                      ''.format(
                        er=detail,
                        proc_id=multiprocessing.current_process().name))

        log.debug('{proc_id}: {tbl} partition {p} dump complete'
                  ''.format(proc_id=multiprocessing.current_process().name,
                            tbl=table_tuple[0], p=table_tuple[2]))
        return_value.add(SUCCESS_ENTRY)

    def check_dump_success(self, return_value):
        """ Check to see if a dump query succeeded

        Args:
        return_value -  A set which if it includes SUCCESS_ENTRY shows that
                        the query succeeded
        """
        if SUCCESS_ENTRY not in return_value:
            raise Exception('{}: dump failed'
                            ''.format(multiprocessing.current_process().name))

    def upload_pitr_data(self, db, tbl, pitr_data):
        """ Upload a file of PITR data to s3 for each table

        Args:
        db - the db that was backed up.
        tbl - the table that was backed up.
        pitr_data - a dict of various data that might be helpful for running a
                    PITR
        """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        s3_path = PATH_PITR_DATA.format(replica_set=replica_set,
                                        date=self.datestamp,
                                        db_name=db, table=tbl)
        log.debug('{proc_id}: {db}.{tbl} Uploading pitr data to {s3_path}'
                  ''.format(s3_path=s3_path,
                            proc_id=multiprocessing.current_process().name,
                            db=db, tbl=tbl))
        boto_conn = boto.connect_s3()
        bucket = boto_conn.get_bucket(self.upload_bucket, validate=False)
        key = bucket.new_key(s3_path)
        key.set_contents_from_string(json.dumps(pitr_data))

    def upload_schema(self, db, table, tmp_dir_db):
        """ Upload the schema of a table to s3

        Args:
            db - the db to be backed up
            table - the table to be backed up
            tmp_dir_db - temporary storage used for all tables in the db
        """
        (schema_path, _, _) = backup.get_csv_backup_paths(
                                    self.instance, db, table, self.datestamp)
        create_stm = mysql_lib.show_create_table(self.instance, db, table)
        log.debug('{proc_id}: Uploading schema to {schema_path}'
                  ''.format(schema_path=schema_path,
                            proc_id=multiprocessing.current_process().name))
        boto_conn = boto.connect_s3()
        bucket = boto_conn.get_bucket(self.upload_bucket, validate=False)
        key = bucket.new_key(schema_path)
        key.set_contents_from_string(create_stm)

    def partition_lock_exists(self, table_tuple):
        """ Find out if there is already a lock on one partition of a
            partitioned table from a host other than us.  If so, we
            cannot backup that table here.
        Args:
            table_tuple - the tuple of table information.

        Returns:
            True if there is such a lock, False if not.
        """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        master = zk.get_mysql_instance_from_replica_set(
                    replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
        master_conn = mysql_lib.connect_mysql(master, role='dbascript')
        cursor = master_conn.cursor()
        params = {'table_name': table_tuple[0],
                  'hostname': self.instance.hostname,
                  'port': self.instance.port,
                  'active': ACTIVE}

        sql = ("SELECT COUNT(*) AS cnt FROM {db}.{tbl} WHERE "
               "lock_active = %(active)s AND "
               "table_name = %(table_name)s AND "
               "hostname <> %(hostname)s AND "
               "port = %(port)s").format(db=mysql_lib.METADATA_DB,
                                         tbl=CSV_BACKUP_LOCK_TABLE_NAME)
        cursor.execute(sql, params)
        row = int(cursor.fetchone()['cnt'])
        return (row > 0)

    def take_backup_lock(self, table_tuple):
        """ Write a lock row on to the master

        Args:
            table_tuple - the tuple containing info about the table/partition
                          to be backed up.

        Returns:
            a uuid lock identifier
        """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        master = zk.get_mysql_instance_from_replica_set(
                    replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
        master_conn = mysql_lib.connect_mysql(master, role='dbascript')
        cursor = master_conn.cursor()

        lock_identifier = str(uuid.uuid4())
        log.debug('Taking backup lock: {replica_set} {tbl} partition {p}'
                  ''.format(replica_set=replica_set,
                            tbl=table_tuple[0], p=table_tuple[2]))
        params = {'lock': lock_identifier,
                  'table_name': table_tuple[0],
                  'partition_number': table_tuple[2],
                  'hostname': self.instance.hostname,
                  'port': self.instance.port,
                  'active': ACTIVE}
        sql = ("INSERT INTO {db}.{tbl} "
               "SET "
               "lock_identifier = %(lock)s, "
               "lock_active = %(active)s, "
               "created_at = NOW(), "
               "expires = NOW() + INTERVAL {locks_held_time}, "
               "released = NULL, "
               "table_name = %(table_name)s, "
               "partition_number = %(partition_number)s, "
               "hostname = %(hostname)s, "
               "port = %(port)s"
               "").format(db=mysql_lib.METADATA_DB,
                          tbl=CSV_BACKUP_LOCK_TABLE_NAME,
                          locks_held_time=LOCKS_HELD_TIME)
        cursor = master_conn.cursor()
        try:
            cursor.execute(sql, params)
            master_conn.commit()
        except _mysql_exceptions.IntegrityError:
            lock_identifier = None
            sql = ("SELECT hostname, port, expires "
                   "FROM {db}.{tbl} "
                   "WHERE "
                   "    lock_active = %(active)s AND "
                   "    table_name = %(table_name)s AND "
                   "    partition_number = %(partition_number)s"
                   "").format(db=mysql_lib.METADATA_DB,
                              tbl=CSV_BACKUP_LOCK_TABLE_NAME)
            cursor.execute(sql,
                           {'table_name': table_tuple[0],
                            'partition_number': table_tuple[2],
                            'active': ACTIVE})
            ret = cursor.fetchone()
            log.debug('Table {tbl} (partition {p}) is being backed '
                      'up on {hostname}:{port}, '
                      'lock will expire at {expires}.'
                      ''.format(tbl=table_tuple[0],
                                p=table_tuple[2],
                                hostname=ret['hostname'],
                                port=ret['port'],
                                expires=str(ret['expires'])))

        log.debug(cursor._executed)
        return lock_identifier

    def extend_backup_lock(self, lock_identifier, extend_lock_stop_event):
        """ Extend a backup lock. This is to be used by a thread

        Args:
        lock_identifier - Corrosponds to a lock identifier row in the
                          CSV_BACKUP_LOCK_TABLE_NAME.
        extend_lock_stop_event - An event that will be used to inform this
                                 thread to stop extending the lock
        """
        # Assumption is that this is callled right after creating the lock
        last_update = time.time()
        while(not extend_lock_stop_event.is_set()):
            if (time.time() - last_update) > LOCK_EXTEND_FREQUENCY:
                zk = host_utils.MysqlZookeeper()
                replica_set = zk.get_replica_set_from_instance(self.instance)
                master = zk.get_mysql_instance_from_replica_set(replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
                master_conn = mysql_lib.connect_mysql(master, role='dbascript')
                cursor = master_conn.cursor()

                params = {'lock_identifier': lock_identifier}
                sql = ('UPDATE {db}.{tbl} '
                       'SET expires = NOW() + INTERVAL {locks_held_time} '
                       'WHERE lock_identifier = %(lock_identifier)s'
                       '').format(db=mysql_lib.METADATA_DB,
                                  tbl=CSV_BACKUP_LOCK_TABLE_NAME,
                                  locks_held_time=LOCKS_HELD_TIME)
                cursor.execute(sql, params)
                master_conn.commit()
                log.debug(cursor._executed)
                last_update = time.time()
            extend_lock_stop_event.wait(.5)

    def release_table_backup_lock(self, lock_identifier):
        """ Release a backup lock created by take_backup_lock

        Args:
        lock_identifier - a uuid to identify a lock row
        """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        master = zk.get_mysql_instance_from_replica_set(replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
        master_conn = mysql_lib.connect_mysql(master, role='dbascript')
        cursor = master_conn.cursor()

        params = {'lock_identifier': lock_identifier}
        sql = ('UPDATE {db}.{tbl} '
               'SET lock_active = NULL, released = NOW() '
               'WHERE lock_identifier = %(lock_identifier)s AND '
               '      lock_active is NOT NULL'
               '').format(db=mysql_lib.METADATA_DB,
                          tbl=CSV_BACKUP_LOCK_TABLE_NAME)
        cursor.execute(sql, params)
        master_conn.commit()
        log.debug(cursor._executed)

    def ensure_backup_locks_sanity(self):
        """ Release any backup locks that aren't sane. This means locks
            created by the same host as the caller. The instance level lock
            should allow this assumption to be correct.
        """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        master = zk.get_mysql_instance_from_replica_set(replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
        master_conn = mysql_lib.connect_mysql(master, role='dbascript')
        cursor = master_conn.cursor()

        if not mysql_lib.does_table_exist(master, mysql_lib.METADATA_DB,
                                          CSV_BACKUP_LOCK_TABLE_NAME):
            log.debug('Creating missing metadata table')
            cursor.execute(CSV_BACKUP_LOCK_TABLE.format(
                    db=mysql_lib.METADATA_DB,
                    tbl=CSV_BACKUP_LOCK_TABLE_NAME))

        params = {'hostname': self.instance.hostname,
                  'port': self.instance.port}
        sql = ('UPDATE {db}.{tbl} '
               'SET lock_active = NULL, released = NOW() '
               'WHERE hostname = %(hostname)s AND '
               '      port = %(port)s'
               '').format(db=mysql_lib.METADATA_DB,
                          tbl=CSV_BACKUP_LOCK_TABLE_NAME)
        cursor.execute(sql, params)
        master_conn.commit()

    def release_expired_locks(self):
        """ Release any expired locks """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        master = zk.get_mysql_instance_from_replica_set(replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
        master_conn = mysql_lib.connect_mysql(master, role='dbascript')
        cursor = master_conn.cursor()

        sql = ('UPDATE {db}.{tbl} '
               'SET lock_active = NULL, released = NOW() '
               'WHERE expires < NOW() AND lock_active IS NOT NULL'
               '').format(db=mysql_lib.METADATA_DB,
                          tbl=CSV_BACKUP_LOCK_TABLE_NAME)
        cursor.execute(sql)
        master_conn.commit()
        log.debug(cursor._executed)

    def purge_old_expired_locks(self):
        """ Delete any locks older than 2 days """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        master = zk.get_mysql_instance_from_replica_set(replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
        master_conn = mysql_lib.connect_mysql(master, role='dbascript')
        cursor = master_conn.cursor()

        sql = ('DELETE FROM {db}.{tbl} '
               'WHERE expires < NOW() - INTERVAL 2 DAY'
               '').format(db=mysql_lib.METADATA_DB,
                          tbl=CSV_BACKUP_LOCK_TABLE_NAME)
        cursor.execute(sql)
        master_conn.commit()
        log.debug(cursor._executed)

    def already_backed_up(self, table_tuple):
        """ Check to see if a particular partition has already been uploaded
            to s3

        Args:
            table_tuple - (table, partition name, part number)

        Returns:
            bool - True if the partition has already been backed up,
                   False otherwise
        """
        boto_conn = boto.connect_s3()
        bucket = boto_conn.get_bucket(self.upload_bucket, validate=False)
        (_, data_path, _) = backup.get_csv_backup_paths(self.instance,
                                           *table_tuple[0].split('.'),
                                           date=self.datestamp,
                                           partition_number=table_tuple[2])
        if not bucket.get_key(data_path):
            return False
        return True

    def check_replication_for_backup(self):
        """ Confirm that replication is caught up enough to run """
        while True:
            heartbeat = mysql_lib.get_heartbeat(self.instance)
            if heartbeat.date() < self.timestamp.date():
                log.warning('Replication is too lagged ({}) to run daily '
                            'backup, sleeping'.format(heartbeat))
                time.sleep(10)
            elif heartbeat.date() > self.timestamp.date():
                raise Exception('Replication is later than expected day')
            else:
                log.info('Replication is ok ({}) to run daily '
                         'backup'.format(heartbeat))
                return

    def setup_and_get_tmp_path(self):
        """ Figure out where to temporarily store csv backups,
            and clean it up
        """
        tmp_dir_root = os.path.join(host_utils.find_root_volume(),
                                    'csv_export',
                                    str(self.instance.port))
        if not os.path.exists(tmp_dir_root):
            os.makedirs(tmp_dir_root)
        host_utils.change_owner(tmp_dir_root, 'mysql', 'mysql')
        self.dump_base_path = tmp_dir_root


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
