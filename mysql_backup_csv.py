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
  `db` varchar(64) NOT NULL,
  `hostname` varchar(90) NOT NULL DEFAULT '',
  `port` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`lock_identifier`),
  UNIQUE KEY `lock_active` (`db`,`lock_active`),
  INDEX `backup_location` (`hostname`, `port`),
  INDEX `expires` (`expires`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1"""
MAX_THREAD_ERROR = 5
LOCKS_HELD_TIME = '5 MINUTE'
# How long locks are held and updated
LOCK_EXTEND_FREQUENCY = 10
# LOCK_EXTEND_FREQUENCY in seconds

PATH_PITR_DATA = 'pitr/{replica_set}/{db_name}/{date}'
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
        self.timestamp = datetime.datetime.utcnow()
        # datestamp is for s3 files which are by convention -1 day
        self.datestamp = (self.timestamp -
                          datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        self.dbs_to_backup = multiprocessing.Queue()
        if db:
            self.dbs_to_backup.put(db)
        else:
            for db in self.get_dbs_to_backup():
                self.dbs_to_backup.put(db)

        self.dev_bucket = dev_bucket
        self.force_table = force_table
        self.force_reupload = force_reupload
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
        host_lock = host_utils.bind_lock_socket(backup.BACKUP_LOCK_SOCKET)

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

        workers = []
        for _ in range(multiprocessing.cpu_count() / 2):
            proc = multiprocessing.Process(target=self.mysql_backup_csv_dbs)
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

        if not self.dbs_to_backup.empty():
            raise Exception('All worker processes have completed, but '
                            'work remains in the queue')

        log.info('CSV backup is complete, will run a check')
        mysql_backup_status.verify_csv_instance_backup(
            self.instance,
            self.datestamp,
            self.dev_bucket)
        host_utils.release_lock_socket(host_lock)

    def mysql_backup_csv_dbs(self):
        """ Worker for backing up a queue of dbs """
        proc_id = multiprocessing.current_process().name
        conn = mysql_lib.connect_mysql(self.instance,
                    backup.USER_ROLE_MYSQLDUMP)
        mysql_lib.start_consistent_snapshot(conn, read_only=True)
        pitr_data = mysql_lib.get_pitr_data(self.instance)
        err_count = 0
        while not self.dbs_to_backup.empty():
            db = self.dbs_to_backup.get()
            try:
                self.mysql_backup_csv_db(db, conn, pitr_data)
            except:
                self.dbs_to_backup.put(db)
                log.error('{proc_id}: Could not dump {db}, '
                          'error: {e}'.format(db=db,
                                              e=traceback.format_exc(),
                                              proc_id=proc_id))
                err_count = err_count + 1
                if err_count > MAX_THREAD_ERROR:
                    log.error('{}: Error count in thread > MAX_THREAD_ERROR. '
                              'Aborting :('.format(proc_id))
                    return

    def mysql_backup_csv_db(self, db, conn, pitr_data):
        """ Back up a single db

        Args:
        db - the db to be backed up
        conn - a connection the the mysql instance
        pitr_data - data describing the position of the db data in replication
        """
        proc_id = multiprocessing.current_process().name
        if not self.force_reupload and self.already_backed_up(db):
            log.info('{proc_id}: {db} is already backed up, skipping'
                     ''.format(proc_id=proc_id,
                               db=db))
            return

        # attempt to take lock by writing a lock to the master
        tmp_dir_db = None
        lock_identifier = None
        extend_lock_thread = None
        try:
            self.release_expired_locks()
            lock_identifier = self.take_backup_lock(db)
            extend_lock_stop_event = threading.Event()
            extend_lock_thread = threading.Thread(
                    target=self.extend_backup_lock,
                    args=(lock_identifier, extend_lock_stop_event))
            extend_lock_thread.daemon = True
            extend_lock_thread.start()
            if not lock_identifier:
                return

            log.info('{proc_id}: {db} db backup start'
                     ''.format(db=db,
                               proc_id=proc_id))

            tmp_dir_db = os.path.join(self.dump_base_path, db)
            if not os.path.exists(tmp_dir_db):
                os.makedirs(tmp_dir_db)
            host_utils.change_owner(tmp_dir_db, 'mysql', 'mysql')

            self.upload_pitr_data(db, pitr_data)

            for table in self.get_tables_to_backup(db):
                self.mysql_backup_csv_table(db, table, tmp_dir_db, conn)

            log.info('{proc_id}: {db} db backup complete'
                     ''.format(db=db,
                               proc_id=proc_id))
        finally:
            if extend_lock_thread:
                extend_lock_stop_event.set()
                log.debug('{proc_id}: {db} waiting for lock expiry thread to'
                          'end'.format(db=db,
                                       proc_id=proc_id))
                extend_lock_thread.join()
            if lock_identifier:
                log.debug('{proc_id}: {db} releasing lock'
                          ''.format(db=db,
                                    proc_id=proc_id))
                self.release_db_backup_lock(lock_identifier)

    def mysql_backup_csv_table(self, db, table, tmp_dir_db, conn):
        """ Back up a single table of a single db

        Args:
        db - the db to be backed up
        table - the table to be backed up
        tmp_dir_db - temporary storage used for all tables in the db
        conn - a connection the the mysql instance
        """
        proc_id = multiprocessing.current_process().name
        (_, data_path, _) = backup.get_csv_backup_paths(self.instance, db,
                                table, self.datestamp)
        log.debug('{proc_id}: {db}.{table} dump to {path} started'
                  ''.format(proc_id=proc_id,
                            db=db,
                            table=table,
                            path=data_path))
        self.upload_schema(db, table, tmp_dir_db)
        fifo = os.path.join(tmp_dir_db, table)
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
                                            args=(db, table, fifo, conn,
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
            log.debug('{proc_id}: {db}.{table} clean up complete'
                      ''.format(proc_id=proc_id,
                                db=db,
                                table=table))
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

    def run_dump_query(self, db, table, fifo, conn, cat_proc, return_value):
        """ Run a SELECT INTO OUTFILE into a fifo

        Args:
        db - The db to dump
        table - The table of the db to dump
        fifo - The fifo to dump the table.db into
        conn - The connection to MySQL
        cat_proc - The process reading from the fifo
        return_value - A set to be used to populated the return status. This is
                       a semi-ugly hack that is required because of the use of
                       threads not being able to return data, however being
                       able to modify objects (like a set).
        """
        log.debug('{proc_id}: {db}.{table} dump started'
                  ''.format(proc_id=multiprocessing.current_process().name,
                            db=db,
                            table=table))
        sql = ("SELECT * "
               "INTO OUTFILE '{fifo}' "
               "FROM {db}.{table} "
               "").format(fifo=fifo,
                          db=db,
                          table=table)
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

        log.debug('{proc_id}: {db}.{table} dump complete'
                  ''.format(proc_id=multiprocessing.current_process().name,
                            db=db,
                            table=table))
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

    def upload_pitr_data(self, db, pitr_data):
        """ Upload a file of PITR data to s3 for each schema

        Args:
        db - the db that was backed up.
        pitr_data - a dict of various data that might be helpful for running a
                    PITR
        """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        s3_path = PATH_PITR_DATA.format(replica_set=replica_set,
                                        date=self.datestamp,
                                        db_name=db)
        log.debug('{proc_id}: {db} Uploading pitr data to {s3_path}'
                  ''.format(s3_path=s3_path,
                            proc_id=multiprocessing.current_process().name,
                            db=db))
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

    def take_backup_lock(self, db):
        """ Write a lock row on to the master

        Args:
        db - the db to be backed up

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
        log.debug('Taking backup lock: {replica_set} {db} '
                  ''.format(replica_set=replica_set,
                            db=db))
        params = {'lock': lock_identifier,
                  'db': db,
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
               "db = %(db)s,"
               "hostname = %(hostname)s,"
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
                   "    db = %(db)s"
                   "").format(db=mysql_lib.METADATA_DB,
                              tbl=CSV_BACKUP_LOCK_TABLE_NAME)
            cursor.execute(sql,
                           {'db': db, 'active': ACTIVE})
            ret = cursor.fetchone()
            log.debug('DB {db} is being backed up on {hostname}:{port}, '
                      'lock will expire at {expires}.'
                      ''.format(db=db,
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

    def release_db_backup_lock(self, lock_identifier):
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
               '     port = %(port)s'
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
               'WHERE expires < NOW()'
               '').format(db=mysql_lib.METADATA_DB,
                          tbl=CSV_BACKUP_LOCK_TABLE_NAME)
        cursor.execute(sql)
        master_conn.commit()
        log.debug(cursor._executed)

    def purge_old_expired_locks(self):
        """ Delete any locks older than a week """
        zk = host_utils.MysqlZookeeper()
        replica_set = zk.get_replica_set_from_instance(self.instance)
        master = zk.get_mysql_instance_from_replica_set(replica_set,
                    host_utils.REPLICA_ROLE_MASTER)
        master_conn = mysql_lib.connect_mysql(master, role='dbascript')
        cursor = master_conn.cursor()

        sql = ('DELETE FROM {db}.{tbl} '
               'WHERE expires < NOW() - INTERVAL 1 WEEK AND '
               '        lock_active is NOT NULL '
               '').format(db=mysql_lib.METADATA_DB,
                          tbl=CSV_BACKUP_LOCK_TABLE_NAME)
        cursor.execute(sql)
        master_conn.commit()
        log.debug(cursor._executed)

    def already_backed_up(self, db):
        """ Check to see if a db has already been uploaded to s3

        Args:
        db - The db to check for being backed up

        Returns:
        bool - True if the db has already been backed up, False otherwise
        """
        boto_conn = boto.connect_s3()
        bucket = boto_conn.get_bucket(self.upload_bucket, validate=False)
        for table in self.get_tables_to_backup(db):
            (_, data_path, _) = backup.get_csv_backup_paths(self.instance,
                                           db, table, self.datestamp)
            if not bucket.get_key(data_path):
                return False
        return True

    def get_dbs_to_backup(self):
        """ Determine which tables should be backed up in an instance

        Returns:
            a set of dbs
        """
        zk = host_utils.MysqlZookeeper()
        rs = zk.get_replica_set_from_instance(self.instance)
        dbs = zk.get_sharded_dbs_by_replica_set()[rs]
        if dbs:
            return dbs
        else:
            return mysql_lib.get_dbs(self.instance)

    def get_tables_to_backup(self, db):
        """ Determine which tables should be backed up in a db

        Args:
        db -  The db for which we need a list of tables eligible for backup

        Returns:
        a set of table names
        """
        tables = backup.filter_tables_to_csv_backup(
                     self.instance, db,
                     mysql_lib.get_tables(self.instance, db, skip_views=True))
        if not self.force_table:
            return tables

        if self.force_table not in tables:
            raise Exception('Requested table {} is not available to backup'
                            ''.format(self.force_table))
        else:
            return set([self.force_table])

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
