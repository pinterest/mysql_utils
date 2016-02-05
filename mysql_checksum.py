#!/usr/bin/env python
import argparse
import time
import socket
import sys
import re
from lib import environment_specific

from lib import host_utils
from lib import mysql_lib

# These are used when running pt-table-checksum
CHECKSUM_DEFAULTS = ' '.join(('--chunk-time=0.1',
                              '--no-check-replication-filters',
                              '--no-check-binlog-format',
                              '--no-replicate-check',
                              '--no-version-check',
                              '--max-lag=1s',
                              '--progress=time,30',
                              '--replicate={METADATA_DB}.checksum')).format(METADATA_DB=mysql_lib.METADATA_DB)

# These are used when running pt-table-sync, which we'll use if
# we found some chunk diffs during step 1.
CHECKSUM_SYNC_DEFAULTS = ' '.join(('--sync-to-master',
                                   '--chunk-size=500',
                                   '--print',
                                   '--verbose'))

# Arbitrary bounds on how many chunks can be different in a given
# table before we do or do not *not* to do a detailed comparison
# of the table.
MIN_DIFFS = 1
MAX_DIFFS = 5

# Check this fraction (1/K) of databases on an instance.
DB_CHECK_FRACTION = 10

CHECKSUM_TBL = 'checksum_detail'

TABLE_DEF = ("CREATE TABLE IF NOT EXISTS {db}.{tbl} ( "
             "id              INT UNSIGNED NOT NULL AUTO_INCREMENT,"
             "reported_at     DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',"
             "instance        VARCHAR(40) NOT NULL DEFAULT '',"
             "master_instance VARCHAR(40) NOT NULL DEFAULT '',"
             "db              VARCHAR(30) NOT NULL DEFAULT '',"
             "tbl             VARCHAR(64) NOT NULL DEFAULT '',"
             "elapsed_time_ms INT NOT NULL DEFAULT -1,"
             "chunk_count     INT NOT NULL DEFAULT -1,"
             "chunk_errors    INT NOT NULL DEFAULT -1,"
             "chunk_diffs     INT NOT NULL DEFAULT -1,"
             "chunk_skips     INT NOT NULL DEFAULT -1,"
             "row_count       INT NOT NULL DEFAULT -1,"
             "row_diffs       INT NOT NULL DEFAULT -1,"
             "rows_checked    ENUM('YES','NO') NOT NULL DEFAULT 'NO',"
             "checksum_status ENUM('GOOD', 'CHUNK_DIFFS_FOUND_BUT_OK',"
             "                     'ROW_DIFFS_FOUND', 'TOO_MANY_CHUNK_DIFFS',"
             "                     'CHUNKS_WERE_SKIPPED',"
             "                     'ERRORS_IN_CHECKSUM_PROCESS',"
             "                     'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',"
             "checksum_cmd    TEXT,"
             "checksum_stdout TEXT,"
             "checksum_stderr TEXT,"
             "checksum_rc     INT NOT NULL DEFAULT -1,"
             "sync_cmd        TEXT,"
             "sync_stdout     TEXT,"
             "sync_stderr     TEXT,"
             "sync_rc         INT NOT NULL DEFAULT -1,"
             "PRIMARY KEY(id),"
             "UNIQUE KEY(master_instance, instance, db, tbl, reported_at),"
             "INDEX(reported_at),"
             "INDEX(checksum_status, reported_at) )")


# create_checksum_detail_table
#
def create_checksum_detail_table(instance):
    """ Args:
            instance: the master instance for this replica set

        Returns: Nothing.  If this fails, throw an exception.
    """

    try:
        conn = mysql_lib.connect_mysql(instance, 'scriptrw')
        cursor = conn.cursor()
        cursor.execute(TABLE_DEF.format(db=mysql_lib.METADATA_DB, tbl=CHECKSUM_TBL))
        cursor.close()
        conn.close()
    except Exception as e:
        raise Exception("Failed to create checksum detail "
                        "table: {e}".format(e=e))


# parse_checksum_row
#
def parse_checksum_row(row):
    """ Args:
            row: a line of text from pt-table-checksum

        Returns: An array of elements, if the regex matches
            [ts, errors, diffs, rows, chunks, chunks_skipped,
             elapsed_time, db, tbl]

        Ex: [ '08-30T06:25:33', '0', '0', '28598', '60', '0', '0.547',
              'pbdata04159', 'userstats' ]

        If the regex doesn't match, return nothing.
    """

    p = re.compile(''.join("^(\d+-\d+T\d+:\d+:\d+)\s+(\d+)\s+(\d+)\s+"
                           "(\d+)\s+(\d+)\s+(\d+)\s+(\d+\.\d+)\s+"
                           "(.+?)\.(.+)$"))
    m = p.match(row)
    if m:
        return m.groups()


# parse_sync_row
#
def parse_sync_row(row):
    """ Args:
            row: a line of text from pt-table-sync

        Returns:
            diff_count: the number of diffs found

        Ex: 5L

        If the pattern doesn't match (or no diffs) found,
        return 0.
    """

    fields = row.split()
    diff_count = 0
    if len(fields) == 10 and fields[0] == "#":
        try:
            delete_count = fields[1]
            replace_count = fields[2]
            insert_count = fields[3]
            update_count = fields[4]

            diff_count = int(delete_count) + int(replace_count) +\
                int(insert_count) + int(update_count)

        except ValueError, TypeError:
            pass

    return diff_count


# MAIN
def main():
    description = ("MySQL checksum wrapper\n\n"
                   "Wrapper of pt-table-checksum and pt-table-sync.\n"
                   "Defaults to checksumming 1/{k}th of databases on instance.\n"
                   "If diffs are found, use pt-table-sync to measure actual "
                   "divergence,\nbut only if the number of diffs is between "
                   "--min_diffs and --max_diffs.").format(k=DB_CHECK_FRACTION)

    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-i',
                        '--instance',
                        help='Instance to act on if other than localhost:3306',
                        default=''.join((socket.getfqdn(),
                                         ':3306')))
    parser.add_argument('-a',
                        '--all',
                        help='Checksums all dbs rather than the default',
                        action='store_true',
                        default=False)
    parser.add_argument('-d',
                        '--dbs',
                        help=("Comma separated list of db's to check rather "
                              "than the default"),
                        default=False)
    parser.add_argument('-q',
                        '--quiet',
                        help=("Do not print output to stdout"),
                        action='store_true',
                        default=False)
    parser.add_argument('-m',
                        '--min_diffs',
                        help=("Do per-row check if chunk diff count is at "
                              "least this value"),
                        dest='min_diffs',
                        default=MIN_DIFFS)
    parser.add_argument('-M',
                        '--max_diffs',
                        help=("Do not do per-row check if chunk diff count "
                              "is greater than this value"),
                        dest='max_diffs',
                        default=MAX_DIFFS)
    parser.add_argument('-C',
                        '--no_create_table',
                        help=("If test.checksum_detail is missing, do "
                              "not try to create it."),
                        dest='create_table',
                        action='store_false',
                        default=True)
    parser.add_argument('-v',
                        '--verbose',
                        help=("Store raw output from PT tools in the DB?"),
                        action='store_true',
                        default=False)
    parser.add_argument('-c',
                        '--check_fraction',
                        help=('Check this fraction of databases.'),
                        default=DB_CHECK_FRACTION)

    args = parser.parse_args()
    instance = host_utils.HostAddr(args.instance)
    zk = host_utils.MysqlZookeeper()

    if instance not in \
            zk.get_all_mysql_instances_by_type(host_utils.REPLICA_ROLE_MASTER):
        raise Exception("Instance is not a master in ZK")

    # If enabled, try to create the table that holds the checksum info.
    # If not enabled, make sure that the table exists.
    if not mysql_lib.does_table_exist(instance, mysql_lib.METADATA_DB, CHECKSUM_TBL):
        if args.create_table:
            create_checksum_detail_table(instance)
        else:
            raise Exception("Checksum table not found.  Unable to continue."
                            "Consider not using the -C option or create it "
                            "yourself.")

    # Determine what replica set we belong to and get a list of slaves.
    replica_set = zk.get_replica_set_from_instance(instance)[0]
    slaves = set()
    for rtype in host_utils.REPLICA_ROLE_SLAVE, host_utils.REPLICA_ROLE_DR_SLAVE:
        s = zk.get_mysql_instance_from_replica_set(replica_set, rtype)
        if s:
            slaves.add(s)

    if len(slaves) == 0:
        log.info("This server has no slaves.  Nothing to do.")
        sys.exit(0)

    # before we even start this, make sure replication is OK.
    for slave in slaves:
        mysql_lib.assert_replication_sanity(slave)

    if args.dbs:
        db_to_check = set(args.dbs.split(','))
    else:
        dbs = mysql_lib.get_dbs(instance)

        if args.all:
            db_to_check = dbs
        else:
            # default behaviour, check a given DB every N days based on
            # day of year.  minimizes month-boundary issues.
            db_to_check = set()
            check_modulus = int(time.strftime("%j")) % int(args.check_fraction)
            counter = 0
            for db in dbs:
                modulus = counter % int(args.check_fraction)
                if modulus == check_modulus:
                    db_to_check.add(db)
                counter = counter + 1

    # Iterate through the list of DBs and check one table at a time.
    # We do it this way to ensure more coverage in case pt-table-checksum
    # loses its DB connection and errors out before completing a full scan
    # of a given database.
    #
    for db in db_to_check:
        tables_to_check = mysql_lib.get_tables(instance, db, skip_views=True)
        for tbl in tables_to_check:
            c_cmd, c_out, c_err, c_ret = checksum_tbl(instance, db, tbl)
            if not args.quiet:
                log.info("Checksum command executed was:\n{cmd}".format(cmd=c_cmd))
                log.info("Standard out:\n{out}".format(out=c_out))
                log.info("Standard error:\n{err}".format(err=c_err))
                log.info("Return code: {ret}".format(ret=c_ret))

            # parse each line of STDOUT (there should only be one with
            # actual data).  We only care about errors, rows, chunks, and
            # skipped, since we'll need to figure out diffs separately for
            # each slave box.
            for line in c_out.split("\n"):
                results = parse_checksum_row(line)
                if results:
                    chunk_errors = int(results[1])
                    row_count = int(results[3])
                    chunk_count = int(results[4])
                    chunk_skips = int(results[5])

                    for slave in slaves:
                        rows_checked = 'NO'
                        sync_cmd = ""
                        sync_out = ""
                        sync_err = ""
                        sync_ret = -1
                        row_diffs = 0

                        elapsed_time_ms,\
                            chunk_diffs = check_one_replica(slave,
                                                            db, tbl)

                        # if we skipped some chunks or there were errors,
                        # this means we can't have complete information about the
                        # state of the replica. in the case of a hard error,
                        # we'll just stop.  in the case of a skipped chunk, we will
                        # treat it as a different chunk for purposes of deciding
                        # whether or not to do a more detailed analysis.
                        #
                        checkable_chunks = chunk_skips + chunk_diffs

                        if chunk_errors > 0:
                            checksum_status = 'ERRORS_IN_CHECKSUM_PROCESS'
                        elif checkable_chunks == 0:
                            checksum_status = 'GOOD'
                        else:
                            if checkable_chunks > int(args.max_diffs):
                                # too many chunk diffs, don't bother checking
                                # further.  not good.
                                checksum_status = 'TOO_MANY_CHUNK_DIFFS'
                            elif checkable_chunks < int(args.min_diffs):
                                # some diffs, but not enough that we care.
                                checksum_status = 'CHUNK_DIFFS_FOUND_BUT_OK'
                            else:
                                start_time = int(time.time()*1000)
                                rows_checked = 'YES'

                                # set the proper status - did we do a sync-based check
                                # because of explicit diffs or because of skipped chunks?
                                if chunk_diffs > 0:
                                    checksum_status = 'ROW_DIFFS_FOUND'
                                else:
                                    checksum_status = 'CHUNKS_WERE_SKIPPED'

                                sync_cmd, sync_out, sync_err, sync_ret, \
                                    row_diffs = checksum_tbl_via_sync(slave,
                                                                      db,
                                                                      tbl)

                                # Add in the time it took to do the sync.
                                elapsed_time_ms += int(time.time()*1000) - start_time

                                if not args.quiet:
                                    log.info("Sync command executed was:\n{cmd} ".format(cmd=sync_cmd))
                                    log.info("Standard out:\n {out}".format(out=sync_out))
                                    log.info("Standard error:\n {err}".format(err=sync_err))
                                    log.info("Return code: {ret}".format(ret=sync_ret))
                                    log.info("Row diffs found: {cnt}".format(cnt=row_diffs))

                        # Checksum process is complete, store the results.
                        #
                        data = {'instance': slave,
                                'master_instance': instance,
                                'db': db,
                                'tbl': tbl,
                                'elapsed_time_ms': elapsed_time_ms,
                                'chunk_count': chunk_count,
                                'chunk_errors': chunk_errors,
                                'chunk_diffs': chunk_diffs,
                                'chunk_skips': chunk_skips,
                                'row_count': row_count,
                                'row_diffs': row_diffs,
                                'rows_checked': rows_checked,
                                'checksum_status': checksum_status,
                                'checksum_cmd': None,
                                'checksum_stdout': None,
                                'checksum_stderr': None,
                                'checksum_rc': c_ret,
                                'sync_cmd': None,
                                'sync_stdout': None,
                                'sync_stderr': None,
                                'sync_rc': sync_ret}

                        if args.verbose:
                            data.update({'checksum_cmd': c_cmd,
                                         'checksum_stdout': c_out,
                                         'checksum_stderr': c_err,
                                         'sync_cmd': sync_cmd,
                                         'sync_stdout': sync_out,
                                         'sync_stderr': sync_err,
                                         'sync_rc': sync_ret})

                        write_checksum_status(instance, data)


# write_checksum_status
#
def write_checksum_status(instance, data):
    """ Args:
            instance: Host info for the master that we'll connect to.
            data: A dictionary containing the row to insert.  See
                  the table definition at the top of the script for info.

        Returns: Nothing
    """
    try:
        conn = mysql_lib.connect_mysql(instance, 'scriptrw')
        cursor = conn.cursor()
        sql = ("INSERT INTO test.checksum_detail SET "
               "reported_at=NOW(), "
               "instance=%(instance)s, "
               "master_instance=%(master_instance)s, "
               "db=%(db)s, tbl=%(tbl)s, "
               "elapsed_time_ms=%(elapsed_time_ms)s, "
               "chunk_count=%(chunk_count)s, "
               "chunk_errors=%(chunk_errors)s, "
               "chunk_diffs=%(chunk_diffs)s, "
               "chunk_skips=%(chunk_skips)s, "
               "row_count=%(row_count)s, "
               "row_diffs=%(row_diffs)s, "
               "rows_checked=%(rows_checked)s, "
               "checksum_status=%(checksum_status)s, "
               "checksum_cmd=%(checksum_cmd)s, "
               "checksum_stdout=%(checksum_stdout)s, "
               "checksum_stderr=%(checksum_stderr)s, "
               "checksum_rc=%(checksum_rc)s, "
               "sync_cmd=%(sync_cmd)s, "
               "sync_stdout=%(sync_stdout)s, "
               "sync_stderr=%(sync_stderr)s, "
               "sync_rc=%(sync_rc)s")
        cursor.execute(sql, data)
    except Exception as e:
        log.error("Unable to write to the database: {e}".format(s=sql, e=e))
    finally:
        conn.commit()
        conn.close()


# check_one_replica
#
def check_one_replica(slave_instance, db, tbl):
    diff_count = -1
    elapsed_time_ms = -1

    try:
        conn = mysql_lib.connect_mysql(slave_instance, 'scriptro')
        cursor = conn.cursor()

        # first, count the diffs
        sql = ("SELECT COUNT(*) AS diffs FROM test.checksum "
               "WHERE (master_cnt <> this_cnt "
               "OR master_crc <> this_crc "
               "OR ISNULL(master_crc) <> ISNULL(this_crc)) "
               "AND (db=%(db)s AND tbl=%(tbl)s)")
        cursor.execute(sql, {'db': db, 'tbl': tbl})
        row = cursor.fetchone()
        if row is not None:
            diff_count = row['diffs']

        # second, sum up the elapsed time.
        sql = ("SELECT ROUND(SUM(chunk_time)*1000) AS time_ms "
               "FROM test.checksum WHERE db=%(db)s AND tbl=%(tbl)s")
        cursor.execute(sql, {'db': db, 'tbl': tbl})
        row = cursor.fetchone()
        if row is not None:
            elapsed_time_ms = row['time_ms']
        cursor.close()
        conn.close()
    except Exception as e:
        raise Exception("An error occurred polling the "
                        "replica: {e}".format(e=e))

    return elapsed_time_ms, diff_count


# checksum_tbl
#
def checksum_tbl(instance, db, tbl):
    """ Args:
            instance: the master instance to run against
            db: the database to checksum
            tbl: the table within the database to checksum

        Returns:
            cmd: the command line(s) executed
            out: any output written to STDOUT
            err: any output written to STDERR
            ret: the return code of the checksum process
    """

    username, password = mysql_lib.get_mysql_user_for_role('ptchecksum')
    cmd = (' '.join(('/usr/bin/pt-table-checksum',
                     CHECKSUM_DEFAULTS,
                     '--tables={db}.{tbl}',
                     '--user={username}',
                     '--password={password}',
                     '--host={host}',
                     '--port={port}')).format(tbl=tbl,
                                              db=db,
                                              username=username,
                                              password=password,
                                              host=instance.hostname,
                                              port=instance.port))

    out, err, ret = host_utils.shell_exec(cmd)
    return cmd.replace(password, 'REDACTED'), out, err, ret


# Run pt-table-sync in read-only (print, verbose) mode to find the
# actual number of rows which differ between master and slave.
#
def checksum_tbl_via_sync(instance, db, tbl):
    username, password = mysql_lib.get_mysql_user_for_role('ptchecksum')
    cmd = (' '.join(('/usr/bin/pt-table-sync',
                     CHECKSUM_SYNC_DEFAULTS,
                     '--tables={db}.{tbl}',
                     '--user={username}',
                     '--password={password}',
                     'h={host},P={port}')).format(db=db,
                                                  tbl=tbl,
                                                  username=username,
                                                  password=password,
                                                  host=instance.hostname,
                                                  port=instance.port))

    out, err, ret = host_utils.shell_exec(cmd)

    diff_count = 0
    for line in out.split("\n"):
        diff_count += parse_sync_row(line)

    # strip out the password in case we are storing it in the DB.
    return cmd.replace(password, 'REDACTED'), out, err, ret, diff_count


if __name__ == "__main__":
    log = environment_specific.setup_logging_defaults(__name__)
    main()
