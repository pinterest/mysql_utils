#!/usr/bin/env python

""" find_gtid_for_timestamp.py

    Given a timestamp in MySQL standard format (YYYY-MM-DD HH:MM:SS) and
    an instance name, find the first GTID for the aforementioned timestamp
    or the closest one to it.  We do this by finding all the binlogs
    on the server, looking at the timestamp that the binlog was created,
    and then looking for the first one that contains a created-time before
    the timestamp we want.  We then read that binlog and look for the first
    GTID events around $TS - 1 second.
"""
__author__ = 'Ernie Souhrada <esouhrada@pinterest.com>'

import argparse
import datetime as dt
import logging
import subprocess
import sys

from lib import mysql_lib
from lib import host_utils
from lib import environment_specific

DESCRIPTION = "Timestamp to GTID calculator utility"
MYSQL_DT_FORMAT = '%Y-%m-%d %H:%M:%S'
BINLOG_DT_FORMAT = '%y%m%d %H:%M:%S'
DELTA_ONE_SECOND = dt.timedelta(seconds=1)

log = logging.getLogger(__name__)


def pipe_wait(procs):
    """ Given an array of Popen objects returned by the
        pipe method, wait for all processes to terminate
        and return the text from the final one.
    """
    last = procs.pop(-1)
    result, _ = last.communicate()
    while procs:
        last = procs.pop()
        last.wait()
    return result


def pipe_runner(args):
    """ chain a bunch of processes together in a pipeline
        and return the array of procs created.
    """
    # set STDOUT for all processes
    for i in args:
        i['stdout'] = subprocess.PIPE

    procs = [subprocess.Popen(**args[0])]

    # set STDIN for all but the first process
    # and connect them together.
    for i in xrange(1, len(args)):
        args[i]['stdin'] = procs[i-1].stdout
        procs.append(subprocess.Popen(**args[i]))
        procs[i-1].stdout.close()

    return procs


def get_binlog_start(binlog_file, instance, username, password):
    """ Read the first event in a binlog so that we can extract
        the timestamp.  This should help us skip over binlogs
        that can't possibly contain the GTID we're looking for.

    Args:
        binlog_file: the binlog to examine
        instance: a hostaddr object
        username: the user to connect as
        password: the password to connect as
    Returns:
        A timestamp in MySQL-friendly format, or we throw
        an exception if something doesn't work.
    """

    # first, make sure we have the proper log positions.
    # most likely, this is always 4 and 120, and that's what
    # we'll default to, but it could be different with
    # different versions of MySQL.
    #
    start_pos = 4
    stop_pos = 120
    try:
        conn = mysql_lib.connect_mysql(instance)
        cursor = conn.cursor()
        sql = 'SHOW BINLOG EVENTS in %(binlog)s LIMIT 0,1'
        cursor.execute(sql, {'binlog': binlog_file})
        row = cursor.fetchone()

        start_pos = row['Pos']
        stop_pos = row['End_log_pos']
    except Exception as e:
        log.error('Unable to retrieve binlog positions: {}'.format(e))
        raise

    binlog_cmd = ['/usr/bin/mysqlbinlog',
        '--read-from-remote-server',
        '--host={}'.format(instance.hostname),
        '--user={}'.format(username),
        '--password={}'.format(password),
        '--start-position="{}"'.format(start_pos),
        '--stop-position="{}"'.format(stop_pos),
        binlog_file, '2>/dev/null']

    pipeline = list()
    pipeline.append(dict(args=' '.join(binlog_cmd), shell=True))
    pipeline.append(dict(args='/bin/egrep created', shell=True))
    procs = pipe_runner(pipeline)
    results = pipe_wait(procs)

    try:
        (date, time) = results.split()[-2:]
        timestamp = dt.datetime.strptime('{} {}'.format(date, time),
                                         BINLOG_DT_FORMAT)
        return timestamp
    except Exception as e:
        log.error("Invalid value/format for binlog create time: {}".format(e))
        raise


def check_one_binlog(timestamp, binlog_file, instance, username, password):
    """ See if there are any GTIDs in the supplied binlog
        that match the timestamp we're looking for.

    Args:
        timestamp: the timestamp to look for
        binlog_file: the binlog file
        instance: a hostaddr object
        username: the username to connect as
        password: the password to connect as
    Returns:
        A GTID if we've found one matching our timestamp
        or None if we haven't.
    """
    ts_minus_one = (timestamp - DELTA_ONE_SECOND).strftime(MYSQL_DT_FORMAT)
    ts_plus_one = (timestamp + DELTA_ONE_SECOND).strftime(MYSQL_DT_FORMAT)

    binlog_cmd = ['/usr/bin/mysqlbinlog',
        '--read-from-remote-master=BINLOG-DUMP-GTIDS',
        '--host={}'.format(instance.hostname),
        '--user={}'.format(username),
        '--password={}'.format(password),
        '--start-datetime="{}"'.format(ts_minus_one),
        '--stop-datetime="{}"'.format(ts_plus_one),
        binlog_file, '2>/dev/null']

    log.debug(' '.join(binlog_cmd))

    pipeline = list()
    pipeline.append(dict(args=' '.join(binlog_cmd), shell=True))
    pipeline.append(dict(args='/bin/egrep -A1 GTID.*commit', shell=True))
    pipeline.append(dict(args='/bin/egrep GTID_NEXT', shell=True))
    pipeline.append(dict(args='head -1', shell=True))

    procs = pipe_runner(pipeline)
    results = pipe_wait(procs)
    if results:
        return results.split("'")[1]


def find_gtid_for_timestamp(instance, timestamp):
    """ Find the GTID for the supplied timestamp on the specified
        instance. 

    Args:
        instance: a HostAddr object
        timestamp: the timestamp to search for
    Returns:
        If the instance doesn't support GTID, return None.
        If no GTID was found in the binlogs for the supplied
        timestamp, return a blank string.
        Otherwise, return a GTID.
    """
    vars = mysql_lib.get_global_variables(instance)

    # we are not generating GTIDs / no GTID support
    if vars['gtid_mode'] == 'OFF' or vars['gtid_deployment_step'] == 'ON':
        log.warning('This replica set does not currently support GTID')
        return None

    # go in reverse order, because odds are that the log we want
    # is closer to the end than the beginning.
    master_logs = list(reversed(mysql_lib.get_master_logs(instance)))

    (username, password) = mysql_lib.get_mysql_user_for_role('replication')
    for binlog in master_logs:
        # if the timestamp we want is prior to the first entry in the
        # binlog, it can't possibly be in there.
        log_start = get_binlog_start(binlog['Log_name'], instance, username,
                                     password)
        if timestamp < log_start:
            log.debug('Skipping binlog {bl} because desired {ts} < '
                      '{ls}'.format(bl=binlog['Log_name'], ts=timestamp,
                                    ls=log_start))
            continue

        # The binlog that we end up checking, if we check one at all,
        # is the first one that could possibly contain our GTID, so
        # if it isn't in this one, we're not going to find anything.
        log.debug('Checking for matching GTID in {}'.format(binlog['Log_name']))
        gtid = check_one_binlog(timestamp, binlog['Log_name'],
                                instance, username, password)
        if gtid:
            return gtid
        else:
            break

    log.warning("No matching GTID was found for that timestamp.")
    return ''


def main():
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('-i',
                        '--instance',
                        help='The instance to query.  This should '
                             'be the master of a replica set, but '
                             'if you supply a non-master, the script '
                             'will query the master anyway.')
    parser.add_argument('timestamp',
                        help='The timestamp to rewind to.  This must '
                             'be in MySQL format: YYYY-MM-DD HH:MM:SS')
    args = parser.parse_args()
    try:
        instance = host_utils.HostAddr(args.instance)
        zk = host_utils.MysqlZookeeper()
        rt = zk.get_replica_type_from_instance(instance)
        if rt != host_utils.REPLICA_ROLE_MASTER:
            instance = zk.get_mysql_instance_from_replica_set(
                zk.get_replica_set_from_instance(instance),
                host_utils.REPLICA_ROLE_MASTER)
            log.info('Detected master of {i} as {m}'.format(i=args.instance,
                                                            m=instance))
        timestamp = dt.datetime.strptime(args.timestamp, MYSQL_DT_FORMAT)
    except Exception as e:
        log.error("Error in argument parsing: {}".format(e))

    gtid = find_gtid_for_timestamp(instance, timestamp)
    if gtid:
        print gtid
    else:
        sys.exit(255)


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
