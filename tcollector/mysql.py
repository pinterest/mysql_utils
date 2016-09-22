#!/usr/bin/env python
# This file is part of tcollector.
# Copyright (C) 2011 The tcollector Authors.
# Copyright (C) 2013-2016 Pinterest
#
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
""" Collector for MySQL. """

import errno
import glob
import re
import socket
import sys
import time
import traceback

import MySQLdb

sys.path.append("/usr/local/bin/mysql_utils")
from lib import host_utils
from lib import mysql_lib

MASTER_COLLECTION_INTERVAL = 5
SLAVE_COLLECTION_INTERVAL = 60
MYSQL_USER_ROLE = 'monit'

CONNECT_TIMEOUT = 2  # seconds
# How frequently we try to find new databases.
DB_REFRESH_INTERVAL = 60  # seconds
# How to search for mysql socket files
SOCKET_GLOB = '/var/run/mysqld/*.sock'
# Query time histogram percentiles to report on
PERCENTILES = [50, 90, 95, 99, 99.9, 99.9999]

# Terrible evil global var so we are not incorrectly reporting
# the time of a reading OR having to generate this before each
# collector. Terrible.
connection_time = 0


class DB(object):
    """ Represents a MySQL server (as we can monitor more than 1 MySQL). """
    def __init__(self, sockfile, db, cursor, version, port, db_type):
        """ Constructor.

        Args:
            sockfile: Path to the socket file.
            dbname: Name of the database for that socket file.
            db: A MySQLdb connection opened to that socket file.
            cursor: A cursor acquired from that connection.
            version: What version is this MySQL running (from `SELECT VERSION()').
            port: What port the instance is running on.
            db_type: Is the db a master, a slave or undefined? This is based
                     on service discovery
        """
        host = socket.gethostname()
        self.sockfile = sockfile
        self.dbname = ':'.join([host, port])
        self.db = db
        self.replica_set = host[0:host.rindex('-')]
        self.cursor = cursor
        self.version = version
        self.port = port
        self.db_type = db_type
        version = version.split(".")
        try:
            self.major = int(version[0])
            self.medium = int(version[1])
        except (ValueError, IndexError):
            self.major = self.medium = 0

    def __str__(self):
        return "DB(%r, %r, version=%r)" % (self.sockfile, self.dbname,
                                           self.version)

    def __repr__(self):
        return self.__str__()

    def query(self, sql):
        """ Executes the given SQL statement and returns a sequence of rows. """
        assert self.cursor, "%s already closed?" % (self,)
        try:
            self.cursor.execute(sql)
        except MySQLdb.OperationalError, (errcode, msg):
            if errcode != 2006:  # "MySQL server has gone away"
                raise
            self._reconnect()
        return self.cursor.fetchall()

    def close(self):
        """ Closes the connection to this MySQL server. """
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.db:
            self.db.close()
            self.db = None

    def _reconnect(self):
        """ Reconnects to this MySQL server. """
        self.close()
        self.db = mysql_connect(self.sockfile)
        self.cursor = self.db.cursor()


def todict(db, row):
    """ Transforms a row (returned by DB.query) into a dict keyed by column
    names.

        Args:
            db: The DB instance from which this row was obtained.
            row: A row as returned by DB.query
    """
    d = {}
    for i, field in enumerate(db.cursor.description):
        column = field[0].lower()  # Lower-case to normalize field names.
        d[column] = row[i]
    return d


def err(msg):
    print >> sys.stderr, msg


def printmetric(db, metric, value, tags=""):
    global collection_time, last_collection_time
    # Note: We don't run non-3306 ports, so we don't include port
    print ("mysql.{metric} {collection_time} {value} "
           "replica_set={replica_set} db_type={db_type} {tags}"
           "".format(metric=metric,
                     collection_time=int(collection_time),
                     value=value,
                     replica_set=db.replica_set,
                     db_type=db.db_type,
                     tags=tags).strip())


def mysql_connect(sockfile):
    """ Connects to the MySQL server using the specified socket file. """
    user, passwd = mysql_lib.get_mysql_user_for_role(MYSQL_USER_ROLE)
    return MySQLdb.connect(unix_socket=sockfile,
                           connect_timeout=CONNECT_TIMEOUT,
                           user=user, passwd=passwd)


def find_databases(dbs=None):
    """ Returns a map of dbname (string) to DB instances to monitor.

    Args:
        dbs: A map of dbname (string) to DB instances already monitored.
             This map will be modified in place if it's not None.
    """
    if dbs is None:
        dbs = {}
    for sockfile in glob.glob(SOCKET_GLOB):
        try:
            db = mysql_connect(sockfile)
            cursor = db.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            cursor.execute(
                "SELECT VARIABLE_VALUE FROM "
                "INFORMATION_SCHEMA.GLOBAL_VARIABLES WHERE VARIABLE_NAME = "
                "'PORT'")
            port = cursor.fetchone()[0]
            db_type = get_db_type(port)
        except:
            err("Couldn't connect to %s: %s" % (
                sockfile, traceback.format_exc()))
            continue
        dbs[port] = DB(sockfile, db, cursor, version, port, db_type)
    return dbs


def get_db_type(port):
    """ Get status in replica set via service discover

    Args:
        port: the port on localhost

    Returns: 'master', 'slave', 'dr_slave' or 'undef'
    """
    try:
        instance = host_utils.HostAddr(':'.join((socket.gethostname(),
                                                 str(port))))
        zk = host_utils.MysqlZookeeper()
        (_, replica_type) = zk.get_replica_set_from_instance(instance)
        return replica_type
    except:
        return 'undef'


def main(args):
    """ Collects and dumps stats from a MySQL server. """
    if not find_databases():  # Nothing to monitor.
        return 13  # Ask tcollector to not respawn us.
    global collection_time, last_collection_time
    last_db_refresh = 0
    first_run = True
    while True:
        sleep_time = SLAVE_COLLECTION_INTERVAL
        # if we have a master, we use a short interval, default to slave and
        # longer
        # capture last collection time
        if first_run:
            last_collection_time = collection_time = time.time()
            first_run = False
        else:
            last_collection_time = collection_time
            collection_time = time.time()
        if collection_time - last_db_refresh >= DB_REFRESH_INTERVAL:
            dbs = find_databases()
            last_db_refresh = collection_time
        for dbname, db in dbs.iteritems():
            for collectFunction in [collectGlobalStatus,
                                    collectAllResponseTimes,
                                    collectReplicationStatus,
                                    collectProcessList,
                                    collectUserStats, collectTableStats]:
                try:
                    collectFunction(db)
                except IOError as e:
                    if e.errno == errno.EPIPE:
                        # Broken pipe, no one is listening
                        raise
                    else:
                        err(traceback.format_exc())
                except:
                    err(traceback.format_exc())
            if db.db_type == 'master':
                sleep_time = MASTER_COLLECTION_INTERVAL
        sys.stdout.flush()
        time.sleep(sleep_time)


def collectGlobalStatus(db):
    """ Pull in a variety of metrics from a db instance

    Args:
    db - A db object
    """
    collect_innodb = False
    for metric, value in db.query("SHOW GLOBAL STATUS"):
        try:
            if "." in value:
                value = float(value)
            else:
                value = int(value)
        except ValueError:
            continue
        metric = metric.lower()
        printmetric(db, metric, value)
        if metric.startswith("innodb"):
            collect_innodb = True
    if collect_innodb:
        collectInnodbStatus(db)


def collectInnodbStatus(db):
    """ Collects and prints InnoDB stats about the given DB instance. """
    def match(regexp):
        return re.match(regexp, line)
    innodb_status = db.query("SHOW ENGINE INNODB STATUS")[0][2]
    line = None
    for line in innodb_status.split("\n"):
        # SEMAPHORES
        m = match(
            "OS WAIT ARRAY INFO: reservation count (\d+), signal count (\d+)")
        if m:
            printmetric(db, "innodb.oswait_array.reservation_count", m.group(1))
            printmetric(db, "innodb.oswait_array.signal_count", m.group(2))
            continue
        m = match("Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)")
        if m:
            printmetric(db, "innodb.locks.spin_waits", m.group(1),
                        " type=mutex")
            printmetric(db, "innodb.locks.rounds", m.group(2), " type=mutex")
            printmetric(db, "innodb.locks.os_waits", m.group(3), " type=mutex")
            continue
        m = match("RW-shared spins (\d+), OS waits (\d+);"
                  " RW-excl spins (\d+), OS waits (\d+)")
        if m:
            printmetric(db, "innodb.locks.spin_waits", m.group(1),
                        " type=rw-shared")
            printmetric(db, "innodb.locks.os_waits", m.group(2),
                        " type=rw-shared")
            printmetric(db, "innodb.locks.spin_waits", m.group(3),
                        " type=rw-exclusive")
            printmetric(db, "innodb.locks.os_waits", m.group(4),
                        " type=rw-exclusive")
            continue
        # INSERT BUFFER AND ADAPTIVE HASH INDEX
        # TODO(tsuna): According to the code in ibuf0ibuf.c, this line and
        # the following one can appear multiple times.  I've never seen this.
        # If that happens, we need to aggregate the values here instead of
        # printing them directly.
        m = match(
            "Ibuf: size (\d+), free list len (\d+), seg size (\d+), "
            "(\d+) merges")
        if m:
            printmetric(db, "innodb.ibuf.size", m.group(1))
            printmetric(db, "innodb.ibuf.free_list_len", m.group(2))
            printmetric(db, "innodb.ibuf.seg_size", m.group(3))
            printmetric(db, "innodb.ibuf.merges", m.group(4))
            continue
        # Multi line metric for insert buffer
        if match("merged operations:"):
            merged = True
            continue
        else:
            merged = False
        # old syntax was 2431891 inserts, 2672643 merged recs, 1059730 merges
        # new syntax is ' insert 426, delete mark 1, delete 0'
        m = match("insert (\d+), delete mark (\d+), delete (\d+)")
        if merged and m:
            printmetric(db, "innodb.ibuf.inserts", m.group(1))
            printmetric(db, "innodb.ibuf.delete_mark", m.group(2))
            printmetric(db, "innodb.ibuf.delete", m.group(3))
            continue
        # ROW OPERATIONS
        m = match("\d+ queries inside InnoDB, (\d+) queries in queue")
        if m:
            printmetric(db, "innodb.queries_queued", m.group(1))
            continue
        m = match("(\d+) read views open inside InnoDB")
        if m:
            printmetric(db, "innodb.opened_read_views", m.group(1))
            continue
        # TRANSACTION
        m = match("History list length (\d+)")
        if m:
            printmetric(db, "innodb.history_list_length", m.group(1))
            continue


def collectAllResponseTimes(db):
    """ Collect the various response time metrics """
    collectResponseTime(db, '')
    collectResponseTime(db, 'read')
    collectResponseTime(db, 'write')
    flushResponseTime(db)


def collectResponseTime(db, response_type):
    """ Collect and print Query Response Time

    Args:
    db: a queryable MySQL server object
    response_type: If not blank, all querys, otherwise read or write queries
    """
    if response_type == '':
        tbl = 'INFORMATION_SCHEMA.QUERY_RESPONSE_TIME'
        metric = 'response_time'
    elif response_type == 'read':
        tbl = 'INFORMATION_SCHEMA.QUERY_RESPONSE_TIME_READ'
        metric = 'response_time_read'
    elif response_type == 'write':
        tbl = 'INFORMATION_SCHEMA.QUERY_RESPONSE_TIME_WRITE'
        metric = 'response_time_write'
    else:
        return
    try:
        # Query histogram response time
        sql = 'SELECT * from {tbl}'.format(tbl=tbl)
        response_time = db.query(sql)
    except Exception:
        return
    # Parse results
    histogram = parse_histogram_data(response_time)
    for p in PERCENTILES:
        try:
            response_time_ms = float(get_percentile(histogram, p))
            printmetric(db, '.'.join((metric, str(p))),
                        str(response_time_ms))
        except Exception:
            pass


def parse_histogram_data(result):
    """" Parse a histogram from QRT

    Args:
    result -  A result set from querying the QRT

    Returns:
    A dictionary of a historam
    """
    histogram = {"bucket": {"index": 0, "values": []},
                 "count": {"index": 1, "values": []},
                 "total": {"index": 2, "values": []}}
    for r in result:
        for v in histogram:
            try:
                histogram[v]["values"].append(float(r[histogram[v]["index"]]))
            except ValueError:
                histogram[v]["values"].append(r[histogram[v]["index"]])
    return histogram


def get_percentile(histo, percentile=90):
    """ Walks the histogram from low to high, printing
    out the average value of the bucket holding the percentile
    value. """
    total_points = sum(histo['count']['values'])
    percentile_point = total_points * percentile * .01
    current_count = 0
    for i, value in enumerate(histo['count']['values']):
        current_count += value
        if current_count >= percentile_point:
            return histo['total']['values'][i] / histo['count']['values'][
                i] * 1000


def flushResponseTime(db):
    """ Reset response time metrics """
    if db.major > 5 or (db.major == 5 and db.medium >= 6):
        db.query('SET GLOBAL query_response_time_flush = ON')
    else:
        db.query('FLUSH NO_WRITE_TO_BINLOG QUERY_RESPONSE_TIME;')


def collectReplicationStatus(db):
    """ Collect replication stats using mysql_lib.calc_slave_lag """
    instance = host_utils.HostAddr(':'.join((socket.gethostname(),
                                             db.port)))
    ret = mysql_lib.calc_slave_lag(instance)
    printmetric(db, "slave.seconds_behind_master", ret['sbm'])
    printmetric(db, "slave.io_bytes_behind", ret["io_bytes"])
    printmetric(db, "slave.sql_bytes_behind", ret["sql_bytes"])
    printmetric(db, "slave.thread_io_running",
                int('yes' == ret['ss']['Slave_IO_Running'].lower()))
    printmetric(db, "slave.thread_sql_running",
                int('yes' == ret['ss']['Slave_SQL_Running'].lower()))


def collectProcessList(db):
    """ Collect metrics on the processlist

    Args:
    db - a db object
    """
    sql = ("SELECT SUBSTRING_INDEX(host, ':', 1) as 'source', "
           "       lower(COMMAND) as 'command', "
           "       count(*) as 'cnt',"
           "   lower(USER) as 'user'"
           "FROM information_schema.processlist "
           "GROUP BY processlist.USER, SUBSTRING_INDEX(host, ':', 1),"
           " processlist.COMMAND ")
    for row in db.query(sql):
        row_dict = todict(db, row)
        command = row_dict['command'].replace(' ', '')
        if not row_dict['source']:
            row_dict['source'] = 'unknown'
        printmetric(db, "processlist", row_dict['cnt'],
                    "command={cmd} source={source} user={user}"
                    "".format(cmd=command,
                              source=row_dict['source'],
                              user=row_dict['user']))


def collectUserStats(db):
    """ Collect user stats

    Args:
    db - a db object
    """
    global collection_time, last_collection_time
    user_stats = db.query('SELECT * FROM '
                          'INFORMATION_SCHEMA.USER_STATISTICS')
    for row in user_stats:
        row_dict = todict(db, row)
        for metric in row_dict:
            user = re.sub('[^a-zA-Z0-9_]', '', row_dict['user'])
            if metric == 'USER' or metric == 'USER_NAME':
                continue
            try:
                metric_value = int(row_dict[metric])
                metric_value /= int(collection_time) - int(
                    last_collection_time)
                printmetric(db, "userstats.{}".format(metric), metric_value,
                            "user={}".format(user))
            except:
                return
    db.query("FLUSH NO_WRITE_TO_BINLOG USER_STATISTICS")


def collectTableStats(db):
    """ Collect table stats

    Args:
    db - a db object
    """
    # First we are going to pull stats aggregated by schema
    # and namespace, if applicable
    global collection_time, last_collection_time
    instance = host_utils.HostAddr(':'.join((socket.gethostname(),
                                             db.port)))
    namespace_dbs_map = dict()
    non_namespace_dbs = set()
    for schema in mysql_lib.get_dbs(instance):
        namespace = get_namespace_from_schema(schema)
        if namespace:
            if namespace not in namespace_dbs_map:
                namespace_dbs_map[namespace] = set()
            namespace_dbs_map[namespace].add(schema)
        else:
            non_namespace_dbs.add(schema)
    for namespace in namespace_dbs_map:
        for row in get_tablestats(db, namespace_dbs_map[namespace]):
            printmetrics_tablestat(db, row, namespace)
    if non_namespace_dbs:
        for row in get_tablestats(db, non_namespace_dbs):
            printmetrics_tablestat(db, row)
    # next we want table stats aggregated by table and namespace.
    for namespace in namespace_dbs_map:
        for row in get_schemastats(db, namespace_dbs_map[namespace]):
            printmetrics_schemastats(db, row, namespace)
    if non_namespace_dbs:
        for row in get_schemastats(db, non_namespace_dbs):
            printmetrics_schemastats(db, row)
    db.query("FLUSH NO_WRITE_TO_BINLOG TABLE_STATISTICS")


def get_tablestats(db, schemas):
    """ Query I_S for tablestats stats

    Args:
    schemas - A set of schemas

    Returns:
    A list of tuples (TABLE_NAME, ROWS_READ, ROWS_CHANGED)
    """
    return db.query("SELECT TABLE_NAME, ROWS_READ, ROWS_CHANGED "
                    "FROM INFORMATION_SCHEMA.TABLE_STATISTICS "
                    "WHERE TABLE_SCHEMA IN ('{placeholder}') "
                    "GROUP BY TABLE_NAME"
                    "".format(placeholder="', '".join(schemas)))


def printmetrics_tablestat(db, row, namespace=None):
    """ Print tablestats statistics

    Args:
    db - A db object
    row - A row returned from a db object
    namespace - The namespace of the the tables
    """
    row_dict = todict(db, row)
    if namespace:
        namespace_placeholder = '='.join(['namespace', namespace])
    else:
        namespace_placeholder = ''
    try:
        duration = int(collection_time) - int(last_collection_time)
        rows_changed = row_dict['rows_changed'] / duration
        rows_read = row_dict['rows_read'] / duration
        printmetric(db, "tablestats.rows_changed", rows_changed,
                    "table={table} {namespace_placeholder}"
                    "".format(table=row_dict['table_name'],
                              namespace_placeholder=namespace_placeholder))
        printmetric(db, "tablestats.rows_read", rows_read,
                    "table={table} {namespace_placeholder}"
                    "".format(table=row_dict['table_name'],
                              namespace_placeholder=namespace_placeholder))
    except:
        return


def get_schemastats(db, schemas):
    """ Query I_S for schema stats

    Args:
    schemas - A set of schemas

    Returns:
    A list of tuples (TABLE_SCHEMA, ROWS_READ, ROWS_CHANGED)
    """
    return db.query("SELECT TABLE_SCHEMA, ROWS_READ, ROWS_CHANGED "
                    "FROM INFORMATION_SCHEMA.TABLE_STATISTICS "
                    "WHERE TABLE_SCHEMA IN ('{placeholder}') "
                    "GROUP BY TABLE_SCHEMA"
                    "".format(placeholder="', '".join(schemas)))


def printmetrics_schemastats(db, row, namespace=None):
    """ Print schema statistics

    Args:
    db - A db object
    row - A row returned from a db object
    namespace - The namespace of the the schemas
    """
    row_dict = todict(db, row)
    if namespace:
        namespace_placeholder = '='.join(['namespace', namespace])
    else:
        namespace_placeholder = ''
    try:
        duration = int(collection_time) - int(last_collection_time)
        rows_changed = row_dict['rows_changed'] / duration
        rows_read = row_dict['rows_read'] / duration
        printmetric(db, "schemastats.rows_changed", rows_changed,
                    "schema={schema} {namespace_placeholder}"
                    "".format(schema=row_dict['table_schema'],
                              namespace_placeholder=namespace_placeholder))
        printmetric(db, "schemastats.rows_read", rows_read,
                    "schema={schema} {namespace_placeholder}"
                    "".format(schema=row_dict['table_schema'],
                              namespace_placeholder=namespace_placeholder))
    except:
        return


def get_namespace_from_schema(schema):
    """ Get the namespace (if applicable) from a schema name

    Args:
    schema - the schema name

    Returns:
    A namespace name
    """
    # does the schema end in a number, if so it is sharded as far as we care
    if not re.match('.+[0-9]+$', schema):
        return None
    # if the db name ends in a number and has a dash, we throw away the dash
    # and use the first chunk of the hostname
    hostname_preface = socket.gethostname().split('-')[0]
    if '_' in schema:
        return '_'.join([hostname_preface,
                         schema.split('_')[1]])
    else:
        return '_'.join([hostname_preface, 'default'])


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
