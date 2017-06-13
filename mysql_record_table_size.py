#!/usr/bin/env python

import argparse
import glob
import logging
import os
import re

from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

INNODB_EXTENSION = 'ibd'
TABLE_SIZE_TBL = 'historic_table_size'
TABLE_DEF = ("CREATE TABLE {db}.{tbl} ( "
             "`hostname` varchar(90) NOT NULL DEFAULT '', "
             "`port` int(11) NOT NULL DEFAULT '0', "
             "`db` varchar(64) NOT NULL, "
             "`table_name` varchar(64) NOT NULL, "
             "`partition_name` varchar(64) NOT NULL DEFAULT '', "
             "`reported_at` date NOT NULL, "
             "`size_mb` int(10) unsigned NOT NULL, "
             "PRIMARY KEY (`hostname`,`port`,`db`,`table_name`, `partition_name`, `reported_at`) "
             ") ENGINE=InnoDB DEFAULT CHARSET=latin1")

log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--port',
                        help=('Port on localhost on to record db size. '
                              'Default 3306.'),
                        default='3306')

    args = parser.parse_args()
    log_table_sizes(args.port)


def log_table_sizes(port):
    """ Determine and record the size of tables on a MySQL instance

    Args:
    port - int
    """
    instance = host_utils.HostAddr(':'.join((host_utils.HOSTNAME, port)))
    zk = host_utils.MysqlZookeeper()

    replica_set = zk.get_replica_set_from_instance(instance)
    master = zk.get_mysql_instance_from_replica_set(replica_set,
                                                    host_utils.REPLICA_ROLE_MASTER)
    if not mysql_lib.does_table_exist(master,
                                      mysql_lib.METADATA_DB,
                                      TABLE_SIZE_TBL):
        create_table_size_table(master)

    sizes = get_all_table_sizes(instance)
    conn = mysql_lib.connect_mysql(master, 'dbascript')
    for db in sizes:
        for table in sizes[db]:
            for partition in sizes[db][table]:
                cursor = conn.cursor()
                sql = ('REPLACE INTO {metadata_db}.{tbl} '
                       'SET '
                       'hostname = %(hostname)s, '
                       'port = %(port)s, '
                       'db = %(db)s, '
                       'table_name = %(table)s, '
                       'partition_name = %(partition)s, '
                       'reported_at = CURDATE(), '
                       'size_mb = %(size)s ')
                cursor.execute(sql.format(metadata_db=mysql_lib.METADATA_DB,
                               tbl=TABLE_SIZE_TBL),
                               {'hostname': instance.hostname,
                                'port': instance.port,
                                'db': db,
                                'table': table,
                                'partition': partition,
                                'size': sizes[db][table][partition]})
                conn.commit()
                log.info(cursor._executed)
                cursor.close()


def get_db_size_from_log(instance, db):
    """ Get yesterdays db size for an instance

    Args:
    instance - A hostaddr object
    db - A database that exists on the instance

    Returns: size in MB
    """
    conn = mysql_lib.connect_mysql(instance, 'dbascript')
    cursor = conn.cursor()
    sql = ("SELECT SUM(size_mb) as 'mb', "
           "        COUNT(1) as 'table_count' "
           "FROM  {metadata_db}.{tbl} "
           "WHERE db = %(db)s "
           "    AND reported_at=CURDATE() - INTERVAL 1 DAY "
           "    AND hostname=%(hostname)s and port=%(port)s "
           "GROUP BY db;")
    params = {'hostname': instance.hostname,
              'port': instance.port,
              'db': db}
    cursor.execute(sql.format(metadata_db=mysql_lib.METADATA_DB,
                              tbl=TABLE_SIZE_TBL), params)
    ret = cursor.fetchone()

    expected_tables = mysql_lib.get_tables(instance, db, skip_views=True)
    if ret['table_count'] != len(expected_tables):
        raise Exception('Size data appears to be missing for {db} on {inst}'
                        ''.format(db=db, inst=instance))
    return ret['mb']


def create_table_size_table(instance):
    """ Create the table_size_historic table

    Args:
    a hostAddr object for the master of the replica set
    """
    conn = mysql_lib.connect_mysql(instance, 'dbascript')
    cursor = conn.cursor()
    cursor.execute(TABLE_DEF.format(db=mysql_lib.METADATA_DB,
                   tbl=TABLE_SIZE_TBL))
    cursor.close()
    conn.close()


def get_all_table_sizes(instance):
    """ Get size of all innodb tables
        NOTE: At this point tables should always be innodb
        NOTE2: file per table should always be on.

    Args:
    instance - A hostAddr object
    """
    datadir = host_utils.get_cnf_setting('datadir', instance.port)
    ret = dict()
    for db in mysql_lib.get_dbs(instance):
        ret[db] = dict()
        db_dir = os.path.join(datadir, db)
        for table_path in glob.glob(''.join([db_dir, '/*', INNODB_EXTENSION])):
            (table, partition) = parse_table_file_name(table_path)
            if table not in ret[db]:
                ret[db][table] = dict()
            ret[db][table][partition] = os.stat(table_path).st_size / 1048576

    return ret


def parse_table_file_name(table_path):
    """ Parse a filename into a tablename and partition name

    Args:
    filepath - a file path to a innodb table

    Returns: A tuple whose first element is the table name and second
             element is the partition name or an empty string for
             non-partitioned tables.
    """
    res = re.match(''.join(['([^#.]+)(#P#)?(.+)?\.', INNODB_EXTENSION]),
                   os.path.basename(table_path))
    if res.group(3) is None:
        return (res.group(1), '')
    else:
        return (res.group(1), res.group(3))


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
