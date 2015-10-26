#!/usr/bin/env python
import argparse
import hashlib
import sys
import difflib
from lib import host_utils
from lib import mysql_lib

MODSHARDDB_PREFIX = 'moddb'
SHARDDB_PREFIX = 'db'


def main():
    parser = argparse.ArgumentParser(description='MySQL schema verifier')
    parser.add_argument('instance_type',
                        help='Type of MySQL instance to verify',
                        choices=('sharddb',
                                 'modsharddb'))
    parser.add_argument('table',
                        help='Table to check',)
    parser.add_argument('seed_instance',
                        help=('Which host from which to fetch a table '
                              ' definition. (format hostname[:port])'),)
    parser.add_argument('seed_db',
                        help=('Which db on --seed_instance from which to fetch'
                              ' a table definition. (ex pbdata012345)'))
    args = parser.parse_args()
    if args.instance_type == 'sharddb':
        zk_prefix = SHARDDB_PREFIX
    elif args.instance_type == 'modsharddb':
        zk_prefix = MODSHARDDB_PREFIX
    seed_instance = host_utils.HostAddr(args.seed_instance)
    seed_conn = mysql_lib.connect_mysql(seed_instance)
    desired = mysql_lib.show_create_table(seed_conn, args.seed_db, args.table)
    tbl_hash = hashlib.md5(desired).hexdigest()
    print ("Desired table definition:\n{desired}").format(desired=desired)
    incorrect = check_schema(zk_prefix, args.table, tbl_hash)
    if len(incorrect) == 0:
        print "It appears that all schema is synced"
        sys.exit(0)

    d = difflib.Differ()
    for problem in incorrect.iteritems():
        represenative = list(problem[1])[0].split(' ')
        hostaddr = host_utils.HostAddr(represenative[0])
        conn = mysql_lib.connect_mysql(hostaddr)
        create = mysql_lib.show_create_table(conn,
                                             represenative[1],
                                             args.table)
        diff = d.compare(desired.splitlines(), create.splitlines())
        print 'The following difference has been found:'
        print '\n'.join(diff)
        print "It is present on the following db's:"
        print '\n'.join(list(problem[1]))
    sys.exit(1)


def check_schema(zk_prefix, tablename, tbl_hash):
    """Verify that a table across an entire tier has the expected schema

    Args:
    zk_prefix - The prefix of the key in the DS KZ node
    table - the name of the table to verify
    tbl_hash - the md5sum of the desired CREATE TABLE for the table

    Returns:
    A dictionary with keys that are the hash of the CREATE TABLE statement
    and the values are sets of hostname:port followed by a space and then the
    db one which the incorrect schema was found.
    """
    incorrect = dict()
    zk = host_utils.MysqlZookeeper()
    config = zk.get_ds_mysql_config()
    for db in config.iteritems():
        if db[0].startswith(zk_prefix):
            master = host_utils.HostAddr(''.join((db[1]['master']['host'],
                                                  ':',
                                                  str(db[1]['master']['port']))))
            slave = host_utils.HostAddr(''.join((db[1]['slave']['host'],
                                                 ':',
                                                 str(db[1]['slave']['port']))))
            master_hashes = check_instance_table(master, tablename, tbl_hash)
            slave_hashes = check_instance_table(slave, tablename, tbl_hash)
            for entry in master_hashes.iteritems():
                if entry[0] not in incorrect:
                    incorrect[entry[0]] = set()
                incorrect[entry[0]] = incorrect[entry[0]].union(entry[1])
            for entry in slave_hashes.iteritems():
                if entry[0] not in incorrect:
                    incorrect[entry[0]] = set()
                incorrect[entry[0]] = incorrect[entry[0]].union(entry[1])
    return incorrect


def check_instance_table(hostaddr, table, desired_hash):
    """ Check that a table on a MySQL instance has the expected schema

    Args:
    hostaddr - object describing which mysql instance to connect to
    table - the name of the table to verify
    desired_hash - the md5sum of the desired CREATE TABLE for the table

    Returns:
    A dictionary with keys that are the hash of the CREATE TABLE statement
    and the values are sets of hostname:port followed by a space and then the
    db one which the incorrect schema was found.
    """
    ret = dict()
    conn = mysql_lib.connect_mysql(hostaddr)
    for db in mysql_lib.get_dbs(conn):
        definition = mysql_lib.show_create_table(conn, db, table)
        tbl_hash = hashlib.md5(definition).hexdigest()
        if tbl_hash != desired_hash:
            if tbl_hash not in ret:
                ret[tbl_hash] = set()
            ret[tbl_hash].add(''.join((hostaddr.__str__(),
                                       ' ',
                                       db)))
    return ret

if __name__ == "__main__":
    main()
