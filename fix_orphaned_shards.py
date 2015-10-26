#!/usr/bin/env python
import argparse
import socket
import sys
import find_shard_mismatches
from lib import host_utils
from lib import mysql_lib


DB_PREPEND = 'dropme_'


def main():

    action_desc = """Action description

rename - after checking no recent changes and shard not in zk,
         create a db with the old name appended to 'dropme_'. Then
         copy all tables to the new db
revert_rename - Copy all tables back from a 'dropme_' to their original table
drop - This should be run a few days after a rename. Drop the empty original
       db, and drop the 'dropme_' db.
"""

    parser = argparse.ArgumentParser(description='MySQL shard cleanup utility',
                                     epilog=action_desc,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-i',
                        '--instance',
                        help='Instance to act on if other than localhost:3306',
                        default=''.join((socket.getfqdn(), ':3306')))
    parser.add_argument('-a',
                        '--action',
                        choices=('rename',
                                 'revert_rename',
                                 'drop',),
                        required=True)
    parser.add_argument('-d',
                        '--dbs',
                        help=("Comma seperated list of db's to act upon"),
                        required=True)
    parser.add_argument('-r',
                        '--dry_run',
                        help=("Do not change any state"),
                        default=False,
                        action='store_true')
    parser.add_argument('-v',
                        '--verbose',
                        default=False,
                        action='store_true')

    args = parser.parse_args()
    dbs = set(args.dbs.split(','))
    instance = host_utils.HostAddr(args.instance)

    if args.action == 'rename':
        rename_db_to_drop(instance, dbs, args.verbose, args.dry_run)
    elif args.action == 'revert_rename':
        conn = mysql_lib.connect_mysql(instance)
        for db in dbs:
            mysql_lib.move_db_contents(conn=conn,
                                       old_db=''.join((DB_PREPEND, db)),
                                       new_db=db,
                                       verbose=args.verbose,
                                       dry_run=args.dry_run)
    elif args.action == 'drop':
        drop_db_after_rename(instance, dbs, args.verbose, args.dry_run)


def rename_db_to_drop(instance, dbs, verbose=False, dry_run=False):
    """ Create a new empty db and move the contents of the original db there

    Args:
    instance - a hostaddr object
    dbs -  a set of database names
    verbose - bool, will direct sql to stdout
    dry_run - bool, will make no changes to
    """
    # confirm db is not in zk and not in use
    orphaned, _, _ = find_shard_mismatches.find_shard_mismatches(instance)
    if not orphaned:
        print "Detected no orphans"
        sys.exit(1)

    instance_orphans = orphaned[instance.__str__()]
    unexpected = dbs.difference(instance_orphans)
    if unexpected:
        print ''.join(("Cowardly refusing to act on the following dbs: ",
                       ','.join(unexpected)))
        sys.exit(1)

    # confirm that renames would not be blocked by an existing table
    conn = mysql_lib.connect_mysql(instance)

    cursor = conn.cursor()
    for db in dbs:
        renamed_db = ''.join((DB_PREPEND, db))

        sql = ''.join(("SELECT CONCAT(t2.TABLE_SCHEMA, \n",
                       "              '.', t2.TABLE_NAME) as tbl \n",
                       "FROM information_schema.tables t1 \n",
                       "INNER JOIN information_schema.tables t2 \n",
                       "    USING(TABLE_NAME) \n",
                       "WHERE t1.TABLE_SCHEMA = %(old_db)s AND \n"
                       "      t2.TABLE_SCHEMA = %(new_db)s;"))

        params = {'old_db': db,
                  'new_db': renamed_db}
        cursor = conn.cursor()
        cursor.execute(sql, params)
        dups = cursor.fetchall()

        if dups:
            for dup in dups:
                print "Table rename blocked by {tbl}".format(tbl=dup['tbl'])
            sys.exit(1)

        # We should be safe to create the new db and rename
        if not dry_run:
            mysql_lib.create_db(conn, renamed_db)
        mysql_lib.move_db_contents(conn=conn,
                                   old_db=db,
                                   new_db=renamed_db,
                                   verbose=verbose,
                                   dry_run=dry_run)


def drop_db_after_rename(instance, dbs, verbose, dry_run):
    """ Drop the original empty db and a non-empty rename db

    Args:
    instance - a hostaddr object
    dbs -  a set of database names
    verbose - bool, will direct sql to stdout
    dry_run - bool, will make no changes to
    """

    # confirm db is not in zk and not in use
    orphaned, _, _ = find_shard_mismatches.find_shard_mismatches(instance)
    instance_orphans = orphaned[instance.__str__()]
    unexpected = dbs.difference(instance_orphans)
    if unexpected:
        print ''.join(("Cowardly refusing to act on the following dbs: ",
                       ','.join(unexpected)))
        sys.exit(1)

    # make sure the original db is empty
    conn = mysql_lib.connect_mysql(instance)
    cursor = conn.cursor()
    for db in dbs:
        if mysql_lib.get_tables(conn, db):
            print ''.join(("Cowardly refusing to drop non-empty db:",
                           db))
            sys.exit(1)

    for db in dbs:
        # we should be good to drop the old empty dbs
        raw_sql = 'DROP DATABASE IF EXISTS `{db}`;'
        sql = raw_sql.format(db=db)
        if verbose:
            print sql
        if not dry_run:
            cursor.execute(sql)

        # and we should be ok to drop the non-empty 'dropme_' prepended db
        renamed_db = ''.join((DB_PREPEND, db))
        sql = raw_sql.format(db=renamed_db)
        if verbose:
            print sql
        if not dry_run:
            cursor.execute(sql)


if __name__ == "__main__":
    main()
