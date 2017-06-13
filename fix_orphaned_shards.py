#!/usr/bin/env python
import argparse
import logging
import sys

import find_shard_mismatches
from lib import environment_specific
from lib import host_utils
from lib import mysql_lib


DB_PREPEND = 'dropme_'
log = logging.getLogger(__name__)


def main():
    action_desc = """Action description

rename - after checking no recent changes and shard not in zk,
         create a db with the old name appended to 'dropme_'. Then
         copy all tables to the new db
revert_rename - Copy all tables back from a 'dropme_' to their original table
drop - This should be run a few days after a rename. Drop the empty original
       db, and drop the 'dropme_' db.
"""

    parser = argparse.ArgumentParser(
        description='MySQL shard cleanup utility',
        epilog=action_desc,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-i',
                        '--instance',
                        help='Instance to act on if other than localhost:3306',
                        default=':'.join((host_utils.HOSTNAME, '3306')))
    parser.add_argument('-a',
                        '--action',
                        choices=('rename',
                                 'revert_rename',
                                 'drop'),
                        required=True)
    parser.add_argument('-d',
                        '--dbs',
                        help="Comma separated list of dbs to act upon")
    parser.add_argument('--dry_run',
                        help="Do not change any state",
                        default=False,
                        action='store_true')

    args = parser.parse_args()
    dbs = set(args.dbs.split(',')) if args.dbs else None
    instance = host_utils.HostAddr(args.instance)

    if args.action == 'rename':
        rename_db_to_drop(instance, dbs, args.dry_run)
    elif args.action == 'revert_rename':
        for db in dbs:
            mysql_lib.move_db_contents(instance,
                                       old_db=''.join((DB_PREPEND, db)),
                                       new_db=db,
                                       dry_run=args.dry_run)
    elif args.action == 'drop':
        drop_db_after_rename(instance, dbs, args.dry_run)


def rename_db_to_drop(instance, dbs=None, dry_run=False, skip_check=False):
    """ Create a new empty db and move the contents of the original db
        into it

    Args:
        instance - a hostaddr object
        dbs -  a set of database names
        dry_run - bool, will make no changes to anything
        skip_check - Do not verify that db is not in production
    """

    orphaned, _, _ = find_shard_mismatches.find_shard_mismatches(instance)
    if not dbs:
        if instance not in orphaned:
            log.info("No orphaned shards, returning now.")
            return

        dbs = orphaned[instance]
        log.info('Detected orphaned shareds: {}'.format(dbs))

    if not skip_check:
        # confirm db is not in ZK and not in use.
        if not orphaned:
            log.info("No orphans detected, returning now.")
            return

        instance_orphans = orphaned[instance]
        unexpected = dbs.difference(instance_orphans)
        if unexpected:
            raise Exception('Cowardly refusing to act on the following'
                            'dbs: {}'.format(unexpected))

    # confirm that renames would not be blocked by an existing table
    conn = mysql_lib.connect_mysql(instance)

    cursor = conn.cursor()
    for db in dbs:
        # already dealt with
        if db.startswith(DB_PREPEND):
            continue

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
                log.error('Table rename blocked by {}'.format(dup['tbl']))
            sys.exit(1)

        # We should be safe to create the new db and rename
        if not dry_run:
            mysql_lib.create_db(instance, renamed_db)
        mysql_lib.move_db_contents(instance,
                                   old_db=db,
                                   new_db=renamed_db,
                                   dry_run=dry_run)

        if dbs and not dry_run:
            log.info('To finish cleanup, wait a bit and then run:')
            log.info('/usr/local/bin/mysql_utils/fix_orphaned_shards.py -a'
                     'drop -i {}'.format(instance))


def drop_db_after_rename(instance, dbs=None, dry_run=False):
    """ Drop the original empty db and a non-empty rename db

    Args:
        instance - a hostaddr object
        dbs -  a set of database names
        dry_run - bool, will make no changes to the servers
    """
    if not dbs:
        dbs = set()
        for db in mysql_lib.get_dbs(instance):
            if db.startswith(DB_PREPEND):
                dbs.add(db[len(DB_PREPEND):])

    # confirm db is not in zk and not in use
    orphaned, _, _ = find_shard_mismatches.find_shard_mismatches(instance)
    instance_orphans = orphaned[instance]
    unexpected = dbs.difference(instance_orphans)
    if unexpected:
        raise Exception('Cowardly refusing to act on the following '
                        'dbs: {}'.format(unexpected))

    # make sure the original db is empty
    for db in dbs:
        if mysql_lib.get_tables(instance, db):
            raise Exception('Cowardly refusing to drop non-empty '
                            'db: {}'.format(db))

    for db in dbs:
        renamed_db = ''.join((DB_PREPEND, db))
        if dry_run:
            log.info('dry_run is enabled, not dropping '
                     'dbs: {db} {renamed}'.format(db=db, renamed=renamed_db))
        else:
            mysql_lib.drop_db(instance, db)
            mysql_lib.drop_db(instance, renamed_db)


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
