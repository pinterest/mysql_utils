#!/usr/bin/env python
import argparse
import socket
import sys
from lib import host_utils
from lib import mysql_lib

LINE_TEMPLATE = ('{master_instance:<MSPC}'
                 '{instance:<RSPC}'
                 '{reported_at:<22}'
                 '{db:<DBSPC}'
                 '{tbl:<TSPC}'
                 '{row_count:<RCSPC}'
                 '{row_diffs:<DCSPC}'
                 '{checksum_status}')


def main():
    parser = argparse.ArgumentParser(description='MySQL checksum interface')
    parser.add_argument('-i',
                        '--instance',
                        help='Defaults to localhost:3306',
                        default=''.join((socket.getfqdn(), ':3306')),
                        required=True)
    parser.add_argument('-d',
                        '--db',
                        help="Restrict results to a single named db",
                        default=False)
    parser.add_argument('-t',
                        '--table',
                        help='Restrict results to a single table',
                        default=False)
    args = parser.parse_args()
    instance = host_utils.HostAddr(args.instance)

    checksums = get_checksums(instance, args.db)
    if not checksums:
        print "No data found!"
        sys.exit(1)

    format_str, line_length = generate_format_string(checksums)
    header = format_str.format(master_instance='Master',
                               instance='Replica',
                               reported_at='Date',
                               db='Database',
                               tbl='Table',
                               row_count='Row Count',
                               row_diffs='Diff Count',
                               checksum_status='Status')
    print header
    print '=' * line_length

    # now actually print the output.
    for checksum in checksums:
        if args.table is False or args.table == checksum['tbl']:
            checksum['reported_at'] = str(checksum['reported_at'])
            print format_str.format(**checksum)


def generate_format_string(checksums):
    """ Use the base template and proper string lengths to make the output
        look nicer.

        Args:
        checksums - a collection of checksum rows.

        Returns:
        format_str - a format string with spacing offsets filled in.
        line_length - the maximum length of the line + some extra space
    """
    # initial padding values
    padding = {'master_instance': len('Master'),
               'instance': len('Replica'),
               'db': len('Database'),
               'tbl': len('Table'),
               'reported_at': len('Date'),
               'row_count': len('Row Count'),
               'row_diffs': len('Diff Count'),
               'checksum_status': len('Status')}

    line_length = 40 + sum(padding.values())

    for checksum in checksums:
        # Humans don't care about false positives for diffs
        if (checksum['checksum_status'] == 'ROW_DIFFS_FOUND' and
                checksum['rows_checked'] == 'YES' and
                checksum['row_diffs'] == 0):
            checksum['checksum_status'] = 'GOOD'

        for key, value in padding.items():
            if len(str(checksum[key])) > padding[key]:
                line_length += len(str(checksum[key])) - padding[key]
                padding[key] = len(str(checksum[key]))

    # regenerate the output template based on padding.
    format_str = LINE_TEMPLATE.replace(
        'MSPC', str(padding['master_instance'] + 3)).replace(
        'RSPC', str(padding['instance'] + 3)).replace(
        'DBSPC', str(padding['db'] + 3)).replace(
        'TSPC', str(padding['tbl'] + 3)).replace(
        'RCSPC', str(padding['row_count'] + 3)).replace(
        'DCSPC', str(padding['row_diffs'] + 3))

    return format_str, line_length


def get_checksums(instance, db=False):
    """ Get recent mysql replication checksums

    Args:
    instance - a hostaddr object for what server to pull results for
    db - a string of a data to for which to restrict results

    Returns:
    A list of dicts from a select * on the relevant rows
    """

    vars_for_query = dict()
    vars_for_query['instance'] = instance

    zk = host_utils.MysqlZookeeper()
    host_shard_map = zk.get_host_shard_map()

    # extra SQL if this is a sharded data set.
    SHARD_DB_IN_SQL = ' AND db in ({sp}) '

    if db is False:
        cnt = 0
        shard_param_set = set()
        try:
            for entry in host_shard_map[instance.__str__()]:
                key = ''.join(('shard', str(cnt)))
                vars_for_query[key] = entry
                shard_param_set.add(key)
                cnt += 1
            shard_param = ''.join(('%(',
                                   ')s,%('.join(shard_param_set),
                                   ')s'))
        except KeyError:
            # if this is not a sharded data set, don't use this.
            shard_param = None

    else:
        shard_param = '%(shard1)s'
        vars_for_query['shard1'] = db

    # connect to the instance we care about and get some data.
    conn = mysql_lib.connect_mysql(instance, 'dbascript')

    # We only care about the most recent checksum
    cursor = conn.cursor()

    sql_base = ("SELECT detail.master_instance, "
                "       detail.instance, "
                "       detail.db, "
                "       detail.tbl, "
                "       detail.reported_at, "
                "       detail.checksum_status, "
                "       detail.rows_checked, "
                "       detail.row_count, "
                "       detail.row_diffs "
                "FROM "
                "  (SELECT master_instance,"
                "          instance, "
                "          db, "
                "          tbl, "
                "          MAX(reported_at) AS reported_at "
                "   FROM test.checksum_detail "
                "   WHERE master_instance=%(instance)s "
                "   {in_db}"
                "   GROUP BY 1,2,3,4 "
                "  ) AS most_recent "
                "JOIN test.checksum_detail AS detail "
                "USING(master_instance, instance, db, "
                "tbl, reported_at) ")

    # and then fill in the variables.
    if shard_param:
        sql = sql_base.format(in_db=SHARD_DB_IN_SQL.format(sp=shard_param))
    else:
        sql = sql_base.format(in_db='')

    cursor.execute(sql, vars_for_query)
    checksums = cursor.fetchall()
    return checksums


if __name__ == "__main__":
    main()
