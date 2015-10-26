#!/usr/bin/env python
import argparse
import socket
import sys
from lib import host_utils
from lib import mysql_lib


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

    header = ''
    for checksum in checksums:
        # Humans don't care about false positives for diffs
        if (checksum['checksum_status'] == 'ROW_DIFFS_FOUND' and
                checksum['rows_checked'] == 'YES' and
                checksum['row_diffs'] == 0):
            checksum['checksum_status'] = 'GOOD'

        # build up and print a reasonable header.
        if header == '':
            m_spc = ' ' * 26
            r_spc = ' ' * 25
            tbl_spc = ' ' * 35
            header = ("Master{m_spc}Replica{r_spc}Date"
                      "\t\t\tDatabase\tTable{tbl_spc}Row Count\t"
                      "Diff Count\tStatus").format(m_spc=m_spc,
                                                   r_spc=r_spc,
                                                   tbl_spc=tbl_spc)
            print header
            print '=' * 190

        tbl_padding = ' ' * (40 - len(checksum['tbl']))
        master_padding = ' ' * (32 - len(checksum['master_instance']))
        slave_padding = ' ' * (32 - len(checksum['instance']))

        if args.table is False or args.table == checksum['tbl']:
            print ("{m}{m_spc}{r}{s_spc}{dt}\t{db}\t{tbl}{t_spc}{count}\t\t"
                   "{row_diffs}\t\t{status}"
                   "".format(m=checksum['master_instance'],
                             r=checksum['instance'],
                             db=checksum['db'],
                             tbl=checksum['tbl'],
                             t_spc=tbl_padding,
                             m_spc=master_padding,
                             s_spc=slave_padding,
                             status=checksum['checksum_status'],
                             count=checksum['row_count'],
                             row_diffs=checksum['row_diffs'],
                             dt=checksum['reported_at']))


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

    if db is False:
        cnt = 0
        shard_param_set = set()
        for entry in host_shard_map[instance.__str__()]:
            key = ''.join(('shard', str(cnt)))
            vars_for_query[key] = entry
            shard_param_set.add(key)
            cnt += 1

        shard_param = ''.join(('%(',
                               ')s,%('.join(shard_param_set),
                               ')s'))
    else:
        shard_param = '%(shard1)s'
        vars_for_query['shard1'] = db

    # connect to the instance we care about and get some data.
    conn = mysql_lib.connect_mysql(instance, 'scriptrw')

    # We only care about the most recent checksum
    cursor = conn.cursor()
    sql = ("SELECT detail.master_instance, "
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
           "   WHERE  master_instance=%(instance)s "
           "          AND db IN ({sp}) "
           "   GROUP BY 1,2,3,4 "
           "  ) AS most_recent "
           "JOIN test.checksum_detail AS detail "
           "USING(master_instance, instance, db, tbl, reported_at) ").format(sp=shard_param)

    cursor.execute(sql, vars_for_query)
    checksums = cursor.fetchall()
    return checksums


if __name__ == "__main__":
    main()
