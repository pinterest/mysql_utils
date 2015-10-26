#!/usr/bin/env python
import argparse
import difflib
import pprint
import re
import MySQLdb
import sys
from lib import host_utils
from lib import mysql_lib


def main():
    action_desc = """Action description:

stdout - dump grants to stdout
check - check grants on the instance and ouput errors to stdout
import - import grants on to the instance and then check
nuke_then_import - DANGEROUS! Delete all grants, reimport and then recheck

Note: Grants do *NOT* run through replication. If you need to make a change,
you will need to run it against the entire replica set.
"""

    parser = argparse.ArgumentParser(description='MySQL grant manager',
                                     epilog=action_desc,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-i',
                        '--instance',
                        help='Instance to act on if other than localhost:3306',
                        default=''.join((host_utils.HOSTNAME, ':3306')))
    parser.add_argument('-a',
                        '--action',
                        choices=('stdout',
                                 'check',
                                 'import',
                                 'nuke_then_import'),
                        required=True)

    args = parser.parse_args()
    instance = host_utils.HostAddr(args.instance)

    if args.action == 'stdout':
        grants = mysql_lib.get_all_mysql_grants()
        for grant in grants.iteritems():
            print format_grant(grant[1])
    else:
        problems = manage_mysql_grants(instance, args.action)
        if len(problems) > 0:
            print "Current problems:"
            print '\n'.join(problems)
            sys.exit(1)


def format_grant(grant):
    """ Convert a dict describing mysql grants into a GRANT command

    Args:
    grant - dict with keys string privileges, username, source_host, password
            and a bool grant_option

    Returns:
    sql - A GRANT command in string format
    """
    if grant['grant_option']:
        grant_option = ' WITH GRANT OPTION'
    else:
        grant_option = ''
    sql_format = "GRANT {privs} ON *.* TO `{user}`@`{host}` " +\
                 "IDENTIFIED BY '{password}' {grant_option};"
    sql = sql_format.format(privs=grant['privileges'],
                            user=grant['username'],
                            host=grant['source_host'],
                            password=grant['password'],
                            grant_option=grant_option)
    return sql


def parse_grant(raw_grant):
    """ Convert a MySQL GRANT into a dict

    Args:
    sql - A GRANT command in string format

    Returns:
    grant - dict with keys string privileges, username, source_host, password
            and a bool grant_option
    """
    ret = dict()
    pattern = "GRANT (?P<privileges>.+) ON (?:.+) TO '(?P<username>.+)'@'(?P<source_host>[^']+)'"
    match = re.match(pattern, raw_grant)
    ret['privileges'] = match.group(1)
    ret['username'] = match.group(2)
    ret['source_host'] = match.group(3)

    pattern = ".+PASSWORD '(?P<privileges>[^']+)'(?P<grant_option> WITH GRANT OPTION)?"
    match = re.match(pattern, raw_grant)
    if match:
        ret['hashed_password'] = match.group(1)
    else:
        ret['hashed_password'] = "NONE"

    pattern = ".+WITH GRANT OPTION+"
    match = re.match(pattern, raw_grant)
    if match:
        ret['grant_option'] = True
    else:
        ret['grant_option'] = False
    return ret


def manage_mysql_grants(instance, action):
    """ Nuke/import/check MySQL grants

    Args:
    instance - an object identify which host to act upon
    action - available options:
            check - check grants on the instance and ouput errors to stdout
            import - import grants on to the instance and then check
            nuke_then_import - delete all grants, reimport and then recheck

    Returns:
    problems -  a list of problems

    """
    try:
        conn = mysql_lib.connect_mysql(instance)
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if (error_code != mysql_lib.MYSQL_ERROR_HOST_ACCESS_DENIED and
                error_code != mysql_lib.MYSQL_ERROR_ACCESS_DENIED):
            raise

        if instance.hostname == host_utils.HOSTNAME.split('.')[0]:
            print ('Could not connect to instance, but it looks like '
                   'instance is on localhost. Going to try defaults for '
                   'authentication.')
            conn = mysql_lib.connect_mysql(instance, 'bootstrap')
        else:
            raise

    grants = mysql_lib.get_all_mysql_grants()

    # nuke
    conn.query("SET SQL_LOG_BIN=0")
    if action == 'nuke_then_import':
        conn.query("SET SQL_SAFE_UPDATES = 0")
        conn.query("delete from mysql.user")
        conn.query("delete from mysql.db")
        conn.query("delete from mysql.proxies_priv")
    # import
    if action in ('import', 'nuke_then_import'):
        for grant in grants.iteritems():
            sql = format_grant(grant[1])
            conn.query(sql)
        conn.query('flush privileges')
    # check
    if action in ('check', 'import', 'nuke_then_import'):
        problems = []
        on_server = dict()
        cursor = conn.cursor()

        # PK on (user, host), so this returns all distinct users
        cursor.execute("SELECT user, host FROM mysql.user")
        users = cursor.fetchall()
        for row in users:
            user = "`{user}`@`{host}`".format(user=row['user'],
                                              host=row['host'])
            sql = "SHOW GRANTS FOR {user}".format(user=user)
            try:
                cursor.execute(sql)
            except MySQLdb.OperationalError as detail:
                (error_code, msg) = detail.args
                if error_code != mysql_lib.MYSQL_ERROR_NO_DEFINED_GRANT:
                    raise

                problems.append('Grant {user} is not active, probably due to '
                                'skip-name-resolve being on'.format(user=user))
                continue
            returned_grants = cursor.fetchall()

            if len(returned_grants) > 1:
                problems.append('Grant for {user} is too complicated, '
                                'ignoring grant'.format(user=user))
                continue
            unparsed_grant = returned_grants[0][returned_grants[0].keys()[0]]
            on_server[user] = parse_grant(unparsed_grant)

        expected_users = set(grants.keys())
        active_users = set(on_server.keys())

        missing_users = expected_users.difference(active_users)
        for user in missing_users:
            problems.append('Missing user: {user}'.format(user=user))

        unexpected_user = active_users.difference(expected_users)
        for user in unexpected_user:
            problems.append('Unexpected user: {user}'.format(user=user))

        # need hashes from passwords. We could store this in zk, but it just
        # another thing to screw up
        for key in grants.keys():
            password = grants[key]['password']
            sql = "SELECT PASSWORD('{pw}') pw".format(pw=password)
            cursor.execute(sql)
            ret = cursor.fetchone()
            grants[key]['hashed_password'] = ret['pw']
            del grants[key]['password']

        for key in set(grants.keys()).intersection(set(on_server.keys())):
            if grants[key] != on_server[key]:
                diff = difflib.unified_diff(pprint.pformat(on_server[key]).splitlines(),
                                            pprint.pformat(grants[key]).splitlines())
                problems.append('Grant for user "{user}" does not match:'
                                '{problem}'.format(user=key,
                                                   problem='\n'.join(diff)))

        return problems


if __name__ == "__main__":
    main()
