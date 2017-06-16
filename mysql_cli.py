#!/usr/bin/env python
import argparse
import logging
import socket
import string
import subprocess

from lib import environment_specific
from lib import host_utils
from lib import mysql_lib

MYSQL_CLI = ('/usr/bin/mysql -A -h {host} -P {port} {sql_safe} '
             '--user={user} --password={password} '
             '--prompt="\h:\p \d \u> " {db}')

# if we just want to run a command and disconnect, no
# point in setting a prompt.
MYSQL_CLI_EX = ('/usr/bin/mysql -A -h {host} -P {port} {sql_safe} '
                '--user={user} --password={password} '
                '{db} -e "{execute}"')

DEFAULT_ROLE = 'read-only'

log = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db',
                        help='What server, shard or replica set to connect to '
                             '(e.g., sharddb-21-2[:3306], db00003, pbdata03862, '
                             'zenfollowermysql_zendata001002, '
                             'zenshared_video_zendata000002, etc.)')
    parser.add_argument('-p',
                        '--privileges',
                        help=''.join(('Default is ', DEFAULT_ROLE)),
                        default=DEFAULT_ROLE,
                        choices=environment_specific.CLI_ROLES.keys())
    parser.add_argument('-l',
                        '--longquery',
                        default=False,
                        action='store_true',
                        help='For standard read or write access, use this '
                             'flag if you expect the query to take more than '
                             '10 seconds.')
    parser.add_argument('--trust_me_im_a_doctor',
                        default=False,
                        action='store_true',
                        help='If this is set, we bypass any paranoid replica '
                              'set checks.  User assumes all risk.')
    parser.add_argument('-e',
                        '--execute',
                        help='An optional SQL command to run.',
                        default=False)
    args = parser.parse_args()
    zk = host_utils.MysqlZookeeper()
    host = None
    db = ''

    role_modifier = 'default'
    long_query = ''
    if args.longquery:
        role_modifier = 'long'
        long_query = '(long queries enabled)'

    # check if db exists in dns, if so the supplied argument will be considered
    # a hostname, otherwise a replica set.
    try:
        socket.gethostbyname(args.db)
        host = host_utils.HostAddr(args.db)
        log.info('{} appears to be a hostname'.format(args.db))
    except:
        log.info('{} appears not to be a hostname'.format(args.db))

    # Maybe it is a replica set
    if not host:
        try:
            host = zk.get_mysql_instance_from_replica_set(args.db)
            log.info('{} appears to be a replica set'.format(args.db))
        except:
            log.info('{} appears not to be a replica set'.format(args.db))

    # Perhaps a shard?
    if not host:
        try:
            (replica_set, db) = zk.map_shard_to_replica_and_db(args.db)
            host = zk.get_mysql_instance_from_replica_set(replica_set)
            log.info('{} appears to be a shard'.format(args.db))
        except:
            log.info('{} appears not to be a shard'.format(args.db))
            raise

    if not host:
        raise Exception('Could not determine what host to connect to')

    log.info('Will connect to {host} with {privileges} '
             'privileges {lq}'.format(host=host,
                                      privileges=args.privileges,
                                      lq=long_query))
    (username, password) = mysql_lib.get_mysql_user_for_role(
        environment_specific.CLI_ROLES[args.privileges][role_modifier])

    # we may or may not know what replica set we're connecting to at
    # this point.
    sql_safe = ''
    try:
        replica_set = zk.get_replica_set_from_instance(host)
    except Exception as e:
        if 'is not in zk' in e.message:
            log.warning('SERVER IS NOT IN ZK!!!')
            replica_set = None
        else:
            raise

    if not args.trust_me_im_a_doctor:
        try:
            # do we need a prompt?
            if replica_set in environment_specific.EXTRA_PARANOID_REPLICA_SETS:
                warn = environment_specific.EXTRA_PARANOID_ALERTS.get(replica_set)
                if args.privileges in ['read-write', 'admin']:
                    resp = raw_input("You've asked for {priv} access to replica "
                                     "set {rs}.  Are you sure? (Y/N): ".format(
                                        priv=args.privileges,
                                        rs=replica_set))
                    if not resp or resp[0] not in ['Y', 'y']:
                        raise Exception('Connection aborted by user!')
                    else:
                        print warn

            # should we enable safe-updates?
            if replica_set in environment_specific.PARANOID_REPLICA_SETS:
                if args.privileges in ['read-write', 'admin']:
                    sql_safe = '--init-command="SET SESSION SQL_SAFE_UPDATES=ON"'

        except Exception as e:
            log.error("Unable to continue: {}".format(e))
            return
    else:
        log.warning("OK, we trust you know what you're doing, but "
                    "don't say we didn't warn you.")

    if args.execute:
        execute_escaped = string.replace(args.execute, '"', '\\"')
        cmd = MYSQL_CLI_EX.format(host=host.hostname,
                                  port=host.port,
                                  db=db,
                                  user=username,
                                  password=password,
                                  sql_safe=sql_safe,
                                  execute=execute_escaped)
    else:
        cmd = MYSQL_CLI.format(host=host.hostname,
                               port=host.port,
                               db=db,
                               user=username,
                               password=password,
                               sql_safe=sql_safe)
    log.info(cmd)
    proc = subprocess.Popen(cmd, shell=True)
    proc.wait()

if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
