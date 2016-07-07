#!/usr/bin/env python
import argparse
from lib import environment_specific
import socket
import string
import subprocess

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db',
                        help='What server, shard or replica set to connect to '
                             '(ie sharddb021b[:3306], db00003, pbdata03862, '
                             'follower_zendata001002)')
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
        log.info('{db} appears to be a hostname'.format(db=args.db))
    except:
        log.info('{db} appears not to be a hostname'.format(db=args.db))

    # Maybe it is a replica set
    if not host:
        config = zk.get_all_mysql_config()
        if args.db in config:
            master = config[args.db]['master']
            log.info('{db} appears to be a replica set'.format(db=args.db))
            host = host_utils.HostAddr(''.join((master['host'],
                                                ':',
                                                str(master['port']))))
        else:
            log.info('{db} appears not to be a replica set'.format(db=args.db))

    # Perhaps a shard?
    if not host:
        shard_map = zk.get_host_shard_map()
        for master in shard_map:
            if args.db in shard_map[master]:
                log.info('{db} appears to be a shard'.format(db=args.db))
                host = host_utils.HostAddr(master)
                db = environment_specific.convert_shard_to_db(args.db)
                break
        if not host:
            log.info('{db} appears not to be a shard'.format(db=args.db))

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
        replica_set, _ = zk.get_replica_set_from_instance(host)
    except Exception as e:
        if 'is not in zk' in e.message:
            log.warning('SERVER IS NOT IN ZK!!!')
            replica_set = None
        else:
            raise

    try:
        # do we need a prompt?
        if replica_set in environment_specific.EXTRA_PARANOID_REPLICA_SETS:
            if args.privileges in ['read-write', 'admin']:
                resp = raw_input("You've asked for {priv} access to replica "
                                 "set {rs}.  Are you sure? (Y/N): ".format(
                                    priv=args.privileges,
                                    rs=replica_set))
                if not resp or resp[0] not in ['Y', 'y']:
                    raise Exception('Connection aborted by user!')

        # should we enable safe-updates?
        if replica_set in environment_specific.PARANOID_REPLICA_SETS:
            if args.privileges in ['read-write', 'admin']:
                sql_safe = '--init-command="SET SESSION SQL_SAFE_UPDATES=ON"'

    except Exception as e:
        log.error("Unable to continue: {}".format(e))
        return

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
    log = environment_specific.setup_logging_defaults(__name__)
    main()
