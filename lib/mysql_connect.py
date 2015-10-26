#!/usr/bin/env python
import argparse
import json

AUTH_FILE = '/var/config/config.services.mysql_auth'
MYSQL_DS_ZK = '/var/config/config.services.dataservices.mysql_databases'
MYSQL_GEN_ZK = '/var/config/config.services.general_mysql_databases_config'
MASTER = 'master'
SLAVE = 'slave'
DR_SLAVE = 'dr_slave'
REPLICA_ROLES = [MASTER, SLAVE, DR_SLAVE]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('replica_set_name',
                        help='The replica_set to pull hostname/port/username/password')
    parser.add_argument('user_role',
                        help=('Which user role to pull username and password for.'
                              'Default is scriptro/scriptrw'),
                        default=None,
                        nargs='?')
    parser.add_argument('--writeable',
                        help='Should the user have a writeable connection?',
                        default=False,
                        action='store_true')
    parser.add_argument('--replica_set_role',
                        help='Pull the hostname/password for a server other than the master',
                        default=None,
                        choices=REPLICA_ROLES)
    args = parser.parse_args()

    ret = get_mysql_connection(args.replica_set_name, writeable=args.writeable,
                               user_role=args.user_role, replica_set_role=args.replica_set_role)
    print "{hostname} {port} {username} {password}".format(hostname=ret[0],
                                                           port=ret[1],
                                                           username=ret[2],
                                                           password=ret[3])


def get_mysql_connection(replica_set_name, writeable=False,
                         user_role=None, replica_set_role=None):
    """ Get MySQL connection information. This code also exists in
    the wiki and is copied numberous places across the pinterest code base.

    Args:
    replica_set_name - The name of a replica set in zk, ie db00047
                       or blackopsdb001.
    writeable - If the connection should be writeable.
    user_role - A named user role to pull. If this is supplied, writeable
                is not respected.
    replica_set_role - Default role is master, can also be slave or dr_slave.
                       If this is supplied, writeable is not respected.

    Returns:
    hostname - (str) The master host of the named replica set
    port - The port of the named replica set. Please do not assume 3306.
    username - The MySQL username to be used.
    password - The password that corrosponds to the username
    """
    hostname = None
    port = None
    password = None
    username = None

    if user_role is None:
        if (replica_set_role == MASTER or writeable is True):
            user_role = 'scriptrw'
        else:
            user_role = 'scriptro'

    if replica_set_role is None:
        replica_set_role = MASTER

    with open(MYSQL_DS_ZK) as f:
        ds = json.loads(f.read())

    for entry in ds.iteritems():
        if replica_set_name == entry[0]:
            hostname = entry[1][replica_set_role]['host']
            port = entry[1][replica_set_role]['port']

    if hostname is None or port is None:
        with open(MYSQL_GEN_ZK) as f:
            gen = json.loads(f.read())

        for entry in gen.iteritems():
            if replica_set_name == entry[0]:
                hostname = entry[1][replica_set_role]['host']
                port = entry[1][replica_set_role]['port']

    if hostname is None or port is None:
        err = ("Replica set '{rs}' does not exist in zk"
               ''.format(rs=replica_set_name))
        raise NameError(err)

    with open(AUTH_FILE) as f:
        grants = json.loads(f.read())

    for entry in grants.iteritems():
        if user_role == entry[0]:
            for user in entry[1]['users']:
                if user['enabled'] is True:
                    username = user['username']
                    password = user['password']

    if username is None or password is None:
        err = ("Userrole '{role}' does not exist in zk"
               ''.format(role=user_role))
        raise NameError(err)
    return hostname, port, username, password


if __name__ == "__main__":
    main()
