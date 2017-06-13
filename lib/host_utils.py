import ConfigParser
import StringIO
import boto3
import getpass
import json
import multiprocessing
import os
import pycurl
import re
import shutil
import socket
import subprocess
import time

import mysql_lib
from lib import environment_specific

ACCEPTABLE_ROOT_VOLUMES = ['/raid0', '/mnt']
OLD_CONF_ROOT = '/etc/mysql/my-{port}.cnf'
DEFAULTS_FILE_ARG = '--defaults-file={defaults_file}'
DEFAULTS_FILE_EXTRA_ARG = '--defaults-extra-file={defaults_file}'
HIERA_ROLE_FILE = '/etc/roles.txt'
DEFAULT_HIERA_ROLE = 'mlpv2'
DEFAULT_PINFO_CLOUD = 'undefined'
MASTERFUL_PUPPET_ROLES = ['singleshard', 'modshard']
HOSTNAME = socket.getfqdn().split('.')[0]
MYSQL_CNF_FILE = '/etc/mysql/my.cnf'
MYSQL_INIT_FILE = '/etc/mysql/init.sql'
MYSQL_UPGRADE_CNF_FILE = '/etc/mysql/mysql_upgrade.cnf'
MYSQL_NOREPL_CNF_FILE = '/etc/mysql/skip_slave_start.cnf'
MYSQL_DS_ZK = '/var/config/config.services.dataservices.mysql_databases'
MYSQL_DR_ZK = '/var/config/config.services.disaster_recoverydb'
MYSQL_GEN_ZK = '/var/config/config.services.general_mysql_databases_config'
MYSQL_SHARD_MAP_ZK = '/var/config/config.services.generaldb.mysql_shards'
MYSQL_SHARD_MAP_ZK_TEST = \
    "/var/config/config.services.generaldb.test_mysql_shards"
MYSQL_MAX_WAIT = 300
MYSQL_STARTED = 0
MYSQL_STOPPED = 1
MYSQL_SUPERVISOR_PROC = 'mysqld-3306'
MYSQL_UPGRADE = '/usr/bin/mysql_upgrade'
REPLICA_ROLE_DR_SLAVE = 'dr_slave'
REPLICA_ROLE_MASTER = 'master'
REPLICA_ROLE_SLAVE = 'slave'
REPLICA_TYPES = [REPLICA_ROLE_MASTER,
                 REPLICA_ROLE_SLAVE,
                 REPLICA_ROLE_DR_SLAVE]
TESTING_DATA_DIR = '/tmp/'
TESTING_PINFO_CLOUD = 'vagrant'

# /raid0 and /mnt are interchangable; use whichever one we have.
REQUIRED_MOUNTS = ['/raid0:/mnt']
SUPERVISOR_CMD = '/usr/local/bin/supervisorctl {action} mysql:mysqld-{port}'
INIT_CMD = '/etc/init.d/mysqld_multi {options} {action} {port}'
PTKILL_CMD = '/usr/sbin/service pt-kill-{port} {action}'
KILL_CHECKSUM_CMD= 'ssh root@{} " ps -ef | grep mysql_checksu[m]  | awk \'{{print \$2}}\' | xargs kill -9 "'
PTHEARTBEAT_CMD = '/usr/sbin/service pt-heartbeat-{port} {action}'
MAXWELL_CMD = '/usr/sbin/service maxwell-{port} {action}'
# Tcollector will restart automatically
RESTART_TCOLLECTOR = '/usr/bin/pkill -f "/opt/tcollector/"'
ZK_CACHE = [MYSQL_DS_ZK, MYSQL_DR_ZK, MYSQL_GEN_ZK]

log = environment_specific.setup_logging_defaults(__name__)


def bind_lock_socket(lock_name='singleton'):
    """ Create and bind an abstract socket with the specified name.
        This is an alternative approach to flock-based locking which
        needs no write permissions on the filesystem, automatically
        cleans itself up upon process exit, and is root-proof.  If
        the bind fails, we should exit the program immediately right
        here, but we'll leave that to the caller and just raise an
        exception.  !!LINUX ONLY!!

        Args:
            lock_name - a descriptive name for the socket
        Returns:
            The socket, if successful.
    """
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind('\0' + lock_name)
    return s


def release_lock_socket(socket_handle):
    """ There is one place where we need to release the lock and then
        reacquire it, so being able to explicitly do so becomes useful.

    Args:
        socket_handle: A handle to the socket
    Returns:
        Nothing
    """
    socket_handle.close()
    socket_handle = None


def find_root_volume():
    """ Figure out what top-level mount/directory we are installing to.
        Whichever one is a mount point is the valid one.  We take the
        first one that we find; /raid0 will eventually be going away
        in favor of /mnt with masterless puppet anyway.
    """
    root_volume = None
    for mount_point in ACCEPTABLE_ROOT_VOLUMES:
        if os.path.ismount(mount_point):
            root_volume = mount_point
            break

    if get_pinfo_cloud() == TESTING_PINFO_CLOUD:
        return TESTING_DATA_DIR

    if not root_volume:
        raise Exception("No acceptable root volume mount point was found.")
    else:
        return root_volume


def get_user():
    """ Return the username of the caller, or unknown if we can't
        figure it out (should never happen)
    """
    try:
        username = getpass.getuser()
    except:
        log.warning("Can't determine caller's username. Setting to unknown.")
        username = 'unknown'

    return username


def stop_mysql(port):
    """ Stop a MySQL instance

    Args:
    pid_file - A file with the pid of the MySQL instance
    """
    pid_file = get_cnf_setting('pid_file', port)
    log_error = get_cnf_setting('log_error', port)
    proc_pid = None
    if os.path.exists(pid_file):
        with open(pid_file) as f:
            pid = f.read().strip()
        proc_pid = os.path.join('/proc/', pid)

    if not proc_pid or not os.path.exists(proc_pid):
        log.info('It appears MySQL is not runnng, sending a kill anyways')
        proc_pid = None

    if os.path.isfile(log_error):
        st_results = os.stat(log_error)
        error_start = st_results[6]
    else:
        error_start = 0

    # send the kill
    if get_hiera_role() in MASTERFUL_PUPPET_ROLES:
        cmd = SUPERVISOR_CMD.format(action='stop', port=port)
    else:
        cmd = INIT_CMD.format(action='stop', port=port, options='')
    log.info(cmd)
    shell_exec(cmd)

    if proc_pid:
        ret = tail_mysql_log_for_start_stop(log_error, error_start)
        if ret != MYSQL_STOPPED:
            raise Exception('It appears MySQL shutdown rather than started up')
    else:
        log.info('Sleeping 10 seconds just to be safe')
        time.sleep(10)


def start_mysql(port, options=''):
    """ Start a MySQL instance

    Args:
    instance - A hostaddr object for the instance to be started
    """
    log_error = get_cnf_setting('log_error', port)
    # If MySQL has never been started, this file will not exist
    if os.path.isfile(log_error):
        st_results = os.stat(log_error)
        error_start = st_results[6]
    else:
        error_start = 0

    if get_hiera_role() in MASTERFUL_PUPPET_ROLES:
        cmd = SUPERVISOR_CMD.format(action='start', port=port)
    else:
        cmd = INIT_CMD.format(action='start', port=port, options=options)
    log.info(cmd)
    shell_exec(cmd)
    log.info('Waiting for MySQL on port {port} to start, '
             'tailing error log {log_error}'.format(port=port,
                                                    log_error=log_error))
    ret = tail_mysql_log_for_start_stop(log_error, error_start)
    if ret != MYSQL_STARTED:
        raise Exception('It appears MySQL shutdown rather than started up')


def tail_mysql_log_for_start_stop(log_error, start_pos=0):
    """ Tail a MySQL error log watching for a start or stop

    Args:
    log_error - The MySQL error log to tail watching for a start or stop
    start_pos - The position to start tailing the error log

    Returns:
    an int - if MySQL has started 0, if MySQL has ended 1
    """
    log.info('Tailing MySQL error log {log_error} starting from position '
             '{start_pos}'.format(log_error=log_error,
                                  start_pos=start_pos))
    start = time.time()
    err = None
    while True:
        if (time.time() - start) > MYSQL_MAX_WAIT:
            log.error('Waited too long, giving up')
            raise Exception('MySQL did change state after {wait} seconds'
                            ''.format(wait=MYSQL_MAX_WAIT))

        if not err and os.path.isfile(log_error):
            err = open(log_error)
            err.seek(start_pos)

        if err:
            while True:
                line = err.readline()
                if line:
                    log.info(line.strip())
                    if 'ready for connections' in line:
                        log.info('MySQL is UP :)')
                        return MYSQL_STARTED
                    if 'mysqld: Shutdown complete' in line:
                        log.info('MySQL is DOWN :(')
                        return MYSQL_STOPPED
                    if 'A mysqld process already exists' in line:
                        raise Exception('Something attempted to start MySQL '
                                        ' while it was already up')
                else:
                    # break out of the inner while True, sleep for a bit...
                    break
        time.sleep(.5)


def restart_pt_daemons(port):
    """ Restart various daemons after a (re)start of MySQL

    Args:
    port - the of the mysql instance on localhost to act on
    """
    restart_pt_heartbeat(port)
    restart_pt_kill(port)


def restart_pt_heartbeat(port):
    log.info('Restarting pt-heartbeat')
    cmd = PTHEARTBEAT_CMD.format(port=port, action='restart')
    log.info(cmd)
    (std_out, std_err, return_code) = shell_exec(cmd)
    log.info(std_out.rstrip())


def restart_pt_kill(port):
    log.info('Restart pt-kill')
    cmd = PTKILL_CMD.format(port=port, action='restart')
    log.info(cmd)
    (std_out, std_err, return_code) = shell_exec(cmd)
    log.info(std_out.rstrip())


def restart_maxwell(port):
    log.info('Restart Maxwell')
    cmd = MAXWELL_CMD.format(port=port, action='restart')
    log.info(cmd)
    (stdout, stderr, return_code) = shell_exec(cmd)
    if stdout.rstrip(): 
        log.info(stdout.rstrip())


def restart_tcollector():
    log.info('Restart tcollector')
    log.info(RESTART_TCOLLECTOR)
    shell_exec(RESTART_TCOLLECTOR)


def kill_checksum(instance):
    log.info("Killing the checksum ")
    cmd = KILL_CHECKSUM_CMD.format(instance.hostname)
    log.info(cmd)
    (std_out, std_err, return_code) = shell_exec(cmd)
    if return_code != 0:
        log.info("Checksum didn't get killed.")
        log.info(std_err)
    else:
        log.warning("Checksum got killed !")
    return return_code
        

def upgrade_auth_tables(port):
    """ Run mysql_upgrade

    Args:
    port - the port of the instance on localhost to act on
    """
    start_mysql(port,
                DEFAULTS_FILE_ARG.format(defaults_file=MYSQL_UPGRADE_CNF_FILE))
    socket = get_cnf_setting('socket', port)
    username, password = mysql_lib.get_mysql_user_for_role('admin')
    cmd = ''.join((MYSQL_UPGRADE, ' ',
                   '--upgrade-system-tables ',
                   '-S ', socket, ' ',
                   '-u ', username, ' ',
                   '-p', password))
    log.info(cmd)
    (std_out, std_err, return_code) = shell_exec(cmd)
    log.info(std_out)
    if return_code != 0:
        log.warning(std_err)
        raise Exception('MySQL Upgrade failed with return code '
                        'of: {ret}'.format(ret=return_code))
    stop_mysql(port)


_shard_map = None
_shard_map_refresh = None
_ds_map = None
_ds_refresh = None
_gen_map = None
_gen_refresh = None
_dr_map = None
_dr_refresh = None
_all_instances = dict()
_instance_rs_map = dict()


class MysqlZookeeper:
    """Class for reading MySQL settings stored on the filesystem"""

    def get_ds_mysql_config(self):
        """ Query for Data Services MySQL replica set mappings.

        Returns:
        A dict of all Data Services MySQL replication configuration.

        Example:
        {u'db00001': {u'db': None,
                  u'master': {u'host': u'sharddb-1-31',
                              u'port': 3306},
                  u'passwd': u'redacted',
                  u'slave': {u'host': u'sharddb-1-32',
                             u'port': 3306},
                  u'user': u'pbuser'},
        ...
        """
        global _ds_map
        global _ds_refresh
        if _ds_map and (_ds_refresh > time.time() - 1):
            return _ds_map

        with open(MYSQL_DS_ZK) as f:
            _ds_map = json.loads(f.read())
            _ds_refresh = time.time()

        return _ds_map

    def get_gen_mysql_config(self):
        """ Query for non-Data Services MySQL replica set mappings.

        Returns:
        A dict of all non-Data Services MySQL replication configuration.

        Example:
        {u'abexperimentsdb001': {u'db': u'abexperimentsdb',
                                 u'master': {u'host': u'abexperimentsdb-1-1',
                                             u'port': 3306},
                                u'passwd': u'redacted',
                                u'slave': {u'host': u'abexperimentsdb-12',
                                           u'port': 3306},
                                u'user': u'redacted'},
        ...
        """
        global _gen_map
        global _gen_refresh
        if _gen_map and (_gen_refresh > time.time() - 1):
            return _gen_map

        with open(MYSQL_GEN_ZK) as f:
            _gen_map = json.loads(f.read())
            _gen_refresh = time.time()

        return _gen_map

    def get_dr_mysql_config(self):
        """ Query for disaster recovery MySQL instance mappings.

        Returns:
        A dict of all MySQL disaster recovery instances.

        Example:
        {u'db00018': {u'dr_slave': {u'host': u'sharddb-18-33', u'port': 3306}},
         u'db00015': {u'dr_slave': {u'host': u'sharddb-15-31', u'port': 3306}},
        ...
        """
        global _dr_map
        global _dr_refresh
        if _dr_map and (_dr_refresh > time.time() - 1):
            return _dr_map

        with open(MYSQL_DR_ZK) as f:
            _dr_map = json.loads(f.read())
            _dr_refresh = time.time()

        return _dr_map

    def get_sharded_services(self):
        """ For sharded datasets

        Returns:
        A set of sharded services
        """
        s_map = self.get_zk_mysql_shard_map()
        sharded_services = set()
        for s in s_map['services']:
            for ns in s_map['services'][s]['namespaces']:
                # We store service discovery for java apps in the same place as
                # our sharded datasets. So anything with just one shard we
                # will just ignore
                if len(s_map['services'][s]['namespaces'][ns]['shards']) > 1:
                    sharded_services.add(s)
        return sharded_services

    def get_sharded_types(self):
        """ For sharded datasets

        Returns:
        A set of sharded types (not to be confused with replica types)

        Note: This isn't that different from the previous function, except
        that the previous function returns service-level descriptions
        and this one includes namespace data where present.
        """
        s_map = self.get_zk_mysql_shard_map()
        sharded_db_types = set()
        for s in s_map['services']:
            for ns in s_map['services'][s]['namespaces']:
                # We store service discovery for java apps in the same place as
                # our sharded datasets. So anything with just one shard we
                # will just ignore
                if len(s_map['services'][s]['namespaces'][ns]['shards']) > 1:
                    if ns == '':
                        sharded_db_types.add(s)
                    else:
                        sharded_db_types.add('_'.join([s, ns]))
        return sharded_db_types

    def map_shard_type_to_service_and_namespace(self, shard_type):
        """ Convert a shard type to a service and namespace

        Args:
        shard_type - A shard type

        Returns:
        A tuple of service name and namespace
        """
        if '_' in shard_type:
            (service, namespace) = shard_type.split('_')
        else:
            service = shard_type
            namespace = ''
        return (service, namespace)

    def map_shard_to_replica_and_db(self, shard):
        """ Map a shard to a replica set and a db

        Args:
        shard - A shard name

        Returns:
        A tuple of a replica set name and a db name
        """
        (s, ns, s_num) = environment_specific.deconstruct_shard_name(shard)
        s_map = self.get_zk_mysql_shard_map()
        s_data = s_map['services'][s]['namespaces'][ns]['shards'][str(s_num)]
        return (s_data['replica_set'], s_data['mysqldb'])

    def get_example_db_and_replica_set_for_shard_type(self, shard_type):
        """ Get an example shard for a given shard type.
        Note: by convention, the example shard is shard number 0.

        Args:
        shard_type - A shard type

        Returns:
        A tuple of a replica set name and a db name
        """
        (s, ns) = self.map_shard_type_to_service_and_namespace(shard_type)
        shard_map = self.get_zk_mysql_shard_map()
        shard_data = shard_map['services'][s]['namespaces'][ns]['shards']['0']
        return (shard_data['replica_set'], shard_data['mysqldb'])
    
    def get_replica_sets_by_shard_type(self, shard_type):
        """ Get all replica sets for a given shard type.
        Args:
        shard_type - A shard type
        Returns:
                A tuple of a replica set name
        """
        (s, ns) = self.map_shard_type_to_service_and_namespace(shard_type)
        shard_map = self.get_zk_mysql_shard_map()
        shard_data = shard_map['services'][s]['namespaces'][ns]['shards']
        replica_set = set()
        for shard in shard_data.values():
            replica_set.add(shard['replica_set'])
            
        return replica_set

    def get_zk_mysql_shard_map(self, use_test=False):
        """ Load the ZK-based shard-to-server mapping data.

        Returns:
            A dict where the keys are service names and the values are
            dicts containing namespace and shard mapping info.

        Example:
        {u'zenfollowermysql': {
            u'namespaces': {
                u'': {
                    u'shards': {
                        u'0': {
                            u'mysqldb': u'zendata000000',
                            u'replica_set': u'myzenfollower16db001'
                        }, ...
                    }
                }
            }
        }
        """
        if use_test:
            with open(MYSQL_SHARD_MAP_ZK_TEST) as f:
                return json.loads(f.read())

        global _shard_map
        global _shard_map_refresh
        if _shard_map and (_shard_map_refresh > time.time() - 1):
            return _shard_map

        with open(MYSQL_SHARD_MAP_ZK) as f:
            _shard_map = json.loads(f.read())
            _shard_map_refresh = time.time()

        return _shard_map

    def get_all_mysql_config(self):
        """ Get all MySQL shard mappings.

        Returns:
        A dict of all MySQL replication configuration.

        Example:
        {u'db00001': {u'db': None,
                  u'master': {u'host': u'sharddb-1-43',
                              u'port': 3306},
                  u'passwd': u'redacted',
                  u'slave': {u'host': u'sharddb-1-42',
                             u'port': 3306},
                  u'dr_slave': {u'host': u'sharddb-1-44',
                                u'port': 3306},
                  u'user': u'pbuser'},
         u'abexperimentsdb001': {u'db': u'abexperimentsdb',
                                 u'master': {u'host': u'abexperimentsdb-1-10',
                                             u'port': 3306},
                                u'passwd': u'redacted',
                                u'slave': {u'host': u'abexperimentsdb-1-12',
                                           u'port': 3306},
                                u'user': u'redacted'},
        ...
        """
        mapping_dict = self.get_gen_mysql_config()
        mapping_dict.update(self.get_ds_mysql_config())

        dr = self.get_dr_mysql_config()
        for key in dr:
            mapping_dict[key][REPLICA_ROLE_DR_SLAVE] = \
                dr[key][REPLICA_ROLE_DR_SLAVE]

        return mapping_dict

    def get_all_mysql_replica_sets(self):
        """ Get a list of all MySQL replica sets

        Returns:
        A set of all replica sets
        """
        return set(self.get_all_mysql_config().keys())

    def get_all_mysql_instances_by_type(self, repl_type):
        """ Query for all MySQL instances of a given type (role)

        Args:
        repl_type - A replica type, valid options are entries in REPLICA_TYPES

        Returns:
        A set of HostAddr objects of all MySQL instances of the type repl_type

        Example:
        set([modsharddb-3-20:3306, sharddb-43-36:3306, .... ])
        """
        global _all_instances
        if repl_type in _all_instances and \
                (_all_instances[repl_type]['refresh'] > time.time() - 1):
            return _all_instances[repl_type]['instances']

        if repl_type not in REPLICA_TYPES:
            raise Exception('Invalid repl_type {repl_type}. Valid options are'
                            '{REPLICA_TYPES}'.format(
                                repl_type=repl_type,
                                REPLICA_TYPES=REPLICA_TYPES))
        hosts = set()
        for replica_set in self.get_all_mysql_config().iteritems():
            if repl_type in replica_set[1]:
                host = replica_set[1][repl_type]
                hostaddr = HostAddr(':'.join((host['host'],
                                              str(host['port']))))
                hosts.add(hostaddr)

        _all_instances[repl_type] = {'refresh': time.time(),
                                     'instances': hosts}
        return _all_instances[repl_type]['instances']

    def get_all_mysql_instances(self):
        """ Query ZooKeeper for all MySQL instances

        Returns:
        A set of HostAddr objects of all MySQL instances.

        Example:
        set([modsharddb-3-20:3306, sharddb-43-36:3306, ....])
        """

        hosts = set()
        config = self.get_all_mysql_config()
        for replica_set in config:
            for rtype in REPLICA_TYPES:
                if rtype in config[replica_set]:
                    host = config[replica_set][rtype]
                    hostaddr = HostAddr(':'.join((host['host'],
                                                  str(host['port']))))
                    hosts.add(hostaddr)

        return hosts

    def get_mysql_instance_from_replica_set(self, replica_set,
                                            repl_type=REPLICA_ROLE_MASTER):
        """ Get an instance for a mysql replica set by replica type

        Args:
        replica_set - string name of a replica set, ie db00666
        repl_type - Optional, a replica type with valid options are entries
                    in REPLICA_TYPES. Default is 'master'.

        Returns:
        A hostaddr object or None
        """
        if repl_type not in REPLICA_TYPES:
            raise Exception('Invalid repl_type {repl_type}. Valid options are'
                            '{REPLICA_TYPES}'.format(
                                repl_type=repl_type,
                                REPLICA_TYPES=REPLICA_TYPES))

        all_config = self.get_all_mysql_config()
        if replica_set not in all_config:
            raise Exception('Unknown replica set {}'.format(replica_set))

        if repl_type not in all_config[replica_set]:
            return None

        instance = all_config[replica_set][repl_type]
        hostaddr = HostAddr(':'.join((instance['host'],
                                      str(instance['port']))))
        return hostaddr

    def get_replica_set_from_instance(self, instance):
        """ Get the replica set based on zk info

        Args:
        instance - a hostaddr object

        Returns:
        replica_set - A replica set of which the instance is part
        """
        global _instance_rs_map

        if not _instance_rs_map:
            config = self.get_all_mysql_config()
            for rs in config:
                for rtype in REPLICA_TYPES:
                    if rtype in config[rs]:
                        hostaddr = HostAddr('{h}:{p}'.format(
                            h=config[rs][rtype]['host'],
                            p=config[rs][rtype]['port']))

                        _instance_rs_map[hostaddr] = rs

        if instance not in _instance_rs_map:
            raise Exception('{} is not in zk'.format(instance))

        return _instance_rs_map[instance]

    def get_replica_type_from_instance(self, instance):
        """ Get the replica set role (master, slave, etc) based on zk info
        ## TODO: use consistent naming.  role or type, not both.
        Args:
        instance - a hostaddr object

        Returns:
        replica_set_role - A replica set role of master, slave, etc...
        """
        config = self.get_all_mysql_config()
        for rs in config:
            for rtype in REPLICA_TYPES:
                if rtype in config[rs]:
                    if (instance.hostname == config[rs][rtype]['host'] and
                            instance.port == config[rs][rtype]['port']):
                        return rtype

        raise Exception('{} is not in zk'.format(instance))

    def find_shard(self, replica_set, db):
        """ Find shard metadata based on a replica_set and a db

        Args:
            replica_set - A replica set
            db - A database name

        Returns:
            service - A service name such as zenshared
            namespace - A namespace name such as video, etc..
            shard - A shard id
        """
        zk_shard_map = self.get_zk_mysql_shard_map()
        for s in zk_shard_map['services'].keys():
            namespaces = zk_shard_map['services'][s]['namespaces'].keys()
            for ns in namespaces:
                for shard in zk_shard_map['services'][s]['namespaces'][ns]['shards']:
                    if (zk_shard_map['services'][s]['namespaces'][ns]['shards'][shard]['replica_set'] == replica_set and
                            str(zk_shard_map['services'][s]['namespaces'][ns]['shards'][shard]['mysqldb']) == db):
                        return s, ns, shard

        raise Exception('Could not find db {db} on replica_set {rs}'
                        ''.format(db=db, rs=replica_set))

    def get_sharded_dbs_by_replica_set(self):
        """ Get a mapping of what db (not shard name!) exist on all
            replica sets

        Returns:
            A dict where the key is the MySQL replica sets and the
            value is the set of dbs on that master instance.
        """
        zk_shard_map = self.get_zk_mysql_shard_map()
        rs_to_dbs_map = dict()
        for replica_set in self.get_all_mysql_replica_sets():
            rs_to_dbs_map[replica_set] = set()

        for s in self.get_sharded_services():
            for ns in zk_shard_map['services'][s]['namespaces']:
                for shard_num in zk_shard_map['services'][s]['namespaces'][ns]['shards']:
                    rs_to_dbs_map[zk_shard_map['services'][s]['namespaces'][ns]['shards'][shard_num]['replica_set']].add(
                        zk_shard_map['services'][s]['namespaces'][ns]['shards'][shard_num]['mysqldb'])

        return rs_to_dbs_map

    def get_shards_by_replica_set(self):
        """ Get a mapping of what shards (not db names!) exist on all replica
            sets

        Returns:
            A dict where the key is the replica set and the value
            is a set of shards.
        """
        zk_shard_map = self.get_zk_mysql_shard_map()
        rs_to_shards_map = dict()
        for replica_set in self.get_all_mysql_replica_sets():
            rs_to_shards_map[replica_set] = set()

        for s in self.get_sharded_services():
            for ns in zk_shard_map['services'][s]['namespaces']:
                for shard_num in zk_shard_map['services'][s]['namespaces'][ns]['shards']:
                    rs_to_shards_map[zk_shard_map['services'][s]['namespaces'][ns]['shards'][shard_num]['replica_set']].add(
                        environment_specific.construct_shard_name(s, ns,
                                                                  shard_num))

        return rs_to_shards_map

    def shard_to_instance(self, shard, repl_type=REPLICA_ROLE_MASTER):
        """ Convert a shard to hostname

        Args:
            shard - A shard name
            repl_type - Replica type, master is default

        Returns:
            A hostaddr object for an instance of the replica set
        """
        (rs, db) = self.map_shard_to_replica_and_db(shard)
        instance = self.get_mysql_instance_from_replica_set(
                        rs, repl_type=repl_type)
        return instance

    def get_shards_by_shard_type(self, shard_type):
        """ Get a set of all shards in a shard type

        Args:
        shard_type - The type of shards, i.e. 'sharddb'

        Returns:
            A set of all shard names
        """
        shards = set()
        (s, ns) = self.map_shard_type_to_service_and_namespace(shard_type)
        zk_shard_map = self.get_zk_mysql_shard_map()
        for shard in zk_shard_map['services'][s]['namespaces'][ns]['shards']:
            shards.add(environment_specific.construct_shard_name(s, ns, shard))

        return shards


class HostAddr:
    """Basic abtraction for hostnames"""

    def __init__(self, host):
        """
        Args:
        host - A hostname. We have one fully supported format:
               {prefix}-{replicaSetNum}-{hostNum}
        """
        self.hostname_prefix = None
        self.replica_set_num = None
        self.host_identifier = None
        self.standardized_replica_set = None

        host_params = host.split(':')
        self.hostname = host_params[0].split('.')[0]
        if len(host_params) > 1:
            self.port = int(host_params[1])
        else:
            self.port = 3306

        # New style hostnames are of the form prefix-replicaSetNum-hostNum
        # ie: sharddb-1-1
        try:
            (self.hostname_prefix, self.replica_set_num,
             self.host_identifier) = self.hostname.split('-')

            ## TODO: COMPATIBILITY
            self.replica_type = self.hostname_prefix
            ## COMPATIBILITY
        except:
            raise Exception('Invalid instance {}'.format(self.hostname))

    def get_standardized_replica_set(self):
        """ Return an easily parsible replica set name

        Returns:
            A replica set name which is hyphen seperated with the first part
            being the replica set type (ie sharddb), followed by a the replica
            set identifier.
        """
        if self.standardized_replica_set:
            return self.standardized_replica_set

        self.standardized_replica_set = '-'.join((self.hostname_prefix,
                                                  self.replica_set_num))
        return self.standardized_replica_set

    def guess_zk_replica_set(self):
        """ Determine what replica set a host would belong to.

        Returns:
            A replica set name
        """
        zk = MysqlZookeeper()
        std_to_rs_map = dict()
        for m in zk.get_all_mysql_instances_by_type(REPLICA_ROLE_MASTER):
            std_to_rs_map[m.get_standardized_replica_set()] = \
                zk.get_replica_set_from_instance(m)

        return std_to_rs_map[self.get_standardized_replica_set()]

    ## TODO: COMPATIBILITY - REMOVE LATER
    def get_zk_replica_set(self):
        return self.guess_zk_replica_set()
    ## COMPATIBILITY

    def __str__(self):
        """ Returns a human readable string version of object similar to
            'sharddb-12-3:3309'
        """
        return ''.join((self.hostname, ':', str(self.port)))

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        if (self.hostname == other.hostname and self.port == other.port):
            return 1
        else:
            return 0

    def __ne__(self, other):
        if (self.hostname != other.hostname or self.port != other.port):
            return 1
        else:
            return 0

    def __hash__(self):
        return hash(''.join((self.hostname, ':', str(self.port))))


def shell_exec(cmd):
    """ Run a shell command

    Args:
    cmd - String to execute via a shell. DO NOT use this if the stdout or
          stderr will be very large (more than 100K bytes or so)

    Returns:
        std_out - Standard out results from the execution of the command
        std_err - Standard error results from the execution of the command
        return_code - Return code from the execution of the command
    """
    proc = subprocess.Popen(cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    proc.wait()
    std_out = proc.stdout.read()
    std_err = proc.stderr.read()
    return_code = proc.returncode

    return (std_out, std_err, return_code)


def check_dict_of_procs(proc_dict):
    """ Check a dict of process for exit, error, etc...

    Args:
    A dict of processes

    Returns: True if all processes have completed with return status 0
             False is some processes are still running
             An exception is generated if any processes have completed with a
             returns status other than 0
    """
    success = True
    for proc in proc_dict:
        ret = proc_dict[proc].poll()
        if ret is None:
            # process has not yet terminated
            success = False
        elif ret != 0:
            raise Exception('{proc_id}: {proc} encountered an error'
                            ''.format(
                proc_id=multiprocessing.current_process().name,
                proc=proc))
    return success


def get_cnf_setting(variable, port):
    """ Get the value of a variab from a mysql cnf

    Args:
    variable - a MySQL variable located in configuration file
    port - Which instance of mysql, ie 3306.

    Returns:
    The value of the variable in the configuration file
    """
    if get_hiera_role() in MASTERFUL_PUPPET_ROLES:
        cnf = OLD_CONF_ROOT.format(port=str(port))
        group = 'mysqld'
    else:
        cnf = MYSQL_CNF_FILE
        group = 'mysqld{port}'.format(port=port)
    parser = ConfigParser.RawConfigParser(allow_no_value=True)
    if not os.path.exists(cnf):
        raise Exception("MySQL conf {cnf} does not exist".format(cnf=cnf))
    parser.read(cnf)

    try:
        value = parser.get(group, variable)
    except ConfigParser.NoOptionError:
        if '_' in variable:
            variable = variable.replace('_', '-')
            value = parser.get(group, variable)
        else:
            raise
    return value


def change_owner(directory, user, group):
    """ Chown a directory

    Args:
    directory - A string of the directored to be chown -R
    user - The string of the username of a user
    group - The string of the groupname of a group

    """
    path = os.path.realpath(directory)
    cmd = '/bin/chown -R {user}:{group} {path}'.format(user=user,
                                                       group=group,
                                                       path=path)
    log.debug(cmd)
    (out, err, ret) = shell_exec(cmd)
    if ret != 0:
        print err
        raise Exception("Error chown'ing directory:{err}".format(err=err))


def change_perms(directory, numeric_perms):
    """ Chmod a directory

    Aruments:
    directory - The directory to be chmod -R'ed
    numeric_perms - The numeric permissions desired
    """
    path = os.path.realpath(directory)
    cmd = '/bin/chmod -R {perms} {path}'.format(perms=str(numeric_perms),
                                                path=path)
    log.debug(cmd)
    (out, err, ret) = shell_exec(cmd)
    if ret != 0:
        print err
        raise Exception("Error chown'ing directory:{err}".format(err=err))


def clean_directory(directory):
    """ Remove all contents of a directory

    Args:
    directory - The directored to be emptied
    """
    for entry in os.listdir(directory):
        path = os.path.join(directory, entry)
        if os.path.isfile(path):
            os.unlink(path)
        if os.path.isdir(path):
            shutil.rmtree(path)


def get_local_instance_id():
    """ Get the aws instance_id

    Returns:
    A string with the aws instance_id or None
    """
    (out, err, ret) = shell_exec('ec2metadata --instance-id')
    if out.strip():
        return out.strip()
    else:
        return None


def get_hiera_role():
    """ Pull the hiera role for the localhost

    Returns:
    A string with the hiera role
    """
    if not os.path.exists(HIERA_ROLE_FILE):
        return DEFAULT_HIERA_ROLE

    with open(HIERA_ROLE_FILE) as f:
        return f.read().strip()


def get_pinfo_cloud():
    """ Get the value of the variable of pinfo_cloud from facter

    Returns pinfo_cloud
    """
    (std_out, std_err, return_code) = shell_exec('facter pinfo_cloud')

    if not std_out:
        return DEFAULT_PINFO_CLOUD

    return std_out.strip()


def get_instance_type():
    """ Get the name of the hardware type in use

    Returns:
    A string describing the hardware of the server
    """

    (std_out, std_err, return_code) = shell_exec('ec2metadata --instance-type')

    if not std_out:
        raise Exception('Could not determine hardware, error:'
                        '{std_err}'.format(std_err=std_err))

    return std_out.strip()


def get_iam_role():
    """ Get the IAM role for the local server

    Returns: The IAM role of the local server
    """
    buf = StringIO.StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://169.254.169.254/latest/meta-data/iam/info')
    c.setopt(c.WRITEFUNCTION, buf.write)
    c.perform()
    c.close()
    profile = json.loads(buf.getvalue())['InstanceProfileArn']
    return profile[(1 + profile.index("/")):]


def get_security_group():
    """ Get the Security group name for the local server

    Returns: security group name
    """
    buf = StringIO.StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'http://169.254.169.254/latest/meta-data/security-groups')
    c.setopt(c.WRITEFUNCTION, buf.write)
    c.perform()
    c.close()
    return buf.getvalue()


def get_security_role():
    """ Get the security role of the current environment

    Returns a security role
    """
    arn = boto3.client('sts').get_caller_identity().get('Arn')
    return re.search('.+assumed-role/([^/]+).+', arn).group(1)
