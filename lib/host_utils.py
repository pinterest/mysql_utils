import ConfigParser
import fcntl
import json
import multiprocessing
import os
import pycurl
import re
import shutil
import socket
import StringIO
import subprocess
import time
import getpass

import mysql_lib
from lib import environment_specific
from lib import timeout

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
MYSQL_MAX_WAIT = 120
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
PTHEARTBEAT_CMD = '/usr/sbin/service pt-heartbeat-{port} {action}'
ZK_CACHE = [MYSQL_DS_ZK, MYSQL_DR_ZK, MYSQL_GEN_ZK]

log = environment_specific.setup_logging_defaults(__name__)


def take_flock_lock(file_name):
    """ Take a flock for throw an exception

    Args:
    file_name - The name of the file to flock

    Returns:
    file_handle - This will be passed to release_flock_lock for relase
    """
    success = False
    try:
        with timeout.timeout(1):
            file_handle = open(file_name, 'w')
            fcntl.flock(file_handle.fileno(), fcntl.LOCK_EX)
            log.info('Lock taken')
            success = True
    except:
        pass
        # If success has not been set we will raise an exception just below

    if not success:
        raise Exception('Could not attain lock '
                        'on {file_name}'.format(file_name=file_name))
    return file_handle


def release_flock_lock(file_handle):
    """ Release a lock created by take_flock_lock

    Args:
    file_handle - The return from take_flock_lock()
    """
    fcntl.flock(file_handle.fileno(), fcntl.LOCK_UN)


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


def upgrade_auth_tables(port):
    """ Run mysql_upgrade

    Args:
    port - the port of the instance on localhost to act on
    """
    start_mysql(port,
                DEFAULTS_FILE_ARG.format(defaults_file=MYSQL_UPGRADE_CNF_FILE))
    socket = get_cnf_setting('socket', port)
    username, password = mysql_lib.get_mysql_user_for_role('admin')
    cmd = '' .join((MYSQL_UPGRADE, ' ',
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


class MysqlZookeeper:
    """Class for reading MySQL settings stored on the filesystem"""

    def get_ds_mysql_config(self):
        """ Query for Data Services MySQL shard mappings.

        Returns:
        A dict of all Data Services MySQL replication configuration.

        Example:
        {u'db00001': {u'db': None,
                  u'master': {u'host': u'sharddb001h',
                              u'port': 3306},
                  u'passwd': u'redacted',
                  u'slave': {u'host': u'sharddb001i',
                              u'port': 3306},
                  u'user': u'pbuser'},
        ...
        """
        with open(MYSQL_DS_ZK) as f:
            ds = json.loads(f.read())

        return ds

    def get_gen_mysql_config(self):
        """ Query for non-Data Services MySQL shard mappings.

        Returns:
        A dict of all non-Data Services MySQL replication configuration.

        Example:
        {u'abexperimentsdb001': {u'db': u'abexperimentsdb',
                                 u'master': {u'host': u'abexperimentsdb001a',
                                             u'port': 3306},
                                u'passwd': u'redacted',
                                u'slave': {u'host': u'abexperimentsdb001b',
                                           u'port': 3306},
                                u'user': u'redacted'},
        ...
        """
        with open(MYSQL_GEN_ZK) as f:
            gen = json.loads(f.read())

        return gen

    def get_dr_mysql_config(self):
        """ Query for disaster recovery MySQL shard mappings.

        Returns:
        A dict of all MySQL disaster recovery instances.

        Example:
        {u'db00018': {u'dr_slave': {u'host': u'sharddb018h', u'port': 3306}},
         u'db00015': {u'dr_slave': {u'host': u'sharddb015g', u'port': 3306}},
        ...
        """
        with open(MYSQL_DR_ZK) as f:
            dr = json.loads(f.read())

        return dr

    def get_all_mysql_config(self):
        """ Get all MySQL shard mappings.

        Returns:
        A dict of all MySQL replication configuration.

        Example:
        {u'db00001': {u'db': None,
                  u'master': {u'host': u'sharddb001h',
                              u'port': 3306},
                  u'passwd': u'redacted',
                  u'slave': {u'host': u'sharddb001i',
                              u'port': 3306},
                  u'dr_slave': {u'host': u'sharddb001j',
                              u'port': 3306},
                  u'user': u'pbuser'},
         u'abexperimentsdb001': {u'db': u'abexperimentsdb',
                                 u'master': {u'host': u'abexperimentsdb001a',
                                             u'port': 3306},
                                u'passwd': u'redacted',
                                u'slave': {u'host': u'abexperimentsdb001b',
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
        """ Query for all MySQL dr_slaves

        Args:
        repl_type - A replica type, valid options are entries in REPLICA_TYPES

        Returns:
        A list of all MySQL instances of the type repl_type

        Example:
        set([u'sharddb017g:3306',
             u'sharddb014d:3306',
             u'sharddb004h:3306',
        """

        if repl_type not in REPLICA_TYPES:
            raise Exception('Invalid repl_type {repl_type}. Valid options are'
                            '{REPLICA_TYPES}'.format(repl_type=repl_type,
                                                     REPLICA_TYPES=REPLICA_TYPES))
        hosts = set()
        for replica_set in self.get_all_mysql_config().iteritems():
            if repl_type in replica_set[1]:
                host = replica_set[1][repl_type]
                hostaddr = HostAddr(':'.join((host['host'],
                                              str(host['port']))))
                hosts.add(hostaddr)

        return hosts

    def get_all_mysql_instances(self):
        """ Query ZooKeeper for all MySQL instances

        Returns:
        A list of all MySQL instances.

        Example:
        set([u'sharddb017g:3306',
             u'sharddb017h:3306',
             u'sharddb004h:3306',
        """

        hosts = set()
        config = self.get_all_mysql_config()
        for replica_set in config:
            for rtype in REPLICA_TYPES:
                if rtype in config[replica_set]:
                    host = config[replica_set][rtype]
                    hostaddr = HostAddr(''.join((host['host'],
                                                 ':',
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
                            '{REPLICA_TYPES}'.format(repl_type=repl_type,
                                                     REPLICA_TYPES=REPLICA_TYPES))

        all_config = self.get_all_mysql_config()
        if replica_set not in all_config:
            raise Exception('Unknown replica set '
                            '{replica_set}'.format(replica_set=replica_set))

        if repl_type not in all_config[replica_set]:
            return None

        instance = all_config[replica_set][repl_type]
        hostaddr = HostAddr(':'.join((instance['host'],
                                     str(instance['port']))))
        return hostaddr

    def get_replica_set_from_instance(self, instance, rtypes=REPLICA_TYPES):
        """ Get the replica set based on zk info

        Args:
        instance - a hostaddr object
        rtypes - a list of replica types to check, default is REPLICA_TYPES

        Returns:
        (replica_set, replica_type)
        replica_set - A replica set which the instance is part
        replica_type - The role of the instance in the replica_set
        """
        config = self.get_all_mysql_config()
        for replica_set in config:
            for rtype in rtypes:
                if rtype in config[replica_set]:
                    if (instance.hostname == config[replica_set][rtype]['host'] and
                            instance.port == config[replica_set][rtype]['port']):
                        return (replica_set, rtype)
        raise Exception('{instance} is not in zk for replication '
                        'role(s): {rtypes}'.format(instance=instance,
                                                   rtypes=rtypes))

    def get_host_shard_map(self, repl_type=REPLICA_ROLE_MASTER):
        """ Get a mapping of what shards exist on MySQL master servers

        Args:
        repl_type: optionally specify a replica type

        Returns:
        A dict with a key of the MySQL master instance and the value a set
        of shards
        """
        global_shard_map = dict()
        for sharded_db in environment_specific.SHARDED_DBS_PREFIX_MAP.values():
            shard_map = self.compute_shard_map(sharded_db['mappings'],
                                               sharded_db['prefix'],
                                               sharded_db['zpad'])
            for entry in shard_map:
                if entry in global_shard_map:
                    global_shard_map[entry].update(shard_map[entry])
                else:
                    global_shard_map[entry] = shard_map[entry]

        host_shard_map = dict()
        for replica_set in global_shard_map:
            instance = self.get_mysql_instance_from_replica_set(replica_set,
                                                                repl_type)
            host_shard_map[instance.__str__()] = global_shard_map[replica_set]

        return host_shard_map

    def compute_shard_map(self, mapping, prefix, zpad):
        """ Get mapping of shards to replica_sets

        Args:
        mapping - A list of dicts representing shard ranges mapping to
                  a replica set. Example:
                  {'range':(    0,   63), 'host':'db00001'}
        preface - The preface of the db name. Formula for dbname is
                  preface + z padded shard number
        zpad - The amount of z padding to use

        Returns:
        A dict with a key of the replica set name and the value being
        a set of strings which are shard names
        """
        shard_mapping = dict()
        # Note there may be multiple ranges for each replica set
        for replica_set in mapping:
            for shard_num in range(replica_set['range'][0],
                                   replica_set['range'][1] + 1):
                shard_name = ''.join((prefix, str(shard_num).zfill(zpad)))
                # Note: host in this context means replica set name
                if replica_set['host'] not in shard_mapping:
                    shard_mapping[replica_set['host']] = set()
                shard_mapping[replica_set['host']].add(shard_name)

        return shard_mapping

    def shard_to_instance(self, shard, repl_type=REPLICA_ROLE_MASTER):
        """ Convert a shard to  hostname

        Args:
        shard - A shard name
        repl_type - Replica type, master is default

        Returns:
        A hostaddr object for an instance of the replica set
        """
        shard_map = self.get_host_shard_map(repl_type)
        for instance in shard_map:
            if shard in shard_map[instance]:
                return HostAddr(instance)

        raise Exception('Could not determine shard replica set for shard {shard}'.format(shard=shard))

    def get_shards_by_shard_type(self, shard_type):
        """ Get a set of all shards in a shard type

        Args:
        shard_type - The type of shards, i.e. 'sharddb'

        Returns:
        A set of all shard names
        """
        sharding_info = environment_specific.SHARDED_DBS_PREFIX_MAP[shard_type]
        shards = set()
        for replica_set in sharding_info['mappings']:
            for shard_num in range(replica_set['range'][0],
                                   replica_set['range'][1] + 1):
                shards.add(''.join((sharding_info['prefix'],
                                    str(shard_num).zfill(sharding_info['zpad']))))
        return shards


class HostAddr:
    """Basic abtraction for hostnames"""
    def __init__(self, host):
        """
        Args:
        host - A hostname. We have two fully supported formats:
               {replicaType}-{replicaSetNum}-{hostNum} - new style
               {replicaType}{replicaSetNum}{hostLetter} - old style
        """
        self.replica_type = None
        self.replica_set_num = None
        self.host_identifier = None

        host_params = re.split(':', host)
        self.hostname = re.split('\.', host_params[0])[0]
        if len(host_params) > 1:
            self.port = int(host_params[1])
        else:
            self.port = 3306

        # New style hostnames are of the form replicaType-replicaSetNum-hostNum
        # ie: sharddb-1-1
        try:
            (self.replica_type, self.replica_set_num,
             self.host_identifier) = self.hostname.split('-')
        except ValueError:
            # Maybe a old sytle hostname
            # form is replicaTypereplicaSetNumhostLetter
            # ie: sharddb001a
            replica_set_match = re.match('([a-zA-z]+)0+([0-9]+)([a-z])', self.hostname)
            if replica_set_match:
                try:
                    (self.replica_type, self.replica_set_num, self.host_identifier)\
                        = replica_set_match.groups()
                except ValueError:
                    # Not an old style hostname either, weird.
                    pass
            else:
                replica_set_match = re.match('([a-zA-z0-9]+db)0+([0-9]+)([a-z])', self.hostname)
                try:
                    (self.replica_type, self.replica_set_num, self.host_identifier)\
                        = replica_set_match.groups()
                    self.replica_type = ''.join((self.replica_type, 'db'))
                except:
                    pass

    def get_standardized_replica_set(self):
        """ Return an easily parsible replica set name

        Returns:
        A replica set name which is hyphen seperated with the first part
        being the replica set type (ie sharddb), followed by a the replica
        set identifier. If this is not availible, return None.
        """
        if self.replica_type and self.replica_set_num:
            return '-'.join((self.replica_type, self.replica_set_num))
        else:
            return None

    def get_zk_replica_set(self):
        """ Determine what replica set a host would belong to

        Returns:
        A replica set name
        """
        zk = MysqlZookeeper()
        for master in zk.get_all_mysql_instances_by_type(REPLICA_ROLE_MASTER):
            if self.get_standardized_replica_set() == master.get_standardized_replica_set():
                return zk.get_replica_set_from_instance(master)

    def __str__(self):
        """
        Returns
        a  human readible string version of object similar to
        'shardb123a:3309'
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
    cmd - String to execute via a shell. DO NOT use this if the stdout or stderr
          will be very large (more than 100K bytes or so)

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
                            ''.format(proc_id=multiprocessing.current_process().name,
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
