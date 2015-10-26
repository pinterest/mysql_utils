import json
import datetime
import MySQLdb
import MySQLdb.cursors
import re
import time
import warnings

import _mysql_exceptions

import host_utils
import mysql_connect
from lib import environment_specific


# Max IO thread lag in bytes. If more than MAX_IO_LAG refuse to modify zk, etc
# 10k bytes of lag is just a few seconds normally
MAX_IO_LAG = 10485760
# Max lag in seconds. If more than MAX_HEARTBEAT_LAG refuse to modify zk, etc
MAX_HEARTBEAT_LAG = 120
HEARTBEAT_SAFETY_MARGIN = 10

AUTH_FILE = '/var/config/config.services.mysql_auth'
CONNECT_TIMEOUT = 2
METADATA_DB = 'test'
MYSQLADMIN = '/usr/bin/mysqladmin'
MYSQL_ERROR_ACCESS_DENIED = 1045
MYSQL_ERROR_CONN_HOST_ERROR = 2003
MYSQL_ERROR_HOST_ACCESS_DENIED = 1130
MYSQL_ERROR_NO_SUCH_TABLE = 1146
MYSQL_ERROR_NO_DEFINED_GRANT = 1141
MYSQL_ERROR_NO_SUCH_THREAD = 1094
MYSQL_ERROR_UNKNOWN_VAR = 1193
MYSQL_ERROR_FUNCTION_EXISTS = 1125
MYSQL_VERSION_COMMAND = '/usr/sbin/mysqld --version'


class ReplicationError(Exception):
    pass


class AuthError(Exception):
    pass


class InvalidVariableForOperation(Exception):
    pass


log = environment_specific.setup_logging_defaults(__name__)


def get_all_mysql_grants():
    """Fetch all MySQL grants

    Returns:
    A dictionary describing all MySQL grants.

    Example:
    {'`admin2`@`%`': {'grant_option': True,
                  'password': u'REDACTED',
                  'privileges': u'ALL PRIVILEGES',
                  'source_host': '%',
                  'username': u'admin2'},
     '`admin`@`%`': {'grant_option': True,
                 'password': u'REDACTED',
                 'privileges': u'ALL PRIVILEGES',
                 'source_host': '%',
                 'username': u'admin'},
     '`etl2`@`%`': {'grant_option': False,
                'password': u'REDACTED',
                'privileges': u'SELECT, SHOW DATABASES',
                'source_host': '%',
                'username': u'etl2'},
    ...
    """
    grants = {}
    for _, role in get_mysql_auth_roles().iteritems():
        source_hosts = role.get('source_hosts', '%')
        grant_option = role.get('grant_option', False)
        privileges = role['privileges']

        for user in role['users']:
            key = '`{user}`@`{host}`'.format(user=user['username'],
                                             host=source_hosts)

            if key in grants.keys():
                raise AuthError('Duplicate username defined for %s' % key)
            grants[key] = {'username': user['username'].encode('ascii', 'ignore'),
                           'password': user['password'].encode('ascii', 'ignore'),
                           'privileges': privileges.encode('ascii', 'ignore'),
                           'grant_option': grant_option,
                           'source_host': source_hosts.encode('ascii', 'ignore')}
    return grants


def get_mysql_user_for_role(role):
    """Fetch the credential for a role from a mysql role

    Args:
    role - a string of the name of the mysql role to use for username/password

    Returns:
    username - string of the username enabled for the role
    password - string of the password enabled for the role
    """
    grants = get_mysql_auth_roles()[role]
    for user in grants['users']:
        if user['enabled']:
            return user['username'], user['password']


def get_mysql_auth_roles():
    """Get all mysql roles from zk updater

    Returns:
    a dict describing the replication status.

    Example:
    {u'dataLayer': {u'privileges': u'SELECT',
                    u'users': [
                        {u'username': u'pbdataLayer',
                         u'password': u'REDACTED',
                         u'enabled': True},
                        {u'username': u'pbdataLayer2',
                         u'password': u'REDACTED',
                         u'enabled': False}]},
...
"""
    with open(AUTH_FILE) as f:
        json_grants = json.loads(f.read())
    return json_grants


def connect_mysql(instance, role='admin'):
    """Connect to a MySQL instance as admin

    Args:
    hostaddr - object describing which mysql instance to connect to
    role - a string of the name of the mysql role to use. A bootstrap role can
           be called for MySQL instances lacking any grants. This user does not
           exit in zk.

    Returns:
    a connection to the server as administrator
    """
    if role == 'bootstrap':
        socket = host_utils.get_cnf_setting('socket', instance.port)
        username = 'root'
        password = ''
        db = MySQLdb.connect(unix_socket=socket,
                             user=username,
                             passwd=password,
                             cursorclass=MySQLdb.cursors.DictCursor)

    else:
        username, password = get_mysql_user_for_role(role)
        db = MySQLdb.connect(host=instance.hostname,
                             port=instance.port,
                             user=username,
                             passwd=password,
                             cursorclass=MySQLdb.cursors.DictCursor,
                             connect_timeout=CONNECT_TIMEOUT)
    return db


def get_master_from_instance(instance):
    """ Determine if an instance thinks it is a slave and if so from where

    Args:
    instance - A hostaddr object

    Returns:
    master - A hostaddr object or None
    """
    conn = connect_mysql(instance, 'admin')
    try:
        ss = get_slave_status(conn)
    except ReplicationError:
        return None

    return host_utils.HostAddr(''.join((ss['Master_Host'],
                                        ':',
                                        str(ss['Master_Port']))))


def get_slave_status(conn):
    """ Get MySQL replication status

    Args:
    db - a connection to the server as administrator

    Returns:
    a dict describing the replication status.

    Example:
    {'Connect_Retry': 60L,
     'Exec_Master_Log_Pos': 98926487L,
     'Last_Errno': 0L,
     'Last_Error': '',
     'Last_IO_Errno': 0L,
     'Last_IO_Error': '',
     'Last_SQL_Errno': 0L,
     'Last_SQL_Error': '',
     'Master_Host': 'sharddb015e',
     'Master_Log_File': 'mysql-bin.000290',
     'Master_Port': 3306L,
     'Master_SSL_Allowed': 'No',
     'Master_SSL_CA_File': '',
     'Master_SSL_CA_Path': '',
     'Master_SSL_Cert': '',
     'Master_SSL_Cipher': '',
     'Master_SSL_Key': '',
     'Master_SSL_Verify_Server_Cert': 'No',
     'Master_Server_Id': 946544731L,
     'Master_User': 'replicant',
     'Read_Master_Log_Pos': 98926487L,
     'Relay_Log_File': 'mysqld_3306-relay-bin.000237',
     'Relay_Log_Pos': 98926633L,
     'Relay_Log_Space': 98926838L,
     'Relay_Master_Log_File': 'mysql-bin.000290',
     'Replicate_Do_DB': '',
     'Replicate_Do_Table': '',
     'Replicate_Ignore_DB': '',
     'Replicate_Ignore_Server_Ids': '',
     'Replicate_Ignore_Table': '',
     'Replicate_Wild_Do_Table': '',
     'Replicate_Wild_Ignore_Table': '',
     'Seconds_Behind_Master': 0L,
     'Skip_Counter': 0L,
     'Slave_IO_Running': 'Yes',
     'Slave_IO_State': 'Waiting for master to send event',
     'Slave_SQL_Running': 'Yes',
     'Until_Condition': 'None',
     'Until_Log_File': '',
     'Until_Log_Pos': 0L}
    """
    cursor = conn.cursor()
    cursor.execute("SHOW SLAVE STATUS")
    slave_status = cursor.fetchone()
    if slave_status is None:
        raise ReplicationError('Server is not a replica')
    return slave_status


def get_master_status(conn):
    """ Get poisition of most recent write to master replication logs

    Args:
    db - a connection to the server as administrator

    Returns:
    a dict describing the master status

    Example:
    {'Binlog_Do_DB': '',
     'Binlog_Ignore_DB': '',
     'File': 'mysql-bin.019324',
     'Position': 61559L}
    """
    cursor = conn.cursor()
    cursor.execute("SHOW MASTER STATUS")
    master_status = cursor.fetchone()
    if master_status is None:
        raise ReplicationError('Server is not setup to write replication logs')
    return master_status


def get_master_logs(conn):
    """ Get MySQL binary log names and size

    Args
    db - a connection to the server as administrator

    Returns
    A tuple of dicts describing the replication status.

    Example:
    ({'File_size': 104857859L, 'Log_name': 'mysql-bin.000281'},
     {'File_size': 104858479L, 'Log_name': 'mysql-bin.000282'},
     {'File_size': 104859420L, 'Log_name': 'mysql-bin.000283'},
     {'File_size': 104859435L, 'Log_name': 'mysql-bin.000284'},
     {'File_size': 104858059L, 'Log_name': 'mysql-bin.000285'},
     {'File_size': 104859233L, 'Log_name': 'mysql-bin.000286'},
     {'File_size': 104858895L, 'Log_name': 'mysql-bin.000287'},
     {'File_size': 104858039L, 'Log_name': 'mysql-bin.000288'},
     {'File_size': 104858825L, 'Log_name': 'mysql-bin.000289'},
     {'File_size': 104857726L, 'Log_name': 'mysql-bin.000290'},
     {'File_size': 47024156L, 'Log_name': 'mysql-bin.000291'})
    """
    cursor = conn.cursor()
    cursor.execute("SHOW MASTER LOGS")
    master_status = cursor.fetchall()
    return master_status


def calc_binlog_behind(log_file_num, log_file_pos, master_logs):
    """ Calculate replication lag in bytes

    Args:
    log_file_num - The integer of the binlog
    log_file_pos - The position inside of log_file_num
    master_logs - A tuple of dicts describing the replication status

    Returns:
    bytes_behind - bytes of lag across all log file
    binlogs_behind - number of binlogs lagged
    """

    binlogs_behind = 0
    bytes_behind = 0
    for binlog in master_logs:
        _, binlog_num = re.split('\.', binlog['Log_name'])
        if binlog_num >= log_file_num:
            if binlog_num == log_file_num:
                bytes_behind += binlog['File_size'] - log_file_pos
            else:
                binlogs_behind += 1
                bytes_behind += binlog['File_size']
    return bytes_behind, binlogs_behind


def get_global_variables(conn):
    """ Get MySQL global variables

    Args:
    conn - a connection to the MySQL instance

    Returns:
    A dict with the key the variable name
    """

    ret = dict()
    cursor = conn.cursor()
    cursor.execute("SHOW GLOBAL VARIABLES")
    list_variables = cursor.fetchall()
    for entry in list_variables:
        ret[entry['Variable_name']] = entry['Value']

    return ret


def get_dbs(conn):
    """ Get MySQL databases other than mysql, information_schema,
    performance_schema and test

    Args:
    conn - a connection to the MySQL instance

    Returns
    A set of databases
    """

    ret = set()
    cursor = conn.cursor()
    cursor.execute(' '.join(("SELECT schema_name",
                             "FROM information_schema.schemata",
                             "WHERE schema_name NOT IN('mysql',",
                             "                         'information_schema',",
                             "                         'performance_schema',",
                             "                         'test')",
                             "ORDER BY schema_name")))
    dbs = cursor.fetchall()
    for db in dbs:
        ret.add(db['schema_name'])
    return ret


def does_table_exist(conn, db, table):
    """ Return True if a given table exists in a given database.

    Args:
        conn - A connection to the MySQL instance
        db - A string that contains the database name we're looking for
        table - A string containing the name of the table we're looking for

    Returns:
        True if the table was found.
        False if not or there was an exception.
    """
    table_exists = False
    cursor = conn.cursor()
    try:
        sql = ("SELECT COUNT(*) AS cnt FROM information_schema.tables "
               "WHERE table_schema=%(db)s AND table_name=%(tbl)s")
        cursor.execute(sql, {'db': db, 'tbl': table})
        row = cursor.fetchone()
        if row['cnt'] == 1:
            table_exists = True
    except:
        # If it doesn't work, we can't know anything about the
        # state of the table.
        log.info('Ignoring an error checking for existance of '
                 '{db}.{table}'.format(db=db, table=table))

    return table_exists


def get_tables(conn, db, skip_views=False):
    """ Get a list of tables and views in a given database or just
        tables.  Default to include views so as to maintain backward
        compatibility.

    Args:
    conn - a connection to the MySQL instance
    db - a string which contains a name of a db
    skip_views - true if we want tables only, false if we want everything

    Returns
    A set of tables
    """
    ret = set()
    param = {'db': db}
    cursor = conn.cursor()
    sql = ''.join(("SELECT TABLE_NAME ",
                   "FROM information_schema.tables ",
                   "WHERE TABLE_SCHEMA=%(db)s "))
    if skip_views:
        sql = sql + ' AND TABLE_TYPE="BASE TABLE" '

    cursor.execute(sql, param)
    tables = cursor.fetchall()
    for table in tables:
        ret.add(table['TABLE_NAME'])

    return ret


def setup_semisync_plugins(instance):
    """ Install the semi-sync replication plugins.  We may or may
        not actually use them on any given replica set, but this
        ensures that we've got them.  Semi-sync exists on all versions
        of MySQL that we might support, but we'll never use it on 5.5.

        Args:
        instance - A hostaddr object
    """
    conn = connect_mysql(instance)
    version = get_global_variables(conn)['version']
    if version[0:3] == '5.5':
        return

    cursor = conn.cursor()
    try:
        cursor.execute("INSTALL PLUGIN rpl_semi_sync_master SONAME 'semisync_master.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise
        # already loaded, no work to do

    try:
        cursor.execute("INSTALL PLUGIN rpl_semi_sync_slave SONAME 'semisync_slave.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise


def setup_response_time_metrics(instance):
    """ Add Query Response Time Plugins

    Args:
    instance -  A hostaddr object
    """
    conn = connect_mysql(instance)
    version = get_global_variables(conn)['version']
    if version[0:3] != '5.6':
        return

    cursor = conn.cursor()
    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME_AUDIT SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise
        # already loaded, no work to do

    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise

    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME_READ SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise

    try:
        cursor.execute("INSTALL PLUGIN QUERY_RESPONSE_TIME_WRITE SONAME 'query_response_time.so'")
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_FUNCTION_EXISTS:
            raise
    cursor.execute("SET GLOBAL QUERY_RESPONSE_TIME_STATS=ON")


def enable_and_flush_activity_statistics(conn):
    """ Reset counters for table statistics

    Args:
    conn - a connection to the MySQL instance
    """
    global_vars = get_global_variables(conn)
    if global_vars['userstat'] != 'ON':
        set_global_variable(conn, 'userstat', True)
    cursor = conn.cursor()

    sql = 'FLUSH TABLE_STATISTICS'
    log.info(sql)
    cursor.execute(sql)

    sql = 'FLUSH USER_STATISTICS'
    log.info(sql)
    cursor.execute(sql)


def get_dbs_activity(conn):
    """ Return rows read and changed from a MySQL instance by db

    Args:
    conn - a connection to the MySQL instance

    Returns:
    A dict with a key of the db name and entries for rows read and rows changed
    """
    global_vars = get_global_variables(conn)
    if global_vars['userstat'] != 'ON':
        raise InvalidVariableForOperation('Userstats must be enabled on ',
                                          'for table_statistics to function. '
                                          'Perhaps run "SET GLOBAL userstat = '
                                          'ON" to fix this.')
    ret = dict()
    cursor = conn.cursor()

    sql = ("SELECT SCHEMA_NAME, "
           "    SUM(ROWS_READ) AS 'ROWS_READ', "
           "    SUM(ROWS_CHANGED) AS 'ROWS_CHANGED' "
           "FROM information_schema.SCHEMATA "
           "LEFT JOIN information_schema.TABLE_STATISTICS "
           "    ON SCHEMA_NAME=TABLE_SCHEMA "
           "GROUP BY SCHEMA_NAME ")
    cursor.execute(sql)
    raw_activity = cursor.fetchall()
    for row in raw_activity:
        if row['ROWS_READ'] is None:
            row['ROWS_READ'] = 0

        if row['ROWS_CHANGED'] is None:
            row['ROWS_CHANGED'] = 0

        ret[row['SCHEMA_NAME']] = {'ROWS_READ': int(row['ROWS_READ']),
                                   'ROWS_CHANGED': int(row['ROWS_CHANGED'])}
    return ret


def get_user_activity(conn):
    """ Return information about activity broken down by mysql user accoutn

    Args:
    conn - a connection to the MySQL instance

    Returns:
    a dict of user activity since last flush
    """
    global_vars = get_global_variables(conn)
    if global_vars['userstat'] != 'ON':
        raise InvalidVariableForOperation('Userstats must be enabled on ',
                                          'for table_statistics to function. '
                                          'Perhaps run "SET GLOBAL userstat = '
                                          'ON" to fix this.')
    ret = dict()
    cursor = conn.cursor()

    sql = 'SELECT * FROM information_schema.USER_STATISTICS'
    cursor.execute(sql)
    raw_activity = cursor.fetchall()
    for row in raw_activity:
        user = row['USER']
        del(row['USER'])
        ret[user] = row

    return ret


def get_connected_users(conn):
    """ Get all currently connected users

    Args:
    conn - a connection to the MySQL instance

    Returns:
    a set of users
    """
    cursor = conn.cursor()
    sql = ("SELECT user "
           "FROM information_schema.processlist "
           "GROUP BY user")
    cursor.execute(sql)
    results = cursor.fetchall()

    ret = set()
    for result in results:
        ret.add(result['user'])

    return ret


def show_create_table(conn, db, table, standardize=True):
    """ Get a standardized CREATE TABLE statement

    Args:
    conn - a connection to the MySQL instance
    db - the MySQL database to run against
    table - the table on the db database to run against
    standardize - Remove AUTO_INCREMENT=$NUM and similar

    Returns:
    A string of the CREATE TABLE statement
    """

    cursor = conn.cursor()
    try:
        cursor.execute('SHOW CREATE TABLE `{db}`.`{table}`'.format(table=table,
                                                                   db=db))
        ret = cursor.fetchone()['Create Table']
        if standardize is True:
            ret = re.sub('AUTO_INCREMENT=[0-9]+ ', '', ret)
    except MySQLdb.ProgrammingError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_NO_SUCH_TABLE:
            raise
        ret = ''

    return ret


def create_db(conn, db):
    """ Create a database if it does not already exist

    Args:
    conn - a connection to the MySQL instance
    db - the name of the to be created
    """
    cursor = conn.cursor()
    sql = ('CREATE DATABASE IF NOT EXISTS '
           '`{db}`;'.format(db=db))
    log.info(sql)

    # We don't care if the db already exists and this was a no-op
    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    cursor.execute(sql)
    warnings.resetwarnings()


def copy_db_schema(conn, old_db, new_db, verbose=False, dry_run=False):
    """ Copy the schema of one db into a different db

    Args:
    conn - a connection to the MySQL instance
    old_db - the source of the schema copy
    new_db - the destination of the schema copy
    verbose - print out SQL commands
    dry_run - do not change any state
    """
    cursor = conn.cursor()
    tables = get_tables(conn, old_db)
    for table in tables:
        raw_sql = "CREATE TABLE IF NOT EXISTS `{new_db}`.`{table}` LIKE `{old_db}`.`{table}`"
        sql = raw_sql.format(old_db=old_db, new_db=new_db, table=table)
        if verbose:
            print sql

        if not dry_run:
            cursor.execute(sql)


def move_db_contents(conn, old_db, new_db, verbose=False, dry_run=False):
    """ Move the contents of one db into a different db

    Args:
    conn - a connection to the MySQL instance
    old_db - the source from which to move data
    new_db - the destination to move data
    verbose - print out SQL commands
    dry_run - do not change any state
    """
    cursor = conn.cursor()
    tables = get_tables(conn, old_db)
    for table in tables:
        raw_sql = "RENAME TABLE `{old_db}`.`{table}` to `{new_db}`.`{table}`"
        sql = raw_sql.format(old_db=old_db, new_db=new_db, table=table)
        if verbose:
            print sql

        if not dry_run:
            cursor.execute(sql)


def setup_replication(new_master, new_replica):
    """ Set an instance as a slave of another

    Args:
    new_master - A hostaddr object for the new master
    new_slave - A hostaddr object for the new slave
    """
    log.info('Setting {new_replica} as a replica of new master '
             '{new_master}'.format(new_master=new_master,
                                   new_replica=new_replica))
    new_master_conn = connect_mysql(new_master)
    new_master_coordinates = get_master_status(new_master_conn)
    change_master(new_replica, new_master,
                  new_master_coordinates['File'],
                  new_master_coordinates['Position'])


def restart_replication(conn):
    """ Stop then start replication

    Args:
    conn - a connection to the replica
    """
    cursor = conn.cursor()
    cursor.execute('STOP SLAVE')
    cursor.execute('START SLAVE')
    time.sleep(1)


def reset_slave(conn):
    """ Stop replicaion and remove all repl settings

    Args:
    conn - a connection to the replica
    """
    cursor = conn.cursor()

    try:
        ss = get_slave_status(conn)
        log.info(('Previous replication settings:', ss))
        if ss['Slave_IO_Running'] and ss['Slave_SQL_Running']:
            cmd = 'STOP SLAVE'
        else:
            if ss['Slave_IO_Running']:
                cmd = 'STOP SLAVE IO_THREAD'
            if ss['Slave_SQL_Running']:
                cmd = 'STOP SLAVE SQL_THREAD'
        log.info(cmd)
        cursor.execute(cmd)
        cmd = 'RESET SLAVE ALL'
        log.info(cmd)
        cursor.execute(cmd)
    except ReplicationError:
        # SHOW SLAVE STATUS failed, previous state does not matter so pass
        pass


def change_master(slave_hostaddr, master_hostaddr, master_log_file,
                  master_log_pos, no_start=False):
    """ Setup MySQL replication on new replica

    Args:
    slave_hostaddr -  hostaddr object for the new replica
    hostaddr - A hostaddr object for the master db
    master_log_file - Replication log file to begin streaming
    master_log_pos - Position in master_log_file
    no_start - Don't run START SLAVE after CHANGE MASTER
    """
    conn = connect_mysql(slave_hostaddr)
    set_global_variable(conn, 'read_only', True)
    reset_slave(conn)
    cursor = conn.cursor()
    master_user, master_password = get_mysql_user_for_role('replication')
    parameters = {'master_user': master_user,
                  'master_password': master_password,
                  'master_host': master_hostaddr.hostname,
                  'master_port': master_hostaddr.port,
                  'master_log_file': master_log_file,
                  'master_log_pos': master_log_pos}
    sql = ''.join(("CHANGE MASTER TO "
                   "MASTER_USER=%(master_user)s, "
                   "MASTER_PASSWORD=%(master_password)s, "
                   "MASTER_HOST=%(master_host)s, "
                   "MASTER_PORT=%(master_port)s, "
                   "MASTER_LOG_FILE=%(master_log_file)s, "
                   "MASTER_LOG_POS=%(master_log_pos)s "))
    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    cursor.execute(sql, parameters)
    warnings.resetwarnings()
    log.info(cursor._executed)

    if not no_start:
        cmd = 'START SLAVE'
        log.info(cmd)
        cursor.execute(cmd)
        # Replication reporting is wonky for the first second
        time.sleep(1)
        replication = calc_slave_lag(slave_hostaddr)

        if replication['ss']['Slave_SQL_Running'] != 'Yes':
            raise Exception("SQL thread is not running")
        if replication['ss']['Slave_IO_Running'] != 'Yes':
            raise Exception("IO thread is not running")


def wait_replication_catch_up(slave_hostaddr):
    """ Watch replication until it is caught up

    Args:
    slave_hostaddr - A HostAddr object
    """
    last_sbm = None
    catch_up_sbm = MAX_HEARTBEAT_LAG - HEARTBEAT_SAFETY_MARGIN
    remaining_time = 'Not yet availible'
    sleep_duration = 60
    while True:
        replication = calc_slave_lag(slave_hostaddr)
        if replication['sql_bytes'] is None:
            log.info(replication)
            raise Exception("Could not compute replication lag")

        if replication['ss']['Slave_IO_Running'] != 'Yes':
            log.warning('IO thread is not running, going to sleep 15 '
                        'seconds in case things get better on their own')
            time.sleep(15)
            replication = calc_slave_lag(slave_hostaddr)
            if replication['ss']['Slave_IO_Running'] != 'Yes':
                raise Exception("IO thread is not running")

        if replication['ss']['Slave_SQL_Running'] != 'Yes':
            raise Exception("SQL thread is not running")

        if replication['sbm'] < catch_up_sbm:
            log.info('Replication computed seconds behind master {sbm} < '
                     '{catch_up_sbm}'.format(sbm=replication['sbm'],
                                             catch_up_sbm=catch_up_sbm))
            break
        else:
            if last_sbm:
                catch_up_rate = (last_sbm - replication['sbm']) / sleep_duration
                if catch_up_rate > 0:
                    remaining_time = datetime.timedelta(seconds=((replication['sbm'] - catch_up_sbm) / catch_up_rate))
                else:
                    remaining_time = '> heat death of the universe'
            log.info('Replication is lagged by {sbm} seconds, waiting '
                     'for < {catch_up}. Guestimate time to catch up: {eta}'
                     ''.format(sbm=replication['sbm'],
                               catch_up=catch_up_sbm,
                               eta=str(remaining_time)))
            last_sbm = replication['sbm']
            if last_sbm > 24 * 60 * 60:
                sleep_duration = 5 * 60
            elif last_sbm > 60 * 60:
                sleep_duration = 60
            else:
                sleep_duration = 5
            time.sleep(sleep_duration)


def calc_slave_lag(slave_hostaddr, dead_master=False):
    """Determine MySQL replication lag in bytes and binlogs

    Args:
    slave_hostaddr - object of host:port for the replica

    Returns:
    io_binlogs - Number of undownloaded binlogs. This is only slightly useful
                 as io_bytes spans binlogs. It mostly exists for dba amussement
    io_bytes - Bytes of undownloaded replication logs.
    sbm - Number of seconds of replication lag as determined by computing
          the difference between current time and what exists in a heartbeat
          table as populated by replication
    sql_binlogs - Number of unprocessed binlogs. This is only slightly useful
                  as sql_bytes spans binlogs. It mostly exists for dba
                  amussement
    sql_bytes - Bytes of unprocessed replication logs
    ss - None or the results of running "show slave status'
    """

    ret = {'sql_bytes': 'INVALID',
           'sql_binlogs': 'INVALID',
           'io_bytes': 'INVALID',
           'io_binlogs': 'INVALID',
           'sbm': 'INVALID',
           'ss': {'Slave_IO_Running': 'INVALID',
                  'Slave_SQL_Running': 'INVALID',
                  'Master_Host': 'INVALID',
                  'Master_Port': 'INVALID'}}
    try:
        slave_conn = connect_mysql(slave_hostaddr)
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code == MYSQL_ERROR_CONN_HOST_ERROR:
            return ret
        else:
            raise

    try:
        ss = get_slave_status(slave_conn)
    except ReplicationError:
        return ret

    ret['ss'] = ss
    slave_sql_pos = ss['Exec_Master_Log_Pos']
    slave_sql_binlog = ss['Relay_Master_Log_File']
    _, slave_sql_binlog_num = re.split('\.', slave_sql_binlog)
    slave_io_pos = ss['Read_Master_Log_Pos']
    slave_io_binlog = ss['Master_Log_File']
    _, slave_io_binlog_num = re.split('\.', slave_io_binlog)

    master_hostaddr = host_utils.HostAddr(':'.join((ss['Master_Host'],
                                                    str(ss['Master_Port']))))
    if not dead_master:
        try:
            master_conn = connect_mysql(master_hostaddr)
            master_logs = get_master_logs(master_conn)

            (ret['sql_bytes'], ret['sql_binlogs']) = calc_binlog_behind(slave_sql_binlog_num,
                                                                        slave_sql_pos,
                                                                        master_logs)
            (ret['io_bytes'], ret['io_binlogs']) = calc_binlog_behind(slave_io_binlog_num,
                                                                      slave_io_pos,
                                                                      master_logs)
        except _mysql_exceptions.OperationalError as detail:
            (error_code, msg) = detail.args
            if error_code != MYSQL_ERROR_CONN_HOST_ERROR:
                raise
            # we can compute real lag because the master is dead

    try:
        ret['sbm'] = calc_alt_sbm(slave_conn, ss)
    except MySQLdb.ProgrammingError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_NO_SUCH_TABLE:
            raise
        # We can not compute a real sbm, so the caller will get
        # None default
        pass
    return ret


def calc_alt_sbm(conn, slave_status):
    """ Calculate seconds behind using heartbeat + time on slave server

    Args:
    conn - a connection to the MySQL instance
    slave_status - a dict of slave status

    Returns:
    An int of the calculated seconds behind master or None
    """
    cursor = conn.cursor()
    sql = ''.join(("SELECT TIMESTAMPDIFF(SECOND,ts, NOW()) AS 'sbm' "
                   "FROM {METADATA_DB}.heartbeat "
                   "WHERE server_id= %(Master_Server_Id)s"))

    cursor.execute(sql.format(METADATA_DB=METADATA_DB), slave_status)
    row = cursor.fetchone()
    if row:
        return row['sbm']
    else:
        return None


def set_global_variable(conn, variable, value):
    """ Modify MySQL global variables

    Args:
    conn - a connection to the MySQL instance
    variable - a string the MySQL global variable name
    value - a string or bool of the deisred state of the variable
    """
    cursor = conn.cursor()

    # If we are enabling read only we need to kill all long running trx
    # so that they don't block the change
    if (variable == 'read_only' or variable == 'super_read_only') and value:
        gvars = get_global_variables(conn)
        if 'super_read_only' in gvars and gvars['super_read_only'] == 'ON':
            # no use trying to set something that is already turned on
            return
        kill_long_trx(conn)

    parameters = {'value': value}
    # Variable is not a string and can not be paramaretized as per normal
    sql = 'SET GLOBAL {variable} = %(value)s'.format(variable=variable)
    cursor.execute(sql, parameters)
    log.info(cursor._executed)


def get_long_trx(conn):
    """ Get the thread id's of long (over 2 sec) running transactions

    Args:
    conn - A mysql connection

    Returns -  A set of thread_id's
    """
    cursor = conn.cursor()
    sql = ('SELECT trx_mysql_thread_id '
           'FROM information_schema.INNODB_TRX '
           'WHERE trx_started < NOW() - INTERVAL 2 SECOND ')
    cursor.execute(sql)
    transactions = cursor.fetchall()
    threads = set()
    for trx in transactions:
        threads.add(trx['trx_mysql_thread_id'])

    return threads


def kill_long_trx(conn):
    """ Kill long running transaction.

    Args:
    conn - A mysql connection
    """
    cursor = conn.cursor()
    threads_to_kill = get_long_trx(conn)
    for thread in threads_to_kill:
        try:
            sql = 'kill %(thread)s'
            cursor.execute(sql, {'thread': thread})
            log.info(cursor._executed)
        except MySQLdb.OperationalError as detail:
            (error_code, msg) = detail.args
            if error_code != MYSQL_ERROR_NO_SUCH_THREAD:
                raise
            else:
                log.info('Thread {thr} no longer '
                         'exists'.format(thr=thread))

    log.info('Confirming that long running transactions have gone away')
    while True:
        long_threads = get_long_trx(conn)
        not_dead = threads_to_kill.intersection(long_threads)

        if not_dead:
            log.info('Threads of not dead yet: '
                     '{threads}'.format(threads=not_dead))
            time.sleep(.5)
        else:
            log.info('All long trx are now dead')
            return


def shutdown_mysql(instance):
    """ Send a mysqladmin shutdown to an instance

    Args:
    instance - a hostaddr object
    """
    username, password = get_mysql_user_for_role('admin')
    cmd = ''.join((MYSQLADMIN,
                   ' -u ', username,
                   ' -p', password,
                   ' -h ', instance.hostname,
                   ' -P ', str(instance.port),
                   ' shutdown'))
    log.info(cmd)
    host_utils.shell_exec(cmd)


def get_mysqlops_connections():
    """ Get a connection to mysqlops for reporting

    Returns:
    A mysql connection
    """
    (reporting_host, port, _, _) = mysql_connect.get_mysql_connection('mysqlopsdb001')
    reporting = host_utils.HostAddr(''.join((reporting_host, ':', str(port))))
    return connect_mysql(reporting, 'scriptrw')


def get_mysql_backups(conn, instance, date, bu_type='xbstream'):
    """  Get a logged mysql backup by instance and date

    Args:
        conn - A connection to the restorting instance
        instance - a hostaddr object
        date - the date to look for a backup
        bu_type - either 'sql' or 'xbstream', default 'xbstream'

    Returns:
        A backup file or None. We explicitly do not differentiate between
        a single valid backup and multiple.

    Example:
        [u'mysql-abexperimentsdb001b-3306-2014-02-08.tar.gz',
         u'mysql-abexperimentsdb001b-3306-2014-02-09.tar.gz',
         u'mysql-abexperimentsdb001b-3306-2014-02-10.tar.gz',
         ...
    """
    params = {'hostname': instance.hostname,
              'port': instance.port,
              'backup_type': bu_type,
              'start': ''.join((date, ' 00:00:00')),
              'end': ''.join((date, ' 23:59:59'))}
    sql = ('SELECT filename '
           'FROM mysqlops.mysql_backups '
           'WHERE hostname = %(hostname)s '
           '      AND port = %(port)s '
           '      AND backup_type = %(backup_type)s'
           '      AND finished BETWEEN %(start)s and %(end)s')
    cursor = conn.cursor()
    cursor.execute(sql, params)
    ret = cursor.fetchone()
    if ret:
        return ret['filename']
    else:
        return None


def get_installed_mysqld_version():
    """ Get the version of mysqld installed on localhost

    Returns the numeric MySQL version

    Example: 5.6.22-72.0
    """
    (std_out, std_err, return_code) = host_utils.shell_exec(MYSQL_VERSION_COMMAND)
    if return_code or not std_out:
        raise Exception('Could not determine installed mysql version: '
                        '{std_err}')
    return re.search('.+Ver ([0-9.-]+)', std_out).groups()[0]
