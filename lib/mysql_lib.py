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


# Max IO thread lag in bytes. If more than NORMAL_IO_LAG refuse to modify zk, etc
# 10k bytes of lag is just a few seconds normally
NORMAL_IO_LAG = 10485760
# Max lag in seconds. If more than NORMAL_HEARTBEAT_LAG refuse to modify zk, or
# attempt a live master failover
NORMAL_HEARTBEAT_LAG = 120
HEARTBEAT_SAFETY_MARGIN = 10
# Max lag in second for a dead master failover
LOOSE_HEARTBEAT_LAG = 3600

CHECK_SQL_THREAD = 'sql'
CHECK_IO_THREAD = 'io'
CHECK_CORRECT_MASTER = 'master'
ALL_REPLICATION_CHECKS = set([CHECK_SQL_THREAD,
                              CHECK_IO_THREAD,
                              CHECK_CORRECT_MASTER])
REPLICATION_THREAD_SQL = 'SQL'
REPLICATION_THREAD_IO = 'IO'
REPLICATION_THREAD_ALL = 'ALL'
REPLICATION_THREAD_TYPES = set([REPLICATION_THREAD_SQL,
                                REPLICATION_THREAD_IO,
                                REPLICATION_THREAD_ALL])

AUTH_FILE = '/var/config/config.services.mysql_auth'
CONNECT_TIMEOUT = 2
INVALID = 'INVALID'
METADATA_DB = 'test'
MYSQL_DATETIME_TO_PYTHON = '%Y-%m-%dT%H:%M:%S.%f'
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
REPLICATION_TOLERANCE_NONE = 'None'
REPLICATION_TOLERANCE_NORMAL = 'Normal'
REPLICATION_TOLERANCE_LOOSE = 'Loose'


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
    try:
        ss = get_slave_status(instance)
    except ReplicationError:
        return None

    return host_utils.HostAddr(''.join((ss['Master_Host'],
                                        ':',
                                        str(ss['Master_Port']))))


def get_slave_status(instance):
    """ Get MySQL replication status

    Args:
    instance - A hostaddr object

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
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    cursor.execute("SHOW SLAVE STATUS")
    slave_status = cursor.fetchone()
    if slave_status is None:
        raise ReplicationError('Server is not a replica')
    return slave_status


def flush_master_log(instance):
    """ Flush binary logs

    Args:
    instance - a hostAddr obect
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    cursor.execute("FLUSH BINARY LOGS")

def get_master_status(instance):
    """ Get poisition of most recent write to master replication logs

    Args:
    instance - a hostAddr object

    Returns:
    a dict describing the master status

    Example:
    {'Binlog_Do_DB': '',
     'Binlog_Ignore_DB': '',
     'File': 'mysql-bin.019324',
     'Position': 61559L}
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    cursor.execute("SHOW MASTER STATUS")
    master_status = cursor.fetchone()
    if master_status is None:
        raise ReplicationError('Server is not setup to write replication logs')
    return master_status


def get_master_logs(instance):
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
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    cursor.execute("SHOW MASTER LOGS")
    master_status = cursor.fetchall()
    return master_status


def get_binlog_archiving_lag(instance):
    """ Get date of creation of most recent binlog archived

    Args:
    instance - a hostAddr object

    Returns:
    A datetime object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    sql  = ("SELECT binlog_creation "
            "FROM {db}.{tbl} "
            "WHERE hostname= %(hostname)s  AND "
            "      port = %(port)s "
            "ORDER BY binlog_creation DESC "
            "LIMIT 1;").format(db=METADATA_DB,
                               tbl=environment_specific.BINLOG_ARCHIVING_TABLE_NAME)
    params = {'hostname': instance.hostname,
              'port': instance.port}
    cursor.execute(sql, params)
    res = cursor.fetchone()
    if res:
        return res['binlog_creation']
    else:
        return None


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


def get_global_variables(instance):
    """ Get MySQL global variables

    Args:
    instance - A hostAddr object

    Returns:
    A dict with the key the variable name
    """
    conn = connect_mysql(instance)
    ret = dict()
    cursor = conn.cursor()
    cursor.execute("SHOW GLOBAL VARIABLES")
    list_variables = cursor.fetchall()
    for entry in list_variables:
        ret[entry['Variable_name']] = entry['Value']

    return ret


def get_dbs(instance):
    """ Get MySQL databases other than mysql, information_schema,
    performance_schema and test

    Args:
    instance - A hostAddr object

    Returns
    A set of databases
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = set()

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


def does_table_exist(instance, db, table):
    """ Return True if a given table exists in a given database.

    Args:
    instance - A hostAddr object
    db - A string that contains the database name we're looking for
    table - A string containing the name of the table we're looking for

    Returns:
    True if the table was found.
    False if not or there was an exception.
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    table_exists = False

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


def get_tables(instance, db, skip_views=False):
    """ Get a list of tables and views in a given database or just
        tables.  Default to include views so as to maintain backward
        compatibility.

    Args:
    instance - A hostAddr object
    db - a string which contains a name of a db
    skip_views - true if we want tables only, false if we want everything

    Returns
    A set of tables
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = set()

    param = {'db': db}
    sql = ''.join(("SELECT TABLE_NAME ",
                   "FROM information_schema.tables ",
                   "WHERE TABLE_SCHEMA=%(db)s "))
    if skip_views:
        sql = sql + ' AND TABLE_TYPE="BASE TABLE" '

    cursor.execute(sql, param)
    for table in cursor.fetchall():
        ret.add(table['TABLE_NAME'])

    return ret


def get_columns_for_table(instance, db, table):
    """ Get a list of columns in a table

    Args:
    instance - a hostAddr object
    db - a string which contains a name of a db
    table - the name of the table to fetch columns

    Returns
    A list of columns
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = list()

    param = {'db': db,
             'table': table}
    sql = ("SELECT COLUMN_NAME "
           "FROM information_schema.columns "
           "WHERE TABLE_SCHEMA=%(db)s AND"
           "      TABLE_NAME=%(table)s")
    cursor.execute(sql, param)
    for column in cursor.fetchall():
        ret.append(column['COLUMN_NAME'])

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
    cursor = conn.cursor()

    version = get_global_variables(instance)['version']
    if version[0:3] == '5.5':
        return

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
    cursor = conn.cursor()

    version = get_global_variables(instance)['version']
    if version[0:3] != '5.6':
        return

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


def enable_and_flush_activity_statistics(instance):
    """ Reset counters for table statistics

    Args:
    instance - a hostAddr obect
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    global_vars = get_global_variables(instance)
    if global_vars['userstat'] != 'ON':
        set_global_variable(instance, 'userstat', True)

    sql = 'FLUSH TABLE_STATISTICS'
    log.info(sql)
    cursor.execute(sql)

    sql = 'FLUSH USER_STATISTICS'
    log.info(sql)
    cursor.execute(sql)


def get_dbs_activity(instance):
    """ Return rows read and changed from a MySQL instance by db

    Args:
    instance - a hostAddr object

    Returns:
    A dict with a key of the db name and entries for rows read and rows changed
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = dict()

    global_vars = get_global_variables(instance)
    if global_vars['userstat'] != 'ON':
        raise InvalidVariableForOperation('Userstats must be enabled on ',
                                          'for table_statistics to function. '
                                          'Perhaps run "SET GLOBAL userstat = '
                                          'ON" to fix this.')

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


def get_user_activity(instance):
    """ Return information about activity broken down by mysql user accoutn

    Args:
    instance - a hostAddr object

    Returns:
    a dict of user activity since last flush
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    ret = dict()

    global_vars = get_global_variables(instance)
    if global_vars['userstat'] != 'ON':
        raise InvalidVariableForOperation('Userstats must be enabled on ',
                                          'for table_statistics to function. '
                                          'Perhaps run "SET GLOBAL userstat = '
                                          'ON" to fix this.')

    sql = 'SELECT * FROM information_schema.USER_STATISTICS'
    cursor.execute(sql)
    raw_activity = cursor.fetchall()
    for row in raw_activity:
        user = row['USER']
        del(row['USER'])
        ret[user] = row

    return ret


def get_connected_users(instance):
    """ Get all currently connected users

    Args:
    instance - a hostAddr object

    Returns:
    a set of users
    """
    conn = connect_mysql(instance)
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


def show_create_table(instance, db, table, standardize=True):
    """ Get a standardized CREATE TABLE statement

    Args:
    instance - a hostAddr object
    db - the MySQL database to run against
    table - the table on the db database to run against
    standardize - Remove AUTO_INCREMENT=$NUM and similar

    Returns:
    A string of the CREATE TABLE statement
    """
    conn = connect_mysql(instance)
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


def create_db(instance, db):
    """ Create a database if it does not already exist

    Args:
    instance - a hostAddr object
    db - the name of the to be created
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ('CREATE DATABASE IF NOT EXISTS '
           '`{db}`;'.format(db=db))
    log.info(sql)

    # We don't care if the db already exists and this was a no-op
    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    cursor.execute(sql)
    warnings.resetwarnings()


def copy_db_schema(instance, old_db, new_db, verbose=False, dry_run=False):
    """ Copy the schema of one db into a different db

    Args:
    instance - a hostAddr object
    old_db - the source of the schema copy
    new_db - the destination of the schema copy
    verbose - print out SQL commands
    dry_run - do not change any state
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    tables = get_tables(instance, old_db)
    for table in tables:
        raw_sql = "CREATE TABLE IF NOT EXISTS `{new_db}`.`{table}` LIKE `{old_db}`.`{table}`"
        sql = raw_sql.format(old_db=old_db, new_db=new_db, table=table)
        if verbose:
            print sql

        if not dry_run:
            cursor.execute(sql)


def move_db_contents(instance, old_db, new_db, verbose=False, dry_run=False):
    """ Move the contents of one db into a different db

    Args:
    instance - a hostAddr object
    old_db - the source from which to move data
    new_db - the destination to move data
    verbose - print out SQL commands
    dry_run - do not change any state
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    tables = get_tables(instance, old_db)
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
    new_master_coordinates = get_master_status(new_master)
    change_master(new_replica, new_master,
                  new_master_coordinates['File'],
                  new_master_coordinates['Position'])


def restart_replication(instance):
    """ Stop then start replication

    Args:
    instance - A hostAddr object
    """
    stop_replication(instance)
    start_replication(instance)


def stop_replication(instance, thread_type=REPLICATION_THREAD_ALL):
    """ Stop replication, if running

    Args:
    instance - A hostAddr object
    thread - Which thread to stop. Options are in REPLICATION_THREAD_TYPES.
    """
    if thread_type not in REPLICATION_THREAD_TYPES:
        raise Exception('Invalid input for arg thread: {thread}'
                        ''.format(thread=thread_type))

    conn = connect_mysql(instance)
    cursor = conn.cursor()

    ss = get_slave_status(instance)
    if (ss['Slave_IO_Running'] != 'No' and ss['Slave_SQL_Running'] != 'No' and
            thread_type == REPLICATION_THREAD_ALL):
        cmd = 'STOP SLAVE'
    elif ss['Slave_IO_Running'] != 'No' and thread_type != REPLICATION_THREAD_SQL:
        cmd = 'STOP SLAVE IO_THREAD'
    elif ss['Slave_SQL_Running'] != 'No' and thread_type != REPLICATION_THREAD_IO:
        cmd = 'STOP SLAVE SQL_THREAD'
    else:
        log.info('Replication already stopped')
        return

    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    log.info(cmd)
    cursor.execute(cmd)
    warnings.resetwarnings()


def start_replication(instance, thread_type=REPLICATION_THREAD_ALL):
    """ Start replication, if not running

    Args:
    instance - A hostAddr object
    thread - Which thread to start. Options are in REPLICATION_THREAD_TYPES.
    """
    if thread_type not in REPLICATION_THREAD_TYPES:
        raise Exception('Invalid input for arg thread: {thread}'
                        ''.format(thread=thread_type))

    conn = connect_mysql(instance)
    cursor = conn.cursor()

    ss = get_slave_status(instance)
    if (ss['Slave_IO_Running'] != 'Yes' and ss['Slave_SQL_Running'] != 'Yes' and
            thread_type == REPLICATION_THREAD_ALL):
        cmd = 'START SLAVE'
    elif ss['Slave_IO_Running'] != 'Yes' and thread_type != REPLICATION_THREAD_SQL:
        cmd = 'START SLAVE IO_THREAD'
    elif ss['Slave_SQL_Running'] != 'Yes' and thread_type != REPLICATION_THREAD_IO:
        cmd = 'START SLAVE SQL_THREAD'
    else:
        log.info('Replication already running')
        return

    warnings.filterwarnings('ignore', category=MySQLdb.Warning)
    log.info(cmd)
    cursor.execute(cmd)
    warnings.resetwarnings()
    time.sleep(1)


def reset_slave(instance):
    """ Stop replicaion and remove all repl settings

    Args:
    instance - A hostAddr object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    try:
        stop_replication(instance)
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
    cursor = conn.cursor()

    set_global_variable(slave_hostaddr, 'read_only', True)
    reset_slave(slave_hostaddr)
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
        start_replication(slave_hostaddr)
        # Replication reporting is wonky for the first second
        time.sleep(1)
        # Avoid race conditions for zk update monitor
        assert_replication_sanity(slave_hostaddr,
                                  set([CHECK_SQL_THREAD, CHECK_IO_THREAD]))


def wait_replication_catch_up(slave_hostaddr):
    """ Watch replication until it is caught up

    Args:
    slave_hostaddr - A HostAddr object
    """
    last_sbm = None
    catch_up_sbm = NORMAL_HEARTBEAT_LAG - HEARTBEAT_SAFETY_MARGIN
    remaining_time = 'Not yet availible'
    sleep_duration = 5

    # Confirm that replication is setup at all
    get_slave_status(slave_hostaddr)

    try:
        assert_replication_sanity(slave_hostaddr)
    except:
        log.warning('Replication does not appear sane, going to sleep 60 '
                    'seconds in case things get better on their own.')
        time.sleep(60)
        assert_replication_sanity(slave_hostaddr)

    while True:
        replication = calc_slave_lag(slave_hostaddr)

        if replication['sbm'] is None or replication['sbm'] == INVALID:
            log.info('Computed seconds behind master is unavailable, going to '
                     'sleep for a minute and retry. A likely reason is that '
                     'there was a failover between when a backup was taken '
                     'and when a restore was run so there will not be a '
                     'entry until replication has caught up more. If this is '
                     'a new replica set, read_only is probably ON on the '
                     'master server.')
            time.sleep(60)
            continue

        if replication['sbm'] < catch_up_sbm:
            log.info('Replication computed seconds behind master {sbm} < '
                     '{catch_up_sbm}, which is "good enough".'
                     ''.format(sbm=replication['sbm'],
                               catch_up_sbm=catch_up_sbm))
            return

        # last_sbm is set at the end of the first execution and should always
        # be set from then on
        if last_sbm:
            catch_up_rate = (last_sbm - replication['sbm']) / float(sleep_duration)
            if catch_up_rate > 0:
                remaining_time = datetime.timedelta(seconds=((replication['sbm'] - catch_up_sbm) / catch_up_rate))
                if remaining_time.total_seconds() > 6 * 60 * 60:
                    sleep_duration = 5 * 60
                elif remaining_time.total_seconds() > 60 * 60:
                    sleep_duration = 60
                else:
                    sleep_duration = 5
            else:
                remaining_time = '> heat death of the universe'
                sleep_duration = 60
            log.info('Replication is lagged by {sbm} seconds, waiting '
                     'for < {catch_up}. Guestimate time to catch up: {eta}'
                     ''.format(sbm=replication['sbm'],
                               catch_up=catch_up_sbm,
                               eta=str(remaining_time)))
        else:
            # first time through
            log.info('Replication is lagged by {sbm} seconds.'
                     ''.format(sbm=replication['sbm']))

        last_sbm = replication['sbm']
        time.sleep(sleep_duration)


def assert_replication_unlagged(instance, lag_tolerance, dead_master=False):
    """ Confirm that replication lag is less than tolerance, otherwise
        throw an exception

    Args:
    instance - A hostAddr object of the replica
    lag_tolerance - Possibly values (constants):
                    'REPLICATION_TOLERANCE_NONE'- no lag is acceptable
                    'REPLICATION_TOLERANCE_NORMAL' - replica can be slightly lagged
                    'REPLICATION_TOLERANCE_LOOSE' - replica can be really lagged
    """
    # Test to see if the slave is setup for replication. If not, we are hosed
    replication = calc_slave_lag(instance, dead_master)
    problems = set()
    if lag_tolerance == REPLICATION_TOLERANCE_NONE:
        if replication['sql_bytes'] != 0:
            problems.add('Replica {r} is not fully synced, bytes behind: {b}'
                         ''.format(r=instance,
                                   b=replication['sql_bytes']))
    elif lag_tolerance == REPLICATION_TOLERANCE_NORMAL:
        if replication['sbm'] > NORMAL_HEARTBEAT_LAG:
            problems.add('Replica {r} has heartbeat lag {sbm} > {sbm_limit} seconds'
                         ''.format(sbm=replication['sbm'],
                                   sbm_limit=NORMAL_HEARTBEAT_LAG,
                                   r=instance))

        if replication['io_bytes'] > NORMAL_IO_LAG:
            problems.add('Replica {r} has IO lag {io_bytes} > {io_limit} bytes'
                         ''.format(io_bytes=replication['io_bytes'],
                                   io_limit=NORMAL_IO_LAG,
                                   r=instance))
    elif lag_tolerance == REPLICATION_TOLERANCE_LOOSE:
        if replication['sbm'] > LOOSE_HEARTBEAT_LAG:
            problems.addi('Replica {r} has heartbeat lag {sbm} > {sbm_limit} seconds'
                          ''.format(sbm=replication['sbm'],
                                    sbm_limit=LOOSE_HEARTBEAT_LAG,
                                    r=instance))
    else:
        problems.add('Unkown lag_tolerance mode: {m}'.format(m=lag_tolerance))

    if problems:
        raise Exception(', '.join(problems))


def assert_replication_sanity(instance,
                              checks=ALL_REPLICATION_CHECKS):
    """ Confirm that a replica has replication running and from the correct
        source if the replica is in zk. If not, throw an exception.

    args:
    instance - A hostAddr object
    """
    problems = set()
    slave_status = get_slave_status(instance)
    if (CHECK_IO_THREAD in checks and
            slave_status['Slave_IO_Running'] != 'Yes'):
        problems.add('Replica {r} has IO thread not running'
                     ''.format(r=instance))

    if (CHECK_SQL_THREAD in checks and
            slave_status['Slave_SQL_Running'] != 'Yes'):
        problems.add('Replcia {r} has SQL thread not running'
                     ''.format(r=instance))

    if CHECK_CORRECT_MASTER in checks:
        zk = host_utils.MysqlZookeeper()
        try:
            (replica_set, replica_type) = zk.get_replica_set_from_instance(instance)
        except:
            # must not be in zk, returning
            return
        expected_master = zk.get_mysql_instance_from_replica_set(replica_set)
        actual_master = host_utils.HostAddr(':'.join((slave_status['Master_Host'],
                                                      str(slave_status['Master_Port']))))
        if expected_master != actual_master:
            problems.add('Master is {actual} rather than expected {expected}'
                         'for replica {r}'.format(actual=actual_master,
                                                  expected=expected_master,
                                                  r=instance))

    if problems:
        raise Exception(', '.join(problems))


def calc_slave_lag(slave_hostaddr, dead_master=False):
    """ Determine MySQL replication lag in bytes and binlogs

    Args:
    slave_hostaddr - A HostAddr object for a replica

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

    ret = {'sql_bytes': INVALID,
           'sql_binlogs': INVALID,
           'io_bytes': INVALID,
           'io_binlogs': INVALID,
           'sbm': INVALID,
           'ss': {'Slave_IO_Running': INVALID,
                  'Slave_SQL_Running': INVALID,
                  'Master_Host': INVALID,
                  'Master_Port': INVALID}}
    try:
        ss = get_slave_status(slave_hostaddr)
    except ReplicationError:
        # Not a slave, so return dict of INVALID
        return ret
    except MySQLdb.OperationalError as detail:
        (error_code, msg) = detail.args
        if error_code == MYSQL_ERROR_CONN_HOST_ERROR:
            # Host down, but exists.
            return ret
        else:
            # Host does not exist or something else funky
            raise

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
            master_logs = get_master_logs(master_hostaddr)

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
        ret['sbm'] = calc_alt_sbm(slave_hostaddr, ss['Master_Server_Id'])
    except MySQLdb.ProgrammingError as detail:
        (error_code, msg) = detail.args
        if error_code != MYSQL_ERROR_NO_SUCH_TABLE:
            raise
        # We can not compute a real sbm, so the caller will get
        # None default
        pass
    return ret


def calc_alt_sbm(instance, master_server_id):
    """ Calculate seconds behind using heartbeat + time on slave server

    Args:
    instance - A hostAddr object of a slave server
    master_server_id - The server_id of the master server

    Returns:
    An int of the calculated seconds behind master or None
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    sql = ''.join(("SELECT TIMESTAMPDIFF(SECOND,ts, NOW()) AS 'sbm' "
                   "FROM {METADATA_DB}.heartbeat "
                   "WHERE server_id= %(Master_Server_Id)s"))

    cursor.execute(sql.format(METADATA_DB=METADATA_DB),
                   {'Master_Server_Id': master_server_id})
    row = cursor.fetchone()
    if row:
        return row['sbm']
    else:
        return None


def get_heartbeat(instance):
    """ Get the most recent heartbeat on a slave

    Args:
    instance - A hostAddr object of a slave server

    Returns:
    A datetime.datetime object.
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    slave_status = get_slave_status(instance)
    sql = ''.join(("SELECT ts "
                   "FROM {METADATA_DB}.heartbeat "
                   "WHERE server_id= %(Master_Server_Id)s"))

    cursor.execute(sql.format(METADATA_DB=METADATA_DB), slave_status)
    row = cursor.fetchone()
    if not row:
        return None

    return datetime.datetime.strptime(row['ts'], MYSQL_DATETIME_TO_PYTHON)


def get_pitr_data(instance):
    """ Get all data needed to run a point in time recovery later on

    Args:
    instance - A hostAddr object of a server

    Returns:
    """
    ret = dict()
    ret['heartbeat'] = str(get_heartbeat(instance))
    ret['repl_positions'] = []
    master_status = get_master_status(instance)
    ret['repl_positions'].append((master_status['File'], master_status['Position']))
    if 'Executed_Gtid_Set' in master_status:
        ret['Executed_Gtid_Set'] = master_status['Executed_Gtid_Set']
    else:
        ret['Executed_Gtid_Set'] = None

    try:
        ss = get_slave_status(instance)
        ret['repl_positions'].append((ss['Relay_Master_Log_File'], ss['Exec_Master_Log_Pos']))
    except ReplicationError:
        # we are running on a master, don't care about this exception
        pass

    return ret


def set_global_variable(instance, variable, value):
    """ Modify MySQL global variables

    Args:
    instance - a hostAddr object
    variable - a string the MySQL global variable name
    value - a string or bool of the deisred state of the variable
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    # If we are enabling read only we need to kill all long running trx
    # so that they don't block the change
    if (variable == 'read_only' or variable == 'super_read_only') and value:
        gvars = get_global_variables(instance)
        if 'super_read_only' in gvars and gvars['super_read_only'] == 'ON':
            # no use trying to set something that is already turned on
            return
        kill_long_trx(instance)

    parameters = {'value': value}
    # Variable is not a string and can not be paramaretized as per normal
    sql = 'SET GLOBAL {variable} = %(value)s'.format(variable=variable)
    cursor.execute(sql, parameters)
    log.info(cursor._executed)


def start_consistent_snapshot(conn, read_only=False):
    """ Start a transaction with a consistent view of data

    Args:
    instance - a hostAddr object
    read_only - see the transaction to be read_only
    """
    if read_only:
        read_write_mode = 'READ ONLY'
    else:
        read_write_mode = 'READ WRITE'
    cursor = conn.cursor()
    cursor.execute("SET SESSION TRANSACTION ISOLATION "
                   "LEVEL REPEATABLE READ")
    cursor.execute("START TRANSACTION /*!50625 WITH CONSISTENT SNAPSHOT, {rwm} */".format(rwm=read_write_mode))


def get_long_trx(instance):
    """ Get the thread id's of long (over 2 sec) running transactions

    Args:
    instance - a hostAddr object

    Returns -  A set of thread_id's
    """
    conn = connect_mysql(instance)
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


def kill_user_queries(instance, username):
    """ Kill a users queries

    Args:
    instance - The instance on which to kill the queries
    username - The name of the user to kill
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()
    sql = ("SELECT id "
           "FROM information_schema.processlist "
           "WHERE user= %(username)s ")
    cursor.execute(sql, {'username': username})
    queries = cursor.fetchall()
    for query in queries:
        log.info("Killing connection id {id}".format(id=query['id']))
        cursor.execute("kill %(id)s", {'id': query['id']})


def kill_long_trx(instance):
    """ Kill long running transaction.

    Args:
    instance - a hostAddr object
    """
    conn = connect_mysql(instance)
    cursor = conn.cursor()

    threads_to_kill = get_long_trx(instance)
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
        long_threads = get_long_trx(instance)
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


def start_backup_log(instance, backup_type, timestamp):
    """ Start a log of a mysql backup

    Args:
    instance - A hostaddr object for the instance being backed up
    backup_type - Either xbstream or sql
    timestamp - The timestamp from when the backup began
    """
    row_id = None
    try:
        reporting_conn = get_mysqlops_connections()
        cursor = reporting_conn.cursor()

        sql = ("INSERT INTO mysqlops.mysql_backups "
               "SET "
               "hostname = %(hostname)s, "
               "port = %(port)s, "
               "started = %(started)s, "
               "backup_type = %(backup_type)s ")

        metadata = {'hostname': instance.hostname,
                    'port': str(instance.port),
                    'started': time.strftime('%Y-%m-%d %H:%M:%S', timestamp),
                    'backup_type': backup_type}
        cursor.execute(sql, metadata)
        row_id = cursor.lastrowid
        reporting_conn.commit()
        log.info(cursor._executed)
    except Exception as e:
        log.warning("Unable to write log entry to "
                    "mysqlopsdb001: {e}".format(e=e))
        log.warning("However, we will attempt to continue with the backup.")
    return row_id


def finalize_backup_log(id, filename):
    """ Write final details of a mysql backup

    id - A pk from the mysql_backups table
    filename - The location of the resulting backup
    """
    try:
        reporting_conn = get_mysqlops_connections()
        cursor = reporting_conn.cursor()
        sql = ("UPDATE mysqlops.mysql_backups "
               "SET "
               "filename = %(filename)s, "
               "finished = %(finished)s "
               "WHERE id = %(id)s")
        metadata = {'filename': filename,
                    'finished': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'id': id}
        cursor.execute(sql, metadata)
        reporting_conn.commit()
        reporting_conn.close()
        log.info(cursor._executed)
    except Exception as e:
        log.warning("Unable to update mysqlopsdb with "
                    "backup status: {e}".format(e=e))


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
