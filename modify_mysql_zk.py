#!/usr/bin/python
import argparse
import copy
import pprint
import simplejson

from lib import host_utils
from lib import mysql_lib
from lib import environment_specific


log = environment_specific.setup_logging_defaults(__name__)
chat_handler = environment_specific.BufferingChatHandler()
log.addHandler(chat_handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action',
                        help=("What modification to make. If 'auto', the host "
                              "replacement log will be used to determine what "
                              "what role to use. Default is auto."),
                        choices=['add_slave', 'add_dr_slave', 'auto',
                                 'swap_master_and_slave',
                                 'swap_slave_and_dr_slave'],
                        default='auto')
    parser.add_argument('instance',
                        help='What instance to act upon')
    parser.add_argument('--dry_run',
                        help=('Do not actually modify zk, just show '
                              'what would be modify'),
                        default=False,
                        action='store_true')
    parser.add_argument('--dangerous',
                        help=('If you need to swap_master_and_slave in zk'
                              'outside of the failover script, that is '
                              'dangerous and you will need this flag.'),
                        default=False,
                        action='store_true')
    args = parser.parse_args()
    action = args.action
    instance = host_utils.HostAddr(args.instance)

    if args.dry_run:
        log.removeHandler(chat_handler)

    if action == 'add_slave':
        add_replica_to_zk(instance, host_utils.REPLICA_ROLE_SLAVE,
                          args.dry_run)
    elif action == 'add_dr_slave':
        add_replica_to_zk(instance, host_utils.REPLICA_ROLE_DR_SLAVE,
                          args.dry_run)
    elif action == 'swap_master_and_slave':
        if args.dangerous:
            swap_master_and_slave(instance, args.dry_run)
        else:
            raise Exception('To swap_master_and_slave in zk outside of the '
                            'failover script is very dangerous and the '
                            '--dangerous flag was not supplied.')
    elif action == 'swap_slave_and_dr_slave':
        swap_slave_and_dr_slave(instance, args.dry_run)
    elif action == 'auto':
        auto_add_instance_to_zk(instance, args.dry_run)
    else:
        raise Exception('Invalid action: {action}'.format(action=action))


def auto_add_instance_to_zk(instance, dry_run):
    """ Try to do right thing in adding a server to zk

    Args:
    instance - The replacement instance
    dry_run - If set, do not modify zk
    """
    try:
        conn = mysql_lib.get_mysqlops_connections()
        log.info('Determining replacement for '
                 '{hostname}'.format(hostname=instance.hostname))
        server_metadata = environment_specific.get_server_metadata(instance.hostname)
        if not server_metadata:
            raise Exception('CMDB lacks knowledge of replacement host')
        instance_id = server_metadata['id']
        role = determine_replacement_role(conn, instance_id)
        log.info('Adding server as role: {role}'.format(role=role))
    except Exception, e:
        log.exception(e)
        raise
    add_replica_to_zk(instance, role, dry_run)

    if not dry_run:
        log.info('Updating host_replacement_log')
        update_host_replacement_log(conn, instance_id)


def determine_replacement_role(conn, instance_id):
    """ Try to determine the role an instance should be placed into

    Args:
    conn - A connection to the reporting server
    instance - The replacement instance

    Returns:
    The replication role which should be either 'slave' or 'dr_slave'
    """
    zk = host_utils.MysqlZookeeper()
    cursor = conn.cursor()
    sql = ("SELECT old_host "
           "FROM mysqlops.host_replacement_log "
           "WHERE new_instance = %(new_instance)s ")
    params = {'new_instance': instance_id}
    cursor.execute(sql, params)
    log.info(cursor._executed)
    result = cursor.fetchone()
    if result is None:
        raise Exception('Could not determine replacement host')

    old_host = host_utils.HostAddr(result['old_host'])
    log.info('Host to be replaced is {old_host}'
             ''.format(old_host=old_host.hostname))

    (_, repl_type) = zk.get_replica_set_from_instance(old_host)

    if repl_type == host_utils.REPLICA_ROLE_MASTER:
        raise Exception('Corwardly refusing to replace a master!')
    elif repl_type is None:
        raise Exception('Could not determine replacement role')
    else:
        return repl_type


def get_zk_node_for_replica_set(kazoo_client, replica_set):
    """ Figure out what node holds the configuration of a replica set

    Args:
    kazoo_client - A kazoo_client
    replica_set - A name for a replica set

    Returns:
    zk_node - The node that holds the replica set
    parsed_data - The deserialized data from json in the node
    """
    for zk_node in [environment_specific.DS_ZK, environment_specific.GEN_ZK]:
        znode_data, meta = kazoo_client.get(zk_node)
        parsed_data = simplejson.loads(znode_data)
        if replica_set in parsed_data:
            return (zk_node, parsed_data, meta.version)
    raise Exception('Could not find replica_set {replica_set} '
                    'in zk_nodes'.format(replica_set=replica_set))


def remove_auth(zk_record):
    """ Remove passwords from zk records

    Args:
    zk_record - A dict which may or not have a passwd or userfield.

    Returns:
    A dict which if a passwd or user field is present will have the
    values redacted
    """
    ret = copy.deepcopy(zk_record)
    if 'passwd' in ret:
        ret['passwd'] = 'REDACTED'

    if 'user' in ret:
        ret['user'] = 'REDACTED'

    return ret


def add_replica_to_zk(instance, replica_type, dry_run):
    """ Add a replica to zk

    Args:
    instance - A hostaddr object of the replica to add to zk
    replica_type - Either 'slave' or 'dr_slave'.
    dry_run - If set, do not modify zk
    """
    try:
        if replica_type not in [host_utils.REPLICA_ROLE_DR_SLAVE,
                                host_utils.REPLICA_ROLE_SLAVE]:
            raise Exception('Invalid value "{replica_type}" for argument '
                            "replica_type").format(replica_type=replica_type)

        zk_local = host_utils.MysqlZookeeper()
        kazoo_client = environment_specific.get_kazoo_client()
        if not kazoo_client:
            raise Exception('Could not get a zk connection')

        log.info('Instance is {inst}'.format(inst=instance))
        mysql_lib.assert_replication_sanity(instance)
        mysql_lib.assert_replication_unlagged(instance, mysql_lib.REPLICATION_TOLERANCE_NORMAL)
        master = mysql_lib.get_master_from_instance(instance)
        if master not in zk_local.get_all_mysql_instances_by_type(host_utils.REPLICA_ROLE_MASTER):
            raise Exception('Instance {master} is not a master in zk'
                            ''.format(master=master))

        log.info('Detected master of {instance} '
                 'as {master}'.format(instance=instance,
                                      master=master))

        (replica_set, _) = zk_local.get_replica_set_from_instance(master)
        log.info('Detected replica_set as '
                 '{replica_set}'.format(replica_set=replica_set))

        if replica_type == host_utils.REPLICA_ROLE_SLAVE:
            (zk_node, parsed_data, version) = get_zk_node_for_replica_set(kazoo_client,
                                                                          replica_set)
            log.info('Replica set {replica_set} is held in zk_node '
                     '{zk_node}'.format(zk_node=zk_node,
                                        replica_set=replica_set))
            log.info('Existing config:')
            log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
            new_data = copy.deepcopy(parsed_data)
            new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]['host'] = \
                instance.hostname
            new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]['port'] = \
                instance.port
            log.info('New config:')
            log.info(pprint.pformat(remove_auth(new_data[replica_set])))

            if new_data == parsed_data:
                raise Exception('No change would be made to zk, '
                                'will not write new config')
            elif dry_run:
                log.info('dry_run is set, therefore not modifying zk')
            else:
                log.info('Pushing new configuration for '
                         '{replica_set}:'.format(replica_set=replica_set))
                kazoo_client.set(zk_node, simplejson.dumps(new_data), version)
        elif replica_type == host_utils.REPLICA_ROLE_DR_SLAVE:
            znode_data, dr_meta = kazoo_client.get(environment_specific.DR_ZK)
            parsed_data = simplejson.loads(znode_data)
            new_data = copy.deepcopy(parsed_data)
            if replica_set in parsed_data:
                log.info('Existing dr config:')
                log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
            else:
                log.info('Replica set did not previously have a dr slave')

            new_data[replica_set] = \
                {host_utils.REPLICA_ROLE_DR_SLAVE: {'host': instance.hostname,
                                                    'port': instance.port}}
            log.info('New dr config:')
            log.info(pprint.pformat(remove_auth(new_data[replica_set])))

            if new_data == parsed_data:
                raise Exception('No change would be made to zk, '
                                'will not write new config')
            elif dry_run:
                log.info('dry_run is set, therefore not modifying zk')
            else:
                log.info('Pushing new dr configuration for '
                         '{replica_set}:'.format(replica_set=replica_set))
                kazoo_client.set(environment_specific.DR_ZK, simplejson.dumps(new_data), dr_meta.version)
        else:
            # we should raise an exception above rather than getting to here
            pass
    except Exception, e:
        log.exception(e)
        raise


def swap_master_and_slave(instance, dry_run):
    """ Swap a master and slave in zk. Warning: this does not sanity checks
        and does nothing more than update zk. YOU HAVE BEEN WARNED!

    Args:
    instance - An instance in the replica set. This function will figure
               everything else out.
    dry_run - If set, do not modify configuration.
    """
    zk_local = host_utils.MysqlZookeeper()
    kazoo_client = environment_specific.get_kazoo_client()
    if not kazoo_client:
        raise Exception('Could not get a zk connection')

    log.info('Instance is {inst}'.format(inst=instance))
    (replica_set, version) = zk_local.get_replica_set_from_instance(instance)
    log.info('Detected replica_set as '
             '{replica_set}'.format(replica_set=replica_set))
    (zk_node,
     parsed_data,
     version) = get_zk_node_for_replica_set(kazoo_client, replica_set)
    log.info('Replica set {replica_set} is held in zk_node '
             '{zk_node}'.format(zk_node=zk_node,
                                replica_set=replica_set))

    log.info('Existing config:')
    log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
    new_data = copy.deepcopy(parsed_data)
    new_data[replica_set][host_utils.REPLICA_ROLE_MASTER] = \
        parsed_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]
    new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE] = \
        parsed_data[replica_set][host_utils.REPLICA_ROLE_MASTER]

    log.info('New config:')
    log.info(pprint.pformat(remove_auth(new_data[replica_set])))

    if new_data == parsed_data:
        raise Exception('No change would be made to zk, '
                        'will not write new config')
    elif dry_run:
        log.info('dry_run is set, therefore not modifying zk')
    else:
        log.info('Pushing new configuration for '
                 '{replica_set}:'.format(replica_set=replica_set))
        kazoo_client.set(zk_node, simplejson.dumps(new_data), version)


def swap_slave_and_dr_slave(instance, dry_run):
    """ Swap a slave and a dr_slave in zk

    Args:
    instance - An instance that is either a slave or dr_slave
    """
    zk_local = host_utils.MysqlZookeeper()
    kazoo_client = environment_specific.get_kazoo_client()
    if not kazoo_client:
        raise Exception('Could not get a zk connection')

    log.info('Instance is {inst}'.format(inst=instance))
    (replica_set, _) = zk_local.get_replica_set_from_instance(instance)
    log.info('Detected replica_set as '
             '{replica_set}'.format(replica_set=replica_set))
    (zk_node,
     parsed_data,
     version) = get_zk_node_for_replica_set(kazoo_client, replica_set)
    log.info('Replica set {replica_set} is held in zk_node '
             '{zk_node}'.format(zk_node=zk_node,
                                replica_set=replica_set))

    log.info('Existing config:')
    log.info(pprint.pformat(remove_auth(parsed_data[replica_set])))
    new_data = copy.deepcopy(parsed_data)

    dr_znode_data, dr_meta = kazoo_client.get(environment_specific.DR_ZK)
    dr_parsed_data = simplejson.loads(dr_znode_data)
    new_dr_data = copy.deepcopy(dr_parsed_data)
    if replica_set not in parsed_data:
        raise Exception('Replica set {replica_set} is not present '
                        'in dr_node'.format(replica_set=replica_set))
    log.info('Existing dr config:')
    log.info(pprint.pformat(remove_auth(dr_parsed_data[replica_set])))

    new_data[replica_set][host_utils.REPLICA_ROLE_SLAVE] = \
        dr_parsed_data[replica_set][host_utils.REPLICA_ROLE_DR_SLAVE]
    new_dr_data[replica_set][host_utils.REPLICA_ROLE_DR_SLAVE] = \
        parsed_data[replica_set][host_utils.REPLICA_ROLE_SLAVE]

    log.info('New config:')
    log.info(pprint.pformat(remove_auth(new_data[replica_set])))

    log.info('New dr config:')
    log.info(pprint.pformat(remove_auth(new_dr_data[replica_set])))

    if dry_run:
        log.info('dry_run is set, therefore not modifying zk')
    else:
        log.info('Pushing new configuration for '
                 '{replica_set}:'.format(replica_set=replica_set))
        kazoo_client.set(zk_node, simplejson.dumps(new_data), version)
        try:
            kazoo_client.set(environment_specific.DR_ZK, simplejson.dumps(new_dr_data), dr_meta.version)
        except:
            raise Exception('DR node is incorrect due to a different change '
                            'blocking this change. You need to fix it yourself')


def update_host_replacement_log(conn, instance_id):
    """ Mark a replacement as completed

    conn - A connection to the reporting server
    instance - The replacement instance
    """
    cursor = conn.cursor()
    sql = ("UPDATE mysqlops.host_replacement_log "
           "SET is_completed = 1 "
           "WHERE new_instance = %(new_instance)s ")
    params = {'new_instance': instance_id}
    cursor.execute(sql, params)
    log.info(cursor._executed)
    conn.commit()


if __name__ == "__main__":
    main()
