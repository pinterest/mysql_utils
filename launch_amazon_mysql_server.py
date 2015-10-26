#!/usr/bin/env python
import argparse
import inspect

import boto.ec2

import launch_replacement_db_host
from lib import mysql_lib
from lib import environment_specific

log = environment_specific.setup_logging_defaults(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hostname',
                        required=True)
    parser.add_argument('--instance_type',
                        choices=sorted(environment_specific.SUPPORTED_HARDWARE,
                                       reverse=True),
                        required=True)
    parser.add_argument('--classic_security_group',
                        default=None,
                        help='Used for launching in "classic", used instead of --vpc_security_group')
    parser.add_argument('--vpc_security_group',
                        default=None,
                        help='Used for launching in "vpc", used instead of --classic_security_group',
                        choices=environment_specific.VPC_SECURITY_GROUPS.keys())
    parser.add_argument('--availability_zone',
                        choices=environment_specific.SUPPORTED_AZ,
                        required=True)
    parser.add_argument('--mysql_major_version',
                        choices=launch_replacement_db_host.SUPPORTED_MYSQL_MAJOR_VERSIONS.keys(),
                        default=launch_replacement_db_host.DEFAULT_MYSQL_MAJOR_VERSION,
                        help='Default: {default}'.format(default=launch_replacement_db_host.DEFAULT_MYSQL_MAJOR_VERSION))
    parser.add_argument('--mysql_minor_version',
                        choices=launch_replacement_db_host.SUPPORTED_MYSQL_MINOR_VERSIONS,
                        default=launch_replacement_db_host.DEFAULT_MYSQL_MINOR_VERSION,
                        help='Default: {default}'.format(default=launch_replacement_db_host.DEFAULT_MYSQL_MINOR_VERSION))
    parser.add_argument('--dry_run',
                        help=('Do not actually launch an instance, just show '
                              'the intended configuration'),
                        default=False,
                        action='store_true')

    args = parser.parse_args()

    launch_amazon_mysql_server(hostname=args.hostname,
                               instance_type=args.instance_type,
                               vpc_security_group=args.vpc_security_group,
                               classic_security_group=args.classic_security_group,
                               availability_zone=args.availability_zone,
                               mysql_major_version=args.mysql_major_version,
                               mysql_minor_version=args.mysql_minor_version,
                               dry_run=args.dry_run)


def launch_amazon_mysql_server(hostname, instance_type, vpc_security_group, classic_security_group,
                               availability_zone, mysql_major_version, mysql_minor_version,
                               dry_run, skip_name_check=False):
    """ Launch a mysql server in aws

    Args:
    hostname - hostname of new server
    instance_type - hardware type
    vpc_security_group - VPC firewall rules. This or classic_security_group
                         must be supplied, but not both.
    classic_security_group - AWS classic firewall rules. See vpc_security_group
    availability_zone - AWS availability zone
    mysql_major_version - MySQL major version. Example 5.5 or 5.6
    mysql_minor_version - Which "branch" to use. Values are 'stable', 'staging'
                          and 'latest'.
    dry_run - Do not actually launch a host, just show the expected config.
    skip_name_check - Do not check if a hostname has already been used or log
                      usage. The assumption is the caller has already done this

    Returns:
    An amazon instance id.
    """
    args, _, _, values = inspect.getargvalues(inspect.currentframe())
    for param in args:
        log.info("Requested {param} = {value}".format(param=param,
                                                      value=values[param]))

    config = {'key_name': environment_specific.PEM_KEY,
              'placement': availability_zone,
              'instance_profile_name': environment_specific.INSTANCE_PROFILE_NAME,
              'image_id': environment_specific.SUPPORTED_HARDWARE[instance_type]['ami'],
              'instance_type': instance_type}

    if vpc_security_group and not classic_security_group:
        (subnet_name, config['subnet_id']) = \
            get_subnet_from_sg(vpc_security_group, availability_zone)
        ssh_security = environment_specific.SSH_SECURITY_MAP[subnet_name]['ssh']
        config['instance_profile_name'] = environment_specific.SSH_SECURITY_MAP[subnet_name]['iam']
        config['security_group_ids'] = [environment_specific.VPC_SECURITY_GROUPS[vpc_security_group]]
    elif classic_security_group and not vpc_security_group:
        config['security_groups'] = [classic_security_group]
        if classic_security_group in environment_specific.CLASSIC_SECURE_SG:
            ssh_security = environment_specific.SSH_SECURITY_SECURE
        else:
            ssh_security = environment_specific.SSH_SECURITY_DEV
        config['instance_profile_name'] = environment_specific.INSTANCE_PROFILE_NAME
    else:
        raise Exception('One and only one of vpc_security_group and '
                        'classic_security_group must be specified. Received:\n'
                        'vpc_security_group: {vpc}, \n'
                        'classic_security_group: {classic}'
                        ''.format(vpc=vpc_security_group,
                                  classic_security_group=classic_security_group))

    hiera_config = environment_specific.HIERA_FORMAT.format(
        ssh_security=ssh_security,
        mysql_major_version=launch_replacement_db_host.SUPPORTED_MYSQL_MAJOR_VERSIONS[mysql_major_version],
        mysql_minor_version=mysql_minor_version)
    if hiera_config not in environment_specific.SUPPORTED_HIERA_CONFIGS:
        raise Exception('Hiera config {hiera_config} is not supported.'
                        'Supported configs are: {supported}'
                        ''.format(hiera_config=hiera_config,
                                  supported=environment_specific.SUPPORTED_HIERA_CONFIGS))
    config['user_data'] = ('#cloud-config\n'
                           'pinfo_team: {pinfo_team}\n'
                           'pinfo_env: {pinfo_env}\n'
                           'pinfo_role: {hiera_config}\n'
                           'hostname: {hostname}\n'
                           'raid: true\n'
                           'raid_mount: {raid_mount}\n'
                           'ebs: true\n'
                           'ebs_size: {ebs_size}\n'
                           'ebs_mount: {ebs_mount}'
                           ''.format(pinfo_team=environment_specific.PINFO_TEAM,
                                     pinfo_env=environment_specific.PINFO_ENV,
                                     raid_mount=environment_specific.RAID_MOUNT,
                                     ebs_mount=environment_specific.EBS_MOUNT,
                                     hiera_config=hiera_config,
                                     hostname=hostname,
                                     ebs_size=environment_specific.SUPPORTED_HARDWARE[instance_type]['ebs_vol_size']))

    log.info('Config for new server:\n{config}'.format(config=config))
    conn = mysql_lib.get_mysqlops_connections()
    if not skip_name_check and not launch_replacement_db_host.is_hostname_new(hostname, conn):
        raise Exception('Hostname {hostname} has already been used!'
                        ''.format(hostname=hostname))
    if dry_run:
        log.info('In dry run mode, returning now')
        return
    else:
        conn = boto.ec2.connect_to_region(environment_specific.EC2_REGION)
        instance_id = conn.run_instances(**config).instances[0].id
        log.info('Launched instance {id}'.format(id=instance_id))
        return instance_id


def get_subnet_from_sg(sg, az):
    """ Given a VPC security group and a availiability zone
        return a subnet

    Args:
    sg - A security group
    az - An availibilty zone

    Returns - An AWS subnet
    """
    vpc_subnet = None
    for entry in environment_specific.VPC_SUBNET_SG_MAP.keys():
        if sg in environment_specific.VPC_SUBNET_SG_MAP[entry]:
            vpc_subnet = entry

    if not vpc_subnet:
        raise Exception('Could not determine subnet for sg:{sg}'.format(sg=sg))
    vpc_az_subnet = environment_specific.VPC_AZ_SUBNET_MAP[vpc_subnet][az]

    log.info('Will use subnet "{vpc_az_subnet}" in "{vpc_subnet}" based upon '
             'security group {sg} and availibility zone {az}'
             ''.format(vpc_az_subnet=vpc_az_subnet,
                       vpc_subnet=vpc_subnet,
                       sg=sg,
                       az=az))
    return (vpc_subnet, vpc_az_subnet)

if __name__ == "__main__":
    main()
