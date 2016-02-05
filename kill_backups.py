#!/usr/bin/env python
import logging
import os
import subprocess

from lib import backup
from lib import host_utils
from lib import mysql_lib
from lib import environment_specific

TOUCH_STOP_KILLING = '/etc/mysql/no_backup_killing'

log = logging.getLogger(__name__)


def main():
    instance = host_utils.HostAddr(host_utils.HOSTNAME)
    if os.path.isfile(TOUCH_STOP_KILLING):
        log.info('Found {path}.  Will not kill backups.\n'
                 'Exiting now.'.format(path=TOUCH_STOP_KILLING))
        return
    kill_mysql_backup(instance)
    kill_xtrabackup()


def kill_mysql_backup(instance):
    """ Kill sql, csv and xtrabackup backups

    Args:
    instance - Instance to kill backups, does not apply to csv or sql
    """
    (username, _) = mysql_lib.get_mysql_user_for_role(backup.USER_ROLE_MYSQLDUMP)
    mysql_lib.kill_user_queries(instance, username)
    kill_xtrabackup()


def kill_xtrabackup():
    """ Kill any running xtrabackup processes """
    subprocess.Popen('pkill -f xtrabackup', shell=True).wait()
    subprocess.Popen('pkill -f gof3r', shell=True).wait()


if __name__ == "__main__":
    environment_specific.initialize_logger()
    main()
