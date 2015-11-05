#!/usr/bin/env python

import mysql_backup
from lib import backup
from lib import host_utils

if __name__ == "__main__":
    instance = host_utils.HostAddr(host_utils.HOSTNAME)
    mysql_backup.mysql_backup(instance, backup.BACKUP_TYPE_LOGICAL)
