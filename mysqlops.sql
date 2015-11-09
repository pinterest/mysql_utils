CREATE TABLE `host_replacement_log` (
  `old_host` varchar(90) NOT NULL,
  `old_instance` varchar(15) NOT NULL,
  `old_az` varchar(15) NOT NULL,
  `old_hw_type` varchar(15) DEFAULT NULL,
  `new_host` varchar(90) NOT NULL,
  `new_instance` varchar(15) NOT NULL,
  `new_az` varchar(15) NOT NULL,
  `new_hw_type` varchar(15) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `reason` varchar(200) DEFAULT NULL,
  `is_completed` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`old_instance`),
  UNIQUE KEY `new_instance` (`new_instance`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `mysql_backups` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `hostname` varchar(90) NOT NULL DEFAULT '',
  `port` int(11) NOT NULL DEFAULT '0',
  `filename` varchar(255) DEFAULT NULL,
  `started` datetime NOT NULL,
  `finished` datetime DEFAULT NULL,
  `size` bigint(20) unsigned NOT NULL DEFAULT '0',
  `backup_type` enum('sql.gz','xbstream') DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `filename` (`filename`),
  KEY `hostname` (`hostname`,`port`,`finished`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `promotion_locks` (
  `lock_identifier` varchar(36) NOT NULL,
  `lock_active` enum('active') DEFAULT 'active',
  `created_at` datetime NOT NULL,
  `expires` datetime DEFAULT NULL,
  `released` datetime DEFAULT NULL,
  `replica_set` varchar(64) NOT NULL,
  `promoting_host` varchar(64) NOT NULL,
  `promoting_user` varchar(64) NOT NULL,
  PRIMARY KEY (`lock_identifier`),
  UNIQUE KEY `lock_active` (`replica_set`,`lock_active`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `retirement_protection` (
  `hostname` varchar(90) NOT NULL DEFAULT '',
  `reason` text,
  `protecting_user` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`hostname`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `retirement_queue` (
  `hostname` varchar(90) NOT NULL,
  `instance_id` varchar(15) NOT NULL,
  `activity` varchar(50) DEFAULT NULL,
  `happened` datetime DEFAULT NULL,
  UNIQUE KEY `instance` (`instance_id`,`activity`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE `unique_hostname_index` (
  `hostname` varchar(90) NOT NULL,
  PRIMARY KEY (`hostname`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
