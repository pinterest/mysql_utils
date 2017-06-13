from check_shard_mappings import get_problem_replicasets
from lib.environment_specific import MYSQL_SHARDS_CONFIG_PATH
from lib.environment_specific import TEST_MYSQL_SHARDS_CONFIG_PATH
from lib.environment_specific import MYSQL_SHARDS_S3_PATH_PREFIX
from lib.environment_specific import TEST_MYSQL_SHARDS_S3_PATH_PREFIX
#from lib.environment_specific import MYSQL_SHARDS_CONFIG_LOCAL_PATH
#from lib.environment_specific import TEST_MYSQL_SHARDS_CONFIG_LOCAL_PATH
from lib.environment_specific_dir.config_base import Config

class MySqlShardConfig(Config):

    SERVICES = "services"
    NAMESPACES = "namespaces"
    SHARDS = "shards"

    MYSQLDB = "mysqldb"
    REPLICA_SET = "replica_set"

    def __init__(self, use_test_config=True):
        super(MySqlShardConfig, self).__init__(
            zk_path=TEST_MYSQL_SHARDS_CONFIG_PATH if use_test_config else MYSQL_SHARDS_CONFIG_PATH,
            s3_path_prefix=TEST_MYSQL_SHARDS_S3_PATH_PREFIX if use_test_config else MYSQL_SHARDS_S3_PATH_PREFIX,
            name='test_mysql_shards' if use_test_config else 'mysql_shards')
        # localpath = TEST_MYSQL_SHARDS_CONFIG_LOCAL_PATH if use_test_config else MYSQL_SHARDS_CONFIG_LOCAL_PATH

    def migrate_shard(self, service_name, namespace_name, shard_name, old_replica_set, new_replica_set):
        "changes repplica set of a shard during shard migration"
        service = self.updated_config_dict[self.SERVICES][service_name]
        namespace = service[self.NAMESPACES][namespace_name]
        shard = namespace[self.SHARDS][shard_name]
        if shard[self.REPLICA_SET] == old_replica_set:
            shard[self.REPLICA_SET] = new_replica_set
        elif shard[self.REPLICA_SET] != new_replica_set:
            raise Exception("migrate shard to %s fail: %s %s %s. Old replica set is %s, not %s " %
                (new_replica_set, service_name, namespace_name, shard_name, shard[self.REPLICA_SET],
                 old_replica_set))
        else:
            print ("Warning: %s %s %s already on %s, not on %s " %
                  (service_name, namespace_name, shard_name, new_replica_set, old_replica_set))

    def add_shard(self, service_name, namespace_name, shard_name, replica_set, dbname):
        "adds shard to an existing namespace in existing service"
        service = self.updated_config_dict[self.SERVICES][service_name]
        namespace = service[self.NAMESPACES][namespace_name]
        shard = namespace[self.SHARDS].get(shard_name)
        if shard is None:
           shard = { self.MYSQLDB : dbname , self.REPLICA_SET : replica_set }
           namespace[self.SHARDS][shard_name] = shard
        elif shard[self.MYSQLDB] != dbname or shard[self.REPLICA_SET] != replica_set:
            raise Exception("add shard failed for: %s %s %s. shard exists : %s" %
                (service_name, namespace_name, shard_name, str(shard)))

    def add_new_service(self, service_name):
        """ add new service and default name space """
        if self.updated_config_dict[self.SERVICES].get(service_name) is None:
            self.updated_config_dict[self.SERVICES][service_name] = {
                self.NAMESPACES: {'': {self.SHARDS: {}}}}
        else:
            raise Exception("add service name failed for {}".format(
                service_name))

    def add_new_namespace(self, service_name, namespace_name):
        """ add new namespace to existing service """
        try:
            service = self.updated_config_dict[self.SERVICES][service_name]
        except KeyError as e:
            print 'Service name {} is not in ZK mapping yet'.format(e)
            raise Exception('service name does not exist')
        namespace = service[self.NAMESPACES].get(namespace_name)
        if namespace is None:
            service[self.NAMESPACES][namespace_name] = {self.SHARDS: {}}
        else:
            raise Exception("adding new name space failed for {}".format(namespace_name))

    def check_shard(self, service_name, namespace_name, shard_name, shard):
        "checks if shard contents match what is expected"
        service = self.updated_config_dict[self.SERVICES][service_name]
        namespace = service[self.NAMESPACES][namespace_name]
        return namespace[self.SHARDS].get(shard_name) == shard

    def push_config_with_validation(self):
        problem_replica_sets = get_problem_replicasets(None,
            self.updated_config_dict[self.SERVICES])
        if problem_replica_sets:
            raise Exception('Problem in shard mapping {}'.format(problem_replica_sets))
        self.push_config()
