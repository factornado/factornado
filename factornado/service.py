import yaml
import os
import sys
from tornado import ioloop, web, httpserver
import logging


class AbstractService(object):
    def get_config(self):
        def merge(source, destination):
            if source is None and destination is None:
                return None
            if source is None:
                return destination
            if destination is None:
                return source

            for key, value in source.items():
                if key in destination and isinstance(destination[key], dict):
                    destination[key] = merge(value, destination[key])
                else:
                    destination[key] = value

            return destination

        root_path = os.environ['ARABICA_PATH']
        allConfig = yaml.load(open(os.path.join(root_path, 'config.yml')))
        conf = allConfig.get('common', None)
        conf = merge(conf, allConfig['services'].get(self.service_name, None))
        assert conf is not None
        return conf

    def get_logger(self):
        if self.config['log.level'] < 10:
            logging.basicConfig(
                level=self.config['log.level'],
                format='%(asctime)s %(levelname)s %(message)s',
                filemode='a',
                stream=sys.stdout)
        else:
            logging.basicConfig(
                level=self.config['log.level'],
                format='%(asctime)s %(levelname)s %(message)s',
                filename=(self.config['log.folder'] + 'arabica-' +
                          self.service_name + '.log'),
                filemode='a')

        logger = logging.getLogger(self.service_name)

        return logger

    def get_mongo(self):
        self.mongo_hosts = self.get_mongo_host()
        if self.mongo_hosts is None:
            return

        self.mongo_databases = self.get_mongo_databases()
        if self.mongo_databases is None:
            return

        self.mongo_collections = self.get_mongo_collections()

    def get_mongo_host(self):
        try:
            import pymongo
        except:
            self.logger.info('MONGO - pyMongo dependency not detected')
            return None

        try:
            mongo_host_config = self.config.get('db').get('mongo').get('host')
            assert mongo_host_config is not None
        except:
            self.logger.info('MONGO - no host defined')
            return None

        mongo_hosts = {}

        for host, host_config in mongo_host_config.items():
            mongo_hosts[host] = pymongo.MongoClient(host_config)
            try:
                self.logger.debug('MONGO - trying connection to ' + host + ' [' + host_config + ']')
                mongo_hosts[host].is_mongos
            except Exception as e:
                self.logger.info('MONGO - could not connected to ' + host, e)
                raise e

            self.logger.info('MONGO - connected to ' + host)

        return mongo_hosts

    def get_mongo_databases(self):
        try:
            mongo_db_config = self.config.get('db').get('mongo').get('database')
            assert mongo_db_config is not None
        except:
            self.logger.info('MONGO - no db defined')
            return None

        mongo_db = {}

        for db, db_config in mongo_db_config.items():
            try:
                mongo_db[db] = self.mongo_hosts[db_config['host']][db_config['dbname']]
            except:
                self.logger.error('MONGO - could not find database ' + db_config['dbname'])
                raise Exception('MONGO - could not find database ' + db_config['dbname'])

        return mongo_db

    def get_mongo_collections(self):
        try:
            mongo_collection_config = self.config.get('db').get('mongo').get('collection')
            assert mongo_collection_config is not None
        except:
            self.logger.info('MONGO - no collection defined')
            return None

        mongo_collections = {}

        for coll, coll_config in mongo_collection_config.items():
            if (coll_config['collection'] not in
                    self.mongo_databases[coll_config['database']].collection_names()):
                self.logger.error('MONGO - could not find collection ' + coll_config['collection'])
                raise Exception('could not find collection ' + coll_config['collection'])

            mongo_collections[coll] = (
                self.mongo_databases[coll_config['database']][coll_config['collection']])

        return mongo_collections

    def __init__(self):
        self.service_name = os.environ['ARABICA_SERVICE']
        self.config = self.get_config()
        self.logger = self.get_logger()
        self.get_mongo()
        self.service_config = {
            'config': self.config,
            'logger': self.logger,
        }
        if hasattr(self, 'mongo_collections'):
            self.service_config['mongo_collections'] = self.mongo_collections

    def start(self):
        self.app = web.Application(self.web_routes())
        self.server = httpserver.HTTPServer(self.app)
        self.server.bind(self.config['port'])
        self.server.start(self.config['threads_nb'])
        ioloop.IOLoop.current().start()

    def __del__(self):
        ioloop.IOLoop.current().stop()
        if hasattr(self, 'server'):
            self.server.stop()

    def web_routes(self):
        return []


class ServiceRequestHandler(web.RequestHandler):
    def initialize(self, service_config):
        if 'mongo_collections' in service_config:
            self.mongo_collections = service_config['mongo_collections']
        self.logger = service_config['logger']
        self.config = service_config['config']
