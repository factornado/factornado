import os
import time
import json
import logging
import socket
import yaml
import re
from collections import OrderedDict

import pymongo
import requests
import pandas as pd
from tornado import ioloop, web, httpserver, process, httpclient, escape

__version__ = '0.2.0'


class Kwargs(object):
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            self.__setattr__(key, val)


class WebMethod(object):
    def __init__(self, method, url):
        self.method = method
        self.url = url
        self.params = re.findall("{(.*?)}", self.url)
        self.__doc__ = (
            '\nParameters\n----------\n' +
            '\n'.join(["{} : str".format(x) for x in self.params]))

    def __call__(self, data='', **kwargs):
        response = requests.request(
            method=self.method,
            url=self.url.format(**kwargs),
            data=pd.json.dumps(data),
            )
        if not response.ok:
            logging.warning('Error in {} {}'.format(self.method, self.url.format(**kwargs)))
            raise web.HTTPError(response.status_code, response.reason)
        return response


class Config(object):
    def __init__(self, filename):
        self.conf = yaml.load(open(filename))
        self.mongo = Kwargs()
        _mongo = self.conf.get('db', {}).get('mongo', {})
        self.mongo = Kwargs(**{
            collname: pymongo.MongoClient(host['address'],
                                          connect=False)[db['name']][coll['name']]
            for hostname, host in _mongo.get('host', {}).items()
            for dbname, db in _mongo.get('database', {}).items() if db['host'] == hostname
            for collname, coll in _mongo.get('collection', {}).items() if coll['database'] == dbname
            })
        self.services = Kwargs(**{
            key: Kwargs(**{
                subkey: Kwargs(**{
                    subsubkey: WebMethod(
                        subsubkey,
                        self.conf['registry']['url'].rstrip('/')+subsubval,
                        )
                    for subsubkey, subsubval in subval.items()})
                for subkey, subval in val.items()
                })
            for key, val in self.conf.get('services', {}).items()})

    def get_port(self):
        if 'port' not in self.conf:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            self.conf['port'] = s.getsockname()[1]
            s.close()
        return self.conf['port']

    def register(self):
        request = httpclient.HTTPRequest(
            '{}/register/{}'.format(
                self.conf['registry']['url'].rstrip('/'),
                self.conf['name'],
                ),
            method='POST',
            body=json.dumps({
                'url': 'http://{}:{}'.format(socket.gethostname(),
                                             self.get_port()),
                'config': self.conf,
                }),
            )
        client = httpclient.HTTPClient()
        r = client.fetch(request, raise_error=False)
        logging.debug('HEARTBEAT : {} ({}).'.format(
                r.code, r.reason[:30]))


class Callback(object):
    def __init__(self, config, uri, sleep_duration=0):
        self.config = config
        self.uri = uri
        self.sleep_duration = sleep_duration

    def __call__(self):
        logging.debug('{} callback started'.format(self.uri))
        r = requests.post('http://localhost:{}/{}'.format(
                self.config.get_port(),
                self.uri.lstrip('/')))
        if r.status_code != 200:
            logging.debug('{} callback returned {}. Sleep for a while.'.format(
                self.uri, r.status_code))
            time.sleep(self.sleep_duration)
        logging.debug('{} callback finished : {}'.format(self.uri, r.text))


class Info(web.RequestHandler):
    swagger = {
        "path": "/{name}/{uri}",
        "operations": [
            {
                "notes": "Get the information on the service's parameters.",
                "method": "GET",
                "responseMessages": [
                    {"message": "OK", "code": 200},
                    {"message": "Unauthorized", "code": 401},
                    {"message": "Forbidden", "code": 403},
                    {"message": "Not Found", "code": 404}
                    ],
                "deprecated": False,
                "produces": ["application/json"],
                "parameters": []
                }
            ]}

    def get(self):
        self.write(self.application.config.conf)


class Heartbeat(web.RequestHandler):
    swagger = {
        "path": "/{name}/{uri}",
        "operations": [
            {
                "notes": "Tell the registry that the service is alive.",
                "method": "POST",
                "responseMessages": [
                    {"message": "OK", "code": 200},
                    {"message": "Unauthorized", "code": 401},
                    {"message": "Forbidden", "code": 403},
                    {"message": "Not Found", "code": 404}
                    ],
                "deprecated": False,
                "produces": ["application/json"],
                "parameters": []
                }
            ]}

    def post(self):
        self.application.config.register()
        self.write("ok")


class Todo(web.RequestHandler):
    swagger = {
        "path": "/{name}/{uri}",
        "operations": [
            {
                "notes": "Update the list of tasks to be done.",
                "method": "POST",
                "responseMessages": [
                    {"message": "OK", "code": 200},
                    {"message": "Unauthorized", "code": 401},
                    {"message": "Forbidden", "code": 403},
                    {"message": "Not Found", "code": 404}
                    ],
                "deprecated": False,
                "produces": ["application/json"],
                "parameters": []
                }
            ]}

    def post(self):
        out = self.todo()
        if out['nb'] == 0:
            self.set_status(201)  # Nothing to do.
        self.write(json.dumps(out))

    def todo_loop(self, data):
        raise NotImplementedError()

    def todo(self):
        nb_created_tasks = 0

        # We get the `todo` task.
        r = self.application.config.services.tasks.action.put(
            task=self.application.config.conf['tasks']['todo'],
            key=self.application.config.conf['tasks']['todo'],
            action='stack',
            data={},
            )

        logging.debug('TODO : Start scanning for new tasks')
        nb_loops = 0

        while True:
            # Get and self-assign the task.
            r = self.application.config.services.tasks.assignOne.put(
                    task=self.application.config.conf['tasks']['todo'])
            if r.status_code != 200:
                break

            try:
                task = r.json()

                # If `lastScanObjectId` is not defined, we start from 1970-01-01.
                data = task['data']
                data['nb'] = data.get('nb', 0)

                # Get all documents after `lastScanObjectId`
                # #########################################
                todo_tasks = list(self.todo_loop(data))
                logging.debug('TODO : Found {} tasks'.format(len(todo_tasks)))
                for task_key, task_data in todo_tasks:
                    logging.debug('TODO : Set task {}/{}'.format(task_key, task_data))
                    r = self.application.config.services.tasks.action.put(
                        task=self.application.config.conf['tasks']['do'],
                        key=escape.url_escape(task_key),
                        action='stack',
                        data=task_data,
                        )
                    nb_created_tasks += 1

                # Update the task to `done` if nothing happenned since last GET.
                r = self.application.config.services.tasks.action.put(
                    task=self.application.config.conf['tasks']['todo'],
                    key=self.application.config.conf['tasks']['todo'],
                    action='success',
                    data=data,
                    )
            except Exception as e:
                # Update the task to `done` if nothing happenned since last GET.
                r = self.application.config.services.tasks.action.put(
                    task=self.application.config.conf['tasks']['todo'],
                    key=self.application.config.conf['tasks']['todo'],
                    action='error',
                    data={},
                    )
                logging.exception('TODO : Failed todoing.')
                return {'nb': 0, 'ok': False, 'reason': e.__repr__()}

            nb_loops += 1

        log_str = 'TODO : Finished scanning for new tasks. Found {} in {} loops.'.format(
            nb_created_tasks, nb_loops)
        if nb_created_tasks > 0:
            logging.info(log_str)
        else:
            logging.debug(log_str)

        return {'nb': nb_created_tasks, 'nbLoops': nb_loops}


class Do(web.RequestHandler):
    swagger = {
        "path": "/{name}/{uri}",
        "operations": [
            {
                "notes": "Pick one task and do it.",
                "method": "POST",
                "responseMessages": [
                    {"message": "OK", "code": 200},
                    {"message": "Unauthorized", "code": 401},
                    {"message": "Forbidden", "code": 403},
                    {"message": "Not Found", "code": 404}
                    ],
                "deprecated": False,
                "produces": ["application/json"],
                "parameters": []
                }
            ]}

    def post(self):
        out = self.do()
        if out['nb'] == 0:
            self.set_status(201)  # Nothing to do.
        self.write(json.dumps(out))

    def do_something(self, task_key, task_data):
        raise NotImplementedError()

    def do(self):
        # Get a task and parse it.
        r = self.application.config.services.tasks.assignOne.put(
                task=self.application.config.conf['tasks']['do'])
        if r.status_code != 200:
            return {'nb': 0, 'code': r.status_code, 'reason': r.reason, 'ok': False}

        task = r.json()
        task_key = task['_id'].split('/')[-1]
        task_data = task['data']

        try:
            logging.debug('DO : Got task: {}'.format(task_key))
            logging.debug('DO : Got task data: {}'.format(task_data))
            # Load the statuses.
            out = self.do_something(task_key, task_data)

            # Set the task as `done`.
            self.application.config.services.tasks.action.put(
                task=self.application.config.conf['tasks']['do'],
                key=escape.url_escape(task_key),
                action='success',
                data={},
                )
            return {'nb': 1, 'key': task_key, 'ok': True, 'out': out}
        except Exception as e:
            # Set the task as `fail`.
            self.application.config.services.tasks.action.put(
                task=self.application.config.conf['tasks']['do'],
                key=escape.url_escape(task_key),
                action='error',
                data={},
                )
            logging.exception('DO : Failed doing task {}.'.format(task_key))
            return {'nb': 0, 'key': task_key, 'ok': False, 'reason': e.__repr__()}


class Swagger(web.RequestHandler):
    swagger = {
        "path": "/{name}/{uri}",
        "operations": [
            {
                "notes": "Get the module documentation.",
                "method": "GET",
                "responseMessages": [
                    {"message": "OK", "code": 200},
                    {"message": "Unauthorized", "code": 401},
                    {"message": "Forbidden", "code": 403},
                    {"message": "Not Found", "code": 404}
                    ],
                "deprecated": False,
                "produces": ["application/json"],
                "parameters": []
                }
            ]}

    def initialize(self, handlers=[]):
        self.handlers = handlers

    def get(self):
        sw = OrderedDict([
            ("swaggerVersion", "1.2"),
            ("resourcePath", "/{}".format(self.application.config.conf['name'])),
            ("basePath", "/api"),
            ("apiVersion", "1.0"),
            ("produces", ["*/*", "application/json"]),
            ("apis", []),
            ])

        for h in self.application.handler_list:
            uri, handler = h[:2]
            if hasattr(handler, 'swagger'):
                sw['apis'].append(OrderedDict([
                    ('path', handler.swagger.get('path', "/{name}/{uri}").format(
                        name=self.application.config.conf['name'],
                        uri=uri.lstrip('/'))),
                    ('operations', handler.swagger.get('operations', []))
                    ]))

        self.write(json.dumps(sw, indent=2))


class Application(web.Application):
    def __init__(self, config, handlers, **kwargs):
        self.config = config if isinstance(config, Config) else Config(config)
        self.handler_list = [
            ("/swagger.json", Swagger),
            ("/swagger", web.RedirectHandler, {'url': '/swagger.json'}),
            ("/heartbeat", Heartbeat),
            ("/info", Info),
            ] + handlers
        super(Application, self).__init__(self.handler_list, **kwargs)

    def start_server(self):
        logging.basicConfig(
            level=self.config.conf['log']['level'],  # Set to 10 for debug.
            filename=self.config.conf['log']['file'],
            format='%(asctime)s (%(filename)s:%(lineno)s)- %(levelname)s - %(message)s',
            )
        logging.Formatter.converter = time.gmtime
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('tornado').setLevel(logging.WARNING)
        logging.info('='*80)

        port = self.config.get_port()  # We need to have a fixed port in both forks.
        logging.info('Listening on port {}'.format(port))
        time.sleep(2)  # We sleep for a few seconds to let the registry start.
        if os.fork():
            self.config.register()
            server = httpserver.HTTPServer(self)
            server.bind(self.config.get_port(), address='0.0.0.0')
            server.start(self.config.conf['threads_nb'])
            ioloop.IOLoop.current().start()
        else:
            if self.config.conf.get('callbacks', None) is not None:
                for key, val in self.config.conf['callbacks'].items():
                    if os.fork():
                        if val['threads']:
                            process.fork_processes(val['threads'])
                            ioloop.PeriodicCallback(
                                Callback(self.config, val['uri'],
                                         sleep_duration=val.get('sleep', 0)),
                                val['period']*1000).start()
                            ioloop.IOLoop.instance().start()


if __name__ == '__main__':

    class MyToDo(Todo):
        def todo_loop(self, data):
            for k in range(2):
                data['nb'] += 1
                yield 'ABCDE'[data['nb'] % 5], {}

    class MyDo(Do):
        def do_something(self, task_key, task_data):
            return 'something'

    app = Application('config.yml', [
        ("/todo", MyToDo),
        ("/do", MyDo),
        ], )

    app.start_server()
