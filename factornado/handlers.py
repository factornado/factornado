# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json
from collections import OrderedDict
from subprocess import Popen, PIPE
import traceback
import pandas as pd

from tornado import web, escape, httpclient

from factornado.utils import ArgParseError, MissingArgError


class RequestHandler(web.RequestHandler):
    _args = []
    _kwargs = []

    def parse(self):
        """Parse args and kwargs.
        See self.parse_args and self.parse_kwargs.
        """
        try:
            args = self.parse_args()
            kwargs = self.parse_kwargs()
        except (MissingArgError, ArgParseError) as e:
            raise web.HTTPError(400, e.__repr__(), reason=e.__repr__())
        return args, kwargs

    def parse_args(self):
        """Create a list of arguments required, as specified in self._args.
        Example:
            self._args = [
                ('n', 'int', int),
                ('day', 'timestamp', to_ts),
                ])
        `parse_args(self)` will then return an OrderedDict with keys 'n' and 'day',
        based on the request arguments.
        If they were not specified, `MissingArgError` will be raised.
        If their type is not as expected, `ArgParseError` will be raised.
        """
        args = OrderedDict()
        for arg_name, arg_type, arg_function in self._args:
            arg = self.get_argument(arg_name, None)
            if arg is None:
                raise MissingArgError('Argument "{}" is compulsory'.format(arg_name))
            try:
                args[arg_name] = arg_function(arg)
            except Exception as e:
                raise ArgParseError('{} "{}" is not a {}.'.format(arg_name, arg, arg_type))
        return args

    def parse_kwargs(self):
        """Create a list of optional arguments, as specified in self._kwargs.
        Example:
            self._kwargs = [
                ('n', 'int', int, '12'),
                ('day', 'timestamp', to_ts, '2017-01-01'),
                ])
        `parse_kwargs(self)` will then return an OrderedDict with keys 'n' and 'day',
        based on the request arguments.
        If they were not specified, default values will be used (n=12, day=2017-01-01).
        If their type is not as expected, `ArgParseError` will be raised.
        """
        kwargs = OrderedDict()
        for arg_name, arg_type, arg_function, arg_default in self._kwargs:
            arg = self.get_argument(arg_name, arg_default)
            try:
                kwargs[arg_name] = arg_function(arg)
            except Exception as e:
                raise ArgParseError('{} "{}" is not a {}.'.format(arg_name, arg, arg_type))
        return kwargs


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
        self.write(self.application.config)


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

    @web.asynchronous
    def post(self):
        request = httpclient.HTTPRequest(
            '{}/register/{}'.format(
                self.application.config['registry']['url'].rstrip('/'),
                self.application.config['name'],
                ),
            method='POST',
            body=json.dumps({
                'url': 'http://{}:{}'.format(self.application.get_host(),
                                             self.application.get_port()),
                'config': self.application.config,
                }),
            )
        self.client = httpclient.AsyncHTTPClient()
        self.client.fetch(request, self._on_register_response)

    def _on_register_response(self, response):
        self.application.logger.debug('HEARTBEAT : {} ({}).'.format(
                response.code, response.reason[:30]))

        if response.error is None:
            self.write('ok')
        else:
            self.write('ko : ({}) {}'.format(
                    response.code, response.reason))
        self.client.close()
        self.finish()


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

    todo_task = 'todo'
    do_task = 'do'

    def post(self):
        out = self.todo()
        if out['nb'] == 0:
            self.set_status(201)  # Nothing to do.
        self.write(json.dumps(out))

    def todo_list(self, data):
        raise NotImplementedError()

    def todo(self):
        nb_created_tasks = 0

        # We get the `todo` task.
        r = self.application.services.tasks.action.put(
            task=self.application.config['tasks'][self.todo_task],
            key=self.application.config['tasks'][self.todo_task],
            action='stack',
            data={},
            )

        self.application.logger.debug('TODO : Start scanning for new tasks')
        nb_loops = 0

        while True:
            # Get and self-assign the task.
            r = self.application.services.tasks.assignOne.put(
                    task=self.application.config['tasks'][self.todo_task])
            if r.status_code != 200:
                break

            try:
                task = r.json()

                # If `lastScanObjectId` is not defined, we start from 1970-01-01.
                data = task['data']
                data['nb'] = data.get('nb', 0)

                # Get all documents after `lastScanObjectId`
                # #########################################
                todo_tasks, data = self.todo_list(data)
                self.application.logger.debug('TODO : Found {} tasks'.format(len(todo_tasks)))
                for task_key, task_data in todo_tasks:
                    self.application.logger.debug('TODO : Set task {}/{}'.format(task_key,
                                                                                 task_data))
                    r = self.application.services.tasks.action.put(
                        task=self.application.config['tasks'][self.do_task],
                        key=escape.url_escape(task_key),
                        action='stack',
                        data=task_data,
                        )
                    nb_created_tasks += 1

                # Update the task to `done` if nothing happenned since last GET.
                r = self.application.services.tasks.action.put(
                    task=self.application.config['tasks'][self.todo_task],
                    key=self.application.config['tasks'][self.todo_task],
                    action='success',
                    data=data,
                    )
            except Exception as e:
                # Update the task to `done` if nothing happenned since last GET.
                r = self.application.services.tasks.action.put(
                    task=self.application.config['tasks'][self.todo_task],
                    key=self.application.config['tasks'][self.todo_task],
                    action='error',
                    data={
                        'lastError': {
                            'reason': e.__repr__(),
                            'traceback': traceback.format_exc(),
                            'datetime': pd.Timestamp(pd.Timestamp.utcnow().value).isoformat(),
                            }
                        },
                    )
                self.application.logger.exception('TODO : Failed todoing.')
                return {'nb': 0, 'ok': False, 'reason': e.__repr__()}

            nb_loops += 1

        log_str = 'TODO : Finished scanning for new tasks. Found {} in {} loops.'.format(
            nb_created_tasks, nb_loops)
        if nb_created_tasks > 0:
            self.application.logger.info(log_str)
        else:
            self.application.logger.debug(log_str)

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

    do_task = 'do'

    def post(self):
        out = self.do()
        if out['nb'] == 0:
            self.set_status(201)  # Nothing to do.
        self.write(json.dumps(out))

    def do_something(self, task_key, task_data):
        raise NotImplementedError()

    def do(self):
        # Get a task and parse it.
        r = self.application.services.tasks.assignOne.put(
                task=self.application.config['tasks'][self.do_task])
        if r.status_code != 200:
            return {'nb': 0, 'code': r.status_code, 'reason': r.reason, 'ok': False}

        task = r.json()
        task_key = task['_id'].split('/')[-1]
        task_data = task['data']

        try:
            self.application.logger.debug('DO : Got task: {}'.format(task_key))
            self.application.logger.debug('DO : Got task data: {}'.format(task_data))
            # Load the statuses.
            out = self.do_something(task_key, task_data)

            # Set the task as `done`.
            self.application.services.tasks.action.put(
                task=self.application.config['tasks'][self.do_task],
                key=escape.url_escape(task_key),
                action='success',
                data=task_data,
                )
            return {'nb': 1, 'key': task_key, 'ok': True, 'out': out}
        except Exception as e:
            # Set the task as `fail`.
            self.application.services.tasks.action.put(
                task=self.application.config['tasks'][self.do_task],
                key=escape.url_escape(task_key),
                action='error',
                data={
                    'lastError': {
                        'reason': e.__repr__(),
                        'traceback': traceback.format_exc(),
                        'datetime': pd.Timestamp(pd.Timestamp.utcnow().value).isoformat(),
                        }
                    },
                )
            self.application.logger.exception('DO : Failed doing task {}.'.format(task_key))
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
            ("resourcePath", "/{}".format(self.application.config['name'])),
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
                        name=self.application.config['name'],
                        uri=uri.lstrip('/'))),
                    ('operations', handler.swagger.get('operations', []))
                    ]))

        self.write(json.dumps(sw, indent=2))


class Log(web.RequestHandler):
    swagger = {
        "path": "/{name}/{uri}",
        "operations": [
            {
                "notes": "Get the server logs.",
                "method": "GET",
                "responseMessages": [
                    {"message": "OK", "code": 200},
                    {"message": "Unauthorized", "code": 401},
                    {"message": "Forbidden", "code": 403},
                    {"message": "Not Found", "code": 404}
                    ],
                "deprecated": False,
                "produces": ["application/json"],
                "parameters": [
                    {
                        "name": "n",
                        "type": "integer",
                        "format": "int32",
                        "paramType": "query",
                        "required": False,
                        "defaultValue": 20,
                        "description": "The number of lines to retrieve."
                        }
                    ]
                }
            ]}

    def get(self):
        n = self.get_argument('n', '20')
        try:
            n = int(n)
        except:
            raise web.HTTPError(400, 'Argument {} is not an int'.format(n))

        filename = self.application.config['log']['file']
        tail = Popen(['tail', '-%d' % n, filename], stdout=PIPE).communicate()[0]
        self.write(tail)
