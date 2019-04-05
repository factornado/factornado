# -*- coding: utf-8 -*-

import json
from collections import OrderedDict
from subprocess import Popen, PIPE
import traceback
import pandas as pd
import logging

from tornado import web, escape, httpclient

from factornado.utils import ArgParseError, MissingArgError

factornado_logger = logging.getLogger('factornado')


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
            raise web.HTTPError(400, e.__repr__(), reason=e.__str__())
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
            except Exception:
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
            except Exception:
                raise ArgParseError('{} "{}" is not a {}.'.format(arg_name, arg, arg_type))
        return kwargs


class Info(web.RequestHandler):
    swagger = {
        "/{name}/{uri}": {
            "post": {
                "description": "Get the information on the service's parameters.",
                "parameters": [],
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def get(self):
        self.write(self.application.config)


class Heartbeat(web.RequestHandler):
    swagger = {
        "/{name}/{uri}": {
            "post": {
                "description": "Tell registry that the service is alive.",
                "parameters": [],
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    async def post(self):
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
        response = await self.client.fetch(request)

        factornado_logger.debug('HEARTBEAT: {} ({}).'.format(
                response.code, response.reason[:30]))

        if response.error is None:
            self.write('ok')
        else:
            self.write('ko: ({}) {}'.format(
                    response.code, response.reason))
        self.client.close()


class Todo(web.RequestHandler):
    swagger = {
        "/{name}/{uri}": {
            "post": {
                "description": "Update the list of tasks to be done.",
                "parameters": [],
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

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

        factornado_logger.debug('TODO: Start scanning for new tasks')
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
                factornado_logger.debug('TODO: Found {} tasks'.format(len(todo_tasks)))
                for task_key, task_data in todo_tasks:
                    factornado_logger.debug('TODO: Set task {}/{}'.format(task_key,
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
                factornado_logger.exception('TODO: Failed todoing.')
                return {'nb': 0, 'ok': False, 'reason': e.__repr__()}

            nb_loops += 1

        log_str = 'TODO: Finished scanning for new tasks. Found {} in {} loops.'.format(
            nb_created_tasks, nb_loops)
        if nb_created_tasks > 0:
            factornado_logger.info(log_str)
        else:
            factornado_logger.debug(log_str)

        return {'nb': nb_created_tasks, 'nbLoops': nb_loops}


class Do(web.RequestHandler):
    swagger = {
        "/{name}/{uri}": {
            "post": {
                "description": "Pick one task and do it.",
                "parameters": [],
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

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
            factornado_logger.debug('DO: Got task: {}'.format(task_key))
            factornado_logger.debug('DO: Got task data: {}'.format(task_data))
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
            factornado_logger.exception('DO: Failed doing task {}.'.format(task_key))
            return {'nb': 0, 'key': task_key, 'ok': False, 'reason': e.__repr__()}


class Log(web.RequestHandler):
    swagger = {
        "/{name}/{uri}": {
            "get": {
                "description": "Get the server logs.",
                "parameters": [{
                    "in": "query",
                    "name": "n",
                    "required": False,
                    "description": "The number of lines to retrieve.",
                    "schema": {
                        "type": "integer",
                        "format": "int32",
                        "default": 20
                    }
                }],
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def get(self):
        n = self.get_argument('n', '20')
        try:
            n = int(n)
        except Exception:
            raise web.HTTPError(400, 'Argument {} is not an int'.format(n))

        filename = self.application.config['log']['file']
        tail = Popen(['tail', '-%d' % n, filename], stdout=PIPE).communicate()[0]
        self.write(tail)


class Swagger(web.RequestHandler):
    swagger = {
        "/{name}/{uri}": {
            "get": {
                "description": "Get the service documentation.",
                "parameters": [],
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def initialize(self, handlers=[]):
        self.handlers = handlers

    def get(self):
        sw = OrderedDict([
            ("openapi", "3.0.0"),
            ("info", {
                "title": self.application.config['name'],
                "version": (
                    'v1.0'
                    if 'tag' not in self.application.config
                    else self.application.config['tag']
                ),
            }),
            ("servers", [{
                "url": "/api"
            }]),
            ("paths", {}),
            ("components", {})
        ])

        for h in self.application.handler_list:
            uri, handler = h[:2]
            # Attribute swagger contains path declaration
            # https://swagger.io/specification/#pathsObject
            if hasattr(handler, 'swagger') and len(handler.swagger.keys()) > 0:
                path = list(handler.swagger.keys())[0]
                final_path = path.format(
                    name=self.application.config['name'],
                    uri=uri.lstrip('/')
                )
                sw['paths'][final_path] = handler.swagger[path]

        # Security context in swagger
        if 'sso' in self.application.config:
            sso = self.application.config['sso']
            sw['components']['securitySchemes'] = {
                "oauth": {
                    "type": "oauth2",
                    "description": "This API uses OAuth 2 with the password grant flow",
                    "flows": {
                        "password": {
                            "tokenUrl": "{}realms/{}/protocol/openid-connect/token".format(
                                sso['url'],
                                sso['realm']
                            )
                        }
                    }
                }
            }
        # This object is usefull to declare generic types,
        # service response representation...
        # https://swagger.io/docs/specification/components/
        # https://swagger.io/specification/#schemaObject
        if self.application.swagger_components is not None:
            for key, value in self.application.swagger_components.iteritems():
                sw['components'][key] = value

        self.write(json.dumps(sw, indent=2))
