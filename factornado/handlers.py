import json
import logging
import socket
from collections import OrderedDict

from tornado import web, escape, httpclient

RequestHandler = web.RequestHandler


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
                'url': 'http://{}:{}'.format(socket.gethostname(),
                                             self.application.get_port()),
                'config': self.application.config,
                }),
            )
        client = httpclient.AsyncHTTPClient()
        client.fetch(request, self._on_register_response)

    def _on_register_response(self, response):
        logging.debug('HEARTBEAT : {} ({}).'.format(
                response.code, response.reason[:30]))

        if response.error is None:
            self.write('ok')
        else:
            self.write('ko : ({}) {}'.format(
                    response.code, response.reason))
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

    def todo_loop(self, data):
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

        logging.debug('TODO : Start scanning for new tasks')
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
                todo_tasks = list(self.todo_loop(data))
                logging.debug('TODO : Found {} tasks'.format(len(todo_tasks)))
                for task_key, task_data in todo_tasks:
                    logging.debug('TODO : Set task {}/{}'.format(task_key, task_data))
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
            logging.debug('DO : Got task: {}'.format(task_key))
            logging.debug('DO : Got task data: {}'.format(task_data))
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
