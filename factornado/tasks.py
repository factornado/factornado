# -*- coding: utf-8 -*-
import pandas as pd
import json
import bson
import pymongo
import logging

from tornado import web, escape
from factornado.utils import SwaggerPath, tansform_bson_id

factornado_logger = logging.getLogger('factornado')


class Action(web.RequestHandler):
    swagger = {
        SwaggerPath("/{name}/{uri}/{{task}}/{{key}}/{{action}}"): {
            "put": {
                "description": "Change a task status in applying an action.",
                "parameters": [
                    {
                        "in": "path",
                        "name": "task",
                        "required": True,
                        "description": "The task category.",
                        "schema": {
                            "type": "string",
                            "default": "someTask"
                        }
                    },
                    {
                        "in": "path",
                        "name": "key",
                        "required": True,
                        "description": "The task key: it has to be unique.",
                        "schema": {
                            "type": "string",
                            "default": "someKey"
                        }
                    },
                    {
                        "in": "path",
                        "name": "action",
                        "required": True,
                        "description": "The action to perform: delete|assign|success|stack|error.",
                        "schema": {
                            "type": "string",
                            "enum": ["delete", "assign", "success", "stack", "error"],
                            "default": "stack"
                        }
                    }
                ],
                "requestBody": {
                    "description": "Data attached to the task.",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                'properties': {
                                    "day": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def put(self, task, key, action):

        # Parse arguments
        priority = self.get_argument('priority', None)
        if priority is not None:
            try:
                priority = int(priority)
            except Exception:
                raise web.HTTPError(409, 'priority argument must be an int')

        # Parse data
        try:
            data = escape.json_decode(self.request.body) if len(self.request.body) else {}
        except Exception:
            raise web.HTTPError(
                501,
                reason="Bytes `{}...` are not JSON serializable".format(self.request.body[:30]))

        action = action.lower()
        if action not in self.application.config['actions']:
            raise web.HTTPError(
                411,
                reason="Action '{}' not understood. Expect {}.".format(
                    action, '|'.join(self.application.config['actions'])))
        _id = '/'.join([task, key])

        while True:
            before = self.application.mongo.tasks.find_one({'_id': _id})
            if before is None:
                before = {
                    '_id': _id,
                    'id': None,
                    'task': task,
                    'key': key,
                    'status': 'none',
                    'data': {},
                    'statusSince': None,
                    'try': 0,
                    'priority': 0,
                    }

            next_status = self.application.config['actions'][action].get(before['status'])
            if next_status is None:
                raise web.HTTPError(
                    411,
                    reason="Action '{}' cannot be performed on status '{}'.".format(
                        action, before['status']))
            after = {
                '_id': _id,
                'id': before['id'],
                'task': task,
                'key': key,
                'status': next_status,
                'data': dict(before['data'].copy(), **data),
                'statusSince': (
                    before['statusSince'] if next_status == before['status']
                    else pd.Timestamp.utcnow().value),
                'try': before['try'] + (action == 'error'),
                'priority': priority if priority is not None else before.get('priority')
                }

            changed = (json.dumps(tansform_bson_id(before), sort_keys=True) !=
                       json.dumps(tansform_bson_id(after), sort_keys=True))
            if changed:
                if after['status'] == 'none':
                    change = self.application.mongo.tasks.delete_one({'_id': _id,
                                                                      'id': before['id']})
                    count = change.deleted_count
                    assert change.raw_result['ok']
                elif before['status'] == 'none':
                    factornado_logger.debug('Will insert')
                    after['id'] = bson.ObjectId()
                    try:
                        self.application.mongo.tasks.insert_one(after)
                        count = 1
                    except pymongo.errors.DuplicateKeyError:
                        count = 0
                else:
                    after['id'] = bson.ObjectId()
                    change = self.application.mongo.tasks.replace_one(
                        {'_id': _id, 'id': before['id']}, after, upsert=False)
                    count = change.modified_count
                    assert change.raw_result['ok']

                if count == 0:
                    # Someone came before
                    if action == 'assign':
                        # Cannot assign the task if someone came before.
                        self.set_status(204, reason='No task to do')
                    else:
                        # You can perform the action ; let's try again.
                        pass
                else:
                    # We got the right to write
                    self.write({
                            'changed': changed,
                            'before': tansform_bson_id(before),
                            'after': tansform_bson_id(after)})
                    break
            else:
                # We had nothing to write
                self.write({'changed': changed,
                            'before': tansform_bson_id(before),
                            'after': tansform_bson_id(after)})
                break


class Force(web.RequestHandler):
    swagger = {
        SwaggerPath("/{name}/{uri}/{{task}}/{{key}}/{{status}}"): {
            "put": {
                "description": "Force a task status.",
                "parameters": [
                    {
                        "in": "path",
                        "name": "task",
                        "required": True,
                        "description": "The task category.",
                        "schema": {
                            "type": "string",
                            "default": "someTask"
                        }
                    },
                    {
                        "in": "path",
                        "name": "key",
                        "required": True,
                        "description": "The task key: it has to be unique.",
                        "schema": {
                            "type": "string",
                            "default": "someKey"
                        }
                    },
                    {
                        "in": "path",
                        "name": "status",
                        "required": True,
                        "description": "The status to be set: done|toredo|fail|todo|doing|none.",
                        "schema": {
                            "type": "string",
                            "enum": ["done", "toredo", "fail", "todo", "doing", "none"],
                            "default": "todo"
                        }
                    }
                ],
                "requestBody": {
                    "description": "Data attached to the task.",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                'properties': {
                                    "day": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                },
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def put(self, task, key, status):
        # Parse arguments
        priority = self.get_argument('priority', None)
        if priority is not None:
            try:
                priority = int(priority)
            except Exception:
                raise web.HTTPError(409, 'priority argument must be an int')

        # Parse data
        try:
            data = escape.json_decode(self.request.body) if len(self.request.body) else {}
        except Exception:
            raise web.HTTPError(
                501,
                reason="Bytes `{}...` are not JSON serializable".format(self.request.body[:30]))

        status = status.lower()
        if status not in self.application.config['actions']['delete']:
            raise web.HTTPError(
                411,
                reason="Status '{}' not understood. Expect {}.".format(
                    status, '|'.join(self.application.config['actions']['delete'])))
        _id = '/'.join([task, key])
        before = self.application.mongo.tasks.find_one({'_id': _id})
        if before is None:
            before = {
                '_id': _id,
                'id': None,
                'task': task,
                'key': key,
                'status': 'none',
                'data': {},
                'statusSince': None,
                'try': 0,
                'priority': 0,
                }

        after = {
            '_id': _id,
            'id': before['id'],
            'task': task,
            'key': key,
            'status': status,
            'data': dict(before['data'].copy(), **data),
            'statusSince': (
                before['statusSince'] if status == before['status']
                else pd.Timestamp.utcnow().value),
            'try': before['try'],
            'priority': priority if priority is not None else before.get('priority')
            }
        changed = (json.dumps(tansform_bson_id(before), sort_keys=True) !=
                   json.dumps(tansform_bson_id(after), sort_keys=True))

        if changed:
            if after['status'] == 'none':
                change = self.application.mongo.tasks.delete_one({'_id': _id})
            else:
                after['id'] = bson.ObjectId()
                change = self.application.mongo.tasks.replace_one(
                    {'_id': _id}, after, upsert=True)
            assert change.raw_result['ok']

        self.write({'changed': changed,
                    'before': tansform_bson_id(before),
                    'after': tansform_bson_id(after)})


class AssignOne(web.RequestHandler):
    swagger = {
        SwaggerPath("/{name}/{uri}/{{task}}"): {
            "put": {
                "description": "Pick a task that has not been done yet, and assign it.",
                "parameters": [
                    {
                        "in": "path",
                        "name": "task",
                        "required": True,
                        "description": "The task category.",
                        "schema": {
                            "type": "string",
                            "default": "someTask"
                        }
                    }
                ],
                "responses": {
                    200: {"description": "OK"},
                    204: {"description": "No task to do"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def put(self, task):
        while True:
            cursor = self.application.mongo.tasks.find(
                {'status': 'todo', 'task': task},
                sort=[('priority', -1), ('ldt', 1)])
            yielded = 0
            for todo in cursor:
                r = self.application.mongo.tasks.update_one(
                    {'_id': todo['_id'], 'id': todo['id']},
                    {'$set': {
                            'status': 'doing',
                            'statusSince': pd.Timestamp.utcnow().value,
                            'id': bson.ObjectId(),
                        }})
                if r.modified_count == 1:
                    self.write(pd.io.json.dumps(tansform_bson_id(todo)))
                    yielded = 1
                    break
                else:
                    # Someone got this one before. Let's try another one
                    pass

            if yielded == 1:
                # We got a task. It's finished
                break
            elif cursor.retrieved == 0:
                # There where no task to do.
                self.set_status(204, reason='No task to do')
                break
            else:
                # There where tasks, but they where all got by someone else.
                # Let's retry
                pass


class GetByKey(web.RequestHandler):
    swagger = {
        SwaggerPath("/{name}/{uri}/{{task}}/{{key}}"): {
            "get": {
                "description": "Get a task with given key (alter nothing).",
                "parameters": [
                    {
                        "in": "path",
                        "name": "task",
                        "required": True,
                        "description": "The task category.",
                        "schema": {
                            "type": "string",
                            "default": "someTask"
                        }
                    },
                    {
                        "in": "path",
                        "name": "key",
                        "required": True,
                        "description": "The task key: it has to be unique.",
                        "schema": {
                            "type": "string",
                            "default": "someKey"
                        }
                    }
                ],
                "responses": {
                    200: {"description": "OK"},
                    204: {"description": "No task matching"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def get(self, task, key):
        todo = self.application.mongo.tasks.find_one({'key': key, 'task': task})
        if todo is None:
            self.set_status(204, reason='No task matching')
        else:
            self.write(pd.io.json.dumps(tansform_bson_id(todo)))


class GetByStatus(web.RequestHandler):
    swagger = {
        SwaggerPath("/{name}/{uri}/{{task}}/{{status}}"): {
            "get": {
                "description": "Get tasks with given status (alter nothing).",
                "parameters": [
                    {
                        "in": "path",
                        "name": "task",
                        "required": True,
                        "description": "The task category.",
                        "schema": {
                            "type": "string",
                            "default": "someTask"
                        }
                    },
                    {
                        "in": "path",
                        "name": "status",
                        "required": True,
                        "description": "The status to be set: done|toredo|fail|todo|doing|none.",
                        "schema": {
                            "type": "string",
                            "enum": ["done", "toredo", "fail", "todo", "doing", "none"],
                            "default": "todo"
                        }
                    }
                ],
                "responses": {
                    200: {"description": "OK"},
                    204: {"description": "No task matching"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def get(self, task, status_list):
        status_list = escape.url_unescape(status_list.lower()).split(',')
        self.write(pd.io.json.dumps(
            {status: list(map(tansform_bson_id,
                              self.application.mongo.tasks.find({'status': status,
                                                                 'task': task})))
             for status in status_list}))
