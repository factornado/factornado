import os
from tornado import web, escape
import pandas as pd
import json
import factornado


class Action(web.RequestHandler):
    def put(self, task, key, action):

        # Parse arguments
        priority = self.get_argument('priority', None)
        if priority is not None:
            try:
                priority = int(priority)
            except:
                raise web.HTTPError(409, 'priority argument must be an int')

        # Parse data
        try:
            data = escape.json_decode(self.request.body) if len(self.request.body) else {}
        except:
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
        before = self.application.mongo.tasks.find_one({'_id': _id})
        if before is None:
            before = {
                '_id': _id,
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

        changed = (json.dumps(before, sort_keys=True) != json.dumps(after, sort_keys=True))
        if changed:
            if after['status'] == 'none':
                change = self.application.mongo.tasks.delete_one({'_id': _id})
            else:
                change = self.application.mongo.tasks.replace_one(
                    {'_id': _id}, after, upsert=True)
            assert change.raw_result['ok']

        self.write({'changed': changed, 'before': before, 'after': after})


class Force(web.RequestHandler):
    def put(self, task, key, status):
        # Parse arguments
        priority = self.get_argument('priority', None)
        if priority is not None:
            try:
                priority = int(priority)
            except:
                raise web.HTTPError(409, 'priority argument must be an int')

        # Parse data
        try:
            data = escape.json_decode(self.request.body) if len(self.request.body) else {}
        except:
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
        changed = (json.dumps(before, sort_keys=True) != json.dumps(after, sort_keys=True))

        if changed:
            if after['status'] == 'none':
                change = self.application.mongo.tasks.delete_one({'_id': _id})
            else:
                change = self.application.mongo.tasks.replace_one(
                    {'_id': _id}, after, upsert=True)
            assert change.raw_result['ok']

        self.write({'changed': changed, 'before': before, 'after': after})


class AssignOne(web.RequestHandler):
    def put(self, task):
        todo = self.application.mongo.tasks.find_one(
            {'status': 'todo', 'task': task},
            sort=[('priority', -1), ('ldt', 1)])
        if todo is None:
            self.set_status(204, reason='No task to do')
        else:
            self.application.mongo.tasks.update_one(
                {'_id': todo['_id']},
                {'$set': {
                    'status': 'doing',
                    'statusSince': pd.Timestamp.utcnow().value,
                    }})
            self.write(pd.io.json.dumps(todo))


class GetByKey(web.RequestHandler):
    def get(self, task, key):
        todo = self.application.mongo.tasks.find_one({'key': key, 'task': task})
        if todo is None:
            self.set_status(204, reason='No task matching')
        else:
            self.write(pd.io.json.dumps(todo))


class GetByStatus(web.RequestHandler):
    def get(self, task, status_list):
        status_list = escape.url_unescape(status_list.lower()).split(',')
        self.write(pd.io.json.dumps(
            {status: list(self.application.mongo.tasks.find({'status': status,
                                                             'task': task}))
             for status in status_list}))


class HelloHandler(web.RequestHandler):
    def get(self):
        self.write('This is tasks\n')


config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'tasks.yml')

app = factornado.Application(
    config,
    [
        ("/", HelloHandler),
        ("/action/([^/]*?)/([^/]*?)/([^/]*?)", Action),
        ("/force/([^/]*?)/([^/]*?)/([^/]*?)", Force),
        ("/assignOne/([^/]*?)", AssignOne),
        ("/getByKey/([^/]*?)/([^/]*?)", GetByKey),
        ("/getByStatus/([^/]*?)/([^/]*?)", GetByStatus),
        ])


if __name__ == "__main__":
    app.start_server()
