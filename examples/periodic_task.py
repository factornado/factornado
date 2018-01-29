import os
from tornado import web
import pandas as pd
import factornado

import bson


class HelloHandler(web.RequestHandler):
    def get(self):
        self.write('This is periodic_task\n')


class Todo(factornado.Todo):
    def todo_list(self, data):

        # From data, we get the latest _id read.
        begin = data.get('begin', '0'*24)

        # We get at most 2 data from mongo.
        cur = self.application.mongo.periodic.find(
            {'_id': {'$gt': bson.ObjectId(begin)}},
            sort=[('_id', 1)],
            ).limit(2)

        # For each one, we create a task.
        task_list = []
        _id = begin
        for doc in cur:
            _id = hex(int.from_bytes(doc.pop('_id').binary, 'big'))[2:]
            task_list.append((_id, doc))

        # We update data's begin
        data['begin'] = _id

        return task_list, data


class Do(factornado.Do):
    def do_something(self, task_key, task_data):
        # In this example, doing a task is inserting a new document.
        nb = task_data.get('nb', 0)
        self.application.mongo.periodic.insert_one(
            {'dt': pd.Timestamp.utcnow(),
             'nb': nb + 1})


class LatestDoc(web.RequestHandler):
    swagger = {
        "path": "/{name}/{uri}",
        "operations": [
            {
                "notes": "Provide the latest doc writtent in the database.",
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
        doc = self.application.mongo.periodic.find_one({}, sort=[('nb', -1)])
        if doc is not None:
            doc['_id'] = hex(int.from_bytes(doc['_id'].binary, 'big'))[2:]
            doc['dt'] = pd.Timestamp(doc['dt']).isoformat()
        self.write(pd.io.json.dumps(doc))


config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'periodic_task.yml')

app = factornado.Application(
    config,
    [
        ("/", HelloHandler),
        ("/todo", Todo),
        ("/do", Do),
        ("/latest", LatestDoc),
        ])


if __name__ == "__main__":
    app.start_server()
