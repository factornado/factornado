# -*- coding: utf-8 -*-
"""
Factornados Tests
-----------------
"""

# import os
# import sys
# import factornado.service

import factornado
import yaml
import io

config = yaml.load(io.StringIO("""
name: someModule-dev
version: v1

threads_nb: 4

registry:
    url: http://localhost:8800/

port: 42646

log:
    file: /tmp/someModule.log
    level: 10

callbacks:
    heartbeat:
        threads: 1
        uri: /heartbeat
        period: 120
    todo:
        threads: 1    # Nb of threads
        uri: /todo    # The URI to call
        period: 10    # The callback period (in sec)
        sleep: 10     # If return is not 200, sleep for .... (in sec)
    do:
        threads: 1    # Nb of threads
        uri: /do      # The URI to call
        period: 3     # The callback period (in sec)
        sleep: 10     # If return is not 200, sleep for .... (in sec)

db:
    mongo:
        host:
            host_alias:
                address: 'mongodb://localhost:27017'
        database:
            db_alias:
                host: host_alias
                name: 'db_name'
        collection:
            collection_alias:
                database: db_alias
                name: 'collection_name'

services:
    tasks:
        action:
            put: /tasks-v1/action/{task}/{key}/{action}
        assignOne:
            put: /tasks-v1/assignOne/{task}

tasks:
    todo: todo-someModule-dev
    do: someModule-dev
"""))

class ToDo(factornado.Todo):
    def todo_loop(self, data):
        for k in range(2):
            data['nb'] += 1
            yield 'ABCDE'[data['nb'] % 5], {}

class Do(factornado.Do):
    def do_something(self, task_key, task_data):
        return 'something'

app = factornado.Application(
    config,
    [
        ("/todo", ToDo),
        ("/do", Do),
        ])

#app.start_server()

def test_true():
    assert True


