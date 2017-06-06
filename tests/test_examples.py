import requests
import time
import multiprocessing
import pandas as pd

from examples import minimal, registry, tasks, periodic_task


class TestExamples(object):
    servers = {}

    @classmethod
    def setup_class(self):
        """ setup any state specific to the execution of the given module."""

        self.servers = {
            'registry': {
                'port': registry.app.get_port(),
                'process': multiprocessing.Process(target=registry.app.start_server)
                },
            'minimal': {
                'port': minimal.app.get_port(),
                'process': multiprocessing.Process(target=minimal.app.start_server)
                },
            'tasks': {
                'port': tasks.app.get_port(),
                'process': multiprocessing.Process(target=tasks.app.start_server)
                },
            'periodic_task': {
                'port': periodic_task.app.get_port(),
                'process': multiprocessing.Process(target=periodic_task.app.start_server)
                },
            }

        for server in self.servers:
            self.servers[server]['process'].start()

        # Reset the database for periodic_task.
        tasks.app.mongo.tasks.delete_many({})

        periodic_task.app.mongo.periodic.delete_many({})
        periodic_task.app.mongo.periodic.insert_one(
                {'dt': pd.Timestamp.utcnow(),
                 'nb': 0})

        for i in range(30):
            try:
                time.sleep(2)
                for server in self.servers:
                    url = 'http://127.0.0.1:{port}'.format(
                        port=self.servers[server]['port'])
                    r = requests.post(url + '/heartbeat')
                    r.raise_for_status()
                    assert r.text == 'ok'
            except:
                continue
            break

    @classmethod
    def teardown_class(self):
        """ teardown any state that was previously setup with a setup_module
        method.
        """
        for server in self.servers:
            self.servers[server]['process'].terminate()

    def test_minimal(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['minimal']['port'])
        r = requests.get(url + '/hello')
        r.raise_for_status()
        assert r.text == 'Hello world\n'

    def test_registry_hello(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['registry']['port'])
        r = requests.get(url)
        r.raise_for_status()
        assert r.text == 'This is registry\n'

        r = requests.get(url + '/')
        r.raise_for_status()
        assert r.text == 'This is registry\n'

    def test_registry_register(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['registry']['port'])

        # Register a service named 'foo' at url 'foo_url'
        r = requests.post(url + '/register/foo', data='{"url": "foo_url"}')
        r.raise_for_status()

        # Register a service named 'foo' at url 'foo_url_2'
        r = requests.post(url + '/register/foo', data='{"url": "foo_url_2"}')
        r.raise_for_status()

        # Get urls for service 'foo'
        r = requests.get(url + '/register/foo')
        r.raise_for_status()
        doc = r.json()
        assert "foo" in doc
        assert len(doc["foo"]) > 1
        assert doc["foo"][0]["name"] == "foo"
        assert doc["foo"][0]["info"] == {}
        assert doc["foo"][0]["_id"] == "foo_url_2"
        assert doc["foo"][1]["name"] == "foo"
        assert doc["foo"][1]["info"] == {}
        assert doc["foo"][1]["_id"] == "foo_url"
        former_id = doc["foo"][0]['id']
        for x in doc["foo"]:
            assert x['id'] <= former_id
            former_id = x['id']

        # Get urls for all services
        r = requests.get(url + '/register/all')
        r.raise_for_status()
        doc = r.json()
        assert "foo" in doc
        assert len(doc["foo"]) > 1

    def test_registry_heartbeat(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['registry']['port'])

        # This query shall be proxied to 'minimal' through 'registry'
        r = requests.post(url + '/heartbeat')
        r.raise_for_status()
        assert r.text == 'ok'

    def test_registry_minimal(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['registry']['port'])

        # This query shall be proxied to 'minimal' through 'registry'
        r = requests.get(url + '/minimal/hello')
        r.raise_for_status()
        assert r.text == 'Hello world\n'

    def test_tasks_hello(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        r = requests.get(url)
        r.raise_for_status()
        assert r.text == 'This is tasks\n'

        r = requests.get(url + '/')
        r.raise_for_status()
        assert r.text == 'This is tasks\n'

    def test_tasks_action_simple(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        r = requests.put(url + '/action/task01/key01/stack', data={})
        r.raise_for_status()
        doc = r.json()

        assert 'after' in doc
        assert 'before' in doc
        assert doc['after']['key'] == 'key01'
        assert doc['after']['task'] == 'task01'
        assert doc['after']['_id'] == 'task01/key01'
        assert doc['after']['status'] in ['todo', 'toredo']

    def test_tasks_action_priority(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        r = requests.put(url + '/action/task01/key01/stack',
                         data={},
                         params={'priority': 1})
        r.raise_for_status()
        doc = r.json()

        assert 'after' in doc
        assert 'before' in doc
        assert doc['after']['key'] == 'key01'
        assert doc['after']['task'] == 'task01'
        assert doc['after']['_id'] == 'task01/key01'
        assert doc['after']['status'] in ['todo', 'toredo']
        assert doc['after']['priority'] == 1

    def test_tasks_force_simple(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        r = requests.put(url + '/force/task01/key01/fail', data={})
        r.raise_for_status()
        doc = r.json()

        assert 'after' in doc
        assert 'before' in doc
        assert doc['after']['key'] == 'key01'
        assert doc['after']['task'] == 'task01'
        assert doc['after']['_id'] == 'task01/key01'
        assert doc['after']['status'] == 'fail'

    def test_tasks_force_priority(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        r = requests.put(url + '/force/task01/key01/toredo',
                         data={},
                         params={'priority': 1})
        r.raise_for_status()
        doc = r.json()

        assert 'after' in doc
        assert 'before' in doc
        assert doc['after']['key'] == 'key01'
        assert doc['after']['task'] == 'task01'
        assert doc['after']['_id'] == 'task01/key01'
        assert doc['after']['status'] == 'toredo'
        assert doc['after']['priority'] == 1

    def test_tasks_assignOne_simple(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        while True:
            r = requests.put(url + '/assignOne/task01', data={})
            r.raise_for_status()
            if r.status_code != 200:
                assert r.status_code == 204
                break

        r = requests.put(url + '/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.put(url + '/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 200
        doc = r.json()
        assert doc['key'] == 'key01'
        assert doc['task'] == 'task01'
        assert doc['status'] == 'todo'

    def test_tasks_assignOne_double(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        while True:
            r = requests.put(url + '/assignOne/task01', data={})
            r.raise_for_status()
            if r.status_code != 200:
                assert r.status_code == 204
                break

        r = requests.put(url + '/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.put(url + '/force/task01/key02/todo', data={})
        r.raise_for_status()

        r = requests.put(url + '/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 200
        doc = r.json()
        assert doc['key'] == 'key01'
        assert doc['task'] == 'task01'
        assert doc['status'] == 'todo'

        r = requests.put(url + '/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 200
        doc = r.json()
        assert doc['key'] == 'key02'
        assert doc['task'] == 'task01'
        assert doc['status'] == 'todo'

        r = requests.put(url + '/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 204

    def test_get_by_key(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])
        r = requests.put(url + '/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.get(url + '/getByKey/task01/key01')
        r.raise_for_status()
        doc = r.json()

        assert doc['task'] == 'task01'
        assert doc['key'] == 'key01'
        assert doc['status'] == 'todo'

    def test_get_by_status(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['tasks']['port'])

        r = requests.put(url + '/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.put(url + '/force/task01/key02/done', data={})
        r.raise_for_status()

        r = requests.put(url + '/force/task01/key03/fail', data={})
        r.raise_for_status()

        r = requests.get(url + '/getByStatus/task01/todo%2Cdone%2Cfail', data={})
        r.raise_for_status()
        doc = r.json()

        assert 'done' in doc
        assert 'fail' in doc
        assert 'todo' in doc
        assert 'task01/key01' in [x['_id'] for x in doc['todo']]
        assert 'task01/key02' in [x['_id'] for x in doc['done']]
        assert 'task01/key03' in [x['_id'] for x in doc['fail']]

    def test_periodic_task(self):
        url = 'http://127.0.0.1:{port}'.format(port=self.servers['periodic_task']['port'])

        # We call '/latest' in a loop till at least 3 documents have been created.
        timeout = pd.Timestamp.utcnow() + pd.Timedelta(60, 's')
        while True:
            r = requests.get(url + '/latest')
            r.raise_for_status()
            if r.text != 'null':
                doc = r.json()
                if doc['nb'] > 3:
                    break
                elif pd.Timestamp.utcnow() > timeout:
                    raise TimeoutError('Timout reached with {} docs only'.format(doc['nb']))
                print(r.text)
            time.sleep(1)
