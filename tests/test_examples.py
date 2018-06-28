import requests
import time
import multiprocessing
import pandas as pd
from collections import OrderedDict

from factornado import get_logger
from examples import minimal, registry, tasks, periodic_task
import uuid
import pytest

multiprocessing.set_start_method('fork')

open('/tmp/test_examples.log', 'w').write('')
logger = get_logger(
    file='/tmp/test_examples.log',
    level=10,
    levels={'requests': 30, 'tornado': 30, 'urllib3': 30}
    )


@pytest.fixture(scope="session")
def server():
    servers = OrderedDict([
            ('registry', registry),
            ('tasks', tasks),
            ('minimal', minimal),
            ('periodic_task', periodic_task),
            ])

    tasks.app.mongo.tasks.delete_many({})
    periodic_task.app.mongo.periodic.delete_many({})
    periodic_task.app.mongo.periodic.insert_one({'dt': pd.Timestamp.utcnow(), 'nb': 0})
    for i in range(30):
        try:
            time.sleep(2)
            for key, val in servers.items():
                logger.debug('Try HEARTBEAT on {} (try {})'.format(key, 1+i))
                url = 'http://127.0.0.1:{}'.format(registry.app.get_port())
                if val.app.config['name'] != 'registry':
                    url += '/{}'.format(val.app.config['name'])
                r = requests.post(url + '/heartbeat')
                r.raise_for_status()
                assert r.text == 'ok'
                logger.debug('Success HEARTBEAT on {} (try {})'.format(key, 1+i))
        except Exception:
            raise
            continue
        break

    class s(object):
        url = 'http://127.0.0.1:{port}'.format(port=registry.app.get_port())

    yield s


class TestMinimal(object):
    def test_minimal(self, server):
        r = requests.get(server.url + '/minimal/hello')
        r.raise_for_status()
        assert r.text == 'Hello world\n'

    def test_minimal_logs(self, server):
        r = requests.get(server.url + '/minimal/log', params=dict(n=10000))
        r.raise_for_status()
        assert b"================" in r.content


class TestRegistry(object):
    def test_registry_hello(self, server):
        r = requests.get(server.url)
        r.raise_for_status()
        assert r.text == 'This is registry\n'

        r = requests.get(server.url + '/')
        r.raise_for_status()
        assert r.text == 'This is registry\n'

    def test_registry_register(self, server):
        # Register a service named 'foo' at url 'foo_url'
        r = requests.post(server.url + '/register/foo', data='{"url": "foo_url"}')
        r.raise_for_status()

        # Register a service named 'foo' at url 'foo_url_2'
        r = requests.post(server.url + '/register/foo', data='{"url": "foo_url_2"}')
        r.raise_for_status()

        # Get urls for service 'foo'
        r = requests.get(server.url + '/register/foo')
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
        r = requests.get(server.url + '/register/all')
        r.raise_for_status()
        doc = r.json()
        assert "foo" in doc
        assert len(doc["foo"]) > 1

    def test_registry_heartbeat(self, server):
        # This query shall be proxied to 'minimal' through 'registry'
        r = requests.post(server.url + '/heartbeat')
        r.raise_for_status()
        assert r.text == 'ok'

    def test_registry_minimal(self, server):
        # This query shall be proxied to 'minimal' through 'registry'
        r = requests.get(server.url + '/minimal/hello')
        r.raise_for_status()
        assert r.text == 'Hello world\n'


class TestTasks(object):
    def test_tasks_hello(self, server):
        r = requests.get(server.url + '/tasks')
        r.raise_for_status()
        assert r.text == 'This is tasks\n'

        r = requests.get(server.url + '/tasks/')
        r.raise_for_status()
        assert r.text == 'This is tasks\n'

    def test_tasks_action_simple(self, server):
        r = requests.put(server.url + '/tasks/action/task01/key01/stack', data={})
        r.raise_for_status()
        doc = r.json()

        assert 'after' in doc
        assert 'before' in doc
        assert doc['after']['key'] == 'key01'
        assert doc['after']['task'] == 'task01'
        assert doc['after']['_id'] == 'task01/key01'
        assert doc['after']['status'] in ['todo', 'toredo']

    def test_tasks_action_priority(self, server):
        r = requests.put(
            server.url + '/tasks/action/task01/key01/stack',
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

    def test_tasks_force_simple(self, server):
        r = requests.put(server.url + '/tasks/force/task01/key01/fail', data={})
        r.raise_for_status()
        doc = r.json()

        assert 'after' in doc
        assert 'before' in doc
        assert doc['after']['key'] == 'key01'
        assert doc['after']['task'] == 'task01'
        assert doc['after']['_id'] == 'task01/key01'
        assert doc['after']['status'] == 'fail'

    def test_tasks_force_priority(self, server):
        r = requests.put(
            server.url + '/tasks/force/task01/key01/toredo',
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

    def test_tasks_assignOne_simple(self, server):
        while True:
            r = requests.put(server.url + '/tasks/assignOne/task01', data={})
            r.raise_for_status()
            if r.status_code != 200:
                assert r.status_code == 204
                break

        r = requests.put(server.url + '/tasks/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.put(server.url + '/tasks/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 200
        doc = r.json()
        assert doc['key'] == 'key01'
        assert doc['task'] == 'task01'
        assert doc['status'] == 'todo'

    def test_tasks_assignOne_double(self, server):
        while True:
            r = requests.put(server.url + '/tasks/assignOne/task01', data={})
            r.raise_for_status()
            if r.status_code != 200:
                assert r.status_code == 204
                break

        r = requests.put(server.url + '/tasks/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.put(server.url + '/tasks/force/task01/key02/todo', data={})
        r.raise_for_status()

        r = requests.put(server.url + '/tasks/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 200
        doc = r.json()
        assert doc['key'] == 'key01'
        assert doc['task'] == 'task01'
        assert doc['status'] == 'todo'

        r = requests.put(server.url + '/tasks/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 200
        doc = r.json()
        assert doc['key'] == 'key02'
        assert doc['task'] == 'task01'
        assert doc['status'] == 'todo'

        r = requests.put(server.url + '/tasks/assignOne/task01', data={})
        r.raise_for_status()
        assert r.status_code == 204

    def test_get_by_key(self, server):
        r = requests.put(server.url + '/tasks/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.get(server.url + '/tasks/getByKey/task01/key01')
        r.raise_for_status()
        doc = r.json()

        assert doc['task'] == 'task01'
        assert doc['key'] == 'key01'
        assert doc['status'] == 'todo'

    def test_get_by_status(self, server):
        r = requests.put(server.url + '/tasks/force/task01/key01/todo', data={})
        r.raise_for_status()

        r = requests.put(server.url + '/tasks/force/task01/key02/done', data={})
        r.raise_for_status()

        r = requests.put(server.url + '/tasks/force/task01/key03/fail', data={})
        r.raise_for_status()

        r = requests.get(server.url + '/tasks/getByStatus/task01/todo%2Cdone%2Cfail', data={})
        r.raise_for_status()
        doc = r.json()

        assert 'done' in doc
        assert 'fail' in doc
        assert 'todo' in doc
        assert 'task01/key01' in [x['_id'] for x in doc['todo']]
        assert 'task01/key02' in [x['_id'] for x in doc['done']]
        assert 'task01/key03' in [x['_id'] for x in doc['fail']]

    def test_tasks_multithreading(self, server):
        def log_function(thread_id, r, operation):
            r.raise_for_status()
            open('mylog.log', 'a').write(' '.join([
                pd.Timestamp.utcnow().isoformat(),
                thread_id,
                operation,
                str(r.status_code),
                str(pd.Timedelta(r.elapsed))[-15:],
                ]) + '\n')

        def process_test(server, n=50):
            thread_id = uuid.uuid4().hex[:8]
            for i in range(n):
                r = requests.put(server + '/action/someTask/someKey/stack')
                log_function(thread_id, r, 'stack')

                r = requests.put(server + '/assignOne/someTask')
                log_function(thread_id, r, 'assignOne')

                if r.status_code == 200:
                    r = requests.put(server + '/action/someTask/someKey/success')
                    log_function(thread_id, r, 'success')

        # We launch 10 clients that will ask for tasks in the same time.
        open('mylog.log', 'w').write('')
        for i in range(10):
            multiprocessing.Process(target=process_test,
                                    args=(server.url + '/tasks',),
                                    ).start()

        # We wait for the clients to finish their job.
        for i in range(60):
            data = list(map(lambda x: x.strip().split(), open('mylog.log').readlines()))
            data = pd.DataFrame(
                data,
                columns=['dt', 'thread', 'action', 'code', 'duration'])
            data['dt'] = data['dt'].apply(pd.Timestamp)
            summary = data.groupby([
                    'thread', 'action', 'code']).apply(len).unstack(0).T.fillna(0).astype(int)
            time.sleep(1)
            if 'stack' in summary and summary['stack', '200'].max() == 50:
                break
        # Up to there, the task mechanism has run without failures.
        assert ('stack' in summary and
                summary['stack', '200'].max() == 50), 'No thread ended his job'

        # Let's test if no task has been assigned twice in the same time.
        z = data[data.action.isin(['assignOne', 'success']) & (data.code == '200')].set_index('dt')
        z.sort_index(inplace=True)
        z['nbDoing'] = (z.action == 'assignOne').cumsum() - (z.action == 'success').cumsum()
        z['dt'] = (pd.np.diff(z.index.values).astype(int)*1e-9).tolist() + [None]
        # We check that no task was assigned twice for more than 0.1 sec.
        assert (z[z.nbDoing > 1]['dt'] < 0.1).all()


class TestPeriodicTask(object):
    def test_periodic_task(self, server):
        # We call '/latest' in a loop till at least 3 documents have been created.
        timeout = pd.Timestamp.utcnow() + pd.Timedelta(60, 's')
        while True:
            r = requests.get(server.url + '/periodictask/latest')
            r.raise_for_status()
            if r.text != 'null':
                doc = r.json()
                if doc['nb'] > 3:
                    break
                elif pd.Timestamp.utcnow() > timeout:
                    raise TimeoutError('Timout reached with {} docs only'.format(doc['nb']))
                print(r.text)
            time.sleep(1)
