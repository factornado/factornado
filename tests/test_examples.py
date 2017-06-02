import requests
import time
import multiprocessing

from examples import minimal, registry


class TestExamples(object):
    servers = {}

    @classmethod
    def setup_class(self):
        """ setup any state specific to the execution of the given module."""

        self.servers['registry'] = {
            'port': registry.app.get_port(),
            'process': multiprocessing.Process(target=registry.app.start_server)
            }

        self.servers['minimal'] = {
            'port': minimal.app.get_port(),
            'process': multiprocessing.Process(target=minimal.app.start_server)
            }

        self.servers['registry']['process'].start()
        self.servers['minimal']['process'].start()

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
