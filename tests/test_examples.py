import requests
import time
import multiprocessing

from examples import minimal


class TestExamples(object):
    servers = {}

    @classmethod
    def setup_class(self):
        """ setup any state specific to the execution of the given module."""
        self.servers['minimal'] = {'port': minimal.app.get_port()}
        self.servers['minimal']['process'] = multiprocessing.Process(
            target=minimal.app.start_server)
        self.servers['minimal']['process'].start()
        time.sleep(3)

    @classmethod
    def teardown_class(self):
        """ teardown any state that was previously setup with a setup_module
        method.
        """
        self.servers['minimal']['process'].terminate()

    def test_minimal(self):
        r = requests.get('http://127.0.0.1:{port}/hello'.format(
                port=self.servers['minimal']['port']))
        r.raise_for_status()
        assert r.text == 'Hello world\n'
