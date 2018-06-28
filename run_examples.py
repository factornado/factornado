import requests
import time
from collections import OrderedDict

from factornado import get_logger
from examples import minimal, registry, tasks, periodic_task
import os
import signal
from tornado import ioloop


class ServerList(object):
    def __init__(self):
        self.name = 'serverList'
        self.logger = get_logger(
            'test_factornado',
            file='/tmp/test_examples.log',
            level=10,
            levels={'requests': 30, 'tornado': 30, 'urllib3': 30},
            purge_handlers=True,
            stdout=False,
            )
        self.servers = OrderedDict([
            ('registry', registry),
            ('tasks', tasks),
            ('minimal', minimal),
            ('periodic_task', periodic_task),
            ])
        self.child_processes = []

    def start(self):
        self.process_nb = 0
        for key, val in self.servers.items():
            self.logger.debug('Will run {} on port {}'.format(key, val.app.get_port()))
        for key, val in self.servers.items():
            child_process = os.fork()
            if child_process:
                self.child_processes.append(child_process)
                self.process_nb += 1
            else:
                self.logger.debug(
                    'Start {} : process {}, pid {}'.format(key, self.process_nb, os.getpid()))
                signal.signal(signal.SIGINT, self.stop_instance)
                signal.signal(signal.SIGTERM, self.stop_instance)
                try:
                    val.app.start_server()
                except Exception:
                    self.logger.debug(
                        'Error on {} : process {}, pid {}'.format(key,
                                                                  self.process_nb,
                                                                  os.getpid()))
                    self.logger.warning('An error occurred in a callback loop.')
                    self.stop_server(15, None)
                    raise
                return
        for i in range(30):
            try:
                time.sleep(2)
                for key, val in self.servers.items():
                    self.logger.debug('Try HEARTBEAT on {} (try {})'.format(key, 1+i))
                    url = 'http://127.0.0.1:{}'.format(val.app.get_port())
                    r = requests.post(url + '/heartbeat')
                    r.raise_for_status()
                    assert r.text == 'ok'
                    self.logger.debug('Success HEARTBEAT on {} (try {})'.format(key, 1+i))
            except Exception:
                continue
            break

        signal.signal(signal.SIGINT, self.stop_server)
        signal.signal(signal.SIGTERM, self.stop_server)
        try:
            ioloop.IOLoop.current().start()
        except Exception as e:
            self.logger.warning('An error occurred in the main loop.')
            self.stop_server(15, None)
        return

    def stop_instance(self, sig, frame):
        self.logger.info(
            'stopping instance {} due to signal {} ({})'.format(self.process_nb, sig, os.getpid()))
        ioloop.IOLoop.instance().stop()

    def stop_server(self, sig, frame):
        self.logger.info('STOPPING SERVER {} DUE TO SIGNAL {}'.format(self.name, sig))
        for child_process in self.child_processes:
            try:
                os.kill(child_process, sig)
            except ProcessLookupError:
                pass
        ioloop.IOLoop.current().stop()


if __name__ == "__main__":
    ServerList().start()
