# -*- coding: utf-8 -*-

import os
import time
import logging
import socket
import yaml
import re
import signal
import asyncio

import pymongo
import requests
import pandas as pd
from tornado import ioloop, web, httpserver, iostream, http1connection, concurrent

from factornado.logger import get_logger

factornado_logger = logging.getLogger('factornado')


async def _execute(self):
    """Util function that helps builing Application.request method."""
    # If template cache is disabled (usually in the debug mode),
    # re-compile templates and reload static files on every
    # request so you don't need to restart to see changes
    if not self.application.settings.get("compiled_template_cache", True):
        with web.RequestHandler._template_loader_lock:
            for loader in web.RequestHandler._template_loaders.values():
                loader.reset()
    if not self.application.settings.get('static_hash_cache', True):
        web.StaticFileHandler.reset()

    self.handler = self.handler_class(self.application, self.request,
                                      **self.handler_kwargs)
    self.handler._auto_finish = False
    transforms = [t(self.request) for t in self.application.transforms]

    if self.stream_request_body:
        self.handler._prepared_future = concurrent.Future()
    # Note that if an exception escapes handler._execute it will be
    # trapped in the Future it returns (which we are ignoring here,
    # leaving it to be logged when the Future is GC'd).
    # However, that shouldn't happen because _execute has a blanket
    # except handler, and we cannot easily access the IOLoop here to
    # call add_future (because of the requirement to remain compatible
    # with WSGI)
    await self.handler._execute(transforms, *self.path_args,
                                **self.path_kwargs)
    # If we are streaming the request body, then execute() is finished
    # when the handler has prepared to receive the body.  If not,
    # it doesn't matter when execute() finishes (so we return None)
    return b''.join(self.handler._write_buffer)


class Kwargs(object):
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            self.__setattr__(key, val)


class WebMethod(object):
    def __init__(self, method, url, logger=None):
        self.logger = logger if logger is not None else logging.root
        self.method = method
        self.url = url
        self.params = re.findall("{(.*?)}", self.url)
        self.__doc__ = (
            '\nParameters\n----------\n' +
            '\n'.join(["{} : str".format(x) for x in self.params]))

    def __call__(self, data='', headers=None, **kwargs):
        url = self.url.format(**kwargs)
        response = requests.request(
            method=self.method,
            url=url,
            data=data if isinstance(data, (str, bytes, type(None))) else pd.io.json.dumps(data),
            headers=headers if headers is not None else {},
            )
        try:
            response.raise_for_status()
        except Exception:
            reason = '{} {} > {}'.format(self.method, url, response.reason)
            raise web.HTTPError(response.status_code, reason, reason=reason)
        return response


class Callback(object):
    def __init__(self, application, uri, sleep_duration=0, method='post'):
        self.application = application
        self.uri = uri
        self.sleep_duration = sleep_duration
        self.method = method

    def __call__(self):
        factornado_logger.debug('{} callback started'.format(self.uri))
        url = 'http://localhost:{}/{}'.format(self.application.get_port(), self.uri.lstrip('/'))
        response = requests.request(self.method, url)
        try:
            response.raise_for_status()
        except Exception:
            reason = '{} {} > {}'.format(
                self.method, url, response.reason)
            raise web.HTTPError(response.status_code, reason, reason=reason)
        if response.status_code != 200:
            factornado_logger.debug('{} callback returned {}. Sleep for a while.'.format(
                self.uri, response.status_code))
            time.sleep(self.sleep_duration)
        factornado_logger.debug('{} callback finished : {}'.format(self.uri,
                                                                   response.text))


class Application(web.Application):
    def __init__(self, config, handlers, swagger_components=None, logger=None, **kwargs):
        self.config = config if isinstance(config, dict) else yaml.load(open(config))
        self.child_processes = []
        self.handler_list = handlers
        # Swagger components are usefull share data model between handlers
        self.swagger_components = swagger_components
        super(Application, self).__init__(self.handler_list, **kwargs)

        # Set logging config
        if logger is None:
            self.logger = get_logger(**self.config['log'])
        else:
            self.logger = logger

        # Create mongo attribute
        self.mongo = Kwargs()
        _mongo = self.config.get('db', {}).get('mongo', {})
        self.mongo = Kwargs(**{
            collname: pymongo.MongoClient(host['address'],
                                          connect=False)[db['name']][coll['name']]
            for hostname, host in _mongo.get('host', {}).items()
            for dbname, db in _mongo.get('database', {}).items() if db['host'] == hostname
            for collname, coll in _mongo.get('collection', {}).items() if coll['database'] == dbname
            })

        # Create service attribute
        self.services = Kwargs(**{
            key: Kwargs(**{
                subkey: Kwargs(**{
                    subsubkey: WebMethod(
                        subsubkey,
                        (self.config.get('services_prefix', '').rstrip('/') + subsubval
                         if subsubval.lower().startswith('/')
                         else subsubval),
                        logger=self.logger,
                        )
                    for subsubkey, subsubval in subval.items()})
                for subkey, subval in val.items()
                })
            for key, val in self.config.get('services', {}).items()})

    def request(self, **kwargs):
        """Performs a request in the application without going through the network.

        Parameters
        ----------
        **kwargs : see `httpserver.HTTPRequest` for details.
        """
        # Create a HTTPRequest corresponding to the request.
        kwargs['connection'] = (
            kwargs.get("connection")
            or http1connection.HTTP1Connection(iostream.BaseIOStream(), True))
        kwargs['server_connection'] = (
            kwargs.get("server_connection")
            or http1connection.HTTP1Connection(iostream.BaseIOStream(), False))
        http_request = httpserver.HTTPRequest(**kwargs)

        # Build the corresponding _HandlerDelegate.
        handler = self.find_handler(http_request)

        loop = asyncio.get_event_loop()
        out = loop.run_until_complete(_execute(handler))
        return out

    def get(self, uri, **kwargs):
        """Performs a GET request over the application, without going through the network.

        Parameters
        ----------
        uri: str
            The uri you want to query. For example '/toto?q=foo&p=bar'
        **kwargs: see `httpserver.HTTPRequest` for details.
        """
        return self.request(method='GET', uri=uri, **kwargs)

    def post(self, uri, **kwargs):
        """Performs a POST request over the application, without going through the network.

        Parameters
        ----------
        uri: str
            The uri you want to query. For example '/toto?q=foo&p=bar'
        **kwargs: see `httpserver.HTTPRequest` for details.
        """
        return self.request(method='POST', uri=uri, **kwargs)

    def put(self, uri, **kwargs):
        """Performs a PUT request over the application, without going through the network.

        Parameters
        ----------
        uri: str
            The uri you want to query. For example '/toto?q=foo&p=bar'
        **kwargs: see `httpserver.HTTPRequest` for details.
        """
        return self.request(method='PUT', uri=uri, **kwargs)

    def get_port(self):
        if 'port' not in self.config:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("", 0))
            self.config['port'] = s.getsockname()[1]
            s.close()
        return self.config['port']

    def get_host(self):
        if 'host' not in self.config:
            self.config['host'] = socket.gethostname()
        return self.config['host']

    def run_callback(self, name, uri, period, sleep=0, method='post'):
        self.logger.debug('Callback {}, pid: {}'.format(name, os.getpid()))
        self.process_nb += 1
        time.sleep(2)  # We sleep for a few seconds to let the registry start.
        ioloop.PeriodicCallback(
            Callback(self, uri, sleep_duration=sleep, method=method),
            period * 1000,
            ).start()
        signal.signal(signal.SIGINT, self.stop_instance)
        signal.signal(signal.SIGTERM, self.stop_instance)
        try:
            ioloop.IOLoop.instance().start()
        except Exception:
            self.logger.warning('An error occurred in a callback loop.')
            self.stop_server(15, None)
        return

    def start_server(self):
        factornado_logger.info('='*80)

        port = self.get_port()  # We need to have a fixed port in both forks.
        factornado_logger.info('Listening on port {}'.format(port))
        self.process_nb = 0

        child_process = os.fork()
        if child_process:
            self.child_processes.append(child_process)
        else:
            self.logger.debug('First heartbeat, pid: {}'.format(os.getpid()))
            self.process_nb += 1
            time.sleep(2)  # We sleep for a few seconds to let the registry start.
            # Send a heartbeat callback
            cb = Callback(self, '/heartbeat', sleep_duration=0, method='post')
            cb()
            return

        if self.config.get('callbacks', None) is not None:
            for key, val in self.config['callbacks'].items():
                if val['threads']:
                    for i in range(val['threads']):
                        child_process = os.fork()
                        if child_process:
                            self.child_processes.append(child_process)
                        else:
                            self.run_callback(
                                key,
                                val['uri'],
                                val['period'],
                                sleep=val.get('sleep', 0),
                                method=val.get('method', 'post'),
                                )
                            return

        self.server = httpserver.HTTPServer(self)
        self.server.bind(self.get_port(), address=self.config.get('ip', '0.0.0.0'))
        self.logger.debug('Server, pid: {}'.format(os.getpid()))
        self.logger.debug('Child processes: {}'.format(self.child_processes))
        self.server.start(self.config['threads_nb'])
        signal.signal(signal.SIGINT, self.stop_server)
        signal.signal(signal.SIGTERM, self.stop_server)
        try:
            ioloop.IOLoop.current().start()
        except Exception:
            self.logger.warning('An error occurred in the main loop.')
            self.stop_server(15, None)
        return

    def stop_instance(self, sig, frame):
        self.logger.info(
            'stopping instance {} due to signal {} ({})'.format(self.process_nb, sig, os.getpid()))
        ioloop.IOLoop.instance().stop()

    def stop_server(self, sig, frame):
        self.logger.info('STOPPING SERVER {} DUE TO SIGNAL {}'.format(self.config['name'], sig))
        for child_process in self.child_processes:
            try:
                os.kill(child_process, sig)
            except ProcessLookupError:
                pass
        self.server.stop()
        ioloop.IOLoop.current().stop()
