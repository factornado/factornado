# -*- coding: utf-8 -*-
"""
Factornado minimal example
--------------------------

You can run this example in typing:

>>> python minimal.py &
[1] 15539

Then you can test it with:

>>> curl http://localhost:3742/hello
Hello world

If 3742 is the port it's running on.

To end up the process, you can use:

>>> kill -SIGTERM -$(ps aux | grep 'python minimal.py' | awk '{print $2}')
"""

import factornado
import os

from factornado.handlers import Swagger, Log, Heartbeat
from tornado import web


class HelloHandler(factornado.handlers.web.RequestHandler):
    swagger = {
        "/{name}/{uri}": {
            "get": {
                "description": "Says hello.",
                "parameters": [],
                "responses": {
                    200: {"description": "OK"},
                    401: {"description": "Unauthorized"},
                    403: {"description": "Forbidden"},
                    404: {"description": "Not Found"},
                }
            }
        }
    }

    def get(self):
        self.write('Hello world\n')


config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'minimal.yml')

app = factornado.Application(config, [
    ("/hello", HelloHandler),
    ("/swagger.json", Swagger),
    ("/swagger", web.RedirectHandler, {'url': '/swagger.json'}),
    ("/heartbeat", Heartbeat),
    ("/log", Log)
])

if __name__ == "__main__":
    app.start_server()
