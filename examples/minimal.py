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


class HelloHandler(factornado.handlers.web.RequestHandler):
    def get(self):
        self.write('Hello world\n')


config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'minimal.yml')

app = factornado.Application(config,
                             [("/hello", HelloHandler)])

if __name__ == "__main__":
    app.start_server()
