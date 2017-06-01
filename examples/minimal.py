# -*- coding: utf-8 -*-
"""
Factornado minimal example
--------------------------

You can run this example in typing:

>>> python minimal.py &
[1] 15539

Then you can test it with:

>>> curl http://localhost:44044/hello
Hello world

To end up the process, you can use:

>>> kill -SIGTERM 15539
where "15539" is to be replaced by the pid you got when launching the server.
"""

import factornado

class HelloHandler(factornado.handlers.web.RequestHandler):
    def get(self):
        self.write('Hello world\n')

app = factornado.Application('minimal.yml',
                             [("/hello", HelloHandler)])

if __name__ == "__main__":
    app.start_server()
