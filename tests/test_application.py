import factornado
from tornado import httpserver


def test_request():
    class Handler(factornado.RequestHandler):
        def get(self):
            self.write('This is GET')

        def post(self):
            self.write('This is POST')

        def put(self):
            self.write('This is PUT')

    app = factornado.Application(
        {'name': 'test', 'threads_nb': 1, 'log': {'stdout': False}},
        [('/', Handler)],
        )

    assert app.request(method='GET', uri='/') == b'This is GET'
    assert app.get('/') == b'This is GET'
    assert app.post('/') == b'This is POST'
    assert app.put('/') == b'This is PUT'
