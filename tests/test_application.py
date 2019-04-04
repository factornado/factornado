import factornado


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


def test_request():
    assert app.request(method='GET', uri='/') == b'This is GET'


def test_get():
    assert app.get('/') == b'This is GET'


def test_post():
    assert app.post('/') == b'This is POST'


def test_put():
    assert app.put('/') == b'This is PUT'
