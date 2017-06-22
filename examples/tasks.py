import os
from tornado import web
import factornado
import factornado.tasks


class HelloHandler(web.RequestHandler):
    def get(self):
        self.write('This is tasks\n')


config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'tasks.yml')

app = factornado.Application(
    config,
    [
        ("/", HelloHandler),
        ("/action/([^/]*?)/([^/]*?)/([^/]*?)", factornado.tasks.Action),
        ("/force/([^/]*?)/([^/]*?)/([^/]*?)", factornado.tasks.Force),
        ("/assignOne/([^/]*?)", factornado.tasks.AssignOne),
        ("/getByKey/([^/]*?)/([^/]*?)", factornado.tasks.GetByKey),
        ("/getByStatus/([^/]*?)/([^/]*?)", factornado.tasks.GetByStatus),
        ])


if __name__ == "__main__":
    app.start_server()
