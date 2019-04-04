import os
import factornado
import factornado.tasks

from factornado.handlers import Swagger, Log, Heartbeat
from tornado import web


class HelloHandler(web.RequestHandler):
    swagger = {
        "/{name}/{uri}" : {
            "get": {
                "description" : "Says hello.",
                "parameters": [],
                "responses": {
                    200 : {"description" : "OK"},
                    401 : {"description" : "Unauthorized"},
                    403 : {"description" : "Forbidden"},
                    404 : {"description" : "Not Found"},
                }
            }
        }
    }

    def get(self):
        self.write('This is tasks\n')


config = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'tasks.yml')

app = factornado.Application(
    config,
    [
        ("/", HelloHandler),
        ("/swagger.json", Swagger),
        ("/swagger", web.RedirectHandler, {'url': '/swagger.json'}),
        ("/heartbeat", Heartbeat),
        ("/log", Log),
        ("/action/([^/]*?)/([^/]*?)/([^/]*?)", factornado.tasks.Action),
        ("/force/([^/]*?)/([^/]*?)/([^/]*?)", factornado.tasks.Force),
        ("/assignOne/([^/]*?)", factornado.tasks.AssignOne),
        ("/getByKey/([^/]*?)/([^/]*?)", factornado.tasks.GetByKey),
        ("/getByStatus/([^/]*?)/([^/]*?)", factornado.tasks.GetByStatus),
    ])


if __name__ == "__main__":
    app.start_server()
