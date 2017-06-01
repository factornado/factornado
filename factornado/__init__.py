from factornado.handlers import Todo, Do, RequestHandler
from factornado.application import Application


__version__ = 'master'


if __name__ == '__main__':

    class MyToDo(Todo):
        def todo_loop(self, data):
            for k in range(2):
                data['nb'] += 1
                yield 'ABCDE'[data['nb'] % 5], {}

    class MyDo(Do):
        def do_something(self, task_key, task_data):
            return 'something'

    app = Application('config.yml', [
        ("/todo", MyToDo),
        ("/do", MyDo),
        ], )

    app.start_server()
