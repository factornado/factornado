import requests
import time
import multiprocessing

from examples import minimal


def test_minimal():
    port = minimal.app.get_port()
    p = multiprocessing.Process(
        target=minimal.app.start_server)
    p.start()
    try:
        time.sleep(3)
        r = requests.get('http://127.0.0.1:{port}/hello'.format(port=port))
        r.raise_for_status()
        assert r.text == 'Hello world\n'
    except:
        p.terminate()
        raise
    p.terminate()
