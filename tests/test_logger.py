import factornado
import io


def test_stream_logger():
    stream = io.StringIO()
    logger = factornado.get_logger(
        format='%(levelname)s - %(message)s',
        stream=stream,
        level=20,
        )
    logger.info('info')
    logger.debug('debug')
    assert stream.getvalue() == ('INFO - info\n')
